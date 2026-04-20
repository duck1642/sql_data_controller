from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .validation import (
    PROTECTED_COLUMNS,
    ValidationError,
    quote_identifier,
    validate_identifier,
    validate_row_name,
    validate_user_column_name,
)


ROW_ORDER_TABLE = "_sdc_row_order"
COLUMN_ORDER_TABLE = "_sdc_column_order"
CHANGE_LOG_TABLE = "_sdc_change_log"
RESERVED_TABLE_PREFIX = "_sdc_"


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    type: str
    not_null: bool
    default: Any
    primary_key: bool


class DatabaseController:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.db_path)
        self._closed = False
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode = TRUNCATE")
        self.connection.execute("PRAGMA foreign_keys = ON")
        self._ensure_metadata_tables()

    def close(self) -> None:
        if not self._closed:
            self.connection.close()
            self._closed = True

    def list_tables(self) -> list[str]:
        rows = self.connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
              AND name NOT GLOB '_sdc_*'
            ORDER BY name
            """
        ).fetchall()
        return [row["name"] for row in rows]

    def table_exists(self, table_name: str) -> bool:
        name = validate_identifier(table_name, "table name")
        row = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND lower(name) = lower(?)",
            (name,),
        ).fetchone()
        return row is not None

    def create_table(self, table_name: str) -> None:
        name = self._validate_new_table_name(table_name)
        if self.table_exists(name):
            raise ValidationError(f"Table '{name}' already exists.")

        table_sql = quote_identifier(name)
        self.connection.execute(
            f"""
            CREATE TABLE {table_sql} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                _row_name TEXT UNIQUE
            )
            """
        )
        self._sync_column_order(name)
        self.connection.commit()

    def create_ordered_copy(self, source_table: str, new_table: str) -> None:
        self._require_table(source_table)
        source_name = validate_identifier(source_table, "source table name")
        new_name = self._validate_new_table_name(new_table)
        if self.table_exists(new_name):
            raise ValidationError(f"Table '{new_name}' already exists.")

        columns, rows = self.fetch_table_data(source_name)
        new_table_sql = quote_identifier(new_name)
        column_definitions = ",\n                ".join(self._column_definition(column) for column in columns)
        insert_columns_sql = ", ".join(quote_identifier(column) for column in columns)
        placeholders = ", ".join("?" for _ in columns)

        try:
            self.connection.execute("BEGIN")
            self.connection.execute(
                f"""
                CREATE TABLE {new_table_sql} (
                    {column_definitions}
                )
                """
            )

            if rows:
                self.connection.executemany(
                    f"INSERT INTO {new_table_sql} ({insert_columns_sql}) VALUES ({placeholders})",
                    [tuple(row[column] for column in columns) for row in rows],
                )

            self.connection.executemany(
                f"""
                INSERT INTO {COLUMN_ORDER_TABLE} (table_name, column_name, position)
                VALUES (?, ?, ?)
                """,
                [(new_name, column, index) for index, column in enumerate(columns)],
            )
            self.connection.executemany(
                f"""
                INSERT INTO {ROW_ORDER_TABLE} (table_name, row_id, position)
                VALUES (?, ?, ?)
                """,
                [(new_name, int(row["id"]), index) for index, row in enumerate(rows)],
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def rename_table(self, old_name: str, new_name: str) -> None:
        self._require_table(old_name)
        old_table = validate_identifier(old_name, "table name")
        new_table = self._validate_new_table_name(new_name)
        if self.table_exists(new_table):
            raise ValidationError(f"Table '{new_table}' already exists.")

        old_sql = quote_identifier(old_table)
        new_sql = quote_identifier(new_table)
        self.connection.execute(f"ALTER TABLE {old_sql} RENAME TO {new_sql}")
        self.connection.execute(
            f"UPDATE {ROW_ORDER_TABLE} SET table_name = ? WHERE table_name = ?",
            (new_table, old_table),
        )
        self.connection.execute(
            f"UPDATE {COLUMN_ORDER_TABLE} SET table_name = ? WHERE table_name = ?",
            (new_table, old_table),
        )
        self.connection.execute(
            f"UPDATE {CHANGE_LOG_TABLE} SET table_name = ? WHERE table_name = ?",
            (new_table, old_table),
        )
        self.connection.commit()

    def delete_table(self, table_name: str) -> None:
        self._require_table(table_name)
        name = validate_identifier(table_name, "table name")
        if name.casefold().startswith(RESERVED_TABLE_PREFIX):
            raise ValidationError(f"Table names starting with {RESERVED_TABLE_PREFIX} are protected.")
        table_sql = quote_identifier(name)

        self.connection.execute(f"DROP TABLE {table_sql}")
        self.connection.execute(f"DELETE FROM {ROW_ORDER_TABLE} WHERE table_name = ?", (name,))
        self.connection.execute(f"DELETE FROM {COLUMN_ORDER_TABLE} WHERE table_name = ?", (name,))
        self.connection.commit()

    def get_columns(self, table_name: str) -> list[ColumnInfo]:
        self._require_table(table_name)
        table_sql = quote_identifier(table_name)
        rows = self.connection.execute(f"PRAGMA table_info({table_sql})").fetchall()
        return [
            ColumnInfo(
                name=row["name"],
                type=row["type"],
                not_null=bool(row["notnull"]),
                default=row["dflt_value"],
                primary_key=bool(row["pk"]),
            )
            for row in rows
        ]

    def get_column_names(self, table_name: str) -> list[str]:
        columns = [column.name for column in self.get_columns(table_name)]
        self._sync_column_order(table_name, columns)
        rows = self.connection.execute(
            f"""
            SELECT column_name
            FROM {COLUMN_ORDER_TABLE}
            WHERE table_name = ?
            ORDER BY position, column_name
            """,
            (table_name,),
        ).fetchall()

        column_set = set(columns)
        ordered = [row["column_name"] for row in rows if row["column_name"] in column_set]
        missing = [column for column in columns if column not in ordered]
        return ordered + missing

    def fetch_rows(self, table_name: str) -> list[dict[str, Any]]:
        self._require_table(table_name)
        self._sync_row_order(table_name)
        table_sql = quote_identifier(table_name)
        rows = self.connection.execute(
            f"""
            SELECT data.*
            FROM {table_sql} AS data
            LEFT JOIN {ROW_ORDER_TABLE} AS row_order
              ON row_order.table_name = ?
             AND row_order.row_id = data.id
            ORDER BY COALESCE(row_order.position, data.id), data.id
            """,
            (table_name,),
        ).fetchall()
        return [dict(row) for row in rows]

    def fetch_table_data(self, table_name: str) -> tuple[list[str], list[dict[str, Any]]]:
        return self.get_column_names(table_name), self.fetch_rows(table_name)

    def get_row(self, table_name: str, row_id: int) -> dict[str, Any]:
        self._require_table(table_name)
        return self._require_row(table_name, row_id)

    def add_row(self, table_name: str, row_name: str | None = None) -> int:
        self._require_table(table_name)
        name = validate_row_name(row_name) if row_name is not None else self._next_row_name(table_name)
        if self.row_name_exists(table_name, name):
            raise ValidationError(f"Row name '{name}' already exists.")

        table_sql = quote_identifier(table_name)
        cursor = self.connection.execute(
            f"INSERT INTO {table_sql} (_row_name) VALUES (?)",
            (name,),
        )
        self.connection.execute(
            f"""
            INSERT INTO {ROW_ORDER_TABLE} (table_name, row_id, position)
            VALUES (?, ?, ?)
            """,
            (table_name, int(cursor.lastrowid), self._next_row_position(table_name)),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def insert_row_snapshot(self, table_name: str, row: dict[str, Any], position: int | None = None) -> int:
        self._require_table(table_name)
        columns = self.get_column_names(table_name)
        row_id = int(row["id"])
        if self.connection.execute(
            f"SELECT 1 FROM {quote_identifier(table_name)} WHERE id = ?",
            (row_id,),
        ).fetchone():
            raise ValidationError(f"Row id '{row_id}' already exists.")

        insert_columns = [column for column in columns if column in row]
        if "id" not in insert_columns:
            insert_columns.insert(0, "id")
        column_sql = ", ".join(quote_identifier(column) for column in insert_columns)
        placeholders = ", ".join("?" for _ in insert_columns)
        values = [row.get(column) for column in insert_columns]

        table_sql = quote_identifier(table_name)
        self.connection.execute(
            f"INSERT INTO {table_sql} ({column_sql}) VALUES ({placeholders})",
            values,
        )
        self.connection.execute(
            f"""
            INSERT OR REPLACE INTO {ROW_ORDER_TABLE} (table_name, row_id, position)
            VALUES (?, ?, ?)
            """,
            (table_name, row_id, position if position is not None else self._next_row_position(table_name)),
        )
        self.connection.commit()
        return row_id

    def rename_row(self, table_name: str, row_id: int, new_row_name: str) -> None:
        self._require_table(table_name)
        name = validate_row_name(new_row_name)
        current = self._require_row(table_name, row_id)
        if self.row_name_exists(table_name, name, exclude_row_id=row_id):
            raise ValidationError(f"Row name '{name}' already exists.")
        if current.get("_row_name") == name:
            return

        table_sql = quote_identifier(table_name)
        self.connection.execute(
            f"UPDATE {table_sql} SET _row_name = ? WHERE id = ?",
            (name, row_id),
        )
        self.connection.commit()

    def delete_row(self, table_name: str, row_id: int) -> None:
        self.delete_rows(table_name, [row_id])

    def delete_rows(self, table_name: str, row_ids: list[int]) -> None:
        self._require_table(table_name)
        unique_row_ids = sorted({int(row_id) for row_id in row_ids})
        if not unique_row_ids:
            raise ValidationError("Select at least one row to delete.")
        for row_id in unique_row_ids:
            self._require_row(table_name, row_id)

        table_sql = quote_identifier(table_name)
        try:
            self.connection.execute("BEGIN")
            self.connection.executemany(
                f"DELETE FROM {table_sql} WHERE id = ?",
                [(row_id,) for row_id in unique_row_ids],
            )
            self.connection.executemany(
                f"DELETE FROM {ROW_ORDER_TABLE} WHERE table_name = ? AND row_id = ?",
                [(table_name, row_id) for row_id in unique_row_ids],
            )
            self._compact_row_order(table_name)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def get_column_values(self, table_name: str, column_name: str) -> list[dict[str, Any]]:
        self._require_table(table_name)
        column = validate_identifier(column_name, "column name")
        if column not in self.get_column_names(table_name):
            raise ValidationError(f"Column '{column}' does not exist.")
        table_sql = quote_identifier(table_name)
        column_sql = quote_identifier(column)
        rows = self.connection.execute(
            f"""
            SELECT id, {column_sql} AS value
            FROM {table_sql}
            ORDER BY id
            """
        ).fetchall()
        return [{"row_id": int(row["id"]), "value": row["value"]} for row in rows]

    def set_column_values(self, table_name: str, column_name: str, values: list[dict[str, Any]]) -> None:
        self._require_table(table_name)
        column = validate_identifier(column_name, "column name")
        if column not in self.get_column_names(table_name):
            raise ValidationError(f"Column '{column}' does not exist.")
        table_sql = quote_identifier(table_name)
        column_sql = quote_identifier(column)
        self.connection.executemany(
            f"UPDATE {table_sql} SET {column_sql} = ? WHERE id = ?",
            [(item.get("value"), int(item["row_id"])) for item in values],
        )
        self.connection.commit()

    def add_column(self, table_name: str, column_name: str) -> None:
        self._require_table(table_name)
        name = validate_user_column_name(column_name)
        if self._name_exists_case_insensitive(self.get_column_names(table_name), name):
            raise ValidationError(f"Column '{name}' already exists.")

        table_sql = quote_identifier(table_name)
        column_sql = quote_identifier(name)
        self.connection.execute(f"ALTER TABLE {table_sql} ADD COLUMN {column_sql} TEXT")
        self.connection.execute(
            f"""
            INSERT OR REPLACE INTO {COLUMN_ORDER_TABLE} (table_name, column_name, position)
            VALUES (?, ?, ?)
            """,
            (table_name, name, self._next_column_position(table_name)),
        )
        self.connection.commit()

    def rename_column(self, table_name: str, old_name: str, new_name: str) -> None:
        self._require_table(table_name)
        old_column = validate_identifier(old_name, "column name")
        new_column = validate_user_column_name(new_name)
        columns = self.get_column_names(table_name)

        if old_column in PROTECTED_COLUMNS:
            raise ValidationError(f"Column '{old_column}' is protected.")
        if old_column not in columns:
            raise ValidationError(f"Column '{old_column}' does not exist.")
        if self._name_exists_case_insensitive(columns, new_column):
            raise ValidationError(f"Column '{new_column}' already exists.")

        table_sql = quote_identifier(table_name)
        old_sql = quote_identifier(old_column)
        new_sql = quote_identifier(new_column)
        self.connection.execute(f"ALTER TABLE {table_sql} RENAME COLUMN {old_sql} TO {new_sql}")
        self.connection.execute(
            f"""
            UPDATE {COLUMN_ORDER_TABLE}
            SET column_name = ?
            WHERE table_name = ?
              AND column_name = ?
            """,
            (new_column, table_name, old_column),
        )
        self.connection.commit()

    def delete_column(self, table_name: str, column_name: str) -> None:
        self.delete_columns(table_name, [column_name])

    def delete_columns(self, table_name: str, column_names: list[str]) -> None:
        self._require_table(table_name)
        unique_columns = list(dict.fromkeys(validate_identifier(column, "column name") for column in column_names))
        if not unique_columns:
            raise ValidationError("Select at least one column to delete.")
        for column in unique_columns:
            if column in PROTECTED_COLUMNS:
                raise ValidationError(f"Column '{column}' is protected.")
            if column not in self.get_column_names(table_name):
                raise ValidationError(f"Column '{column}' does not exist.")

        try:
            self.connection.execute("BEGIN")
            table_sql = quote_identifier(table_name)
            for column in unique_columns:
                column_sql = quote_identifier(column)
                self.connection.execute(f"ALTER TABLE {table_sql} DROP COLUMN {column_sql}")
            self.connection.executemany(
                f"DELETE FROM {COLUMN_ORDER_TABLE} WHERE table_name = ? AND column_name = ?",
                [(table_name, column) for column in unique_columns],
            )
            self._compact_column_order(table_name)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def update_cell(self, table_name: str, row_id: int, column_name: str, value: Any) -> None:
        self._require_table(table_name)
        column = validate_identifier(column_name, "column name")
        if column == "id":
            raise ValidationError("The id column is read-only.")
        if column not in self.get_column_names(table_name):
            raise ValidationError(f"Column '{column}' does not exist.")
        self._require_row(table_name, row_id)

        stored_value = self._normalize_cell_value(column, value)
        if column == "_row_name" and self.row_name_exists(table_name, stored_value, exclude_row_id=row_id):
            raise ValidationError(f"Row name '{stored_value}' already exists.")

        table_sql = quote_identifier(table_name)
        column_sql = quote_identifier(column)
        self.connection.execute(
            f"UPDATE {table_sql} SET {column_sql} = ? WHERE id = ?",
            (stored_value, row_id),
        )
        self.connection.commit()

    def update_cells_raw(self, table_name: str, cells: list[dict[str, Any]]) -> None:
        self._require_table(table_name)
        if not cells:
            return
        table_sql = quote_identifier(table_name)
        try:
            self.connection.execute("BEGIN")
            for cell in cells:
                column = validate_identifier(cell["column"], "column name")
                if column == "id":
                    raise ValidationError("The id column is read-only.")
                if column not in self.get_column_names(table_name):
                    raise ValidationError(f"Column '{column}' does not exist.")
                self._require_row(table_name, int(cell["row_id"]))
                value = self._normalize_cell_value(column, cell.get("value"))
                column_sql = quote_identifier(column)
                self.connection.execute(
                    f"UPDATE {table_sql} SET {column_sql} = ? WHERE id = ?",
                    (value, int(cell["row_id"])),
                )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def clear_cells(self, table_name: str, cells: list[tuple[int, str]]) -> int:
        self._require_table(table_name)
        columns = set(self.get_column_names(table_name))
        unique_cells = list(dict.fromkeys((int(row_id), validate_identifier(column, "column name")) for row_id, column in cells))
        if not unique_cells:
            raise ValidationError("Select at least one editable cell to empty.")

        for row_id, column in unique_cells:
            self._require_row(table_name, row_id)
            if column in PROTECTED_COLUMNS:
                raise ValidationError(f"Column '{column}' cannot be emptied.")
            if column not in columns:
                raise ValidationError(f"Column '{column}' does not exist.")

        table_sql = quote_identifier(table_name)
        try:
            self.connection.execute("BEGIN")
            for row_id, column in unique_cells:
                column_sql = quote_identifier(column)
                self.connection.execute(
                    f"UPDATE {table_sql} SET {column_sql} = NULL WHERE id = ?",
                    (row_id,),
                )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

        return len(unique_cells)

    def row_name_exists(self, table_name: str, row_name: str, exclude_row_id: int | None = None) -> bool:
        self._require_table(table_name)
        table_sql = quote_identifier(table_name)
        if exclude_row_id is None:
            row = self.connection.execute(
                f"SELECT 1 FROM {table_sql} WHERE _row_name = ?",
                (row_name,),
            ).fetchone()
        else:
            row = self.connection.execute(
                f"SELECT 1 FROM {table_sql} WHERE _row_name = ? AND id <> ?",
                (row_name, exclude_row_id),
            ).fetchone()
        return row is not None

    def reorder_rows(self, table_name: str, row_ids: list[int]) -> None:
        self._require_table(table_name)
        current_ids = [int(row["id"]) for row in self.fetch_rows(table_name)]
        proposed_ids = [int(row_id) for row_id in row_ids]
        if sorted(current_ids) != sorted(proposed_ids):
            raise ValidationError("Row order must include every current row exactly once.")

        self.connection.executemany(
            f"""
            INSERT INTO {ROW_ORDER_TABLE} (table_name, row_id, position)
            VALUES (?, ?, ?)
            ON CONFLICT(table_name, row_id) DO UPDATE SET position = excluded.position
            """,
            [(table_name, row_id, index) for index, row_id in enumerate(proposed_ids)],
        )
        self.connection.commit()

    def reorder_columns(self, table_name: str, column_names: list[str]) -> None:
        self._require_table(table_name)
        current_columns = self.get_column_names(table_name)
        proposed_columns = [validate_identifier(column, "column name") for column in column_names]
        if sorted(current_columns) != sorted(proposed_columns):
            raise ValidationError("Column order must include every current column exactly once.")

        self.connection.executemany(
            f"""
            INSERT INTO {COLUMN_ORDER_TABLE} (table_name, column_name, position)
            VALUES (?, ?, ?)
            ON CONFLICT(table_name, column_name) DO UPDATE SET position = excluded.position
            """,
            [(table_name, column, index) for index, column in enumerate(proposed_columns)],
        )
        self.connection.commit()

    def _require_table(self, table_name: str) -> None:
        name = validate_identifier(table_name, "table name")
        if not self.table_exists(name):
            raise ValidationError(f"Table '{name}' does not exist.")

    def _validate_new_table_name(self, table_name: str) -> str:
        name = validate_identifier(table_name, "table name")
        if name.casefold().startswith(RESERVED_TABLE_PREFIX):
            raise ValidationError(f"Table names cannot start with {RESERVED_TABLE_PREFIX}.")
        return name

    def _name_exists_case_insensitive(self, existing_names: list[str], candidate: str) -> bool:
        candidate_key = candidate.casefold()
        return any(existing.casefold() == candidate_key for existing in existing_names)

    def _column_definition(self, column_name: str) -> str:
        column_sql = quote_identifier(column_name)
        if column_name == "id":
            return f"{column_sql} INTEGER PRIMARY KEY AUTOINCREMENT"
        if column_name == "_row_name":
            return f"{column_sql} TEXT UNIQUE"
        validate_user_column_name(column_name)
        return f"{column_sql} TEXT"

    def _require_row(self, table_name: str, row_id: int) -> dict[str, Any]:
        if int(row_id) <= 0:
            raise ValidationError("Row id must be positive.")

        table_sql = quote_identifier(table_name)
        row = self.connection.execute(
            f"SELECT * FROM {table_sql} WHERE id = ?",
            (int(row_id),),
        ).fetchone()
        if row is None:
            raise ValidationError(f"Row id '{row_id}' does not exist.")
        return dict(row)

    def _next_row_name(self, table_name: str) -> str:
        existing = {row["_row_name"] for row in self.fetch_rows(table_name)}
        index = 1
        while True:
            candidate = f"row_{index}"
            if candidate not in existing:
                return candidate
            index += 1

    def _normalize_cell_value(self, column_name: str, value: Any) -> str | None:
        if column_name == "_row_name":
            return validate_row_name(value)

        if value is None:
            return None
        text = str(value)
        if text == "":
            return None
        return text

    def _ensure_metadata_tables(self) -> None:
        self.connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ROW_ORDER_TABLE} (
                table_name TEXT NOT NULL,
                row_id INTEGER NOT NULL,
                position INTEGER NOT NULL,
                PRIMARY KEY (table_name, row_id)
            )
            """
        )
        self.connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {COLUMN_ORDER_TABLE} (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                position INTEGER NOT NULL,
                PRIMARY KEY (table_name, column_name)
            )
            """
        )
        self.connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {CHANGE_LOG_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                action TEXT NOT NULL,
                table_name TEXT,
                target TEXT,
                before_json TEXT,
                after_json TEXT,
                status TEXT NOT NULL,
                undoable INTEGER NOT NULL DEFAULT 1,
                error_text TEXT
            )
            """
        )
        self.connection.commit()

    def _sync_column_order(self, table_name: str, columns: list[str] | None = None) -> None:
        column_names = columns if columns is not None else [column.name for column in self.get_columns(table_name)]
        column_set = set(column_names)

        existing_rows = self.connection.execute(
            f"""
            SELECT column_name, position
            FROM {COLUMN_ORDER_TABLE}
            WHERE table_name = ?
            ORDER BY position
            """,
            (table_name,),
        ).fetchall()
        existing_positions = {
            row["column_name"]: int(row["position"])
            for row in existing_rows
            if row["column_name"] in column_set
        }

        stale_columns = [row["column_name"] for row in existing_rows if row["column_name"] not in column_set]
        self.connection.executemany(
            f"DELETE FROM {COLUMN_ORDER_TABLE} WHERE table_name = ? AND column_name = ?",
            [(table_name, column) for column in stale_columns],
        )

        next_position = max(existing_positions.values(), default=-1) + 1
        inserts = []
        for column in column_names:
            if column not in existing_positions:
                inserts.append((table_name, column, next_position))
                next_position += 1

        self.connection.executemany(
            f"""
            INSERT OR IGNORE INTO {COLUMN_ORDER_TABLE} (table_name, column_name, position)
            VALUES (?, ?, ?)
            """,
            inserts,
        )
        self.connection.commit()

    def _compact_column_order(self, table_name: str) -> None:
        rows = self.connection.execute(
            f"""
            SELECT column_name
            FROM {COLUMN_ORDER_TABLE}
            WHERE table_name = ?
            ORDER BY position, column_name
            """,
            (table_name,),
        ).fetchall()
        self.connection.executemany(
            f"""
            UPDATE {COLUMN_ORDER_TABLE}
            SET position = ?
            WHERE table_name = ?
              AND column_name = ?
            """,
            [(index, table_name, row["column_name"]) for index, row in enumerate(rows)],
        )

    def _sync_row_order(self, table_name: str) -> None:
        table_sql = quote_identifier(table_name)
        row_ids = [
            int(row["id"])
            for row in self.connection.execute(f"SELECT id FROM {table_sql} ORDER BY id").fetchall()
        ]
        row_id_set = set(row_ids)

        existing_rows = self.connection.execute(
            f"""
            SELECT row_id, position
            FROM {ROW_ORDER_TABLE}
            WHERE table_name = ?
            ORDER BY position
            """,
            (table_name,),
        ).fetchall()
        existing_positions = {
            int(row["row_id"]): int(row["position"])
            for row in existing_rows
            if int(row["row_id"]) in row_id_set
        }

        stale_ids = [int(row["row_id"]) for row in existing_rows if int(row["row_id"]) not in row_id_set]
        self.connection.executemany(
            f"DELETE FROM {ROW_ORDER_TABLE} WHERE table_name = ? AND row_id = ?",
            [(table_name, row_id) for row_id in stale_ids],
        )

        next_position = max(existing_positions.values(), default=-1) + 1
        inserts = []
        for row_id in row_ids:
            if row_id not in existing_positions:
                inserts.append((table_name, row_id, next_position))
                next_position += 1

        self.connection.executemany(
            f"""
            INSERT OR IGNORE INTO {ROW_ORDER_TABLE} (table_name, row_id, position)
            VALUES (?, ?, ?)
            """,
            inserts,
        )
        self.connection.commit()

    def _compact_row_order(self, table_name: str) -> None:
        rows = self.connection.execute(
            f"""
            SELECT row_id
            FROM {ROW_ORDER_TABLE}
            WHERE table_name = ?
            ORDER BY position, row_id
            """,
            (table_name,),
        ).fetchall()
        self.connection.executemany(
            f"""
            UPDATE {ROW_ORDER_TABLE}
            SET position = ?
            WHERE table_name = ?
              AND row_id = ?
            """,
            [(index, table_name, int(row["row_id"])) for index, row in enumerate(rows)],
        )

    def _next_row_position(self, table_name: str) -> int:
        row = self.connection.execute(
            f"SELECT COALESCE(MAX(position), -1) + 1 AS next_position FROM {ROW_ORDER_TABLE} WHERE table_name = ?",
            (table_name,),
        ).fetchone()
        return int(row["next_position"])

    def _next_column_position(self, table_name: str) -> int:
        row = self.connection.execute(
            f"""
            SELECT COALESCE(MAX(position), -1) + 1 AS next_position
            FROM {COLUMN_ORDER_TABLE}
            WHERE table_name = ?
            """,
            (table_name,),
        ).fetchone()
        return int(row["next_position"])
