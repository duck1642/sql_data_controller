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
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode = TRUNCATE")
        self.connection.execute("PRAGMA foreign_keys = ON")

    def close(self) -> None:
        self.connection.close()

    def list_tables(self) -> list[str]:
        rows = self.connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()
        return [row["name"] for row in rows]

    def table_exists(self, table_name: str) -> bool:
        name = validate_identifier(table_name, "table name")
        row = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        return row is not None

    def create_table(self, table_name: str) -> None:
        name = validate_identifier(table_name, "table name")
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
        return [column.name for column in self.get_columns(table_name)]

    def fetch_rows(self, table_name: str) -> list[dict[str, Any]]:
        self._require_table(table_name)
        table_sql = quote_identifier(table_name)
        rows = self.connection.execute(f"SELECT * FROM {table_sql} ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def fetch_table_data(self, table_name: str) -> tuple[list[str], list[dict[str, Any]]]:
        return self.get_column_names(table_name), self.fetch_rows(table_name)

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
        self.connection.commit()
        return int(cursor.lastrowid)

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

    def add_column(self, table_name: str, column_name: str) -> None:
        self._require_table(table_name)
        name = validate_user_column_name(column_name)
        if name in self.get_column_names(table_name):
            raise ValidationError(f"Column '{name}' already exists.")

        table_sql = quote_identifier(table_name)
        column_sql = quote_identifier(name)
        self.connection.execute(f"ALTER TABLE {table_sql} ADD COLUMN {column_sql} TEXT")
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
        if new_column in columns:
            raise ValidationError(f"Column '{new_column}' already exists.")

        table_sql = quote_identifier(table_name)
        old_sql = quote_identifier(old_column)
        new_sql = quote_identifier(new_column)
        self.connection.execute(f"ALTER TABLE {table_sql} RENAME COLUMN {old_sql} TO {new_sql}")
        self.connection.commit()

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

    def _require_table(self, table_name: str) -> None:
        name = validate_identifier(table_name, "table name")
        if not self.table_exists(name):
            raise ValidationError(f"Table '{name}' does not exist.")

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
