from __future__ import annotations

import json
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from contextlib import closing
from pathlib import Path
from typing import Any

from .csv_sync import CsvSync
from .db import CHANGE_LOG_TABLE, COLUMN_ORDER_TABLE, DatabaseController, ROW_ORDER_TABLE
from .validation import quote_identifier, validate_identifier


@dataclass(frozen=True)
class TableTrashSnapshot:
    table_name: str
    snapshot_dir: Path
    sqlite_path: Path
    csv_path: Path
    manifest_path: Path
    columns: list[str]
    row_count: int
    column_count: int


def create_table_trash_snapshot(
    db: DatabaseController,
    csv_sync: CsvSync,
    table_name: str,
    trash_root: str | Path | None = None,
) -> TableTrashSnapshot:
    name = validate_identifier(table_name, "table name")
    columns, rows = db.fetch_table_data(name)
    active_csv_path = csv_sync.export_table(db, name)

    deleted_at = datetime.now().isoformat(timespec="seconds")
    snapshot_dir = _unique_snapshot_dir(_trash_tables_dir(db, trash_root), name, deleted_at)
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    sqlite_path = snapshot_dir / "table.sqlite"
    csv_path = snapshot_dir / "table.csv"
    manifest_path = snapshot_dir / "manifest.json"

    _write_table_sqlite_snapshot(db, name, columns, rows, sqlite_path)
    if active_csv_path.exists():
        shutil.copy2(active_csv_path, csv_path)
    else:
        csv_path.write_text("", encoding="utf-8")

    manifest = {
        "type": "table",
        "table_name": name,
        "deleted_at": deleted_at,
        "source_database": str(db.db_path),
        "source_csv": str(active_csv_path),
        "row_count": len(rows),
        "column_count": len(columns),
        "columns": columns,
        "sqlite_snapshot": str(sqlite_path),
        "csv_snapshot": str(csv_path),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    return TableTrashSnapshot(
        name,
        snapshot_dir,
        sqlite_path,
        csv_path,
        manifest_path,
        columns,
        len(rows),
        len(columns),
    )


def _trash_tables_dir(db: DatabaseController, trash_root: str | Path | None) -> Path:
    if trash_root is not None:
        root = Path(trash_root)
    elif db.db_path.parent.name == "data":
        root = db.db_path.parent.parent / "trash"
    else:
        root = db.db_path.parent / "trash"
    return root / "tables"


def _unique_snapshot_dir(base_dir: Path, table_name: str, deleted_at: str) -> Path:
    safe_time = deleted_at.replace(":", "-")
    base_name = f"{table_name}_{safe_time}"
    candidate = base_dir / base_name
    index = 2
    while candidate.exists():
        candidate = base_dir / f"{base_name}_{index}"
        index += 1
    return candidate


def _write_table_sqlite_snapshot(
    db: DatabaseController,
    table_name: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    sqlite_path: Path,
) -> None:
    with closing(sqlite3.connect(sqlite_path)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = TRUNCATE")
        _create_metadata_tables(connection)
        _create_user_table(connection, table_name, columns)
        _insert_user_rows(connection, table_name, columns, rows)
        _insert_order_metadata(connection, table_name, columns, rows)
        _copy_change_log_rows(db, connection, table_name)
        connection.commit()


def _create_metadata_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        f"""
        CREATE TABLE {ROW_ORDER_TABLE} (
            table_name TEXT NOT NULL,
            row_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            PRIMARY KEY (table_name, row_id)
        )
        """
    )
    connection.execute(
        f"""
        CREATE TABLE {COLUMN_ORDER_TABLE} (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            position INTEGER NOT NULL,
            PRIMARY KEY (table_name, column_name)
        )
        """
    )
    connection.execute(
        f"""
        CREATE TABLE {CHANGE_LOG_TABLE} (
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


def _create_user_table(connection: sqlite3.Connection, table_name: str, columns: list[str]) -> None:
    column_definitions = ",\n            ".join(_column_definition(column) for column in columns)
    connection.execute(
        f"""
        CREATE TABLE {quote_identifier(table_name)} (
            {column_definitions}
        )
        """
    )


def _insert_user_rows(
    connection: sqlite3.Connection,
    table_name: str,
    columns: list[str],
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    column_sql = ", ".join(quote_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    connection.executemany(
        f"INSERT INTO {quote_identifier(table_name)} ({column_sql}) VALUES ({placeholders})",
        [tuple(row.get(column) for column in columns) for row in rows],
    )


def _insert_order_metadata(
    connection: sqlite3.Connection,
    table_name: str,
    columns: list[str],
    rows: list[dict[str, Any]],
) -> None:
    connection.executemany(
        f"INSERT INTO {COLUMN_ORDER_TABLE} (table_name, column_name, position) VALUES (?, ?, ?)",
        [(table_name, column, index) for index, column in enumerate(columns)],
    )
    connection.executemany(
        f"INSERT INTO {ROW_ORDER_TABLE} (table_name, row_id, position) VALUES (?, ?, ?)",
        [(table_name, int(row["id"]), index) for index, row in enumerate(rows)],
    )


def _copy_change_log_rows(
    source_db: DatabaseController,
    target_connection: sqlite3.Connection,
    table_name: str,
) -> None:
    rows = source_db.connection.execute(
        f"""
        SELECT id, timestamp, action, table_name, target, before_json, after_json, status, undoable, error_text
        FROM {CHANGE_LOG_TABLE}
        WHERE table_name = ?
        ORDER BY id
        """,
        (table_name,),
    ).fetchall()
    if not rows:
        return
    target_connection.executemany(
        f"""
        INSERT INTO {CHANGE_LOG_TABLE}
        (id, timestamp, action, table_name, target, before_json, after_json, status, undoable, error_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [tuple(row) for row in rows],
    )


def _column_definition(column_name: str) -> str:
    if column_name == "id":
        return f"{quote_identifier(column_name)} INTEGER PRIMARY KEY AUTOINCREMENT"
    if column_name == "_row_name":
        return f"{quote_identifier(column_name)} TEXT UNIQUE"
    return f"{quote_identifier(column_name)} TEXT"
