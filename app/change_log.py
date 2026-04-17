from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .csv_sync import CsvSync
from .db import CHANGE_LOG_TABLE, DatabaseController
from .validation import ValidationError


@dataclass(frozen=True)
class ChangeLogEntry:
    id: int
    timestamp: str
    action: str
    table_name: str | None
    target: str | None
    before: dict[str, Any]
    after: dict[str, Any]
    status: str
    undoable: bool
    error_text: str | None


class ChangeLogService:
    def __init__(self, db: DatabaseController, csv_sync: CsvSync):
        self.db = db
        self.csv_sync = csv_sync

    def bind(self, db: DatabaseController, csv_sync: CsvSync) -> None:
        self.db = db
        self.csv_sync = csv_sync

    def log(
        self,
        action: str,
        table_name: str | None = None,
        target: str | None = None,
        before: dict[str, Any] | None = None,
        after: dict[str, Any] | None = None,
        status: str = "applied",
        undoable: bool = True,
        error_text: str | None = None,
    ) -> int:
        cursor = self.db.connection.execute(
            f"""
            INSERT INTO {CHANGE_LOG_TABLE}
                (timestamp, action, table_name, target, before_json, after_json, status, undoable, error_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now().isoformat(timespec="seconds"),
                action,
                table_name,
                target,
                json.dumps(before or {}, ensure_ascii=False, sort_keys=True),
                json.dumps(after or {}, ensure_ascii=False, sort_keys=True),
                status,
                1 if undoable else 0,
                error_text,
            ),
        )
        self.db.connection.commit()
        return int(cursor.lastrowid)

    def entries(self, limit: int = 300) -> list[ChangeLogEntry]:
        rows = self.db.connection.execute(
            f"""
            SELECT *
            FROM {CHANGE_LOG_TABLE}
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [self._entry_from_row(row) for row in rows]

    def undo_last(self) -> tuple[bool, str, str | None]:
        row = self.db.connection.execute(
            f"""
            SELECT *
            FROM {CHANGE_LOG_TABLE}
            WHERE status = 'applied'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return False, "No applied change to undo.", None

        entry = self._entry_from_row(row)
        if not entry.undoable:
            self._mark(entry.id, "failed", "Undo is not available for this change.")
            return False, f"Undo is not available for {entry.action}.", entry.table_name

        try:
            table_to_sync = self._apply_undo(entry)
            self._mark(entry.id, "undone", None)
            self._sync(table_to_sync)
            return True, f"Undid {entry.action}.", table_to_sync
        except Exception as exc:
            self._mark(entry.id, "failed", str(exc))
            return False, f"Undo failed: {exc}", entry.table_name

    def redo_last(self) -> tuple[bool, str, str | None]:
        row = self.db.connection.execute(
            f"""
            SELECT *
            FROM {CHANGE_LOG_TABLE}
            WHERE status = 'undone'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return False, "No undone change to redo.", None

        entry = self._entry_from_row(row)
        try:
            table_to_sync = self._apply_redo(entry)
            self._mark(entry.id, "applied", None)
            self._sync(table_to_sync)
            return True, f"Redid {entry.action}.", table_to_sync
        except Exception as exc:
            self._mark(entry.id, "failed", str(exc))
            return False, f"Redo failed: {exc}", entry.table_name

    def _apply_undo(self, entry: ChangeLogEntry) -> str | None:
        before = entry.before
        after = entry.after
        table = entry.table_name

        if entry.action == "create_table":
            self.db.delete_table(after["table"])
            self.csv_sync.delete_table_csv(after["table"])
            return None
        if entry.action == "rename_table":
            self.db.rename_table(after["table"], before["table"])
            self._rename_csv(after["table"], before["table"])
            return before["table"]
        if entry.action == "add_row":
            self.db.delete_row(table, int(after["row"]["id"]))
            return table
        if entry.action == "delete_rows":
            for row in before["rows"]:
                self.db.insert_row_snapshot(table, row["data"], row.get("position"))
            if "row_order" in before:
                self.db.reorder_rows(table, before["row_order"])
            return table
        if entry.action == "rename_row":
            self.db.rename_row(table, int(before["row_id"]), before["row_name"])
            return table
        if entry.action == "add_column":
            self.db.delete_column(table, after["column"])
            return table
        if entry.action == "rename_column":
            self.db.rename_column(table, after["column"], before["column"])
            return table
        if entry.action == "delete_columns":
            for column in before["columns"]:
                self.db.add_column(table, column["name"])
                self.db.set_column_values(table, column["name"], column["values"])
            self.db.reorder_columns(table, before["column_order"])
            return table
        if entry.action == "update_cell":
            self.db.update_cell(table, int(before["row_id"]), before["column"], before.get("value"))
            return table
        if entry.action == "clear_cells":
            self.db.update_cells_raw(table, before["cells"])
            return table
        if entry.action == "reorder_rows":
            self.db.reorder_rows(table, before["row_ids"])
            return table
        if entry.action == "reorder_columns":
            self.db.reorder_columns(table, before["columns"])
            return table
        if entry.action == "apply_order_copy":
            self.db.delete_table(after["new_table"])
            self.csv_sync.delete_table_csv(after["new_table"])
            return None

        raise ValidationError(f"Undo is not implemented for {entry.action}.")

    def _apply_redo(self, entry: ChangeLogEntry) -> str | None:
        before = entry.before
        after = entry.after
        table = entry.table_name

        if entry.action == "create_table":
            self.db.create_table(after["table"])
            return after["table"]
        if entry.action == "rename_table":
            self.db.rename_table(before["table"], after["table"])
            self._rename_csv(before["table"], after["table"])
            return after["table"]
        if entry.action == "add_row":
            self.db.insert_row_snapshot(table, after["row"], after.get("position"))
            return table
        if entry.action == "delete_rows":
            self.db.delete_rows(table, [row["data"]["id"] for row in before["rows"]])
            return table
        if entry.action == "rename_row":
            self.db.rename_row(table, int(after["row_id"]), after["row_name"])
            return table
        if entry.action == "add_column":
            self.db.add_column(table, after["column"])
            return table
        if entry.action == "rename_column":
            self.db.rename_column(table, before["column"], after["column"])
            return table
        if entry.action == "delete_columns":
            self.db.delete_columns(table, [column["name"] for column in before["columns"]])
            return table
        if entry.action == "update_cell":
            self.db.update_cell(table, int(after["row_id"]), after["column"], after.get("value"))
            return table
        if entry.action == "clear_cells":
            self.db.clear_cells(table, [(cell["row_id"], cell["column"]) for cell in after["cells"]])
            return table
        if entry.action == "reorder_rows":
            self.db.reorder_rows(table, after["row_ids"])
            return table
        if entry.action == "reorder_columns":
            self.db.reorder_columns(table, after["columns"])
            return table
        if entry.action == "apply_order_copy":
            self.db.create_ordered_copy(before["source_table"], after["new_table"])
            return after["new_table"]

        raise ValidationError(f"Redo is not implemented for {entry.action}.")

    def _sync(self, table_name: str | None) -> None:
        if table_name and self.db.table_exists(table_name):
            self.csv_sync.export_table(self.db, table_name)

    def _rename_csv(self, old_name: str, new_name: str) -> None:
        old_path = self.csv_sync.path_for_table(old_name)
        if old_path.exists():
            try:
                old_path.rename(self.csv_sync.path_for_table(new_name))
            except PermissionError:
                self.csv_sync.delete_table_csv(old_name)

    def _mark(self, entry_id: int, status: str, error_text: str | None) -> None:
        self.db.connection.execute(
            f"UPDATE {CHANGE_LOG_TABLE} SET status = ?, error_text = ? WHERE id = ?",
            (status, error_text, entry_id),
        )
        self.db.connection.commit()

    def _entry_from_row(self, row: Any) -> ChangeLogEntry:
        return ChangeLogEntry(
            id=int(row["id"]),
            timestamp=row["timestamp"],
            action=row["action"],
            table_name=row["table_name"],
            target=row["target"],
            before=json.loads(row["before_json"] or "{}"),
            after=json.loads(row["after_json"] or "{}"),
            status=row["status"],
            undoable=bool(row["undoable"]),
            error_text=row["error_text"],
        )
