from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

from app.change_log import ChangeLogService
from app.csv_sync import CsvSync
from app.db import DatabaseController


class ChangeLogTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        self.temp_dir = temp_root / uuid.uuid4().hex
        self.temp_dir.mkdir()
        self.db = DatabaseController(self.temp_dir / "database.sqlite")
        self.csv_sync = CsvSync(self.temp_dir / "csv")
        self.log = ChangeLogService(self.db, self.csv_sync)

    def tearDown(self) -> None:
        self.db.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_log_entries_are_written(self) -> None:
        self.log.log("open_database", target="database.sqlite", undoable=False)

        entries = self.log.entries()
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].action, "open_database")
        self.assertFalse(entries[0].undoable)

    def test_undo_and_redo_cell_edit(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        row_id = self.db.add_row("customers", "first")
        self.db.update_cell("customers", row_id, "name", "Ali")
        self.log.log(
            "update_cell",
            "customers",
            f"{row_id}.name",
            before={"row_id": row_id, "column": "name", "value": None},
            after={"row_id": row_id, "column": "name", "value": "Ali"},
        )

        ok, _, _ = self.log.undo_last()
        self.assertTrue(ok)
        self.assertIsNone(self.db.get_row("customers", row_id)["name"])

        ok, _, _ = self.log.redo_last()
        self.assertTrue(ok)
        self.assertEqual(self.db.get_row("customers", row_id)["name"], "Ali")

    def test_redo_replays_multiple_undone_changes_in_original_order(self) -> None:
        self.db.create_table("customers")
        self.log.log("create_table", "customers", "customers", after={"table": "customers"})
        row_id = self.db.add_row("customers", "first")
        self.log.log(
            "add_row",
            "customers",
            str(row_id),
            after={"row": self.db.get_row("customers", row_id), "position": 0},
        )

        ok, _, _ = self.log.undo_last()
        self.assertTrue(ok)
        ok, _, _ = self.log.undo_last()
        self.assertTrue(ok)
        self.assertEqual(self.db.list_tables(), [])

        ok, _, table = self.log.redo_last()
        self.assertTrue(ok)
        self.assertEqual(table, "customers")
        self.assertEqual(self.db.list_tables(), ["customers"])
        ok, _, table = self.log.redo_last()
        self.assertTrue(ok)
        self.assertEqual(table, "customers")
        self.assertEqual(self.db.fetch_rows("customers")[0]["_row_name"], "first")

    def test_table_delete_is_logged_but_not_undoable(self) -> None:
        self.log.log("delete_table", "customers", "customers", undoable=False)

        ok, message, _ = self.log.undo_last()

        self.assertFalse(ok)
        self.assertIn("blocked", message)
        entry = self.log.entries()[0]
        self.assertEqual(entry.status, "applied")
        self.assertIsNone(entry.error_text)

    def test_redo_is_blocked_after_table_delete_barrier(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        row_id = self.db.add_row("customers", "first")
        self.db.update_cell("customers", row_id, "name", "Ali")
        self.log.log(
            "update_cell",
            "customers",
            f"{row_id}.name",
            before={"row_id": row_id, "column": "name", "value": None},
            after={"row_id": row_id, "column": "name", "value": "Ali"},
        )
        ok, _, _ = self.log.undo_last()
        self.assertTrue(ok)

        self.db.delete_table("customers")
        self.csv_sync.delete_table_csv("customers")
        self.log.log("delete_table", "customers", "customers", undoable=False)

        ok, message, table = self.log.redo_last()

        self.assertFalse(ok)
        self.assertEqual(table, "customers")
        self.assertIn("blocked", message)

    def test_redo_is_blocked_after_database_session_barrier(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        row_id = self.db.add_row("customers", "first")
        self.db.update_cell("customers", row_id, "name", "Ali")
        self.log.log(
            "update_cell",
            "customers",
            f"{row_id}.name",
            before={"row_id": row_id, "column": "name", "value": None},
            after={"row_id": row_id, "column": "name", "value": "Ali"},
        )
        ok, _, _ = self.log.undo_last()
        self.assertTrue(ok)
        self.log.log("open_database", target="other.sqlite", undoable=False)

        ok, message, _ = self.log.redo_last()

        self.assertFalse(ok)
        self.assertIn("database session changed", message)

    def test_undo_and_redo_table_rename(self) -> None:
        self.db.create_table("customers")
        self.db.rename_table("customers", "clients")
        self.log.log(
            "rename_table",
            "clients",
            "customers->clients",
            before={"table": "customers"},
            after={"table": "clients"},
        )

        ok, _, table = self.log.undo_last()
        self.assertTrue(ok)
        self.assertEqual(table, "customers")
        self.assertEqual(self.db.list_tables(), ["customers"])

        ok, _, table = self.log.redo_last()
        self.assertTrue(ok)
        self.assertEqual(table, "clients")
        self.assertEqual(self.db.list_tables(), ["clients"])


if __name__ == "__main__":
    unittest.main()
