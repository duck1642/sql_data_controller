from __future__ import annotations

import shutil
import unittest
import uuid
import json
from pathlib import Path
from unittest.mock import patch

from PyQt6.QtWidgets import QApplication, QMessageBox

from app.app_logger import AppLogger
from app.csv_sync import CsvSync
from app.db import DatabaseController
from app.main_window import MainWindow


class MainWindowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = QApplication.instance() or QApplication([])

    def setUp(self) -> None:
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        self.temp_dir = temp_root / uuid.uuid4().hex
        self.temp_dir.mkdir()
        self.db = DatabaseController(self.temp_dir / "database.sqlite")
        self.csv_sync = CsvSync(self.temp_dir / "csv")
        self.app_logger = AppLogger(self.temp_dir / "logs")
        self.window = MainWindow(self.db, self.csv_sync, self.app_logger)

    def tearDown(self) -> None:
        self.window.close()
        self.db.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_export_menu_exists_and_current_table_exports_are_disabled_without_table(self) -> None:
        menu_names = [action.text() for action in self.window.menuBar().actions()]

        self.assertIn("Export", menu_names)
        self.assertFalse(self.window.export_current_csv_action.isEnabled())
        self.assertFalse(self.window.export_current_tsv_action.isEnabled())
        self.assertFalse(self.window.export_current_json_action.isEnabled())
        self.assertFalse(self.window.export_current_xlsx_action.isEnabled())
        self.assertFalse(self.window.export_current_sql_action.isEnabled())
        self.assertTrue(self.window.export_all_xlsx_action.isEnabled())
        self.assertTrue(self.window.export_all_sql_action.isEnabled())
        self.assertTrue(self.window.backup_sqlite_action.isEnabled())

    def test_help_diagnostics_menu_exists(self) -> None:
        menu_names = [action.text() for action in self.window.menuBar().actions()]
        help_menu = self.window.menuBar().actions()[menu_names.index("Help")].menu()
        diagnostics_menu = help_menu.actions()[0].menu()
        diagnostic_actions = [action.text() for action in diagnostics_menu.actions()]

        self.assertEqual(help_menu.title(), "Help")
        self.assertEqual(diagnostics_menu.title(), "Diagnostics")
        self.assertEqual(diagnostic_actions, ["Open Log Folder", "Open Current Log", "Copy Last Error"])

    def test_action_wrapper_logs_started_and_success(self) -> None:
        self.window.run_user_action("test_action", lambda: None)

        records = self.read_log_records()
        action_records = [record for record in records if record.get("action") == "test_action"]
        self.assertEqual([record["status"] for record in action_records], ["started", "success"])

    def test_action_wrapper_logs_failed_action(self) -> None:
        self.window.show_error = lambda message: None

        def fail() -> None:
            raise RuntimeError("boom")

        self.window.run_user_action("failing_action", fail)

        records = self.read_log_records()
        failure = [record for record in records if record.get("action") == "failing_action"][-1]
        self.assertEqual(failure["level"], "ERROR")
        self.assertEqual(failure["status"], "failed")
        self.assertEqual(failure["error_type"], "RuntimeError")

    def test_new_database_rejects_existing_file(self) -> None:
        existing_db = self.temp_dir / "existing.sqlite"
        existing_db.write_text("not empty", encoding="utf-8")
        original_db_path = self.window.db.db_path
        messages: list[str] = []
        self.window.show_error = messages.append

        with patch("app.main_window.QFileDialog.getSaveFileName", return_value=(str(existing_db), "")):
            self.window.new_database()

        self.assertEqual(self.window.db.db_path, original_db_path)
        self.assertEqual(messages, ["Database already exists. Use Open DB to open an existing database."])

    def test_delete_table_creates_trash_snapshot_before_real_delete(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        row_id = self.db.add_row("customers", "first")
        self.db.update_cell("customers", row_id, "name", "Ali")
        self.window.refresh_tables(select_name="customers")

        with patch("app.main_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes):
            self.window.delete_table()

        self.assertEqual(self.db.list_tables(), [])
        snapshots = list((self.temp_dir / "trash" / "tables").glob("customers_*"))
        self.assertEqual(len(snapshots), 1)
        self.assertTrue((snapshots[0] / "table.sqlite").exists())
        self.assertTrue((snapshots[0] / "table.csv").exists())
        self.assertTrue((snapshots[0] / "manifest.json").exists())
        delete_entry = self.window.change_log.entries()[0]
        self.assertFalse(delete_entry.undoable)
        self.assertNotIn("rows", delete_entry.before)
        self.assertEqual(delete_entry.before["row_count"], 1)
        self.assertEqual(delete_entry.before["trash_snapshot"], str(snapshots[0]))

    def read_log_records(self) -> list[dict]:
        records: list[dict] = []
        for path in sorted((self.temp_dir / "logs").glob("app_log*.jsonl")):
            records.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines())
        return records


if __name__ == "__main__":
    unittest.main()
