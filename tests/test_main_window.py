from __future__ import annotations

import shutil
import unittest
import uuid
import json
from pathlib import Path

from PyQt6.QtWidgets import QApplication

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

    def read_log_records(self) -> list[dict]:
        records: list[dict] = []
        for path in sorted((self.temp_dir / "logs").glob("app_log*.jsonl")):
            records.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines())
        return records


if __name__ == "__main__":
    unittest.main()
