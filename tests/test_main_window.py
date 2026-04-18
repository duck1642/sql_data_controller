from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

from PyQt6.QtWidgets import QApplication

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
        self.window = MainWindow(self.db, self.csv_sync)

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


if __name__ == "__main__":
    unittest.main()
