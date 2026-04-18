from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

from PyQt6.QtCore import QModelIndex, Qt

from app.csv_sync import CsvSync
from app.db import DatabaseController
from app.table_model import DatabaseTableModel


class TableModelTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        self.temp_dir = temp_root / uuid.uuid4().hex
        self.temp_dir.mkdir()
        self.db = DatabaseController(self.temp_dir / "database.sqlite")
        self.csv_sync = CsvSync(self.temp_dir / "csv")
        self.model = DatabaseTableModel(self.db, self.csv_sync)

    def tearDown(self) -> None:
        self.db.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_search_filter_matches_rows(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        first_id = self.db.add_row("customers", "first")
        second_id = self.db.add_row("customers", "second")
        self.db.update_cell("customers", first_id, "name", "Ali")
        self.db.update_cell("customers", second_id, "name", "Ayse")
        self.model.set_table("customers")

        self.model.set_search_options("ali", True, True, False)

        self.assertEqual(self.model.rowCount(QModelIndex()), 1)
        self.assertEqual(self.model.rows[0]["name"], "Ali")

    def test_search_highlight_uses_readable_foreground(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        row_id = self.db.add_row("customers", "first")
        self.db.update_cell("customers", row_id, "name", "Ali")
        self.model.set_table("customers")

        self.model.set_search_options("ali", True, False, False)
        index = self.model.index(0, self.model.columns.index("name"))

        self.assertIsNotNone(self.model.data(index, Qt.ItemDataRole.BackgroundRole))
        self.assertIsNotNone(self.model.data(index, Qt.ItemDataRole.ForegroundRole))


if __name__ == "__main__":
    unittest.main()
