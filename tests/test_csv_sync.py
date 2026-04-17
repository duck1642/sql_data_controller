from __future__ import annotations

import csv
import shutil
import unittest
import uuid
from pathlib import Path

from app.csv_sync import CsvSync
from app.db import DatabaseController


class CsvSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        self.temp_dir = temp_root / uuid.uuid4().hex
        self.temp_dir.mkdir()
        self.db = DatabaseController(self.temp_dir / "database.sqlite")
        self.csv_sync = CsvSync(self.temp_dir / "csv")

    def tearDown(self) -> None:
        self.db.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_export_writes_headers_rows_and_empty_fields(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        row_id = self.db.add_row("customers", "customer_1")
        self.db.update_cell("customers", row_id, "name", "")

        csv_path = self.csv_sync.export_table(self.db, "customers")
        with csv_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.reader(handle))

        self.assertEqual(rows[0], ["id", "_row_name", "name"])
        self.assertEqual(rows[1], ["1", "customer_1", ""])

    def test_export_uses_renamed_columns(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        self.db.rename_column("customers", "name", "full_name")

        csv_path = self.csv_sync.export_table(self.db, "customers")
        text = csv_path.read_text(encoding="utf-8")

        self.assertIn("full_name", text)
        self.assertNotIn(",name", text)

    def test_export_follows_reordered_rows_and_columns(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        first_id = self.db.add_row("customers", "first")
        second_id = self.db.add_row("customers", "second")
        self.db.update_cell("customers", first_id, "name", "Ali")
        self.db.update_cell("customers", second_id, "name", "Ayse")

        self.db.reorder_rows("customers", [second_id, first_id])
        self.db.reorder_columns("customers", ["name", "_row_name", "id"])

        csv_path = self.csv_sync.export_table(self.db, "customers")
        with csv_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.reader(handle))

        self.assertEqual(rows[0], ["name", "_row_name", "id"])
        self.assertEqual(rows[1], ["Ayse", "second", "2"])
        self.assertEqual(rows[2], ["Ali", "first", "1"])

    def test_export_ordered_copy(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        row_id = self.db.add_row("customers", "first")
        self.db.update_cell("customers", row_id, "name", "Ali")
        self.db.reorder_columns("customers", ["name", "_row_name", "id"])
        self.db.create_ordered_copy("customers", "customers_rebuilt")

        csv_path = self.csv_sync.export_table(self.db, "customers_rebuilt")
        with csv_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.reader(handle))

        self.assertEqual(rows[0], ["name", "_row_name", "id"])
        self.assertEqual(rows[1], ["Ali", "first", "1"])

    def test_delete_table_csv_removes_export(self) -> None:
        self.db.create_table("customers")
        csv_path = self.csv_sync.export_table(self.db, "customers")
        self.assertTrue(csv_path.exists())

        self.csv_sync.delete_table_csv("customers")

        self.assertTrue(not csv_path.exists() or csv_path.read_text(encoding="utf-8") == "")

    def test_export_cleared_cells_as_empty_fields(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        self.db.add_column("customers", "email")
        row_id = self.db.add_row("customers", "first")
        self.db.update_cell("customers", row_id, "name", "Ali")
        self.db.update_cell("customers", row_id, "email", "ali@example.com")
        self.db.clear_cells("customers", [(row_id, "name"), (row_id, "email")])

        csv_path = self.csv_sync.export_table(self.db, "customers")
        with csv_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.reader(handle))

        self.assertEqual(rows[0], ["id", "_row_name", "name", "email"])
        self.assertEqual(rows[1], ["1", "first", "", ""])


if __name__ == "__main__":
    unittest.main()
