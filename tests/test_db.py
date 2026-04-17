from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

from app.db import DatabaseController
from app.validation import ValidationError


class DatabaseControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        self.temp_dir = temp_root / uuid.uuid4().hex
        self.temp_dir.mkdir()
        self.db = DatabaseController(self.temp_dir / "database.sqlite")

    def tearDown(self) -> None:
        self.db.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_table_row_column_workflow(self) -> None:
        self.db.create_table("customers")
        self.assertEqual(self.db.list_tables(), ["customers"])

        row_id = self.db.add_row("customers")
        self.assertEqual(self.db.fetch_rows("customers")[0]["_row_name"], "row_1")

        self.db.add_column("customers", "name")
        self.db.update_cell("customers", row_id, "name", "Ali")
        self.db.rename_row("customers", row_id, "customer_1")
        self.db.rename_column("customers", "name", "full_name")

        columns, rows = self.db.fetch_table_data("customers")
        self.assertEqual(columns, ["id", "_row_name", "full_name"])
        self.assertEqual(rows[0]["_row_name"], "customer_1")
        self.assertEqual(rows[0]["full_name"], "Ali")

        self.db.update_cell("customers", row_id, "full_name", "")
        rows = self.db.fetch_rows("customers")
        self.assertIsNone(rows[0]["full_name"])

    def test_duplicate_and_invalid_names_are_rejected(self) -> None:
        self.db.create_table("customers")
        row_id = self.db.add_row("customers", "customer_1")

        with self.assertRaises(ValidationError):
            self.db.create_table("customers")
        with self.assertRaises(ValidationError):
            self.db.add_column("customers", "bad name")
        with self.assertRaises(ValidationError):
            self.db.add_column("customers", "id")
        with self.assertRaises(ValidationError):
            self.db.add_row("customers", "customer_1")
        with self.assertRaises(ValidationError):
            self.db.update_cell("customers", row_id, "id", "999")

    def test_rename_rejects_protected_and_duplicate_columns(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        self.db.add_column("customers", "email")

        with self.assertRaises(ValidationError):
            self.db.rename_column("customers", "id", "identifier")
        with self.assertRaises(ValidationError):
            self.db.rename_column("customers", "name", "email")


if __name__ == "__main__":
    unittest.main()
