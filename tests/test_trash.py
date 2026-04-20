from __future__ import annotations

import csv
import json
import shutil
import sqlite3
import unittest
import uuid
from contextlib import closing
from pathlib import Path

from app.csv_sync import CsvSync
from app.db import DatabaseController
from app.trash import create_table_trash_snapshot


class TrashSnapshotTests(unittest.TestCase):
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

    def test_table_trash_snapshot_contains_table_csv_metadata_and_manifest(self) -> None:
        self.db.create_table("customers")
        self.db.add_column("customers", "name")
        first_id = self.db.add_row("customers", "first")
        second_id = self.db.add_row("customers", "second")
        self.db.update_cell("customers", first_id, "name", "Ali")
        self.db.update_cell("customers", second_id, "name", "Ayse")
        self.db.reorder_rows("customers", [second_id, first_id])
        self.db.reorder_columns("customers", ["name", "_row_name", "id"])

        snapshot = create_table_trash_snapshot(self.db, self.csv_sync, "customers")

        self.assertTrue(snapshot.snapshot_dir.exists())
        self.assertTrue(snapshot.sqlite_path.exists())
        self.assertTrue(snapshot.csv_path.exists())
        self.assertTrue(snapshot.manifest_path.exists())

        manifest = json.loads(snapshot.manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["type"], "table")
        self.assertEqual(manifest["table_name"], "customers")
        self.assertEqual(manifest["row_count"], 2)
        self.assertEqual(manifest["column_count"], 3)
        self.assertEqual(manifest["columns"], ["name", "_row_name", "id"])

        with snapshot.csv_path.open(newline="", encoding="utf-8") as handle:
            csv_rows = list(csv.reader(handle))
        self.assertEqual(csv_rows[0], ["name", "_row_name", "id"])
        self.assertEqual(csv_rows[1], ["Ayse", "second", "2"])

        with closing(sqlite3.connect(snapshot.sqlite_path)) as connection:
            connection.row_factory = sqlite3.Row
            tables = {
                row["name"]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
            self.assertIn("customers", tables)
            self.assertIn("_sdc_row_order", tables)
            self.assertIn("_sdc_column_order", tables)
            rows = connection.execute(
                """
                SELECT data.name, data._row_name, data.id
                FROM customers AS data
                JOIN _sdc_row_order AS row_order
                  ON row_order.table_name = 'customers'
                 AND row_order.row_id = data.id
                ORDER BY row_order.position
                """
            ).fetchall()
            self.assertEqual([tuple(row) for row in rows], [("Ayse", "second", 2), ("Ali", "first", 1)])


if __name__ == "__main__":
    unittest.main()
