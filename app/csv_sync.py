from __future__ import annotations

import csv
from pathlib import Path

from .db import DatabaseController
from .validation import validate_identifier


class CsvSync:
    def __init__(self, csv_dir: str | Path):
        self.csv_dir = Path(csv_dir)
        self.csv_dir.mkdir(parents=True, exist_ok=True)

    def export_table(self, db: DatabaseController, table_name: str) -> Path:
        name = validate_identifier(table_name, "table name")
        columns, rows = db.fetch_table_data(name)
        csv_path = self.path_for_table(name)

        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(columns)
            for row in rows:
                writer.writerow(["" if row[column] is None else row[column] for column in columns])

        return csv_path

    def read_text(self, table_name: str) -> str:
        csv_path = self.path_for_table(table_name)
        if not csv_path.exists():
            return ""
        return csv_path.read_text(encoding="utf-8")

    def path_for_table(self, table_name: str) -> Path:
        name = validate_identifier(table_name, "table name")
        return self.csv_dir / f"{name}.csv"

