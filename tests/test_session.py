from __future__ import annotations

import unittest
from pathlib import Path

from app.session import csv_dir_for_database


class SessionTests(unittest.TestCase):
    def test_csv_dir_is_next_to_database_by_default(self) -> None:
        db_path = Path("project") / "customers.sqlite"

        self.assertEqual(csv_dir_for_database(db_path), Path("project") / "customers_csv")


if __name__ == "__main__":
    unittest.main()

