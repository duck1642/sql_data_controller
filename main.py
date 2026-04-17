from __future__ import annotations

import sys
from pathlib import Path

from app.csv_sync import CsvSync
from app.db import DatabaseController


def main() -> int:
    try:
        from PyQt6.QtWidgets import QApplication
    except ModuleNotFoundError:
        print("PyQt6 is not installed. Run: pip install -r requirements.txt", file=sys.stderr)
        return 1

    from app.main_window import MainWindow

    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"

    db = DatabaseController(data_dir / "database.sqlite")
    csv_sync = CsvSync(data_dir / "csv")

    app = QApplication(sys.argv)
    window = MainWindow(db, csv_sync)
    window.show()

    exit_code = app.exec()
    db.close()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

