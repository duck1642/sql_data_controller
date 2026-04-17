from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .csv_sync import CsvSync
from .db import DatabaseController


@dataclass(frozen=True)
class DatabaseSession:
    db: DatabaseController
    csv_sync: CsvSync


def csv_dir_for_database(db_path: str | Path) -> Path:
    path = Path(db_path)
    return path.with_name(f"{path.stem}_csv")


def open_session(db_path: str | Path, use_default_csv_dir: bool = False) -> DatabaseSession:
    path = Path(db_path)
    csv_dir = path.parent / "csv" if use_default_csv_dir else csv_dir_for_database(path)
    return DatabaseSession(DatabaseController(path), CsvSync(csv_dir))

