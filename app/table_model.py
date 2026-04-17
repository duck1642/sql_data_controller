from __future__ import annotations

from typing import Any

from PyQt6.QtCore import QAbstractTableModel, QModelIndex, QObject, Qt, pyqtSignal

from .csv_sync import CsvSync
from .db import DatabaseController
from .validation import ValidationError


class DatabaseTableModel(QAbstractTableModel):
    error_occurred = pyqtSignal(str)
    synced = pyqtSignal(str)

    def __init__(self, db: DatabaseController, csv_sync: CsvSync, parent: QObject | None = None):
        super().__init__(parent)
        self.db = db
        self.csv_sync = csv_sync
        self.table_name: str | None = None
        self.columns: list[str] = []
        self.rows: list[dict[str, Any]] = []

    def set_table(self, table_name: str | None) -> None:
        self.beginResetModel()
        self.table_name = table_name
        if table_name is None:
            self.columns = []
            self.rows = []
        else:
            self.columns, self.rows = self.db.fetch_table_data(table_name)
        self.endResetModel()

    def reload(self) -> None:
        self.set_table(self.table_name)

    def row_record(self, row_index: int) -> dict[str, Any] | None:
        if 0 <= row_index < len(self.rows):
            return self.rows[row_index]
        return None

    def column_name(self, column_index: int) -> str | None:
        if 0 <= column_index < len(self.columns):
            return self.columns[column_index]
        return None

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self.rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self.columns)

    def data(self, index: QModelIndex, role: int = int(Qt.ItemDataRole.DisplayRole)) -> Any:
        if not index.isValid() or role not in (Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole):
            return None

        column = self.columns[index.column()]
        value = self.rows[index.row()].get(column)
        return "" if value is None else str(value)

    def headerData(
        self,
        section: int,
        orientation: Qt.Orientation,
        role: int = int(Qt.ItemDataRole.DisplayRole),
    ) -> Any:
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal and 0 <= section < len(self.columns):
            return self.columns[section]
        if orientation == Qt.Orientation.Vertical:
            return str(section + 1)
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:
        if not index.isValid():
            return Qt.ItemFlag.NoItemFlags

        flags = Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable
        column = self.columns[index.column()]
        if column != "id":
            flags |= Qt.ItemFlag.ItemIsEditable
        return flags

    def setData(self, index: QModelIndex, value: Any, role: int = int(Qt.ItemDataRole.EditRole)) -> bool:
        if role != Qt.ItemDataRole.EditRole or not index.isValid() or self.table_name is None:
            return False

        column = self.columns[index.column()]
        if column == "id":
            return False

        row = self.rows[index.row()]
        row_id = int(row["id"])

        try:
            self.db.update_cell(self.table_name, row_id, column, value)
            text_value = str(value)
            stored_value = text_value.strip() if column == "_row_name" else (None if text_value == "" else text_value)
            row[column] = stored_value
            csv_path = self.csv_sync.export_table(self.db, self.table_name)
        except ValidationError as exc:
            self.error_occurred.emit(str(exc))
            return False

        self.dataChanged.emit(index, index, [Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole])
        self.synced.emit(f"Saved and synced to {csv_path}")
        return True
