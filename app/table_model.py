from __future__ import annotations

from typing import Any

from PyQt6.QtCore import QAbstractTableModel, QModelIndex, QObject, Qt, pyqtSignal
from PyQt6.QtGui import QColor, QBrush

from .csv_sync import CsvSync
from .db import DatabaseController
from .validation import ValidationError


SEARCH_MATCH_BACKGROUND = QColor("#f0c84b")
SEARCH_MATCH_FOREGROUND = QColor("#111111")


class DatabaseTableModel(QAbstractTableModel):
    error_occurred = pyqtSignal(str)
    synced = pyqtSignal(str)
    change_applied = pyqtSignal(dict)

    def __init__(self, db: DatabaseController, csv_sync: CsvSync, parent: QObject | None = None):
        super().__init__(parent)
        self.db = db
        self.csv_sync = csv_sync
        self.table_name: str | None = None
        self.columns: list[str] = []
        self.all_rows: list[dict[str, Any]] = []
        self.rows: list[dict[str, Any]] = []
        self.search_text = ""
        self.search_highlight_enabled = True
        self.filter_enabled = False
        self.case_sensitive = False

    def set_table(self, table_name: str | None) -> None:
        self.beginResetModel()
        self.table_name = table_name
        if table_name is None:
            self.columns = []
            self.all_rows = []
            self.rows = []
        else:
            self.columns, self.all_rows = self.db.fetch_table_data(table_name)
            self.rows = self._filtered_rows()
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
        if not index.isValid():
            return None

        column = self.columns[index.column()]
        value = self.rows[index.row()].get(column)
        text = "" if value is None else str(value)

        if role in (Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole):
            return text
        if self._cell_matches_search(text):
            if role == Qt.ItemDataRole.BackgroundRole:
                return QBrush(SEARCH_MATCH_BACKGROUND)
            if role == Qt.ItemDataRole.ForegroundRole:
                return QBrush(SEARCH_MATCH_FOREGROUND)
        return None

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
        old_value = row.get(column)

        try:
            self.db.update_cell(self.table_name, row_id, column, value)
            text_value = str(value)
            stored_value = text_value.strip() if column == "_row_name" else (None if text_value == "" else text_value)
            row[column] = stored_value
            csv_path = self.csv_sync.export_table(self.db, self.table_name)
        except ValidationError as exc:
            self.error_occurred.emit(str(exc))
            return False

        if self.filter_enabled:
            self._reset_visible_rows()
        else:
            self.dataChanged.emit(
                index,
                index,
                [
                    Qt.ItemDataRole.DisplayRole,
                    Qt.ItemDataRole.EditRole,
                    Qt.ItemDataRole.BackgroundRole,
                    Qt.ItemDataRole.ForegroundRole,
                ],
            )
        self.change_applied.emit(
            {
                "action": "update_cell",
                "table_name": self.table_name,
                "target": f"{row_id}.{column}",
                "before": {"row_id": row_id, "column": column, "value": old_value},
                "after": {"row_id": row_id, "column": column, "value": stored_value},
            }
        )
        self.synced.emit(f"Saved and synced to {csv_path}")
        return True

    def set_search_options(
        self,
        text: str,
        highlight_enabled: bool,
        filter_enabled: bool,
        case_sensitive: bool,
    ) -> None:
        self.search_text = text
        self.search_highlight_enabled = highlight_enabled
        self.filter_enabled = filter_enabled
        self.case_sensitive = case_sensitive
        self._reset_visible_rows()

    def _reset_visible_rows(self) -> None:
        self.beginResetModel()
        self.rows = self._filtered_rows()
        self.endResetModel()

    def _filtered_rows(self) -> list[dict[str, Any]]:
        if not self.filter_enabled or not self.search_text:
            return list(self.all_rows)
        return [row for row in self.all_rows if self._row_matches_search(row)]

    def _row_matches_search(self, row: dict[str, Any]) -> bool:
        return any(self._cell_matches_search("" if row.get(column) is None else str(row.get(column))) for column in self.columns)

    def _cell_matches_search(self, text: str) -> bool:
        if not self.search_text or not self.search_highlight_enabled and not self.filter_enabled:
            return False
        if self.case_sensitive:
            return self.search_text in text
        return self.search_text.lower() in text.lower()
