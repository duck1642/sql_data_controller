from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QAction, QFont
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QInputDialog,
    QListWidget,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QTableView,
    QToolBar,
)

from .csv_sync import CsvSync
from .db import DatabaseController
from .table_model import DatabaseTableModel
from .validation import PROTECTED_COLUMNS, ValidationError


class MainWindow(QMainWindow):
    def __init__(self, db: DatabaseController, csv_sync: CsvSync):
        super().__init__()
        self.db = db
        self.csv_sync = csv_sync
        self._resetting_headers = False

        self.setWindowTitle("SQL Data Controller")
        self.resize(1100, 700)

        self.table_list = QListWidget()
        self.table_list.setMinimumWidth(180)
        self.table_list.setMaximumWidth(280)
        self.table_list.currentTextChanged.connect(self.select_table)

        self.model = DatabaseTableModel(db, csv_sync, self)
        self.model.error_occurred.connect(self.show_error)
        self.model.synced.connect(self.show_status)

        self.table_view = QTableView()
        self.table_view.setModel(self.model)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectItems)
        self.table_view.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table_view.setAlternatingRowColors(True)
        self.configure_draggable_headers()

        self.csv_preview = QPlainTextEdit()
        self.csv_preview.setReadOnly(True)
        self.csv_preview.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self.csv_preview.setFont(QFont("Consolas", 10))

        self.tabs = QTabWidget()
        self.tabs.addTab(self.table_view, "Table View")
        self.tabs.addTab(self.csv_preview, "CSV View")
        self.tabs.currentChanged.connect(self.on_tab_changed)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(self.table_list)
        splitter.addWidget(self.tabs)
        splitter.setStretchFactor(1, 1)
        self.setCentralWidget(splitter)

        self.setStatusBar(QStatusBar())
        self.create_toolbar()
        self.refresh_tables()

    def configure_draggable_headers(self) -> None:
        horizontal_header = self.table_view.horizontalHeader()
        horizontal_header.setSectionsMovable(True)
        horizontal_header.setFirstSectionMovable(True)
        horizontal_header.sectionMoved.connect(self.on_column_section_moved)

        vertical_header = self.table_view.verticalHeader()
        vertical_header.setSectionsMovable(True)
        vertical_header.setFirstSectionMovable(True)
        vertical_header.sectionMoved.connect(self.on_row_section_moved)

    def create_toolbar(self) -> None:
        toolbar = QToolBar("Main")
        toolbar.setMovable(False)
        self.addToolBar(toolbar)

        self.create_table_action = QAction("Create Table", self)
        self.create_table_action.triggered.connect(self.create_table)
        toolbar.addAction(self.create_table_action)

        self.add_row_action = QAction("Add Row", self)
        self.add_row_action.triggered.connect(self.add_row)
        toolbar.addAction(self.add_row_action)

        self.rename_row_action = QAction("Rename Row", self)
        self.rename_row_action.triggered.connect(self.rename_row)
        toolbar.addAction(self.rename_row_action)

        self.add_column_action = QAction("Add Column", self)
        self.add_column_action.triggered.connect(self.add_column)
        toolbar.addAction(self.add_column_action)

        self.rename_column_action = QAction("Rename Column", self)
        self.rename_column_action.triggered.connect(self.rename_column)
        toolbar.addAction(self.rename_column_action)

        toolbar.addSeparator()

        self.refresh_action = QAction("Refresh", self)
        self.refresh_action.triggered.connect(self.refresh_current)
        toolbar.addAction(self.refresh_action)

    def refresh_tables(self, select_name: str | None = None) -> None:
        current = select_name or self.current_table()
        self.table_list.blockSignals(True)
        self.table_list.clear()
        tables = self.db.list_tables()
        self.table_list.addItems(tables)
        self.table_list.blockSignals(False)

        if current in tables:
            self.table_list.setCurrentRow(tables.index(current))
        elif tables:
            self.table_list.setCurrentRow(0)
        else:
            self.model.set_table(None)
            self.csv_preview.clear()
            self.show_status("Create a table to begin.")

        self.update_action_state()

    def select_table(self, table_name: str) -> None:
        if not table_name:
            self.model.set_table(None)
            self.csv_preview.clear()
            self.update_action_state()
            return

        try:
            self.model.set_table(table_name)
            csv_path = self.csv_sync.export_table(self.db, table_name)
            self.reset_header_visual_order()
            self.update_csv_preview()
            self.table_view.resizeColumnsToContents()
            self.show_status(f"Loaded {table_name}. Synced to {csv_path}")
        except ValidationError as exc:
            self.show_error(str(exc))
        finally:
            self.update_action_state()

    def create_table(self) -> None:
        name, ok = QInputDialog.getText(self, "Create Table", "Table name:")
        if not ok:
            return

        try:
            self.db.create_table(name)
            self.csv_sync.export_table(self.db, name.strip())
            self.refresh_tables(select_name=name.strip())
            self.show_status(f"Created table {name.strip()}.")
        except ValidationError as exc:
            self.show_error(str(exc))

    def add_row(self) -> None:
        table = self.require_current_table()
        if table is None:
            return

        row_name, ok = QInputDialog.getText(self, "Add Row", "Row name (leave blank for automatic):")
        if not ok:
            return

        try:
            self.db.add_row(table, row_name.strip() or None)
            self.sync_and_reload(table, "Added row.")
        except ValidationError as exc:
            self.show_error(str(exc))

    def rename_row(self) -> None:
        table = self.require_current_table()
        row = self.current_row_record()
        if table is None or row is None:
            return

        new_name, ok = QInputDialog.getText(
            self,
            "Rename Row",
            "New row name:",
            text=str(row.get("_row_name") or ""),
        )
        if not ok:
            return

        try:
            self.db.rename_row(table, int(row["id"]), new_name)
            self.sync_and_reload(table, "Renamed row.")
        except ValidationError as exc:
            self.show_error(str(exc))

    def add_column(self) -> None:
        table = self.require_current_table()
        if table is None:
            return

        column_name, ok = QInputDialog.getText(self, "Add Column", "Column name:")
        if not ok:
            return

        try:
            self.db.add_column(table, column_name)
            self.sync_and_reload(table, "Added column.")
        except ValidationError as exc:
            self.show_error(str(exc))

    def rename_column(self) -> None:
        table = self.require_current_table()
        if table is None:
            return

        editable_columns = [column for column in self.model.columns if column not in PROTECTED_COLUMNS]
        if not editable_columns:
            self.show_error("There are no user columns to rename.")
            return

        selected_column = self.model.column_name(self.table_view.currentIndex().column())
        default_index = editable_columns.index(selected_column) if selected_column in editable_columns else 0
        old_name, ok = QInputDialog.getItem(
            self,
            "Rename Column",
            "Column:",
            editable_columns,
            default_index,
            False,
        )
        if not ok:
            return

        new_name, ok = QInputDialog.getText(self, "Rename Column", "New column name:", text=old_name)
        if not ok:
            return

        try:
            self.db.rename_column(table, old_name, new_name)
            self.sync_and_reload(table, "Renamed column.")
        except ValidationError as exc:
            self.show_error(str(exc))

    def refresh_current(self) -> None:
        table = self.current_table()
        self.refresh_tables(select_name=table)
        if table:
            self.sync_and_reload(table, "Refreshed.")

    def sync_and_reload(self, table: str, message: str) -> None:
        csv_path = self.csv_sync.export_table(self.db, table)
        self.model.reload()
        self.reset_header_visual_order()
        self.update_csv_preview()
        self.table_view.resizeColumnsToContents()
        self.show_status(f"{message} Synced to {csv_path}")

    def on_column_section_moved(self, _: int, __: int, ___: int) -> None:
        if self._resetting_headers:
            return

        table = self.current_table()
        if table is None:
            return

        ordered_columns = self.visual_column_order()
        if len(ordered_columns) != len(self.model.columns):
            return

        try:
            self.db.reorder_columns(table, ordered_columns)
            csv_path = self.csv_sync.export_table(self.db, table)
            self.update_csv_preview()
            self.show_status(f"Column order saved. Synced to {csv_path}")
        except ValidationError as exc:
            self.show_error(str(exc))
            self.sync_and_reload(table, "Restored column order.")

    def on_row_section_moved(self, _: int, __: int, ___: int) -> None:
        if self._resetting_headers:
            return

        table = self.current_table()
        if table is None:
            return

        ordered_row_ids = self.visual_row_order()
        if len(ordered_row_ids) != len(self.model.rows):
            return

        try:
            self.db.reorder_rows(table, ordered_row_ids)
            csv_path = self.csv_sync.export_table(self.db, table)
            self.update_csv_preview()
            self.show_status(f"Row order saved. Synced to {csv_path}")
        except ValidationError as exc:
            self.show_error(str(exc))
            self.sync_and_reload(table, "Restored row order.")

    def visual_column_order(self) -> list[str]:
        header = self.table_view.horizontalHeader()
        columns: list[str] = []
        for visual_index in range(header.count()):
            column_name = self.model.column_name(header.logicalIndex(visual_index))
            if column_name is not None:
                columns.append(column_name)
        return columns

    def visual_row_order(self) -> list[int]:
        header = self.table_view.verticalHeader()
        row_ids: list[int] = []
        for visual_index in range(header.count()):
            row = self.model.row_record(header.logicalIndex(visual_index))
            if row is not None:
                row_ids.append(int(row["id"]))
        return row_ids

    def reset_header_visual_order(self) -> None:
        self._resetting_headers = True
        try:
            for header in (self.table_view.horizontalHeader(), self.table_view.verticalHeader()):
                header.blockSignals(True)
                for logical_index in range(header.count()):
                    visual_index = header.visualIndex(logical_index)
                    if visual_index != logical_index:
                        header.moveSection(visual_index, logical_index)
                header.blockSignals(False)
        finally:
            self._resetting_headers = False

    def update_csv_preview(self) -> None:
        table = self.current_table()
        if table is None:
            self.csv_preview.clear()
            return
        self.csv_preview.setPlainText(self.csv_sync.read_text(table))

    def on_tab_changed(self, _: int) -> None:
        if self.tabs.currentWidget() is self.csv_preview:
            self.update_csv_preview()

    def current_table(self) -> str | None:
        item = self.table_list.currentItem()
        return item.text() if item is not None else None

    def require_current_table(self) -> str | None:
        table = self.current_table()
        if table is None:
            self.show_error("Create or select a table first.")
        return table

    def current_row_record(self) -> dict | None:
        index = self.table_view.currentIndex()
        if not index.isValid():
            self.show_error("Select a row first.")
            return None
        row = self.model.row_record(index.row())
        if row is None:
            self.show_error("Select a valid row first.")
        return row

    def update_action_state(self) -> None:
        has_table = self.current_table() is not None
        for action in (
            self.add_row_action,
            self.rename_row_action,
            self.add_column_action,
            self.rename_column_action,
        ):
            action.setEnabled(has_table)

    def show_status(self, message: str) -> None:
        self.statusBar().showMessage(message, 6000)

    def show_error(self, message: str) -> None:
        self.statusBar().showMessage(message, 8000)
        QMessageBox.warning(self, "SQL Data Controller", message)
