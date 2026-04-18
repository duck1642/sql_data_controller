from __future__ import annotations

import csv
from pathlib import Path
from io import StringIO

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QAction, QFont
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDockWidget,
    QFileDialog,
    QHBoxLayout,
    QInputDialog,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPlainTextEdit,
    QSizePolicy,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QTableView,
    QTableWidget,
    QTableWidgetItem,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from .change_log import ChangeLogService
from .csv_sync import CsvSync
from .csv_highlighter import CsvSyntaxHighlighter
from .db import DatabaseController
from .session import open_session
from .table_model import DatabaseTableModel
from .validation import PROTECTED_COLUMNS, ValidationError


class MainWindow(QMainWindow):
    def __init__(self, db: DatabaseController, csv_sync: CsvSync):
        super().__init__()
        self.db = db
        self.csv_sync = csv_sync
        self.change_log = ChangeLogService(db, csv_sync)
        self._resetting_headers = False
        self.last_row_order: list[int] = []
        self.last_column_order: list[str] = []

        self.setWindowTitle("SQL Data Controller")
        self.resize(1100, 700)

        self.table_list = QListWidget()
        self.table_list.setMinimumWidth(180)
        self.table_list.setMaximumWidth(280)
        self.table_list.currentTextChanged.connect(self.select_table)

        self.model = DatabaseTableModel(db, csv_sync, self)
        self.model.error_occurred.connect(self.show_error)
        self.model.synced.connect(self.show_status)
        self.model.change_applied.connect(self.record_model_change)

        self.table_view = QTableView()
        self.table_view.setModel(self.model)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectItems)
        self.table_view.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table_view.setAlternatingRowColors(True)
        self.configure_draggable_headers()

        self.csv_preview = QPlainTextEdit()
        self.csv_preview.setReadOnly(True)
        self.csv_preview.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self.csv_preview.setFont(QFont("Consolas", 10))
        self.csv_highlighter = CsvSyntaxHighlighter(self.csv_preview.document())

        self.csv_table = QTableWidget()
        self.csv_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.csv_table.setAlternatingRowColors(True)

        self.csv_mode = QComboBox()
        self.csv_mode.addItems(["Raw Text", "CSV Table"])
        self.csv_mode.currentTextChanged.connect(self.update_csv_mode)
        self.csv_syntax_highlight = QCheckBox("CSV Syntax")
        self.csv_syntax_highlight.setChecked(True)
        self.csv_syntax_highlight.toggled.connect(self.apply_search_options)
        self.csv_search_highlight = QCheckBox("Search Highlight")
        self.csv_search_highlight.setChecked(True)
        self.csv_search_highlight.toggled.connect(self.apply_search_options)

        csv_controls = QHBoxLayout()
        csv_controls.addWidget(self.csv_mode)
        csv_controls.addWidget(self.csv_syntax_highlight)
        csv_controls.addWidget(self.csv_search_highlight)
        csv_controls.addStretch(1)

        self.csv_stack = QTabWidget()
        self.csv_stack.tabBar().hide()
        self.csv_stack.addTab(self.csv_preview, "Raw Text")
        self.csv_stack.addTab(self.csv_table, "CSV Table")

        csv_widget = QWidget()
        csv_layout = QVBoxLayout(csv_widget)
        csv_layout.setContentsMargins(0, 0, 0, 0)
        csv_layout.addLayout(csv_controls)
        csv_layout.addWidget(self.csv_stack)

        self.tabs = QTabWidget()
        self.tabs.addTab(self.table_view, "Table View")
        self.tabs.addTab(csv_widget, "CSV View")
        self.tabs.currentChanged.connect(self.on_tab_changed)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search...")
        self.search_input.textChanged.connect(self.apply_search_options)
        self.search_highlight = QCheckBox("Highlight")
        self.search_highlight.setChecked(True)
        self.search_highlight.toggled.connect(self.apply_search_options)
        self.search_filter = QCheckBox("Filter")
        self.search_filter.toggled.connect(self.apply_search_options)
        self.search_case_sensitive = QCheckBox("Case Sensitive")
        self.search_case_sensitive.toggled.connect(self.apply_search_options)
        self.clear_search_action = QAction("Clear Search", self)
        self.clear_search_action.triggered.connect(self.clear_search)

        search_bar = QWidget()
        search_layout = QHBoxLayout(search_bar)
        search_layout.setContentsMargins(0, 0, 0, 0)
        search_layout.addWidget(self.search_input)
        search_layout.addWidget(self.search_highlight)
        search_layout.addWidget(self.search_filter)
        search_layout.addWidget(self.search_case_sensitive)
        search_bar.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        search_bar.setFixedHeight(search_bar.sizeHint().height())

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(self.table_list)
        main_panel = QWidget()
        main_layout = QVBoxLayout(main_panel)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.addWidget(search_bar, 0)
        main_layout.addWidget(self.tabs, 1)
        splitter.addWidget(main_panel)
        splitter.setStretchFactor(1, 1)

        root = QWidget()
        self.root_layout = QVBoxLayout(root)
        self.root_layout.setContentsMargins(0, 0, 0, 0)
        self.root_layout.setSpacing(0)
        self.root_layout.addWidget(splitter, 1)
        self.setCentralWidget(root)

        self.setStatusBar(QStatusBar())
        self.create_toolbar()
        self.create_log_panel()
        self.configure_context_menus()
        self.refresh_tables()
        self.refresh_change_log()

    def configure_draggable_headers(self) -> None:
        horizontal_header = self.table_view.horizontalHeader()
        horizontal_header.setSectionsClickable(True)
        horizontal_header.setSectionsMovable(True)
        horizontal_header.setFirstSectionMovable(True)
        horizontal_header.sectionMoved.connect(self.on_column_section_moved)

        vertical_header = self.table_view.verticalHeader()
        vertical_header.setSectionsClickable(True)
        vertical_header.setSectionsMovable(True)
        vertical_header.setFirstSectionMovable(True)
        vertical_header.sectionMoved.connect(self.on_row_section_moved)

    def configure_context_menus(self) -> None:
        self.table_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self.show_table_context_menu)
        self.table_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_list.customContextMenuRequested.connect(self.show_table_list_context_menu)

    def create_log_panel(self) -> None:
        self.log_table = QTableWidget()
        self.log_table.setColumnCount(7)
        self.log_table.setHorizontalHeaderLabels(["ID", "Time", "Action", "Table", "Target", "Status", "Error"])
        self.log_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.log_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.log_table.setAlternatingRowColors(True)

        dock = QDockWidget("Change Log", self)
        dock.setWidget(self.log_table)
        dock.setObjectName("change_log")
        self.addDockWidget(Qt.DockWidgetArea.BottomDockWidgetArea, dock)

    def show_table_context_menu(self, position) -> None:
        menu = QMenu(self)
        menu.addAction(self.select_row_action)
        menu.addAction(self.select_column_action)
        menu.addAction(self.empty_cell_action)
        menu.addSeparator()
        menu.addAction(self.rename_row_action)
        menu.addAction(self.delete_row_action)
        menu.addSeparator()
        menu.addAction(self.rename_column_action)
        menu.addAction(self.delete_column_action)
        menu.exec(self.table_view.viewport().mapToGlobal(position))

    def show_table_list_context_menu(self, position) -> None:
        menu = QMenu(self)
        menu.addAction(self.create_table_action)
        menu.addAction(self.rename_table_action)
        menu.addAction(self.delete_table_action)
        menu.exec(self.table_list.viewport().mapToGlobal(position))

    def create_toolbar(self) -> None:
        self.new_db_action = QAction("New DB", self)
        self.new_db_action.triggered.connect(self.new_database)

        self.open_db_action = QAction("Open DB", self)
        self.open_db_action.triggered.connect(self.open_database)

        self.create_table_action = QAction("Create Table", self)
        self.create_table_action.triggered.connect(self.create_table)

        self.rename_table_action = QAction("Rename Table", self)
        self.rename_table_action.triggered.connect(self.rename_table)

        self.delete_table_action = QAction("Delete Table", self)
        self.delete_table_action.triggered.connect(self.delete_table)

        self.add_row_action = QAction("Add Row", self)
        self.add_row_action.triggered.connect(self.add_row)

        self.rename_row_action = QAction("Rename Row", self)
        self.rename_row_action.triggered.connect(self.rename_row)

        self.delete_row_action = QAction("Delete Row", self)
        self.delete_row_action.triggered.connect(self.delete_row)

        self.empty_cell_action = QAction("Empty Cell", self)
        self.empty_cell_action.triggered.connect(self.empty_cells)

        self.add_column_action = QAction("Add Column", self)
        self.add_column_action.triggered.connect(self.add_column)

        self.rename_column_action = QAction("Rename Column", self)
        self.rename_column_action.triggered.connect(self.rename_column)

        self.delete_column_action = QAction("Delete Column", self)
        self.delete_column_action.triggered.connect(self.delete_column)

        self.apply_order_action = QAction("Apply Order to New Table", self)
        self.apply_order_action.triggered.connect(self.apply_order_to_new_table)

        self.select_row_action = QAction("Select Row", self)
        self.select_row_action.triggered.connect(self.select_current_row)

        self.select_column_action = QAction("Select Column", self)
        self.select_column_action.triggered.connect(self.select_current_column)

        self.select_all_action = QAction("Select All", self)
        self.select_all_action.triggered.connect(self.table_view.selectAll)

        self.clear_selection_action = QAction("Clear Selection", self)
        self.clear_selection_action.triggered.connect(self.table_view.clearSelection)

        self.undo_action = QAction("Undo Last", self)
        self.undo_action.triggered.connect(self.undo_last)

        self.redo_action = QAction("Redo Last", self)
        self.redo_action.triggered.connect(self.redo_last)

        self.refresh_action = QAction("Refresh", self)
        self.refresh_action.triggered.connect(self.refresh_current)

        self.create_menus()
        self.create_quick_action_bar()

    def create_menus(self) -> None:
        menu_bar = self.menuBar()
        menu_bar.clear()

        file_menu = menu_bar.addMenu("File")
        file_menu.addAction(self.new_db_action)
        file_menu.addAction(self.open_db_action)
        file_menu.addSeparator()
        file_menu.addAction(self.refresh_action)

        table_menu = menu_bar.addMenu("Table")
        table_menu.addAction(self.create_table_action)
        table_menu.addAction(self.rename_table_action)
        table_menu.addAction(self.delete_table_action)
        table_menu.addSeparator()
        table_menu.addAction(self.apply_order_action)

        row_menu = menu_bar.addMenu("Row")
        row_menu.addAction(self.add_row_action)
        row_menu.addAction(self.rename_row_action)
        row_menu.addAction(self.delete_row_action)
        row_menu.addSeparator()
        row_menu.addAction(self.empty_cell_action)

        column_menu = menu_bar.addMenu("Column")
        column_menu.addAction(self.add_column_action)
        column_menu.addAction(self.rename_column_action)
        column_menu.addAction(self.delete_column_action)

        selection_menu = menu_bar.addMenu("Selection")
        selection_menu.addAction(self.select_row_action)
        selection_menu.addAction(self.select_column_action)
        selection_menu.addAction(self.select_all_action)
        selection_menu.addSeparator()
        selection_menu.addAction(self.clear_selection_action)

        edit_menu = menu_bar.addMenu("Edit")
        edit_menu.addAction(self.undo_action)
        edit_menu.addAction(self.redo_action)
        edit_menu.addSeparator()
        edit_menu.addAction(self.clear_search_action)

    def create_quick_action_bar(self) -> None:
        action_bar = QWidget()
        action_bar.setObjectName("quick_action_bar")
        layout = QHBoxLayout(action_bar)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(4)

        quick_actions = [
            self.new_db_action,
            self.open_db_action,
            self.create_table_action,
            self.add_row_action,
            self.add_column_action,
            self.empty_cell_action,
            self.undo_action,
            self.redo_action,
            self.refresh_action,
        ]
        for action in quick_actions:
            button = QToolButton()
            button.setDefaultAction(action)
            button.setAutoRaise(True)
            button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextOnly)
            button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
            layout.addWidget(button)
        layout.addStretch(1)
        action_bar.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        action_bar.setFixedHeight(action_bar.sizeHint().height())
        self.root_layout.insertWidget(0, action_bar)

    def new_database(self) -> None:
        file_name, _ = QFileDialog.getSaveFileName(
            self,
            "New SQLite Database",
            str(Path.cwd() / "database.sqlite"),
            "SQLite Database (*.sqlite *.db);;All Files (*)",
        )
        if not file_name:
            return
        self.switch_database(Path(file_name), "new_database")

    def open_database(self) -> None:
        file_name, _ = QFileDialog.getOpenFileName(
            self,
            "Open SQLite Database",
            str(Path.cwd()),
            "SQLite Database (*.sqlite *.db);;All Files (*)",
        )
        if not file_name:
            return
        self.switch_database(Path(file_name), "open_database")

    def switch_database(self, db_path: Path, action: str) -> None:
        old_db = self.db
        session = open_session(db_path)
        self.db = session.db
        self.csv_sync = session.csv_sync
        self.change_log.bind(self.db, self.csv_sync)
        self.model.db = self.db
        self.model.csv_sync = self.csv_sync
        self.model.set_table(None)
        self.csv_preview.clear()
        self.csv_table.clear()
        self.table_list.clear()
        try:
            old_db.close()
        except Exception:
            pass

        self.change_log.log(
            action,
            target=str(db_path),
            after={"db_path": str(db_path), "csv_dir": str(self.csv_sync.csv_dir)},
            undoable=False,
        )
        self.refresh_tables()
        self.refresh_change_log()
        self.show_status(f"Using database {db_path}")

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
            self.remember_current_order()
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
            table_name = name.strip()
            self.csv_sync.export_table(self.db, table_name)
            self.change_log.log("create_table", table_name, table_name, after={"table": table_name})
            self.refresh_tables(select_name=table_name)
            self.refresh_change_log()
            self.show_status(f"Created table {table_name}.")
        except ValidationError as exc:
            self.show_error(str(exc))

    def rename_table(self) -> None:
        table = self.require_current_table()
        if table is None:
            return

        new_name, ok = QInputDialog.getText(self, "Rename Table", "New table name:", text=table)
        if not ok:
            return
        new_name = new_name.strip()

        try:
            old_csv = self.csv_sync.path_for_table(table)
            self.db.rename_table(table, new_name)
            if old_csv.exists():
                try:
                    old_csv.rename(self.csv_sync.path_for_table(new_name))
                except PermissionError:
                    self.csv_sync.delete_table_csv(table)
            csv_path = self.csv_sync.export_table(self.db, new_name)
            self.change_log.log(
                "rename_table",
                new_name,
                f"{table}->{new_name}",
                before={"table": table},
                after={"table": new_name},
            )
            self.refresh_tables(select_name=new_name)
            self.refresh_change_log()
            self.show_status(f"Renamed table to {new_name}. Synced to {csv_path}")
        except ValidationError as exc:
            self.show_error(str(exc))

    def delete_table(self) -> None:
        table = self.require_current_table()
        if table is None:
            return

        confirm = QMessageBox.question(
            self,
            "Delete Table",
            (
                f"Delete table '{table}' and its CSV mirror?\n\n"
                "This cannot be undone from inside the app."
            ),
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        try:
            columns, rows = self.db.fetch_table_data(table)
            self.db.delete_table(table)
            self.csv_sync.delete_table_csv(table)
            self.change_log.log(
                "delete_table",
                table,
                table,
                before={"table": table, "columns": columns, "rows": rows},
                undoable=False,
            )
            self.model.set_table(None)
            self.csv_preview.clear()
            self.refresh_tables()
            self.refresh_change_log()
            self.show_status(f"Deleted table {table}.")
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
            row_id = self.db.add_row(table, row_name.strip() or None)
            row = self.db.get_row(table, row_id)
            position = len(self.db.fetch_rows(table)) - 1
            self.change_log.log(
                "add_row",
                table,
                str(row_id),
                after={"row": row, "position": position},
            )
            self.refresh_change_log()
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
            old_name = str(row.get("_row_name") or "")
            self.db.rename_row(table, int(row["id"]), new_name)
            self.change_log.log(
                "rename_row",
                table,
                str(row["id"]),
                before={"row_id": int(row["id"]), "row_name": old_name},
                after={"row_id": int(row["id"]), "row_name": new_name.strip()},
            )
            self.refresh_change_log()
            self.sync_and_reload(table, "Renamed row.")
        except ValidationError as exc:
            self.show_error(str(exc))

    def delete_row(self) -> None:
        table = self.require_current_table()
        rows = self.selected_row_records()
        if table is None or not rows:
            return

        row_ids = [int(row["id"]) for row in rows]
        row_names = [str(row.get("_row_name") or row.get("id")) for row in rows]
        row_label = row_names[0] if len(row_names) == 1 else f"{len(row_names)} selected rows"
        confirm = QMessageBox.question(
            self,
            "Delete Row",
            f"Delete {row_label} from '{table}'?",
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        try:
            current_order = [int(row["id"]) for row in self.db.fetch_rows(table)]
            position_by_id = {row_id: index for index, row_id in enumerate(current_order)}
            before_rows = [
                {"data": dict(row), "position": position_by_id[int(row["id"])]}
                for row in rows
            ]
            self.db.delete_rows(table, row_ids)
            self.change_log.log(
                "delete_rows",
                table,
                ",".join(str(row_id) for row_id in row_ids),
                before={"rows": before_rows, "row_order": current_order},
                after={"row_ids": row_ids},
            )
            self.refresh_change_log()
            self.sync_and_reload(table, "Deleted row." if len(row_ids) == 1 else f"Deleted {len(row_ids)} rows.")
        except ValidationError as exc:
            self.show_error(str(exc))

    def empty_cells(self) -> None:
        table = self.require_current_table()
        if table is None:
            return

        cells = self.selected_editable_cells()
        if not cells:
            self.show_error("Select at least one user cell to empty.")
            return

        cell_label = "1 cell" if len(cells) == 1 else f"{len(cells)} cells"
        confirm = QMessageBox.question(
            self,
            "Empty Cell",
            f"Empty {cell_label} in '{table}'?",
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        try:
            before_cells = [
                {"row_id": row_id, "column": column, "value": self.db.get_row(table, row_id).get(column)}
                for row_id, column in cells
            ]
            cleared = self.db.clear_cells(table, cells)
            self.change_log.log(
                "clear_cells",
                table,
                f"{cleared} cells",
                before={"cells": before_cells},
                after={"cells": [{"row_id": row_id, "column": column, "value": None} for row_id, column in cells]},
            )
            self.refresh_change_log()
            self.sync_and_reload(table, "Emptied cell." if cleared == 1 else f"Emptied {cleared} cells.")
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
            column = column_name.strip()
            self.change_log.log("add_column", table, column, after={"column": column})
            self.refresh_change_log()
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
            self.change_log.log(
                "rename_column",
                table,
                f"{old_name}->{new_name.strip()}",
                before={"column": old_name},
                after={"column": new_name.strip()},
            )
            self.refresh_change_log()
            self.sync_and_reload(table, "Renamed column.")
        except ValidationError as exc:
            self.show_error(str(exc))

    def delete_column(self) -> None:
        table = self.require_current_table()
        if table is None:
            return

        editable_columns = [column for column in self.model.columns if column not in PROTECTED_COLUMNS]
        if not editable_columns:
            self.show_error("There are no user columns to delete.")
            return

        selected_columns = self.selected_column_names()
        protected_selected = [column for column in selected_columns if column in PROTECTED_COLUMNS]
        if protected_selected:
            self.show_error("Protected columns cannot be deleted. Select only user columns.")
            return

        columns_to_delete = [column for column in selected_columns if column in editable_columns]
        if not columns_to_delete:
            selected_column = self.model.column_name(self.table_view.currentIndex().column())
            default_index = editable_columns.index(selected_column) if selected_column in editable_columns else 0
            column_name, ok = QInputDialog.getItem(
                self,
                "Delete Column",
                "Column:",
                editable_columns,
                default_index,
                False,
            )
            if not ok:
                return
            columns_to_delete = [column_name]

        column_label = (
            f"column '{columns_to_delete[0]}'"
            if len(columns_to_delete) == 1
            else f"{len(columns_to_delete)} selected columns"
        )
        confirm = QMessageBox.question(
            self,
            "Delete Column",
            f"Delete {column_label} from '{table}'?",
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        try:
            before_columns = [
                {"name": column, "values": self.db.get_column_values(table, column)}
                for column in columns_to_delete
            ]
            before_order = self.db.get_column_names(table)
            self.db.delete_columns(table, columns_to_delete)
            self.change_log.log(
                "delete_columns",
                table,
                ",".join(columns_to_delete),
                before={"columns": before_columns, "column_order": before_order},
                after={"columns": columns_to_delete},
            )
            self.refresh_change_log()
            self.sync_and_reload(
                table,
                "Deleted column." if len(columns_to_delete) == 1 else f"Deleted {len(columns_to_delete)} columns.",
            )
        except ValidationError as exc:
            self.show_error(str(exc))

    def apply_order_to_new_table(self) -> None:
        table = self.require_current_table()
        if table is None:
            return

        default_name = self.default_rebuilt_table_name(table)
        new_name, ok = QInputDialog.getText(
            self,
            "Apply Order to New Table",
            "New table name:",
            text=default_name,
        )
        if not ok:
            return

        new_name = new_name.strip()
        confirm = QMessageBox.question(
            self,
            "Apply Order to New Table",
            (
                f"Create '{new_name}' from '{table}' using the current row and column order?\n\n"
                "The original table will not be changed."
            ),
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        try:
            self.db.create_ordered_copy(table, new_name)
            csv_path = self.csv_sync.export_table(self.db, new_name)
            self.change_log.log(
                "apply_order_copy",
                new_name,
                f"{table}->{new_name}",
                before={"source_table": table},
                after={"new_table": new_name},
            )
            self.refresh_tables(select_name=new_name)
            self.refresh_change_log()
            self.show_status(f"Created {new_name}. Synced to {csv_path}")
        except ValidationError as exc:
            self.show_error(str(exc))

    def record_model_change(self, change: dict) -> None:
        self.change_log.log(
            change["action"],
            change.get("table_name"),
            change.get("target"),
            before=change.get("before"),
            after=change.get("after"),
        )
        self.refresh_change_log()

    def refresh_current(self) -> None:
        table = self.current_table()
        self.refresh_tables(select_name=table)
        if table:
            self.sync_and_reload(table, "Refreshed.")

    def sync_and_reload(self, table: str, message: str) -> None:
        csv_path = self.csv_sync.export_table(self.db, table)
        self.model.reload()
        self.reset_header_visual_order()
        self.remember_current_order()
        self.update_csv_preview()
        self.table_view.resizeColumnsToContents()
        self.show_status(f"{message} Synced to {csv_path}")

    def on_column_section_moved(self, _: int, __: int, ___: int) -> None:
        if self._resetting_headers:
            return

        table = self.current_table()
        if table is None:
            return

        if self.model.filter_enabled:
            self.show_error("Disable filtering before reordering columns.")
            self.sync_and_reload(table, "Restored column order.")
            return

        ordered_columns = self.visual_column_order()
        if len(ordered_columns) != len(self.model.columns):
            return

        try:
            before_columns = list(self.last_column_order)
            self.db.reorder_columns(table, ordered_columns)
            csv_path = self.csv_sync.export_table(self.db, table)
            self.change_log.log(
                "reorder_columns",
                table,
                "columns",
                before={"columns": before_columns},
                after={"columns": ordered_columns},
            )
            self.last_column_order = list(ordered_columns)
            self.refresh_change_log()
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

        if self.model.filter_enabled:
            self.show_error("Disable filtering before reordering rows.")
            self.sync_and_reload(table, "Restored row order.")
            return

        ordered_row_ids = self.visual_row_order()
        if len(ordered_row_ids) != len(self.model.rows):
            return

        try:
            before_row_ids = list(self.last_row_order)
            self.db.reorder_rows(table, ordered_row_ids)
            csv_path = self.csv_sync.export_table(self.db, table)
            self.change_log.log(
                "reorder_rows",
                table,
                "rows",
                before={"row_ids": before_row_ids},
                after={"row_ids": ordered_row_ids},
            )
            self.last_row_order = list(ordered_row_ids)
            self.refresh_change_log()
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

    def remember_current_order(self) -> None:
        self.last_column_order = list(self.model.columns)
        self.last_row_order = [int(row["id"]) for row in self.model.rows]

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

    def on_tab_changed(self, index: int) -> None:
        if index == 1:
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

    def selected_row_records(self) -> list[dict]:
        selection = self.table_view.selectionModel()
        selected_indexes = selection.selectedRows() if selection is not None else []
        row_numbers = sorted({index.row() for index in selected_indexes})

        if not row_numbers:
            row = self.current_row_record()
            return [row] if row is not None else []

        rows = [self.model.row_record(row_number) for row_number in row_numbers]
        return [row for row in rows if row is not None]

    def selected_column_names(self) -> list[str]:
        selection = self.table_view.selectionModel()
        selected_indexes = selection.selectedColumns() if selection is not None else []
        column_numbers = sorted({index.column() for index in selected_indexes})
        columns = [self.model.column_name(column_number) for column_number in column_numbers]
        return [column for column in columns if column is not None]

    def selected_editable_cells(self) -> list[tuple[int, str]]:
        selection = self.table_view.selectionModel()
        if selection is None:
            return []

        cells: set[tuple[int, str]] = set()
        user_columns = [column for column in self.model.columns if column not in PROTECTED_COLUMNS]

        for index in selection.selectedIndexes():
            row = self.model.row_record(index.row())
            column = self.model.column_name(index.column())
            if row is not None and column in user_columns:
                cells.add((int(row["id"]), column))

        for index in selection.selectedRows():
            row = self.model.row_record(index.row())
            if row is not None:
                for column in user_columns:
                    cells.add((int(row["id"]), column))

        for index in selection.selectedColumns():
            column = self.model.column_name(index.column())
            if column in user_columns:
                for row in self.model.rows:
                    cells.add((int(row["id"]), column))

        return sorted(cells)

    def select_current_row(self) -> None:
        index = self.table_view.currentIndex()
        if index.isValid():
            self.table_view.selectRow(index.row())

    def select_current_column(self) -> None:
        index = self.table_view.currentIndex()
        if index.isValid():
            self.table_view.selectColumn(index.column())

    def clear_search(self) -> None:
        self.search_input.clear()

    def apply_search_options(self) -> None:
        text = self.search_input.text()
        self.model.set_search_options(
            text,
            self.search_highlight.isChecked(),
            self.search_filter.isChecked(),
            self.search_case_sensitive.isChecked(),
        )
        self.csv_highlighter.set_options(
            syntax_enabled=self.csv_syntax_highlight.isChecked(),
            search_enabled=self.csv_search_highlight.isChecked(),
            search_text=text,
            case_sensitive=self.search_case_sensitive.isChecked(),
        )
        self.populate_csv_table()

    def update_csv_mode(self) -> None:
        self.csv_stack.setCurrentIndex(0 if self.csv_mode.currentText() == "Raw Text" else 1)
        self.update_csv_preview()

    def update_csv_preview(self) -> None:
        table = self.current_table()
        if table is None:
            self.csv_preview.clear()
            self.csv_table.clear()
            return
        self.csv_preview.setPlainText(self.csv_sync.read_text(table))
        self.populate_csv_table()

    def populate_csv_table(self) -> None:
        csv_text = self.csv_preview.toPlainText()
        rows = list(csv.reader(StringIO(csv_text))) if csv_text else []
        self.csv_table.clear()
        if not rows:
            self.csv_table.setRowCount(0)
            self.csv_table.setColumnCount(0)
            return

        headers = rows[0]
        data_rows = rows[1:]
        self.csv_table.setColumnCount(len(headers))
        self.csv_table.setRowCount(len(data_rows))
        self.csv_table.setHorizontalHeaderLabels(headers)
        for row_index, row in enumerate(data_rows):
            for column_index, value in enumerate(row):
                item = QTableWidgetItem(value)
                if self.search_text_matches(value):
                    item.setBackground(Qt.GlobalColor.yellow)
                self.csv_table.setItem(row_index, column_index, item)

    def search_text_matches(self, value: str) -> bool:
        text = self.search_input.text()
        if not text or not self.csv_search_highlight.isChecked():
            return False
        if self.search_case_sensitive.isChecked():
            return text in value
        return text.lower() in value.lower()

    def refresh_change_log(self) -> None:
        entries = self.change_log.entries()
        self.log_table.setRowCount(len(entries))
        for row_index, entry in enumerate(entries):
            values = [
                str(entry.id),
                entry.timestamp,
                entry.action,
                entry.table_name or "",
                entry.target or "",
                entry.status,
                entry.error_text or "",
            ]
            for column_index, value in enumerate(values):
                self.log_table.setItem(row_index, column_index, QTableWidgetItem(value))
        self.log_table.resizeColumnsToContents()

    def undo_last(self) -> None:
        ok, message, table = self.change_log.undo_last()
        self.after_undo_redo(ok, message, table)

    def redo_last(self) -> None:
        ok, message, table = self.change_log.redo_last()
        self.after_undo_redo(ok, message, table)

    def after_undo_redo(self, ok: bool, message: str, table: str | None) -> None:
        self.refresh_tables(select_name=table if table and self.db.table_exists(table) else None)
        if table and self.db.table_exists(table):
            self.model.set_table(table)
            self.csv_sync.export_table(self.db, table)
        else:
            self.model.set_table(self.current_table())
        self.reset_header_visual_order()
        self.remember_current_order()
        self.update_csv_preview()
        self.refresh_change_log()
        if ok:
            self.show_status(message)
        else:
            self.show_error(message)

    def update_action_state(self) -> None:
        has_table = self.current_table() is not None
        for action in (
            self.delete_table_action,
            self.rename_table_action,
            self.add_row_action,
            self.rename_row_action,
            self.delete_row_action,
            self.empty_cell_action,
            self.add_column_action,
            self.rename_column_action,
            self.delete_column_action,
            self.apply_order_action,
        ):
            action.setEnabled(has_table)

    def default_rebuilt_table_name(self, table: str) -> str:
        base_name = f"{table}_rebuilt"
        existing = set(self.db.list_tables())
        if base_name not in existing:
            return base_name

        index = 2
        while f"{base_name}_{index}" in existing:
            index += 1
        return f"{base_name}_{index}"

    def show_status(self, message: str) -> None:
        details = [f"DB: {self.db.db_path}"]
        table = self.current_table()
        if table:
            details.append(f"Table: {table}")
            details.append(f"Rows: {len(self.model.rows)}")
            details.append(f"Columns: {len(self.model.columns)}")
            details.append(f"CSV: {self.csv_sync.path_for_table(table)}")
        details.append(message)
        self.statusBar().showMessage(" | ".join(details), 8000)

    def show_error(self, message: str) -> None:
        self.statusBar().showMessage(message, 8000)
        QMessageBox.warning(self, "SQL Data Controller", message)
