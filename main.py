from __future__ import annotations

import sys
from pathlib import Path

from app.app_logger import AppLogger, install_global_exception_hooks, install_qt_message_handler
from app.csv_sync import CsvSync
from app.db import DatabaseController


def main() -> int:
    try:
        from PyQt6.QtCore import Qt
        from PyQt6.QtWidgets import QApplication
    except ModuleNotFoundError:
        print("PyQt6 is not installed. Run: pip install -r requirements.txt", file=sys.stderr)
        return 1

    from app.main_window import MainWindow

    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"
    app_logger = AppLogger(base_dir / "logs")
    install_global_exception_hooks(app_logger)

    db = DatabaseController(data_dir / "database.sqlite")
    csv_sync = CsvSync(data_dir / "csv")

    app = QApplication(sys.argv)
    install_qt_message_handler(app_logger)
    for effect in (
        Qt.UIEffect.UI_AnimateMenu,
        Qt.UIEffect.UI_FadeMenu,
        Qt.UIEffect.UI_AnimateTooltip,
        Qt.UIEffect.UI_FadeTooltip,
        Qt.UIEffect.UI_AnimateCombo,
        Qt.UIEffect.UI_AnimateToolBox,
    ):
        app.setEffectEnabled(effect, False)

    app_logger.info("app", "startup", "success", "Application started.", db=str(db.db_path))
    window = MainWindow(db, csv_sync, app_logger)
    window.show()

    exit_code = app.exec()
    app_logger.info("app", "shutdown", "success", "Application closed.", db=str(db.db_path), exit_code=exit_code)
    db.close()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
