from __future__ import annotations

import json
import sys
import threading
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any


class AppLogger:
    def __init__(self, log_dir: str | Path, max_bytes: int = 1_000_000):
        self.log_dir = Path(log_dir)
        self.max_bytes = max_bytes

    def debug(self, event: str, action: str, status: str, message: str = "", **context: Any) -> None:
        self.log("DEBUG", event, action, status, message, **context)

    def info(self, event: str, action: str, status: str, message: str = "", **context: Any) -> None:
        self.log("INFO", event, action, status, message, **context)

    def warning(self, event: str, action: str, status: str, message: str = "", **context: Any) -> None:
        self.log("WARNING", event, action, status, message, **context)

    def error(self, event: str, action: str, status: str, message: str = "", **context: Any) -> None:
        self.log("ERROR", event, action, status, message, **context)

    def critical(self, event: str, action: str, status: str, message: str = "", **context: Any) -> None:
        self.log("CRITICAL", event, action, status, message, **context)

    def exception(
        self,
        event: str,
        action: str,
        exc: BaseException,
        status: str = "failed",
        level: str = "ERROR",
        message: str = "",
        **context: Any,
    ) -> None:
        context = dict(context)
        context["error"] = str(exc)
        context["error_type"] = type(exc).__name__
        context["traceback"] = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        self.log(level, event, action, status, message or str(exc), **context)

    def exception_info(
        self,
        event: str,
        action: str,
        exc_type,
        exc_value,
        exc_traceback,
        status: str = "failed",
        level: str = "ERROR",
        message: str = "",
        **context: Any,
    ) -> None:
        context = dict(context)
        context["error"] = str(exc_value)
        context["error_type"] = getattr(exc_type, "__name__", str(exc_type))
        context["traceback"] = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        self.log(level, event, action, status, message or str(exc_value), **context)

    def log(self, level: str, event: str, action: str, status: str, message: str = "", **context: Any) -> None:
        try:
            record = {
                "ts": datetime.now().isoformat(timespec="milliseconds"),
                "level": level,
                "event": event,
                "action": action,
                "status": status,
                "message": message,
            }
            for key, value in context.items():
                if value is not None:
                    record[key] = self._json_safe(value)
            line = json.dumps(record, ensure_ascii=False, sort_keys=False) + "\n"
            path = self.current_log_path_for_write(len(line.encode("utf-8")))
            with path.open("a", encoding="utf-8") as handle:
                handle.write(line)
        except Exception:
            return

    def current_log_path(self) -> Path:
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            indexes = self._existing_indexes()
            if not indexes:
                return self.log_dir / "app_log1.jsonl"
            return self.log_dir / f"app_log{max(indexes)}.jsonl"
        except Exception:
            return self.log_dir / "app_log1.jsonl"

    def current_log_path_for_write(self, incoming_bytes: int = 0) -> Path:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        indexes = self._existing_indexes()
        if not indexes:
            return self.log_dir / "app_log1.jsonl"
        latest_index = max(indexes)
        latest_path = self.log_dir / f"app_log{latest_index}.jsonl"
        try:
            if latest_path.exists() and latest_path.stat().st_size + incoming_bytes > self.max_bytes:
                return self.log_dir / f"app_log{latest_index + 1}.jsonl"
        except OSError:
            return latest_path
        return latest_path

    def last_error_line(self) -> str:
        try:
            for index in sorted(self._existing_indexes(), reverse=True):
                path = self.log_dir / f"app_log{index}.jsonl"
                lines = path.read_text(encoding="utf-8").splitlines()
                for line in reversed(lines):
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if record.get("level") in {"ERROR", "CRITICAL"}:
                        return line
        except Exception:
            return ""
        return ""

    def _existing_indexes(self) -> list[int]:
        indexes: list[int] = []
        if not self.log_dir.exists():
            return indexes
        for path in self.log_dir.glob("app_log*.jsonl"):
            stem = path.stem
            if not stem.startswith("app_log"):
                continue
            suffix = stem.removeprefix("app_log")
            if suffix.isdigit():
                indexes.append(int(suffix))
        return indexes

    def _json_safe(self, value: Any) -> Any:
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {str(key): self._json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._json_safe(item) for item in value]
        try:
            json.dumps(value)
            return value
        except TypeError:
            return str(value)


def install_global_exception_hooks(app_logger: AppLogger) -> None:
    def excepthook(exc_type, exc_value, exc_traceback) -> None:
        app_logger.exception_info(
            "exception",
            "sys_excepthook",
            exc_type,
            exc_value,
            exc_traceback,
            level="CRITICAL",
            message="Uncaught Python exception.",
        )
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

    def threading_hook(args: threading.ExceptHookArgs) -> None:
        app_logger.exception_info(
            "exception",
            "threading_excepthook",
            args.exc_type,
            args.exc_value,
            args.exc_traceback,
            level="CRITICAL",
            message=f"Uncaught thread exception in {args.thread.name if args.thread else 'unknown'}.",
        )

    sys.excepthook = excepthook
    threading.excepthook = threading_hook


def install_qt_message_handler(app_logger: AppLogger) -> None:
    try:
        from PyQt6.QtCore import QtMsgType, qInstallMessageHandler
    except Exception:
        return

    def handler(mode, context, message: str) -> None:
        level = {
            QtMsgType.QtDebugMsg: "DEBUG",
            QtMsgType.QtInfoMsg: "INFO",
            QtMsgType.QtWarningMsg: "WARNING",
            QtMsgType.QtCriticalMsg: "ERROR",
            QtMsgType.QtFatalMsg: "CRITICAL",
        }.get(mode, "INFO")
        app_logger.log(
            level,
            "qt",
            "qt_message",
            "reported",
            message,
            file=getattr(context, "file", None),
            line=getattr(context, "line", None),
            function=getattr(context, "function", None),
        )

    qInstallMessageHandler(handler)
