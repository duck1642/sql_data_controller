from __future__ import annotations

import json
import shutil
import unittest
import uuid
from pathlib import Path

from app.app_logger import AppLogger


class AppLoggerTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        self.temp_dir = temp_root / uuid.uuid4().hex
        self.temp_dir.mkdir()

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_creates_jsonl_and_appends_newest_at_bottom(self) -> None:
        logger = AppLogger(self.temp_dir / "logs")

        logger.info("app", "first", "success", "first message")
        logger.info("app", "second", "success", "second message")

        log_path = self.temp_dir / "logs" / "app_log1.jsonl"
        lines = log_path.read_text(encoding="utf-8").splitlines()
        records = [json.loads(line) for line in lines]
        self.assertEqual(records[0]["action"], "first")
        self.assertEqual(records[1]["action"], "second")

    def test_rolls_to_next_number_without_renaming_or_deleting_old_logs(self) -> None:
        logger = AppLogger(self.temp_dir / "logs", max_bytes=220)

        logger.info("app", "first", "success", "x" * 160)
        logger.info("app", "second", "success", "x" * 160)

        first_path = self.temp_dir / "logs" / "app_log1.jsonl"
        second_path = self.temp_dir / "logs" / "app_log2.jsonl"
        self.assertTrue(first_path.exists())
        self.assertTrue(second_path.exists())
        self.assertIn('"action": "first"', first_path.read_text(encoding="utf-8"))
        self.assertIn('"action": "second"', second_path.read_text(encoding="utf-8"))

    def test_records_error_traceback_and_last_error(self) -> None:
        logger = AppLogger(self.temp_dir / "logs")

        try:
            raise RuntimeError("boom")
        except RuntimeError as exc:
            logger.exception("exception", "test_failure", exc)

        last_error = json.loads(logger.last_error_line())
        self.assertEqual(last_error["level"], "ERROR")
        self.assertEqual(last_error["error_type"], "RuntimeError")
        self.assertIn("RuntimeError: boom", last_error["traceback"])

    def test_logging_failure_does_not_raise(self) -> None:
        bad_log_dir = self.temp_dir / "not_a_directory"
        bad_log_dir.write_text("x", encoding="utf-8")
        logger = AppLogger(bad_log_dir)

        logger.info("app", "will_not_raise", "success")


if __name__ == "__main__":
    unittest.main()
