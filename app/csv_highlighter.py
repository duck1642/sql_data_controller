from __future__ import annotations

import re

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor, QSyntaxHighlighter, QTextCharFormat


class CsvSyntaxHighlighter(QSyntaxHighlighter):
    def __init__(self, document):
        super().__init__(document)
        self.syntax_enabled = True
        self.search_enabled = True
        self.search_text = ""
        self.case_sensitive = False

        self.header_format = self._format("#dcdcaa", bold=True)
        self.comma_format = self._format("#c586c0")
        self.number_format = self._format("#b5cea8")
        self.quoted_format = self._format("#ce9178")
        self.empty_format = self._format("#6a9955")
        self.search_format = self._format("#111111", background="#ffd966")

    def set_options(
        self,
        syntax_enabled: bool | None = None,
        search_enabled: bool | None = None,
        search_text: str | None = None,
        case_sensitive: bool | None = None,
    ) -> None:
        if syntax_enabled is not None:
            self.syntax_enabled = syntax_enabled
        if search_enabled is not None:
            self.search_enabled = search_enabled
        if search_text is not None:
            self.search_text = search_text
        if case_sensitive is not None:
            self.case_sensitive = case_sensitive
        self.rehighlight()

    def highlightBlock(self, text: str) -> None:
        if self.syntax_enabled:
            if self.currentBlock().blockNumber() == 0:
                self.setFormat(0, len(text), self.header_format)
            for match in re.finditer(",", text):
                self.setFormat(match.start(), 1, self.comma_format)
            for match in re.finditer(r'(?<![^,])(?:\d+(?:\.\d+)?)(?=,|$)', text):
                self.setFormat(match.start(), match.end() - match.start(), self.number_format)
            for match in re.finditer(r'"(?:[^"]|"")*"', text):
                self.setFormat(match.start(), match.end() - match.start(), self.quoted_format)
            for match in re.finditer(r"(?<=,)(?=,|$)", text):
                self.setFormat(match.start(), 1, self.empty_format)

        if self.search_enabled and self.search_text:
            flags = 0 if self.case_sensitive else re.IGNORECASE
            for match in re.finditer(re.escape(self.search_text), text, flags):
                self.setFormat(match.start(), match.end() - match.start(), self.search_format)

    def _format(self, color: str, background: str | None = None, bold: bool = False) -> QTextCharFormat:
        text_format = QTextCharFormat()
        text_format.setForeground(QColor(color))
        if background:
            text_format.setBackground(QColor(background))
        if bold:
            text_format.setFontWeight(700)
        return text_format

