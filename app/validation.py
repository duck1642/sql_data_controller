from __future__ import annotations

import re


class ValidationError(ValueError):
    """Raised when user-controlled names or values are not valid."""


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROTECTED_COLUMNS = {"id", "_row_name"}


def validate_identifier(name: str, kind: str = "identifier") -> str:
    value = str(name).strip()
    if not value:
        raise ValidationError(f"{kind.capitalize()} cannot be empty.")
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValidationError(
            f"{kind.capitalize()} must start with a letter or underscore and contain only letters, numbers, and underscores."
        )
    if value.lower().startswith("sqlite_"):
        raise ValidationError(f"{kind.capitalize()} cannot start with sqlite_.")
    return value


def validate_user_column_name(name: str) -> str:
    value = validate_identifier(name, "column name")
    if value in PROTECTED_COLUMNS:
        raise ValidationError(f"{value} is a protected column name.")
    return value


def validate_row_name(name: str) -> str:
    value = str(name).strip()
    if not value:
        raise ValidationError("Row name cannot be empty.")
    return value


def quote_identifier(name: str) -> str:
    value = validate_identifier(name)
    return f'"{value}"'

