from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation


class ValidationError(ValueError):
    """Raised when untrusted source data does not meet a market-data invariant."""


def require_utc_midnight(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValidationError(f"{field} must be UTC")
    if any((value.hour, value.minute, value.second, value.microsecond)):
        raise ValidationError(f"{field} must be midnight UTC")
    return value.astimezone(UTC)


def require_utc_range(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    start = require_utc_midnight(start, field="start")
    end = require_utc_midnight(end, field="end")
    if end <= start:
        raise ValidationError("end must be after start")
    return start, end


def normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized or not normalized.isalnum():
        raise ValidationError("symbol must contain uppercase letters and digits only")
    return normalized


def parse_decimal(value: str, *, field: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise ValidationError(f"{field} must be a decimal string") from error
    if not parsed.is_finite():
        raise ValidationError(f"{field} must be finite")
    return parsed


def parse_millisecond(value: str, *, field: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise ValidationError(f"{field} must be an integer millisecond timestamp") from error
    if str(parsed) != value:
        raise ValidationError(f"{field} must be an integer millisecond timestamp")
    return parsed
