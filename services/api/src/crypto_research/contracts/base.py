from datetime import datetime, timedelta
from typing import NoReturn, TypeVar

from pydantic import BaseModel, ConfigDict, field_validator


class StrictFrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


Key = TypeVar("Key")
Value = TypeVar("Value")


class FrozenDict(dict[Key, Value]):
    """A JSON-serializable mapping that rejects in-place mutation."""

    @staticmethod
    def _immutable(*args: object, **kwargs: object) -> NoReturn:
        del args, kwargs
        raise TypeError("FrozenDict is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


class UTCModel(StrictFrozenModel):
    @field_validator("*", mode="after")
    @classmethod
    def require_utc_datetimes(cls, value: object) -> object:
        if isinstance(value, datetime) and (
            value.tzinfo is None or value.utcoffset() is None
        ):
            raise ValueError("timestamps must be timezone-aware")
        if isinstance(value, datetime) and value.utcoffset() != timedelta(0):
            raise ValueError("timestamps must use UTC offset +00:00")
        return value
