from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta
from typing import NoReturn, TypeVar

from pydantic import BaseModel, ConfigDict, field_validator


class StrictFrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


Key = TypeVar("Key")
Value = TypeVar("Value")


class FrozenMapping(Mapping[Key, Value]):
    """A mapping backed only by immutable key/value pairs."""

    __slots__ = ("__items",)

    def __init__(self, values: Mapping[Key, Value]) -> None:
        object.__setattr__(self, "_FrozenMapping__items", tuple(values.items()))

    def __getitem__(self, key: Key) -> Value:
        for item_key, item_value in self.__items:
            if item_key == key:
                return item_value
        raise KeyError(key)

    def __iter__(self) -> Iterator[Key]:
        return (key for key, _ in self.__items)

    def __len__(self) -> int:
        return len(self.__items)

    def __setattr__(self, name: str, value: object) -> NoReturn:
        del name, value
        raise AttributeError("FrozenMapping is immutable")

    def __delattr__(self, name: str) -> NoReturn:
        del name
        raise AttributeError("FrozenMapping is immutable")


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
