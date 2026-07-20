from datetime import datetime

from pydantic import BaseModel, ConfigDict, field_validator


class StrictFrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class UTCModel(StrictFrozenModel):
    @field_validator("*", mode="after")
    @classmethod
    def require_utc_datetimes(cls, value: object) -> object:
        if isinstance(value, datetime) and (
            value.tzinfo is None or value.utcoffset() is None
        ):
            raise ValueError("timestamps must be timezone-aware")
        return value
