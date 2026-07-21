from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from crypto_research.contracts.base import StrictFrozenModel, UTCModel
from crypto_research.contracts.manifest import BinanceArchiveSource, DataType

ContractSymbol = Annotated[str, Field(pattern=r"^[A-Z0-9]{3,32}$")]
Checksum = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class SymbolDataStatus(StrEnum):
    REQUESTED = "requested"
    BACKFILLING = "backfilling"
    DATA_READY = "data_ready"
    DEGRADED = "degraded"
    FAILED = "failed"
    DISABLED = "disabled"


class MetadataStatus(StrEnum):
    METADATA_UNVERIFIED = "metadata_unverified"
    PROFILE_BUILDING = "profile_building"
    ELIGIBLE = "eligible"
    INELIGIBLE = "ineligible"


class IngestionJobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SOURCE_PENDING = "source_pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DataPartitionStatus(StrEnum):
    CANDIDATE = "candidate"
    APPROVED = "approved"
    REJECTED = "rejected"


class DataGapStatus(StrEnum):
    OPEN = "open"
    REPAIRED = "repaired"


class BackfillRecheckDisposition(StrEnum):
    UNCHANGED = "unchanged"
    REPLACEMENT_PLANNED = "replacement_planned"
    REPLACEMENT_EXISTS = "replacement_exists"


class StreamStatus(StrEnum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DEGRADED = "degraded"
    DISCONNECTED = "disconnected"


class SourceMode(StrEnum):
    DIRECT = "direct"
    PROXY = "proxy"
    DEGRADED = "degraded"


class EligibilityReasonCode(StrEnum):
    INSUFFICIENT_HISTORY = "insufficient_history"
    STALE_LIVE_DATA = "stale_live_data"
    UNREPAIRED_GAP = "unrepaired_gap"
    INSUFFICIENT_LIQUIDITY = "insufficient_liquidity"
    METADATA_UNVERIFIED = "metadata_unverified"
    PROFILE_INCOMPLETE = "profile_incomplete"
    INSUFFICIENT_COVERAGE = "insufficient_coverage"
    DATA_NOT_READY = "data_not_ready"
    SOURCE_DEGRADED = "source_degraded"


class _PastUTCModel(UTCModel):
    @field_validator("*", mode="after")
    @classmethod
    def reject_future_datetimes(cls, value: object) -> object:
        if isinstance(value, datetime) and value > datetime.now(UTC):
            raise ValueError("timestamps cannot be in the future")
        return value


class _BoundedRangeModel(_PastUTCModel):
    @model_validator(mode="after")
    def validate_range(self) -> "_BoundedRangeModel":
        if self.end <= self.start:
            raise ValueError("end must be after start")
        if self.end - self.start > timedelta(days=366):
            raise ValueError("range cannot exceed 366 days")
        return self


class AddSymbolRequest(_PastUTCModel):
    symbol: ContractSymbol
    history_start: datetime
    history_end: datetime
    include_agg_trades: bool = False

    @model_validator(mode="after")
    def validate_history_range(self) -> "AddSymbolRequest":
        if self.history_end <= self.history_start:
            raise ValueError("history end must be after history start")
        if self.history_end - self.history_start > timedelta(days=366):
            raise ValueError("history range cannot exceed 366 days")
        return self


class BackfillRequest(_BoundedRangeModel):
    symbol: ContractSymbol
    data_types: tuple[DataType, ...] = Field(min_length=1)
    start: datetime
    end: datetime
    include_agg_trades: bool = False

    @model_validator(mode="after")
    def validate_aggregate_trade_opt_in(self) -> "BackfillRequest":
        if DataType.AGG_TRADE in self.data_types and not self.include_agg_trades:
            raise ValueError("aggregate-trade history requires explicit opt-in")
        return self


class BackfillRecheckRequest(StrictFrozenModel):
    partition_id: UUID


class GapReconcileRequest(StrictFrozenModel):
    partition_ids: tuple[UUID, ...] = Field(min_length=1, max_length=100)

    @field_validator("partition_ids")
    @classmethod
    def require_unique_partition_ids(
        cls, value: tuple[UUID, ...]
    ) -> tuple[UUID, ...]:
        if len(value) != len(set(value)):
            raise ValueError("partition_ids must be unique")
        return value


class SymbolView(_PastUTCModel):
    symbol: ContractSymbol
    enabled: bool
    history_start: datetime
    history_end: datetime
    include_agg_trades: bool
    data_status: SymbolDataStatus
    metadata_status: MetadataStatus
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_history_range(self) -> "SymbolView":
        if self.history_end <= self.history_start:
            raise ValueError("history end must be after history start")
        return self


class IngestionJobView(_PastUTCModel):
    job_id: UUID
    symbol: ContractSymbol
    data_type: DataType
    status: IngestionJobStatus
    requested_start: datetime
    requested_end: datetime
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_requested_range(self) -> "IngestionJobView":
        if self.requested_end <= self.requested_start:
            raise ValueError("requested end must be after requested start")
        return self


class BackfillRecheckView(_PastUTCModel):
    job_id: UUID
    partition_id: UUID
    object_id: UUID
    source: BinanceArchiveSource
    previous_checksum: Checksum
    observed_checksum: Checksum
    disposition: BackfillRecheckDisposition
    replacement_job_id: UUID | None = None
    replacement_object_id: UUID | None = None
    checked_at: datetime

    @model_validator(mode="after")
    def validate_replacement_identity(self) -> "BackfillRecheckView":
        replacement_ids = (self.replacement_job_id, self.replacement_object_id)
        if self.disposition is BackfillRecheckDisposition.UNCHANGED:
            if any(value is not None for value in replacement_ids):
                raise ValueError("replacement identity must be absent when unchanged")
        elif any(value is None for value in replacement_ids):
            raise ValueError("changed source requires complete replacement identity")
        return self


class DataPartitionView(_PastUTCModel):
    partition_id: UUID
    symbol: ContractSymbol
    data_type: DataType
    start: datetime
    end: datetime
    parquet_path: str = Field(min_length=1)
    checksum: Checksum
    row_count: int = Field(gt=0)
    version: int = Field(gt=0)
    status: DataPartitionStatus
    created_at: datetime
    approved_at: datetime | None = None

    @field_validator("parquet_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        if value.startswith("/") or ".." in value.split("/"):
            raise ValueError("partition path must be relative and cannot traverse parents")
        return value

    @model_validator(mode="after")
    def validate_range(self) -> "DataPartitionView":
        if self.end <= self.start:
            raise ValueError("partition end must be after partition start")
        return self


class DataGapView(_PastUTCModel):
    gap_id: UUID
    symbol: ContractSymbol
    data_type: DataType
    start: datetime
    end: datetime
    reason: str = Field(min_length=1)
    status: DataGapStatus
    opened_at: datetime
    repaired_at: datetime | None = None

    @model_validator(mode="after")
    def validate_range(self) -> "DataGapView":
        if self.end <= self.start:
            raise ValueError("gap end must be after gap start")
        if self.status is DataGapStatus.REPAIRED and self.repaired_at is None:
            raise ValueError("repaired gaps require repaired_at")
        if self.status is DataGapStatus.OPEN and self.repaired_at is not None:
            raise ValueError("open gaps cannot have repaired_at")
        return self


class ProfileMetricView(UTCModel):
    value: float | None
    sample_count: int = Field(ge=0)
    coverage_fraction: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_evidence(self) -> "ProfileMetricView":
        if self.sample_count == 0:
            if self.value is not None:
                raise ValueError("zero-sample metrics require a null value")
            if self.coverage_fraction != 0:
                raise ValueError("zero-sample metrics require zero coverage")
        elif self.value is None:
            raise ValueError("sampled metrics require a value")
        elif self.coverage_fraction <= 0:
            raise ValueError("sampled metrics require positive coverage")
        return self


class SymbolProfileView(_PastUTCModel):
    symbol: ContractSymbol
    calculated_at: datetime
    coverage_start: datetime
    coverage_end: datetime
    realized_volatility: ProfileMetricView
    jump_frequency: ProfileMetricView
    median_spread_bps: ProfileMetricView
    median_hourly_volume: ProfileMetricView
    funding_rate_mean: ProfileMetricView

    @model_validator(mode="after")
    def validate_coverage_range(self) -> "SymbolProfileView":
        if self.coverage_end <= self.coverage_start:
            raise ValueError("coverage end must be after coverage start")
        non_negative = (
            self.realized_volatility,
            self.jump_frequency,
            self.median_spread_bps,
            self.median_hourly_volume,
        )
        if any(metric.value is not None and metric.value < 0 for metric in non_negative):
            raise ValueError("unsigned profile metric value cannot be negative")
        return self


class EligibilityView(_PastUTCModel):
    symbol: ContractSymbol
    eligible: bool
    reason_codes: tuple[EligibilityReasonCode, ...]
    evaluated_at: datetime

    @model_validator(mode="after")
    def validate_reasons(self) -> "EligibilityView":
        if not self.eligible and not self.reason_codes:
            raise ValueError("ineligible status requires at least one reason code")
        if self.eligible and self.reason_codes:
            raise ValueError("eligible status cannot include blocking reason codes")
        return self


class StreamStateView(_PastUTCModel):
    symbol: ContractSymbol
    stream_name: str = Field(min_length=1)
    status: StreamStatus
    last_event_at: datetime | None
    updated_at: datetime


class MarketDataHealthView(_PastUTCModel):
    source_mode: SourceMode
    archive_healthy: bool
    rest_healthy: bool
    worker_heartbeat_at: datetime | None
    streams: tuple[StreamStateView, ...]
    checked_at: datetime
