from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Path
from pydantic import BaseModel, ConfigDict, StrictBool

from crypto_research.contracts.data import (
    BackfillRecheckRequest,
    BackfillRecheckView,
    BackfillRequest,
    DataGapView,
    DataPartitionView,
    EligibilityView,
    GapReconcileRequest,
    IngestionJobView,
    StreamStateView,
    SymbolProfileView,
)
from crypto_research.market.control import MarketDataControl
from crypto_research.routes import (
    Pagination,
    bounded_pagination,
    get_market_data_control,
    no_query,
)

router = APIRouter(tags=["public-market-data"])
SymbolPath = Annotated[str, Path(pattern=r"^[A-Z0-9]{3,32}$")]
Control = Annotated[MarketDataControl, Depends(get_market_data_control)]


class BackfillBody(BackfillRequest):
    """JSON-mode adapter; the durable contract remains strict in Python mode."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        strict=False,
        title="BackfillRequest",
    )
    include_agg_trades: StrictBool = False


class EmptyActionBody(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BackfillRecheckBody(BackfillRecheckRequest):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=False)


class GapReconcileBody(GapReconcileRequest):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=False)


@router.post(
    "/api/symbols/{symbol}/backfills",
    response_model=list[IngestionJobView],
    dependencies=[Depends(no_query)],
)
async def create_backfills(
    symbol: SymbolPath,
    request: BackfillBody,
    control: Control,
) -> tuple[IngestionJobView, ...]:
    return await control.create_backfills(symbol, request)


@router.get(
    "/api/backfills/{job_id}",
    response_model=IngestionJobView,
    dependencies=[Depends(no_query)],
)
async def get_backfill(job_id: UUID, control: Control) -> IngestionJobView:
    return await control.get_backfill(job_id)


@router.post(
    "/api/backfills/{job_id}/retry",
    response_model=IngestionJobView,
    dependencies=[Depends(no_query)],
)
async def retry_backfill(
    job_id: UUID, _request: EmptyActionBody, control: Control
) -> IngestionJobView:
    return await control.retry_backfill(job_id)


@router.post(
    "/api/backfills/{job_id}/recheck",
    response_model=BackfillRecheckView,
    dependencies=[Depends(no_query)],
)
async def recheck_backfill(
    job_id: UUID, request: BackfillRecheckBody, control: Control
) -> BackfillRecheckView:
    return await control.recheck_backfill(job_id, request)


@router.post(
    "/api/gaps/{gap_id}/reconcile",
    response_model=DataGapView,
    dependencies=[Depends(no_query)],
)
async def reconcile_gap(
    gap_id: UUID, request: GapReconcileBody, control: Control
) -> DataGapView:
    return await control.reconcile_gap(gap_id, request)


@router.get(
    "/api/symbols/{symbol}/partitions", response_model=list[DataPartitionView]
)
async def list_partitions(
    symbol: SymbolPath,
    control: Control,
    pagination: Annotated[Pagination, Depends(bounded_pagination)],
) -> tuple[DataPartitionView, ...]:
    return await control.list_partitions(
        symbol, limit=pagination.limit, offset=pagination.offset
    )


@router.get("/api/symbols/{symbol}/gaps", response_model=list[DataGapView])
async def list_gaps(
    symbol: SymbolPath,
    control: Control,
    pagination: Annotated[Pagination, Depends(bounded_pagination)],
) -> tuple[DataGapView, ...]:
    return await control.list_gaps(
        symbol, limit=pagination.limit, offset=pagination.offset
    )


@router.get(
    "/api/symbols/{symbol}/profile",
    response_model=SymbolProfileView,
    dependencies=[Depends(no_query)],
)
async def get_profile(symbol: SymbolPath, control: Control) -> SymbolProfileView:
    return await control.get_profile(symbol)


@router.get(
    "/api/symbols/{symbol}/eligibility",
    response_model=EligibilityView,
    dependencies=[Depends(no_query)],
)
async def get_eligibility(symbol: SymbolPath, control: Control) -> EligibilityView:
    return await control.get_eligibility(symbol)


@router.get(
    "/api/symbols/{symbol}/streams", response_model=list[StreamStateView]
)
async def list_streams(
    symbol: SymbolPath,
    control: Control,
    pagination: Annotated[Pagination, Depends(bounded_pagination)],
) -> tuple[StreamStateView, ...]:
    return await control.list_streams(
        symbol, limit=pagination.limit, offset=pagination.offset
    )
