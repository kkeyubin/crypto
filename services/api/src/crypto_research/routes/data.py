from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Path
from pydantic import ConfigDict, StrictBool

from crypto_research.contracts.data import (
    BackfillRequest,
    DataGapView,
    DataPartitionView,
    EligibilityView,
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
