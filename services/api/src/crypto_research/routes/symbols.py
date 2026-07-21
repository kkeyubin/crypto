from typing import Annotated

from fastapi import APIRouter, Depends, Path
from pydantic import ConfigDict, StrictBool

from crypto_research.contracts.data import AddSymbolRequest, SymbolView
from crypto_research.market.control import MarketDataControl
from crypto_research.routes import (
    Pagination,
    bounded_pagination,
    get_market_data_control,
    no_query,
)

router = APIRouter(prefix="/api/symbols", tags=["public-market-data"])
SymbolPath = Annotated[str, Path(pattern=r"^[A-Z0-9]{3,32}$")]
Control = Annotated[MarketDataControl, Depends(get_market_data_control)]


class AddSymbolBody(AddSymbolRequest):
    """JSON-mode adapter; the durable contract remains strict in Python mode."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        strict=False,
        title="AddSymbolRequest",
    )
    include_agg_trades: StrictBool = False


@router.get("", response_model=list[SymbolView])
async def list_symbols(
    control: Control,
    pagination: Annotated[Pagination, Depends(bounded_pagination)],
) -> tuple[SymbolView, ...]:
    return await control.list_symbols(
        limit=pagination.limit, offset=pagination.offset
    )


@router.post("", response_model=SymbolView)
async def add_symbol(request: AddSymbolBody, control: Control) -> SymbolView:
    return await control.add_symbol(request)


@router.get("/{symbol}", response_model=SymbolView, dependencies=[Depends(no_query)])
async def get_symbol(symbol: SymbolPath, control: Control) -> SymbolView:
    return await control.get_symbol(symbol)


@router.delete("/{symbol}", response_model=SymbolView, dependencies=[Depends(no_query)])
async def disable_symbol(symbol: SymbolPath, control: Control) -> SymbolView:
    return await control.disable_symbol(symbol)
