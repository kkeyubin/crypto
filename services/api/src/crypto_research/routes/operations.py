from typing import Annotated

from fastapi import APIRouter, Depends

from crypto_research.contracts.data import MarketDataHealthView
from crypto_research.market.control import MarketDataControl
from crypto_research.routes import get_market_data_control, no_query

router = APIRouter(prefix="/api/operations", tags=["public-market-data"])
Control = Annotated[MarketDataControl, Depends(get_market_data_control)]


@router.get(
    "/market-data",
    response_model=MarketDataHealthView,
    dependencies=[Depends(no_query)],
)
async def market_data_health(control: Control) -> MarketDataHealthView:
    return await control.market_data_health()
