from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol

from fastapi import HTTPException, Query, Request

from crypto_research.config import Settings
from crypto_research.market.control import MarketDataControl


class RequestSession(Protocol):
    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...

    async def close(self) -> None: ...


class RequestDatabase(Protocol):
    def session(self) -> RequestSession: ...

    async def close(self) -> None: ...


class MarketDataServiceFactory(Protocol):
    def __call__(
        self, session: RequestSession, settings: Settings
    ) -> MarketDataControl: ...


@dataclass(frozen=True)
class Pagination:
    limit: int
    offset: int


async def get_market_data_control(request: Request) -> AsyncIterator[MarketDataControl]:
    singleton = request.app.state.market_data_service
    if singleton is not None:
        yield singleton
        return
    database: RequestDatabase = request.app.state.database
    session = database.session()
    factory: MarketDataServiceFactory = request.app.state.market_data_service_factory
    try:
        control = factory(session, request.app.state.settings)
        yield control
        await session.commit()
    except BaseException:
        await session.rollback()
        raise
    finally:
        await session.close()


def bounded_pagination(
    request: Request,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0, le=10_000),
) -> Pagination:
    _reject_unknown_query(request, {"limit", "offset"})
    return Pagination(limit, offset)


def no_query(request: Request) -> None:
    _reject_unknown_query(request, set())


def _reject_unknown_query(request: Request, allowed: set[str]) -> None:
    if set(request.query_params) - allowed:
        raise HTTPException(status_code=422, detail="unknown query parameter")
