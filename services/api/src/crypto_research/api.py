from contextlib import asynccontextmanager
from typing import Protocol, cast

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.middleware.trustedhost import TrustedHostMiddleware

from crypto_research import __version__
from crypto_research.config import Settings, get_settings
from crypto_research.database import Database, DatabaseProbe
from crypto_research.market.control import (
    MarketDataConflict,
    MarketDataControl,
    MarketDataNotFound,
    MarketDataValidationError,
    build_market_data_control,
)
from crypto_research.routes import MarketDataServiceFactory, RequestDatabase, RequestSession
from crypto_research.routes.data import router as data_router
from crypto_research.routes.operations import router as operations_router
from crypto_research.routes.symbols import router as symbols_router


class ReadinessProbe(Protocol):
    async def ready(self) -> bool: ...
    async def close(self) -> None: ...


def create_app(
    settings: Settings | None = None,
    database_probe: ReadinessProbe | None = None,
    *,
    database: RequestDatabase | None = None,
    market_data_service: MarketDataControl | None = None,
    market_data_service_factory: MarketDataServiceFactory | None = None,
) -> FastAPI:
    runtime_settings = settings or get_settings()
    probe = database_probe or DatabaseProbe(runtime_settings.database_url)
    request_database = (
        None
        if market_data_service is not None
        else database or Database(runtime_settings.database_url)
    )
    service_factory = market_data_service_factory or _default_service_factory

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            try:
                if request_database is not None:
                    await request_database.close()
            finally:
                await probe.close()

    application = FastAPI(title="Crypto Research API", version=__version__, lifespan=lifespan)
    application.state.settings = runtime_settings
    application.state.database_probe = probe
    application.state.database = request_database
    application.state.market_data_service = market_data_service
    application.state.market_data_service_factory = service_factory
    application.add_middleware(TrustedHostMiddleware, allowed_hosts=runtime_settings.allowed_hosts)

    @application.exception_handler(MarketDataNotFound)
    async def market_data_not_found(
        _request: Request, error: MarketDataNotFound
    ) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(error)})

    @application.exception_handler(MarketDataConflict)
    async def market_data_conflict(
        _request: Request, error: MarketDataConflict
    ) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(error)})

    @application.exception_handler(MarketDataValidationError)
    async def market_data_validation(
        _request: Request, error: MarketDataValidationError
    ) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(error)})

    @application.exception_handler(OperationalError)
    async def persistent_state_unavailable(
        _request: Request, _error: Exception
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"detail": "persistent market-data state is unavailable"},
        )

    @application.exception_handler(RequestValidationError)
    async def redacted_validation_error(
        _request: Request, error: RequestValidationError
    ) -> JSONResponse:
        safe_errors = [
            {
                key: value
                for key, value in item.items()
                if key not in {"ctx", "input", "url"}
            }
            for item in error.errors()
        ]
        return JSONResponse(status_code=422, content={"detail": safe_errors})

    @application.get("/api/health/live")
    def live_health() -> dict[str, str]:
        return {"status": "ok", "service": "api", "version": __version__}

    @application.get("/api/health/ready")
    async def ready_health() -> JSONResponse:
        if not await probe.ready():
            return JSONResponse(status_code=503, content={"status": "not_ready", "service": "api"})
        return JSONResponse(status_code=200, content={"status": "ready", "service": "api"})

    application.include_router(symbols_router)
    application.include_router(data_router)
    application.include_router(operations_router)

    return application


def _default_service_factory(
    session: RequestSession, settings: Settings
) -> MarketDataControl:
    return build_market_data_control(cast(AsyncSession, session), settings)


app = create_app()
