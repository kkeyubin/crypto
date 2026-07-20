from contextlib import asynccontextmanager
from typing import Protocol

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware

from crypto_research import __version__
from crypto_research.config import Settings, get_settings
from crypto_research.database import DatabaseProbe


class ReadinessProbe(Protocol):
    async def ready(self) -> bool: ...
    async def close(self) -> None: ...


def create_app(
    settings: Settings | None = None,
    database_probe: ReadinessProbe | None = None,
) -> FastAPI:
    runtime_settings = settings or get_settings()
    probe = database_probe or DatabaseProbe(runtime_settings.database_url)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        await probe.close()

    application = FastAPI(title="Crypto Research API", version=__version__, lifespan=lifespan)
    application.state.settings = runtime_settings
    application.state.database_probe = probe
    application.add_middleware(TrustedHostMiddleware, allowed_hosts=runtime_settings.allowed_hosts)

    @application.get("/api/health/live")
    def live_health() -> dict[str, str]:
        return {"status": "ok", "service": "api", "version": __version__}

    @application.get("/api/health/ready")
    async def ready_health() -> JSONResponse:
        if not await probe.ready():
            return JSONResponse(status_code=503, content={"status": "not_ready", "service": "api"})
        return JSONResponse(status_code=200, content={"status": "ready", "service": "api"})

    return application


app = create_app()
