"""
FastAPI application — secured, rate-limited, request-traced.
"""

import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from src.api.routers import analytics, weather
from src.security.secrets import get_settings

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API starting")
    yield
    logger.info("API shutting down")


app = FastAPI(
    title="Weather Data Warehouse API",
    version="3.0.0",
    docs_url="/docs",
    redoc_url=None,
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

settings = get_settings()

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins_list,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["Authorization", "X-API-Key", "Content-Type"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)


@app.middleware("http")
async def trace_requests(request: Request, call_next) -> Response:
    """Attach request ID and log method+path+status. Never log query params."""
    rid = str(uuid.uuid4())
    request.state.request_id = rid
    t0 = time.perf_counter()
    response: Response = await call_next(request)
    ms = round((time.perf_counter() - t0) * 1000, 1)
    response.headers["X-Request-ID"] = rid
    response.headers["X-Response-Time-Ms"] = str(ms)
    logger.info(
        "rid=%s method=%s path=%s status=%d ms=%s",
        rid, request.method, request.url.path, response.status_code, ms,
    )
    return response


@app.get("/health", include_in_schema=False)
def health():
    return {"status": "ok"}


app.include_router(weather.router,   prefix="/api/v1/weather",   tags=["weather"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
