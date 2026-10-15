import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from time import perf_counter
from uuid import uuid4

from fastapi import Depends, FastAPI
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from adapters.postgres.client import create_database
from entrypoints.api.v1.payments import router as payments_router
from entrypoints.dependencies import authenticate
from entrypoints.logging import configure_logging
from services.outbox import run_publisher
from settings import Settings

logger = logging.getLogger(__name__)


def create_app(settings=None, sessions=None, publisher_enabled=True):
    configure_logging()
    settings = settings or Settings()
    engine = None
    if sessions is None:
        engine, sessions = create_database(settings)

    @asynccontextmanager
    async def lifespan(app):
        stop = asyncio.Event()
        logger.info("api_starting publisher_enabled=%s", publisher_enabled)
        task = (
            asyncio.create_task(run_publisher(sessions, settings, stop))
            if publisher_enabled
            else None
        )
        try:
            yield
        finally:
            logger.info("api_stopping")
            stop.set()
            if task:
                try:
                    await asyncio.wait_for(task, timeout=10)
                except (TimeoutError, asyncio.CancelledError):
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            if engine:
                await engine.dispose()

    app = FastAPI(
        title="Payment processing", lifespan=lifespan, dependencies=[Depends(authenticate)]
    )
    app.state.settings = settings
    app.state.session_factory = sessions

    @app.middleware("http")
    async def log_http_request(request, call_next):
        request_id = uuid4().hex
        request.state.request_id = request_id
        started_at = perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            logger.error(
                "http_request_failed request_id=%s method=%s path=%s duration_ms=%.2f error=%s",
                request_id,
                request.method,
                request.url.path,
                (perf_counter() - started_at) * 1000,
                type(exc).__name__,
            )
            raise

        response.headers["X-Request-ID"] = request_id
        log = logger.error if response.status_code >= 500 else logger.warning
        if response.status_code < 400:
            log = logger.info
        log(
            "http_request_completed request_id=%s method=%s path=%s "
            "status_code=%s duration_ms=%.2f",
            request_id,
            request.method,
            request.url.path,
            response.status_code,
            (perf_counter() - started_at) * 1000,
        )
        return response

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(request, exc):
        errors = [{"loc": error.get("loc"), "type": error.get("type")} for error in exc.errors()]
        logger.warning(
            "request_validation_failed request_id=%s error_count=%s errors=%s",
            getattr(request.state, "request_id", "-"),
            len(errors),
            errors,
        )
        return await request_validation_exception_handler(request, exc)

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request, exc):
        logger.error(
            "database_unavailable request_id=%s error=%s",
            getattr(request.state, "request_id", "-"),
            type(exc).__name__,
        )
        return JSONResponse(status_code=503, content={"detail": "Service temporarily unavailable"})

    app.include_router(payments_router)
    return app
