import json
import logging
import secrets
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Annotated

from fastapi import Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRoute
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class DecimalRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handler(request: Request):
            if request.method == "POST":
                try:
                    body = json.loads(await request.body(), parse_float=Decimal)
                    if isinstance(body, dict) and "metadata" in body:
                        body["metadata"] = jsonable_encoder(body["metadata"])
                    request._json = body
                except (ValueError, UnicodeDecodeError):
                    pass
            return await original(request)

        return handler


async def authenticate(request: Request, x_api_key: Annotated[str | None, Header()] = None):
    if x_api_key is None or not secrets.compare_digest(
        x_api_key.encode(),
        request.app.state.settings.api_key.get_secret_value().encode(),
    ):
        logger.warning(
            "authentication_failed request_id=%s method=%s path=%s",
            getattr(request.state, "request_id", "-"),
            request.method,
            request.url.path,
        )
        raise HTTPException(401, "Unauthorized")


async def get_async_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.session_factory() as session:
        yield session
