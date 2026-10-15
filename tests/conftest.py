from uuid import uuid4

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from entrypoints.app import create_app
from settings import Settings

pytest_plugins = ["tests.pytest_plugin"]


@pytest.fixture
def settings(testcontainers_manager):
    return Settings(
        _env_file=None,
        database_url=testcontainers_manager.database_url,
        rabbitmq_url=testcontainers_manager.rabbitmq_url,
        api_key="test-key",
        payment_processing_min_delay=0,
        payment_processing_max_delay=0,
    )


@pytest.fixture
async def sessions(postgres_url):
    schema = "test_" + uuid4().hex
    admin = create_async_engine(postgres_url)
    async with admin.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(
        postgres_url, connect_args={"server_settings": {"search_path": schema}}
    )
    try:

        def migrate(connection):
            config = Config("alembic.ini")
            config.attributes["connection"] = connection
            command.upgrade(config, "head")

        async with engine.begin() as conn:
            await conn.run_sync(migrate)
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


@pytest.fixture
async def client(sessions, settings):
    app = create_app(settings, sessions, publisher_enabled=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as client:
        yield client


@pytest.fixture
def body():
    return {
        "amount": "100.00",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": "42"},
        "webhook_url": "https://client.test/hook",
    }
