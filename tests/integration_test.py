import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
from pamqp.commands import Basic
from sqlalchemy import func, select

from adapters.postgres.models import Outbox, Payment
from entrypoints.app import create_app
from entrypoints.schemas import PaymentCreate
from services.outbox import publish_batch
from services.payments import create_payment

pytestmark = pytest.mark.integration


async def test_parallel_idempotent_requests(sessions, settings, body):
    app = create_app(settings, sessions, publisher_enabled=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        responses = await asyncio.gather(
            *[
                client.post(
                    "/api/v1/payments",
                    json=body,
                    headers={"X-API-Key": "test-key", "Idempotency-Key": "parallel"},
                )
                for _ in range(20)
            ]
        )
    assert all(r.status_code == 202 for r in responses)
    assert len({r.json()["payment_id"] for r in responses}) == 1
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_parallel_publishers_skip_locked(sessions, settings, body):
    for i in range(10):
        async with sessions() as session:
            await create_payment(session, PaymentCreate(**body).to_command(), f"parallel-{i}")
    settings.outbox_batch_size = 2
    broker = AsyncMock()

    async def publish(*args, **kwargs):
        await asyncio.sleep(0.01)
        return Basic.Ack()

    broker.publish.side_effect = publish
    async def publish_once():
        async with sessions() as session:
            return await publish_batch(session, broker, settings)

    await asyncio.gather(*[publish_once() for _ in range(5)])
    assert broker.publish.await_count == 10
    assert len({call.args[0]["event_id"] for call in broker.publish.call_args_list}) == 10
    async with sessions() as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(Outbox).where(Outbox.published_at.is_(None))
            )
            == 0
        )


async def test_postgres_decimal_precision(sessions, body):
    body["amount"] = "9999999999999999.99"
    async with sessions() as session:
        payment = await create_payment(session, PaymentCreate(**body).to_command(), "exact")
    async with sessions() as session:
        saved = await session.get(Payment, payment.id)
        assert saved.amount == Decimal(body["amount"])


async def test_migration_matches_models_and_can_be_reapplied(sessions, body):
    from alembic import command
    from alembic.config import Config

    async with sessions() as session:
        payment = await create_payment(session, PaymentCreate(**body).to_command(), "migration")

    def verify(connection):
        config = Config("alembic.ini")
        config.attributes["connection"] = connection
        command.upgrade(config, "head")
        command.check(config)

    async with sessions.begin() as session:
        connection = await session.connection()
        await connection.run_sync(verify)
        saved = await session.get(Payment, payment.id)
        assert saved.amount == payment.amount
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1
