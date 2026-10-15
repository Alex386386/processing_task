from unittest.mock import AsyncMock

from pamqp.commands import Basic
from sqlalchemy import select

from adapters.postgres.models import Outbox
from adapters.rabbitmq.topology import NEW_QUEUE, retry_queues
from entrypoints.schemas import PaymentCreate
from services.outbox import publish_batch
from services.payments import create_payment


async def test_publish_failure_then_confirmed_retry(sessions, settings, body):
    async with sessions() as session:
        await create_payment(session, PaymentCreate(**body).to_command(), "outbox")
    broker = AsyncMock()
    broker.publish.side_effect = [ConnectionError("sensitive broker URL"), Basic.Ack()]
    async with sessions() as session:
        assert await publish_batch(session, broker, settings) == 1
    async with sessions() as session:
        record = (await session.scalars(select(Outbox))).one()
        assert record.published_at is None
        assert record.publication_attempts == 1
        assert record.last_error == "ConnectionError"
    async with sessions() as session:
        assert await publish_batch(session, broker, settings) == 1
        assert await publish_batch(session, broker, settings) == 0
    async with sessions() as session:
        record = (await session.scalars(select(Outbox))).one()
        assert record.published_at is not None
        assert record.publication_attempts == 2
    assert broker.publish.call_args.kwargs["persist"] is True
    assert broker.publish.call_args.kwargs["mandatory"] is True


async def test_no_confirm_does_not_mark_published(sessions, settings, body):
    async with sessions() as session:
        await create_payment(session, PaymentCreate(**body).to_command(), "no-confirm")
    broker = AsyncMock()
    broker.publish.return_value = None
    async with sessions() as session:
        await publish_batch(session, broker, settings)
    async with sessions() as session:
        record = (await session.scalars(select(Outbox))).one()
        assert record.published_at is None


def test_retry_topology(settings):
    from pamqp.encode import field_table

    queues = retry_queues(settings)
    assert [q.arguments["x-message-ttl"] for q in queues] == [1000, 2000]
    assert all(q.arguments["x-dead-letter-routing-key"] == "payments.new" for q in queues)
    assert all(q.arguments["x-dead-letter-strategy"] == "at-least-once" for q in queues)
    assert NEW_QUEUE.arguments["x-single-active-consumer"] is True
    for queue in [NEW_QUEUE, *queues]:
        assert field_table(queue.arguments)
