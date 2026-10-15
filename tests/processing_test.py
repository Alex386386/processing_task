import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest
from pamqp.commands import Basic
from sqlalchemy import select

from adapters.postgres.models import Outbox, Payment, Status, WebhookStatus
from adapters.rabbitmq.consumer import PaymentConsumer
from adapters.rabbitmq.messages import PaymentEvent
from entrypoints.schemas import PaymentCreate
from services.payments import create_payment
from services.processing import AttemptFailed, Processor, emulate_gateway


async def new_event(sessions, body):
    async with sessions() as session:
        payment = await create_payment(session, PaymentCreate(**body).to_command(), "process")
    async with sessions() as session:
        record = (await session.scalars(select(Outbox))).one()
    return PaymentEvent(event_id=record.id, payment_id=payment.id)


async def handle_message(message, processor, broker, settings, sessions=None):
    async def handler(event):
        if sessions is None:
            await processor.process(event)
        else:
            async with sessions() as session:
                await processor.process(event, session)

    consumer = PaymentConsumer(broker, settings, handler)
    await consumer.handle(message)


async def process(processor, event, sessions):
    async with sessions() as session:
        await processor.process(event, session)


def message(event):
    msg = AsyncMock()
    msg.body = event.model_dump_json().encode()
    msg.headers = {}
    return msg


@pytest.mark.parametrize("status", [Status.SUCCEEDED, Status.FAILED])
async def test_business_results_and_duplicate(sessions, settings, body, status):
    event = await new_event(sessions, body)
    gateway = AsyncMock(return_value=status)
    received = []

    def hook(request):
        received.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(hook)) as client:
        processor = Processor(settings, client, gateway)
        await process(processor, event, sessions)
        async with sessions() as session:
            payment = await session.get(Payment, event.payment_id)
            processed_at = payment.processed_at
        await process(processor, event, sessions)
    gateway.assert_awaited_once()
    assert len(received) == 1
    import json

    payload = json.loads(received[0].content)
    assert payload["amount"] == "100.00"
    assert payload["status"] == status.value
    assert payload["event_id"] == str(event.event_id)
    async with sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == status
        assert payment.processed_at == processed_at
        assert payment.webhook_status == WebhookStatus.DELIVERED
        assert payment.webhook_attempts == 1
        assert payment.webhook_delivered_at


@pytest.mark.parametrize("code", [302, 500])
async def test_retry_then_dlq_and_no_budget_reset(sessions, settings, body, code):
    event = await new_event(sessions, body)
    gateway = AsyncMock(return_value=Status.SUCCEEDED)
    broker = AsyncMock()
    broker.publish.return_value = Basic.Ack()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(code)), follow_redirects=False
    ) as client:
        processor = Processor(settings, client, gateway)
        for expected_route in ("payments.retry.1", "payments.retry.2", "payments.dlq"):
            msg = message(event)
            await handle_message(msg, processor, broker, settings, sessions)
            assert broker.publish.call_args.kwargs["routing_key"] == expected_route
            msg.ack.assert_awaited_once()
            msg.nack.assert_not_called()
        await handle_message(message(event), processor, broker, settings, sessions)
    gateway.assert_awaited_once()
    async with sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == Status.SUCCEEDED
        assert payment.webhook_status == WebhookStatus.FAILED
        assert payment.webhook_attempts == 3
        assert payment.webhook_last_error == f"WebhookHTTP{code}"


async def test_temporary_webhook_error_recovers(sessions, settings, body):
    event = await new_event(sessions, body)
    client = AsyncMock()
    client.post.side_effect = [httpx.ConnectError("sensitive URL"), httpx.Response(200)]
    gateway = AsyncMock(return_value=Status.FAILED)
    processor = Processor(settings, client, gateway)
    with pytest.raises(AttemptFailed) as error:
        await process(processor, event, sessions)
    assert error.value.reason == "ConnectError"
    await process(processor, event, sessions)
    gateway.assert_awaited_once()
    async with sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.webhook_attempts == 2
        assert payment.webhook_status == WebhookStatus.DELIVERED


async def test_crash_after_webhook_reservation_keeps_budget(sessions, settings, body):
    event = await new_event(sessions, body)
    client = AsyncMock()
    client.post.side_effect = asyncio.CancelledError
    gateway = AsyncMock(return_value=Status.SUCCEEDED)
    processor = Processor(settings, client, gateway)
    for _ in range(3):
        with pytest.raises(asyncio.CancelledError):
            await process(processor, event, sessions)
    with pytest.raises(AttemptFailed):
        await process(processor, event, sessions)
    assert client.post.await_count == 3
    gateway.assert_awaited_once()
    async with sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.webhook_status == WebhookStatus.FAILED


async def test_retry_publish_failure_requeues_original(settings):
    from uuid import uuid4

    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    msg = message(event)
    processor = AsyncMock()
    processor.process.side_effect = AttemptFailed(1, "Timeout")
    broker = AsyncMock()
    broker.publish.side_effect = ConnectionError
    await handle_message(msg, processor, broker, settings)
    msg.nack.assert_awaited_once_with(requeue=True)
    msg.ack.assert_not_called()


async def test_poison_message_is_dead_lettered(settings):
    msg = AsyncMock(body=b"not json")
    processor = AsyncMock()
    await handle_message(msg, processor, AsyncMock(), settings)
    msg.reject.assert_awaited_once_with(requeue=False)
    processor.process.assert_not_called()


@pytest.mark.parametrize("rate,status", [(1, Status.SUCCEEDED), (0, Status.FAILED)])
async def test_emulator_deterministic(settings, rate, status):
    settings.payment_success_rate = rate
    assert await emulate_gateway(settings) == status
