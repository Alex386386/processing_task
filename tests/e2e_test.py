import asyncio
import json
from time import monotonic
from uuid import uuid4

import aio_pika
import httpx
import pytest

from adapters.rabbitmq.topology import DLQ, NEW_QUEUE, declare_topology, retry_queues
from entrypoints.app import create_app
from entrypoints.workers.payment_worker import create_worker

pytestmark = pytest.mark.integration


@pytest.fixture
async def live_client(settings, sessions):
    settings.outbox_poll_interval = 0.05
    settings.payment_success_rate = 1
    app = create_app(settings, sessions)
    async with httpx.AsyncClient(
        timeout=settings.webhook_timeout,
        follow_redirects=False,
        trust_env=False,
    ) as webhook_client:
        broker = create_worker(settings, sessions, webhook_client)
        try:
            await broker.connect()
            await declare_topology(broker, settings)
            # These queues belong exclusively to this test session's container.
            for spec in [*retry_queues(settings), NEW_QUEUE, DLQ]:
                queue = await broker.declare_queue(spec)
                await queue.purge()
            await broker.start()
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app),
                    base_url="http://test",
                    headers={"X-API-Key": "test-key"},
                ) as client:
                    yield client
        finally:
            await broker.stop()


async def wait_for_payment(client, payment_id, webhook_status):
    deadline = monotonic() + 40
    while monotonic() < deadline:
        response = await client.get(f"/api/v1/payments/{payment_id}")
        assert response.status_code == 200
        if response.json()["webhook_status"] == webhook_status:
            return response.json()
        await asyncio.sleep(0.1)
    pytest.fail(f"Payment {payment_id} did not reach {webhook_status}")


@pytest.mark.parametrize(
    "path,status,attempts",
    [
        ("ok", "delivered", 1),
        ("flaky", "delivered", 2),
        ("fail", "failed", 3),
    ],
)
async def test_live_payment_pipeline(live_client, testcontainers_manager, path, status, attempts):
    response = await live_client.post(
        "/api/v1/payments",
        headers={"Idempotency-Key": str(uuid4())},
        json={
            "amount": "12.34",
            "currency": "EUR",
            "description": "integration",
            "webhook_url": f"{testcontainers_manager.webhook_url}/{path}",
        },
    )
    assert response.status_code == 202
    payment_id = response.json()["payment_id"]
    detail = await wait_for_payment(live_client, payment_id, status)
    assert detail["status"] in ("succeeded", "failed")
    assert detail["processed_at"].endswith("Z")
    assert detail["webhook_attempts"] == attempts
    async with httpx.AsyncClient() as client:
        hooks = (await client.get(testcontainers_manager.webhook_url)).json()
    event_id, entry = next((k, v) for k, v in hooks.items() if v["payment_id"] == payment_id)
    times = entry["attempts"]
    assert len(times) == attempts
    if attempts >= 2:
        assert times[1] - times[0] >= 0.9
    if attempts == 3:
        assert times[2] - times[1] >= 1.9
    rabbit_url = testcontainers_manager.rabbitmq_url
    connection = await aio_pika.connect_robust(rabbit_url)
    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        exchange = await channel.get_exchange("payments")
        await exchange.publish(
            aio_pika.Message(
                json.dumps(
                    {
                        "event_id": event_id,
                        "payment_id": payment_id,
                    }
                ).encode(),
                content_type="application/json",
                delivery_mode=2,
            ),
            routing_key="payments.new",
        )
    await asyncio.sleep(0.5)
    repeated = (await live_client.get(f"/api/v1/payments/{payment_id}")).json()
    assert repeated["processed_at"] == detail["processed_at"]
    assert repeated["webhook_attempts"] == attempts


async def test_live_poison_message_reaches_dlq(live_client, testcontainers_manager):
    rabbit_url = testcontainers_manager.rabbitmq_url
    connection = await aio_pika.connect_robust(rabbit_url)
    marker = str(uuid4())
    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        exchange = await channel.get_exchange("payments")
        await exchange.publish(
            aio_pika.Message(
                b"{invalid-json",
                content_type="application/json",
                message_id=marker,
                delivery_mode=2,
            ),
            routing_key="payments.new",
        )
        queue = await channel.get_queue("payments.dlq")
        seen = []
        try:
            deadline = monotonic() + 10
            while monotonic() < deadline:
                msg = await queue.get(fail=False)
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                seen.append(msg)
                if msg.message_id == marker:
                    return
            pytest.fail("Poison message was not dead-lettered")
        finally:
            for msg in seen:
                await msg.nack(requeue=True)
