from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import event, func, select
from sqlalchemy.exc import OperationalError

from adapters.postgres.models import Outbox, Payment
from entrypoints.app import create_app
from entrypoints.schemas import PaymentCreate
from services.payments import create_payment


async def test_create_repeat_conflict_and_get(client, sessions, body):
    headers = {"Idempotency-Key": "order-42"}
    first = await client.post("/api/v1/payments", json=body, headers=headers)
    assert first.status_code == 202
    data = first.json()
    assert data["status"] == "pending"
    assert data["created_at"].endswith("Z")
    equivalent = {**body, "amount": "100.0", "description": " Order 42 "}
    repeat = await client.post("/api/v1/payments", json=equivalent, headers=headers)
    assert repeat.status_code == 202
    assert repeat.json()["payment_id"] == data["payment_id"]
    conflict = await client.post(
        "/api/v1/payments", json={**body, "amount": "101"}, headers=headers
    )
    assert conflict.status_code == 409
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        record = (await session.scalars(select(Outbox))).one()
        assert str(record.payment_id) == data["payment_id"]
        assert record.published_at is None
    detail = await client.get(f"/api/v1/payments/{data['payment_id']}")
    assert detail.status_code == 200
    assert detail.json()["amount"] == "100.00"
    assert detail.json()["metadata"] == body["metadata"]
    assert detail.json()["webhook_attempts"] == 0
    assert "idempotency_key" not in detail.json()


@pytest.mark.parametrize("key", [None, "wrong"])
@pytest.mark.parametrize(
    "method,path", [("POST", "/api/v1/payments"), ("GET", f"/api/v1/payments/{uuid4()}")]
)
async def test_authentication(client, body, key, method, path):
    client.headers.pop("X-API-Key")
    headers = {"Idempotency-Key": "auth"}
    if key is not None:
        headers["X-API-Key"] = key
    response = await client.request(method, path, json=body, headers=headers)
    assert response.status_code == 401
    assert response.json() == {"detail": "Unauthorized"}


@pytest.mark.parametrize(
    "field,value",
    [
        ("amount", "0"),
        ("amount", "-1"),
        ("amount", "0.001"),
        ("amount", "NaN"),
        ("amount", "10000000000000000"),
        ("currency", "BTC"),
        ("description", " "),
        ("webhook_url", "ftp://client.test"),
        ("webhook_url", "invalid"),
        ("webhook_url", "https://user:password@client.test"),
        ("metadata", []),
    ],
)
async def test_validation(client, body, field, value):
    response = await client.post(
        "/api/v1/payments", json={**body, field: value}, headers={"Idempotency-Key": "validation"}
    )
    assert response.status_code == 422


async def test_missing_key_and_invalid_uuid(client, body):
    assert (await client.post("/api/v1/payments", json=body)).status_code == 422
    assert (await client.get("/api/v1/payments/not-a-uuid")).status_code == 422
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404


async def test_http_response_includes_request_id(client):
    response = await client.get(f"/api/v1/payments/{uuid4()}")
    assert response.status_code == 404
    assert response.headers["X-Request-ID"]


async def test_decimal_json_number_is_not_rounded_through_float(client, sessions):
    response = await client.post(
        "/api/v1/payments",
        headers={"Idempotency-Key": "precision", "Content-Type": "application/json"},
        content=(
            '{"amount":9999999999999999.99,"currency":"USD","description":"Exact",'
            '"webhook_url":"https://client.test"}'
        ),
    )
    assert response.status_code == 202
    async with sessions() as session:
        payment = (await session.scalars(select(Payment))).one()
        assert payment.amount == Decimal("9999999999999999.99")
        assert payment.request_body["amount"] == "9999999999999999.99"


async def test_atomic_rollback_if_outbox_insert_fails(sessions, body):
    def fail(*args):
        raise RuntimeError("injected outbox insert failure")

    event.listen(Outbox, "before_insert", fail)
    try:
        with pytest.raises(RuntimeError):
            async with sessions() as session:
                await create_payment(session, PaymentCreate(**body).to_command(), "atomic")
    finally:
        event.remove(Outbox, "before_insert", fail)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_database_error_is_sanitized(client, monkeypatch, body):
    monkeypatch.setattr(
        "entrypoints.api.v1.payments.create_payment",
        AsyncMock(
            side_effect=OperationalError("secret SQL", {}, Exception("secret DSN")),
        ),
    )
    response = await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "err"})
    assert response.status_code == 503
    assert "secret" not in response.text


async def test_publisher_lifecycle_does_not_block_api(sessions, settings, monkeypatch, body):
    import asyncio

    started, stopped = asyncio.Event(), asyncio.Event()

    async def publisher(sessions, settings, stop):
        started.set()
        await stop.wait()
        stopped.set()

    monkeypatch.setattr("entrypoints.app.run_publisher", publisher)
    app = create_app(settings, sessions)
    async with app.router.lifespan_context(app):
        await asyncio.wait_for(started.wait(), 1)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as c:
            response = await c.post(
                "/api/v1/payments",
                json=body,
                headers={"X-API-Key": "test-key", "Idempotency-Key": "life"},
            )
            assert response.status_code == 202
    assert stopped.is_set()
