from sqlalchemy.exc import IntegrityError

from adapters.postgres.models import Outbox, Payment
from adapters.postgres.outbox import outbox_crud
from adapters.postgres.payments import payment_crud
from domain.errors import IdempotencyConflict
from domain.models import PaymentRequest


async def create_payment(session, body: PaymentRequest, key: str) -> Payment:
    normalized = body.normalized()
    try:
        async with session.begin():
            existing = await payment_crud.get_by_idempotency_key(session, key)
            if existing:
                if existing.request_body != normalized:
                    raise IdempotencyConflict
                return existing
            payment = Payment(
                amount=body.amount,
                currency=body.currency,
                description=body.description,
                metadata_=normalized["metadata"],
                webhook_url=str(body.webhook_url),
                idempotency_key=key,
                request_body=normalized,
            )
            payment_crud.add(session, payment)
            await session.flush()
            outbox_crud.add(session, Outbox(payment_id=payment.id))
            await session.flush()
        return payment
    except IntegrityError:
        existing = await payment_crud.get_by_idempotency_key(session, key)
        if existing is None:
            raise
        if existing.request_body != normalized:
            raise IdempotencyConflict from None
        return existing
