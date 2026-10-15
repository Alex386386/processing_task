import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from adapters.postgres.payments import payment_crud
from domain.errors import IdempotencyConflict
from entrypoints.dependencies import DecimalRoute, get_async_session
from entrypoints.schemas import PaymentAccepted, PaymentCreate, PaymentDetail
from services.payments import create_payment

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/payments", route_class=DecimalRoute)


@router.post("", response_model=PaymentAccepted, status_code=202)
async def post_payment(
    body: PaymentCreate,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255, pattern=r"^\S+$")],
    request: Request,
    session: Annotated[AsyncSession, Depends(get_async_session)],
):
    try:
        payment = await create_payment(session, body.to_command(), idempotency_key)
    except IdempotencyConflict:
        logger.warning(
            "payment_idempotency_conflict request_id=%s",
            request.state.request_id,
        )
        raise HTTPException(409, "Idempotency key already used with a different body") from None
    logger.info(
        "payment_accepted request_id=%s payment_id=%s status=%s",
        request.state.request_id,
        payment.id,
        payment.status.value,
    )
    return PaymentAccepted(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get("/{payment_id}", response_model=PaymentDetail)
async def get_payment(
    payment_id: UUID,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_async_session)],
):
    payment = await payment_crud.get(session, payment_id)
    if payment is None:
        logger.warning(
            "payment_not_found request_id=%s payment_id=%s",
            request.state.request_id,
            payment_id,
        )
        raise HTTPException(404, "Payment not found")
    logger.info(
        "payment_retrieved request_id=%s payment_id=%s status=%s webhook_status=%s",
        request.state.request_id,
        payment.id,
        payment.status.value,
        payment.webhook_status.value,
    )
    return PaymentDetail(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        status=payment.status,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        webhook_status=payment.webhook_status,
        webhook_attempts=payment.webhook_attempts,
        webhook_delivered_at=payment.webhook_delivered_at,
    )
