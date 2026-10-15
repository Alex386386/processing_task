import asyncio
import logging
import random

from adapters.postgres.models import utcnow
from adapters.postgres.outbox import outbox_crud
from adapters.postgres.payments import payment_crud
from domain.errors import AttemptFailed, InvalidEvent
from domain.models import Status, WebhookStatus

logger = logging.getLogger(__name__)


async def emulate_gateway(settings) -> Status:
    await asyncio.sleep(
        random.uniform(
            settings.payment_processing_min_delay,
            settings.payment_processing_max_delay,
        )
    )
    return Status.SUCCEEDED if random.random() < settings.payment_success_rate else Status.FAILED


class Processor:
    def __init__(self, settings, client, gateway=emulate_gateway):
        self.settings = settings
        self.client = client
        self.gateway = gateway

    async def process(self, event, session):
        attempt = event.attempt
        async with session.begin():
            record = await outbox_crud.get(session, event.event_id, for_update=True)
            if record is None or record.payment_id != event.payment_id:
                raise InvalidEvent
            payment = await payment_crud.get(session, event.payment_id)
            if payment.webhook_status == WebhookStatus.DELIVERED:
                return
            if record.processing_attempts >= self.settings.message_max_attempts:
                if payment.status != Status.PENDING:
                    payment.webhook_status = WebhookStatus.FAILED
                exhausted = True
            else:
                exhausted = False
                attempt = max(attempt, record.processing_attempts + 1)
                record.processing_attempts = attempt
        if exhausted:
            raise AttemptFailed(self.settings.message_max_attempts, "AttemptsExhausted")

        try:
            async with session.begin():
                payment = await payment_crud.get(session, event.payment_id, for_update=True)
                if payment.status == Status.PENDING:
                    payment.status = await self.gateway(self.settings)
                    payment.processed_at = utcnow()

            async with session.begin():
                payment = await payment_crud.get(session, event.payment_id, for_update=True)
                if payment.webhook_status == WebhookStatus.DELIVERED:
                    return
                if payment.webhook_attempts >= self.settings.message_max_attempts:
                    raise AttemptFailed(self.settings.message_max_attempts, "WebhookExhausted")
                # Commit the reservation before HTTP: a crash must not reset the attempt budget.
                payment.webhook_attempts += 1
                payload = {
                    "event_id": str(event.event_id),
                    "payment_id": str(payment.id),
                    "status": payment.status.value,
                    "amount": format(payment.amount, ".2f"),
                    "currency": payment.currency.value,
                    "processed_at": payment.processed_at.isoformat(),
                }
                url = payment.webhook_url

            response = await self.client.post(url, json=payload)
            if not 200 <= response.status_code < 300:
                raise AttemptFailed(attempt, f"WebhookHTTP{response.status_code}")
            async with session.begin():
                payment = await payment_crud.get(session, event.payment_id, for_update=True)
                payment.webhook_status = WebhookStatus.DELIVERED
                payment.webhook_delivered_at = utcnow()
                payment.webhook_last_error = None
            logger.info(
                "payment_completed payment_id=%s event_id=%s attempt=%s status=%s",
                event.payment_id,
                event.event_id,
                attempt,
                payload["status"],
            )
        except Exception as exc:
            if isinstance(exc, AttemptFailed):
                attempt, reason = exc.attempt, exc.reason
            else:
                reason = type(exc).__name__
            try:
                async with session.begin():
                    payment = await payment_crud.get(
                        session, event.payment_id, for_update=True
                    )
                    if payment.webhook_status != WebhookStatus.DELIVERED:
                        payment.webhook_last_error = reason
                        if attempt >= self.settings.message_max_attempts:
                            payment.webhook_status = WebhookStatus.FAILED
                    record = await outbox_crud.get(session, event.event_id)
                    record.last_error = reason
            except Exception:
                logger.warning("failure_state_unavailable event_id=%s", event.event_id)
            raise AttemptFailed(attempt, reason) from None
