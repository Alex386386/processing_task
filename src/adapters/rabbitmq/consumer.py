import logging
from collections.abc import Awaitable, Callable

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import ValidationError

from adapters.rabbitmq.messages import PaymentEvent
from adapters.rabbitmq.publishers import publish_event
from adapters.rabbitmq.topology import DLQ, EXCHANGE, NEW_QUEUE
from domain.errors import AttemptFailed, InvalidEvent
from settings import Settings

logger = logging.getLogger(__name__)


class PaymentConsumer:
    def __init__(
        self,
        broker: RabbitBroker,
        settings: Settings,
        handler: Callable[[PaymentEvent], Awaitable[None]],
    ) -> None:
        self._broker = broker
        self._settings = settings
        self._handler = handler

    def register(self) -> None:
        self._broker.subscriber(NEW_QUEUE, EXCHANGE, ack_policy=AckPolicy.MANUAL)(self.handle)

    async def handle(self, message: RabbitMessage) -> None:
        try:
            event = PaymentEvent.model_validate_json(message.body)
        except (ValidationError, ValueError):
            logger.warning("invalid_payment_event")
            await message.reject(requeue=False)
            return

        deliveries = message.headers.get("x-delivery-count", 0)
        if isinstance(deliveries, int):
            event.attempt = min(
                self._settings.message_max_attempts,
                event.attempt + max(deliveries, 0),
            )

        try:
            await self._handler(event)
        except InvalidEvent:
            logger.warning(
                "unknown_event event_id=%s payment_id=%s",
                event.event_id,
                event.payment_id,
            )
            await message.reject(requeue=False)
        except Exception as exc:
            attempt = exc.attempt if isinstance(exc, AttemptFailed) else event.attempt
            reason = exc.reason if isinstance(exc, AttemptFailed) else type(exc).__name__
            route = (
                DLQ.name
                if attempt >= self._settings.message_max_attempts
                else f"payments.retry.{attempt}"
            )
            retry_event = event.model_copy(
                update={"attempt": min(attempt + 1, self._settings.message_max_attempts)}
            )
            try:
                await publish_event(self._broker, self._settings, retry_event, route)
            except Exception as publish_error:
                logger.warning(
                    "retry_publish_failed event_id=%s error=%s",
                    event.event_id,
                    type(publish_error).__name__,
                )
                await message.nack(requeue=True)
                return
            logger.warning(
                "payment_retry event_id=%s payment_id=%s attempt=%s route=%s error=%s",
                event.event_id,
                event.payment_id,
                attempt,
                route,
                reason,
            )
            await message.ack()
        else:
            await message.ack()
