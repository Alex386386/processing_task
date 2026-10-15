import asyncio
import logging
from contextlib import suppress

from adapters.postgres.models import utcnow
from adapters.postgres.outbox import outbox_crud
from adapters.rabbitmq.connection import create_broker
from adapters.rabbitmq.messages import PaymentEvent
from adapters.rabbitmq.publishers import publish_event
from adapters.rabbitmq.topology import declare_topology

logger = logging.getLogger(__name__)


async def publish_batch(session, broker, settings) -> int:
    async with session.begin():
        events = await outbox_crud.get_unpublished_for_update(
            session, settings.outbox_batch_size
        )
        for event in events:
            event.publication_attempts += 1
            try:
                await publish_event(
                    broker,
                    settings,
                    PaymentEvent(
                        event_id=event.id,
                        payment_id=event.payment_id,
                    ),
                )
            except Exception as exc:
                event.last_error = type(exc).__name__
                logger.warning(
                    "outbox_failed event_id=%s payment_id=%s attempt=%s error=%s",
                    event.id,
                    event.payment_id,
                    event.publication_attempts,
                    event.last_error,
                )
            else:
                event.published_at = utcnow()
                event.last_error = None
        return len(events)


async def run_publisher(session_factory, settings, stop: asyncio.Event):
    broker = create_broker(settings)
    connected = False
    try:
        while not stop.is_set():
            try:
                if not connected:
                    await broker.connect()
                    await declare_topology(broker, settings)
                    connected = True
                async with session_factory() as session:
                    await publish_batch(session, broker, settings)
            except Exception as exc:
                logger.warning("outbox_cycle_failed error=%s", type(exc).__name__)
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=settings.outbox_poll_interval)
    finally:
        await broker.stop()
