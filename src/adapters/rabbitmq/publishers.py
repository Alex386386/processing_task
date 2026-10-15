from pamqp.commands import Basic

from adapters.rabbitmq.messages import PaymentEvent
from adapters.rabbitmq.topology import DLQ, DLX, EXCHANGE, NEW_QUEUE


async def publish_event(broker, settings, event: PaymentEvent, routing_key=NEW_QUEUE.name):
    result = await broker.publish(
        event.model_dump(mode="json"),
        exchange=DLX if routing_key == DLQ.name else EXCHANGE,
        routing_key=routing_key,
        persist=True,
        mandatory=True,
        message_id=str(event.event_id),
        correlation_id=str(event.payment_id),
        timeout=settings.broker_timeout,
    )
    if not isinstance(result, Basic.Ack):
        raise RuntimeError("Publication not confirmed")
