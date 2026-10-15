from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue

from settings import Settings

EXCHANGE = RabbitExchange("payments", durable=True)
DLX = RabbitExchange("payments.dead", durable=True)


def reliable_arguments(exchange: str, routing_key: str) -> dict:
    return {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": exchange,
        "x-dead-letter-routing-key": routing_key,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-delivery-limit": 2,
    }


NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={**reliable_arguments(DLX.name, "payments.dlq"), "x-single-active-consumer": True},
)
DLQ = RabbitQueue("payments.dlq", durable=True, arguments={"x-queue-type": "quorum"})


def retry_queues(settings: Settings) -> list[RabbitQueue]:
    return [
        RabbitQueue(
            f"payments.retry.{attempt}",
            durable=True,
            arguments={
                **reliable_arguments(EXCHANGE.name, NEW_QUEUE.name),
                "x-message-ttl": int(
                    settings.retry_base_delay * settings.retry_multiplier ** (attempt - 1) * 1000
                ),
            },
        )
        for attempt in range(1, settings.message_max_attempts)
    ]


async def declare_topology(broker: RabbitBroker, settings: Settings):
    exchange = await broker.declare_exchange(EXCHANGE)
    dead = await broker.declare_exchange(DLX)
    queue = await broker.declare_queue(DLQ)
    await queue.bind(dead, routing_key=DLQ.name)
    queue = await broker.declare_queue(NEW_QUEUE)
    await queue.bind(exchange, routing_key=NEW_QUEUE.name)
    for spec in retry_queues(settings):
        queue = await broker.declare_queue(spec)
        await queue.bind(exchange, routing_key=spec.name)
