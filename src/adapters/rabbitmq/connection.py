from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel

from settings import Settings


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url.get_secret_value(),
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        timeout=settings.broker_timeout,
        graceful_timeout=30,
        logger=None,
        decoder=lambda message: message.body,
    )
