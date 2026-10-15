import asyncio
import logging

import httpx
from faststream import FastStream
from faststream.rabbit import RabbitBroker

from adapters.postgres.client import create_database
from adapters.rabbitmq.connection import create_broker
from adapters.rabbitmq.consumer import PaymentConsumer
from adapters.rabbitmq.topology import declare_topology
from entrypoints.logging import configure_logging
from services.processing import Processor
from settings import Settings

logger = logging.getLogger(__name__)


def create_worker(settings: Settings, sessions, client: httpx.AsyncClient) -> RabbitBroker:
    broker = create_broker(settings)
    processor = Processor(settings, client)

    async def handle(event):
        async with sessions() as session:
            await processor.process(event, session)

    PaymentConsumer(broker, settings, handle).register()
    return broker


async def run() -> None:
    settings = Settings()
    engine, sessions = create_database(settings)
    try:
        async with httpx.AsyncClient(
            timeout=settings.webhook_timeout,
            follow_redirects=False,
            trust_env=False,
        ) as client:
            broker = create_worker(settings, sessions, client)
            app = FastStream(broker, logger=None)

            @app.on_startup
            async def setup() -> None:
                await broker.connect()
                await declare_topology(broker, settings)

            @app.after_startup
            async def ready() -> None:
                logger.info("consumer_ready")

            await app.run()
    finally:
        await engine.dispose()


def main() -> None:
    configure_logging()
    asyncio.run(run())


if __name__ == "__main__":
    main()
