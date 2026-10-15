import asyncio

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.postgres.models import Base
from settings import Settings


def migrate(connection):
    context.configure(connection=connection, target_metadata=Base.metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def online():
    engine = create_async_engine(Settings().database_url.get_secret_value())
    async with engine.connect() as connection:
        await connection.exec_driver_sql("SELECT pg_advisory_lock(682930441)")
        await connection.commit()
        try:
            await connection.run_sync(migrate)
        finally:
            await connection.exec_driver_sql("SELECT pg_advisory_unlock(682930441)")
            await connection.commit()
    await engine.dispose()


if context.is_offline_mode():
    context.configure(
        url=Settings().database_url.get_secret_value(),
        target_metadata=Base.metadata,
        literal_binds=True,
    )
    with context.begin_transaction():
        context.run_migrations()
elif context.config.attributes.get("connection") is not None:
    migrate(context.config.attributes["connection"])
else:
    asyncio.run(online())
