from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from adapters.postgres.crud_base import CRUDBase
from adapters.postgres.models import Outbox


class OutboxCRUD(CRUDBase):
    def __init__(self):
        super().__init__(Outbox)

    async def get(
        self, session: AsyncSession, event_id: UUID, *, for_update: bool = False
    ) -> Outbox | None:
        if for_update:
            return await session.scalar(
                select(Outbox).where(Outbox.id == event_id).with_for_update()
            )
        return await super().get(session, event_id)

    async def get_unpublished_for_update(self, session: AsyncSession, limit: int) -> list[Outbox]:
        result = await session.scalars(
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(result.all())


outbox_crud = OutboxCRUD()
