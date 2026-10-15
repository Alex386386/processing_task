from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


class CRUDBase:
    def __init__(self, model: Any):
        self.model = model

    async def get(self, session: AsyncSession, obj_id: Any):
        return await session.scalar(select(self.model).where(self.model.id == obj_id))

    async def get_multi(self, session: AsyncSession):
        result = await session.scalars(select(self.model))
        return list(result.all())

    def add(self, session: AsyncSession, db_obj: Any) -> None:
        session.add(db_obj)
