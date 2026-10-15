from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from adapters.postgres.crud_base import CRUDBase
from adapters.postgres.models import Payment


class PaymentCRUD(CRUDBase):
    def __init__(self):
        super().__init__(Payment)

    async def get_by_idempotency_key(self, session: AsyncSession, key: str) -> Payment | None:
        return await session.scalar(select(Payment).where(Payment.idempotency_key == key))

    async def get(
        self, session: AsyncSession, payment_id: UUID, *, for_update: bool = False
    ) -> Payment | None:
        if for_update:
            return await session.scalar(
                select(Payment).where(Payment.id == payment_id).with_for_update()
            )
        return await super().get(session, payment_id)


payment_crud = PaymentCRUD()
