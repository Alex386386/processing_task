from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    payment_id: UUID
    event_type: Literal["payment.created.v1"] = "payment.created.v1"
    attempt: int = Field(default=1, ge=1, le=3)
