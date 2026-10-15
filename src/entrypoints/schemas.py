from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from domain.models import Currency, PaymentRequest, Status, WebhookStatus


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict | None = None
    webhook_url: HttpUrl = Field(max_length=2083)

    def to_command(self) -> PaymentRequest:
        return PaymentRequest(
            amount=self.amount,
            currency=self.currency,
            description=self.description,
            metadata=self.model_dump(mode="json")["metadata"],
            webhook_url=str(self.webhook_url),
        )

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        return value.quantize(Decimal("0.01"))

    @field_validator("webhook_url")
    @classmethod
    def no_credentials(cls, value: HttpUrl) -> HttpUrl:
        if value.username or value.password or value.fragment:
            raise ValueError("Webhook URL must not contain credentials or a fragment")
        return value


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Status
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict | None
    processed_at: datetime | None
    webhook_status: WebhookStatus
    webhook_attempts: int
    webhook_delivered_at: datetime | None
