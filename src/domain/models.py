from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


class Status(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class WebhookStatus(StrEnum):
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


@dataclass(frozen=True)
class PaymentRequest:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict | None
    webhook_url: str

    def normalized(self) -> dict:
        return {
            "amount": format(self.amount, ".2f"),
            "currency": self.currency.value,
            "description": self.description,
            "metadata": self.metadata,
            "webhook_url": self.webhook_url,
        }
