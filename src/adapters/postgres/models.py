from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from domain.models import Currency, Status, WebhookStatus


def utcnow() -> datetime:
    return datetime.now(UTC)


def enum_type(cls):
    return Enum(
        cls,
        values_callable=lambda e: [x.value for x in e],
        native_enum=False,
        create_constraint=True,
    )


class Base(DeclarativeBase):
    pass


json_type = JSON().with_variant(JSONB(), "postgresql")


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="positive_amount"),
        CheckConstraint("webhook_attempts BETWEEN 0 AND 3", name="webhook_attempt_limit"),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status <> 'pending' AND processed_at IS NOT NULL)",
            name="processed_state",
        ),
        Index("ix_payments_status_created_at", "status", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(enum_type(Currency))
    description: Mapped[str] = mapped_column(String(1000))
    metadata_: Mapped[dict | None] = mapped_column("metadata", json_type, nullable=True)
    status: Mapped[Status] = mapped_column(enum_type(Status), default=Status.PENDING)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    request_body: Mapped[dict] = mapped_column(json_type)
    webhook_url: Mapped[str] = mapped_column(String(2083))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_status: Mapped[WebhookStatus] = mapped_column(
        enum_type(WebhookStatus),
        default=WebhookStatus.PENDING,
    )
    webhook_attempts: Mapped[int] = mapped_column(Integer, default=0)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_last_error: Mapped[str | None] = mapped_column(String(100))


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        CheckConstraint("processing_attempts BETWEEN 0 AND 3", name="processing_attempt_limit"),
        Index("ix_outbox_unpublished", "created_at", postgresql_where=text("published_at IS NULL")),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    event_type: Mapped[str] = mapped_column(String(40), default="payment.created.v1")
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publication_attempts: Mapped[int] = mapped_column(Integer, default=0)
    processing_attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(String(100))
