"""Payments and transactional outbox."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column(
            "currency",
            sa.Enum(
                "RUB", "USD", "EUR", name="currency", native_enum=False, create_constraint=True
            ),
            nullable=False,
        ),
        sa.Column("description", sa.String(1000), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=True),
        sa.Column(
            "status",
            sa.Enum(
                "pending",
                "succeeded",
                "failed",
                name="status",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("idempotency_key", sa.String(255), nullable=False, unique=True),
        sa.Column("request_body", postgresql.JSONB(), nullable=False),
        sa.Column("webhook_url", sa.String(2083), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "webhook_status",
            sa.Enum(
                "pending",
                "delivered",
                "failed",
                name="webhookstatus",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("webhook_attempts", sa.Integer(), nullable=False),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_last_error", sa.String(100), nullable=True),
        sa.CheckConstraint("amount > 0", name="positive_amount"),
        sa.CheckConstraint("webhook_attempts BETWEEN 0 AND 3", name="webhook_attempt_limit"),
        sa.CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status <> 'pending' AND processed_at IS NOT NULL)",
            name="processed_state",
        ),
    )
    op.create_index("ix_payments_status_created_at", "payments", ["status", "created_at"])
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("event_type", sa.String(40), nullable=False),
        sa.Column(
            "payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False, unique=True
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publication_attempts", sa.Integer(), nullable=False),
        sa.Column("processing_attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.String(100), nullable=True),
        sa.CheckConstraint("processing_attempts BETWEEN 0 AND 3", name="processing_attempt_limit"),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade():
    op.drop_table("outbox")
    op.drop_table("payments")
