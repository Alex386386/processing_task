from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: SecretStr
    rabbitmq_url: SecretStr
    api_key: SecretStr = Field(min_length=1)
    outbox_poll_interval: float = Field(default=1, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    payment_processing_min_delay: float = Field(default=2, ge=0)
    payment_processing_max_delay: float = Field(default=5, ge=0)
    payment_success_rate: float = Field(default=0.9, ge=0, le=1)
    message_max_attempts: int = Field(default=3, ge=1, le=3)
    retry_base_delay: float = Field(default=1, ge=0.001)
    retry_multiplier: float = Field(default=2, ge=1)
    webhook_timeout: float = Field(default=10, gt=0)
    broker_timeout: float = Field(default=5, gt=0)

    @model_validator(mode="after")
    def validate_delays(self):
        if self.payment_processing_max_delay < self.payment_processing_min_delay:
            raise ValueError("Maximum processing delay must be >= minimum")
        if not self.database_url.get_secret_value().startswith("postgresql+asyncpg://"):
            raise ValueError("DATABASE_URL must use postgresql+asyncpg")
        return self
