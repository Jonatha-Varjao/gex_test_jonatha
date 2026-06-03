from pydantic import BaseModel, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    database_url: str = "mysql+asyncmy://gex:gex@localhost:3306/gex"
    database_pool_size: int = 10
    database_max_overflow: int = 20
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    grummer_secret_hex: str = ""
    webhook_site_url: str = ""
    sms_failure_rate: float = 0.1
    distributor_max_retries: int = 3
    log_level: str = "INFO"
    environment: str = "development"
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


class GexConstants(BaseModel):
    # Queues
    queue_lead_received: str = "lead.received"
    queue_dlq_decrypt_failed: str = "lead.dead.decrypt_failed"
    queue_dlq_schema_failed: str = "lead.dead.schema_failed"
    queue_dlq_consumer_failed: str = "lead.dead.consumer_failed"
    queue_dist_sms: str = "dist.sms"
    queue_dist_email: str = "dist.email"
    queue_dist_callcenter: str = "dist.callcenter"
    queue_dist_whatsapp: str = "dist.whatsapp"
    queue_dist_dlq_sms: str = "dist.dead.sms"

    # Gateways
    gateway_grummer: str = "grummer"
    gateway_lous: str = "lous"

    # Events
    event_order_approved: str = "order.approved"
    event_order_refunded: str = "order.refunded"
    event_order_declined: str = "order.declined"
    event_order_pending: str = "order.pending"

    # Payment
    payment_approved: str = "approved"
    payment_refunded: str = "refunded"

    # Channels
    channel_sms: str = "SMS"
    channel_email: str = "EMAIL"
    channel_call_center: str = "CALL_CENTER"
    channel_whatsapp: str = "WHATSAPP"

    # Lead status
    status_accepted: str = "accepted"
    status_decrypt_failed: str = "decrypt_failed"
    status_schema_failed: str = "schema_failed"
    status_duplicate: str = "duplicate"
    status_discarded_non_approved: str = "discarded_non_approved"

    # Distribution status + retry
    dist_status_delivered: str = "delivered"
    dist_status_failed: str = "failed"
    retry_backoffs_ms: list[int] = [1000, 4000, 16000]

    @computed_field
    @property
    def valid_gateways(self) -> set[str]:
        return {self.gateway_grummer, self.gateway_lous}

    @computed_field
    @property
    def all_channels(self) -> list[str]:
        return [
            self.channel_sms, self.channel_email,
            self.channel_call_center, self.channel_whatsapp,
        ]


APP_SETTINGS = AppSettings()
CONSTANTS = GexConstants()
