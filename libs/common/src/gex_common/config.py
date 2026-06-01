from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    database_url: str = "mysql+asyncmy://gex:gex@localhost:3306/gex"
    database_pool_size: int = 10
    database_max_overflow: int = 20
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    grummer_secret_hex: str = ""
    webhook_site_url: str = ""
    sms_failure_rate: float = 0.1
    log_level: str = "INFO"
    environment: str = "development"
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


# Module-level constants (queues, gateways, events, statuses, etc.)
QUEUE_LEAD_RECEIVED = "lead.received"
QUEUE_DLQ_DECRYPT_FAILED = "lead.dead.decrypt_failed"
QUEUE_DLQ_SCHEMA_FAILED = "lead.dead.schema_failed"
QUEUE_DLQ_CONSUMER_FAILED = "lead.dead.consumer_failed"
QUEUE_DIST_SMS = "dist.sms"
QUEUE_DIST_EMAIL = "dist.email"
QUEUE_DIST_CALLCENTER = "dist.callcenter"
QUEUE_DIST_WHATSAPP = "dist.whatsapp"
QUEUE_DIST_DLQ_SMS = "dist.dead.sms"

GATEWAY_GRUMMER = "grummer"
GATEWAY_LOUS = "lous"
VALID_GATEWAYS = {GATEWAY_GRUMMER, GATEWAY_LOUS}

EVENT_ORDER_APPROVED = "order.approved"
EVENT_ORDER_REFUNDED = "order.refunded"
EVENT_ORDER_DECLINED = "order.declined"
EVENT_ORDER_PENDING = "order.pending"

PAYMENT_APPROVED = "approved"
PAYMENT_DECLINED = "declined"
PAYMENT_REFUNDED = "refunded"
PAYMENT_PENDING = "pending"

CHANNEL_SMS = "SMS"
CHANNEL_EMAIL = "EMAIL"
CHANNEL_CALL_CENTER = "CALL_CENTER"
CHANNEL_WHATSAPP = "WHATSAPP"
ALL_CHANNELS = [CHANNEL_SMS, CHANNEL_EMAIL, CHANNEL_CALL_CENTER, CHANNEL_WHATSAPP]

STATUS_PROCESSED = "processed"
STATUS_DECRYPT_FAILED = "decrypt_failed"
STATUS_SCHEMA_FAILED = "schema_failed"
STATUS_DUPLICATE = "duplicate"
STATUS_DISCARDED_NON_APPROVED = "discarded_non_approved"

MAX_RETRIES = 3
RETRY_BACKOFFS_MS = [1000, 4000, 16000]
DIST_STATUS_PENDING = "pending"
DIST_STATUS_DELIVERED = "delivered"
DIST_STATUS_FAILED = "failed"
