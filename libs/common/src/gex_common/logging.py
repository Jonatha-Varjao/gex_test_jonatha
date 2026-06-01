import hashlib
import logging

import structlog


def setup_logging(level: str = "INFO") -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(format="%(message)s", stream=None)
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))


def get_logger(
    correlation_id: str | None = None,
    gateway: str | None = None,
    event: str | None = None,
) -> structlog.BoundLogger:
    logger = structlog.get_logger()
    bindings: dict[str, str] = {}
    if correlation_id:
        bindings["correlation_id"] = correlation_id
    if gateway:
        bindings["gateway"] = gateway
    if event:
        bindings["event"] = event
    if bindings:
        logger = logger.bind(**bindings)
    return logger


def anonymize_customer_id(email: str) -> str:
    return hashlib.sha256(email.encode()).hexdigest()[:8]
