import hashlib
import logging

import structlog

APP_LOGGER_NAME = "gex_receiver.app"
ACCESS_LOGGER_NAME = "gex_receiver.access"


def drop_color_message_key(_, __, event_dict):
    """Uvicorn logs the message twice in `color_message`; drop it."""
    event_dict.pop("color_message", None)
    return event_dict


def setup_logging(level: str = "INFO") -> None:
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.stdlib.ExtraAdder(),
        drop_color_message_key,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level.upper())

    for name in ("uvicorn", "uvicorn.error"):
        lgr = logging.getLogger(name)
        lgr.handlers.clear()
        lgr.propagate = True

    access = logging.getLogger("uvicorn.access")
    access.handlers.clear()
    access.propagate = False


def get_app_logger() -> structlog.stdlib.BoundLogger:
    return structlog.stdlib.get_logger(APP_LOGGER_NAME)


def get_access_logger() -> structlog.stdlib.BoundLogger:
    return structlog.stdlib.get_logger(ACCESS_LOGGER_NAME)


def anonymize_customer_id(email: str) -> str:
    return hashlib.sha256(email.encode()).hexdigest()[:8]
