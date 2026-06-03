def get_dlq_correlation_id() -> str:
    try:
        from faststream import Context

        return Context().get("correlation_id", "no-correlation-id")
    except Exception:
        return "no-correlation-id"
