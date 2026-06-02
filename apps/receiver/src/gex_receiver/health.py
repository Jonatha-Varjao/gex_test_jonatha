from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy import text

from gex_common.logging import get_app_logger

logger = get_app_logger()
router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict:
    """Liveness: is the process up?"""
    return {"status": "ok"}


@router.get("/health/ready")
async def health_ready(request: Request) -> JSONResponse:
    """Readiness: are dependencies (DB, RMQ) reachable?"""
    db = request.app.state.db
    publisher = request.app.state.rmq_publisher

    checks = {"db": "ok", "rmq": "ok"}
    overall_ok = True

    try:
        async with db.session_factory() as session:
            await session.execute(text("SELECT 1"))
    except Exception as e:
        checks["db"] = f"error: {e.__class__.__name__}"
        overall_ok = False

    try:
        if publisher._connection is None or publisher._connection.is_closed:
            checks["rmq"] = "error: connection closed"
            overall_ok = False
    except Exception as e:
        checks["rmq"] = f"error: {e.__class__.__name__}"
        overall_ok = False

    body = {"status": "ok" if overall_ok else "degraded", **checks}
    return JSONResponse(
        status_code=status.HTTP_200_OK if overall_ok else status.HTTP_503_SERVICE_UNAVAILABLE,
        content=body,
    )
