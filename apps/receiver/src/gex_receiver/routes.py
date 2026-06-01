import json
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse

from gex_common.config import (
    EVENT_ORDER_APPROVED,
    GATEWAY_GRUMMER,
    PAYMENT_APPROVED,
    QUEUE_DLQ_DECRYPT_FAILED,
    QUEUE_DLQ_SCHEMA_FAILED,
    STATUS_DECRYPT_FAILED,
    STATUS_DISCARDED_NON_APPROVED,
    STATUS_DUPLICATE,
    STATUS_PROCESSED,
    STATUS_SCHEMA_FAILED,
    VALID_GATEWAYS,
)
from gex_common.crypto import DecryptionError, decrypt_grummer
from gex_common.models import (
    DLQMessage,
    GrummerEncryptedBody,
    LeadReceivedMessage,
    ProcessingResult,
)
from gex_common.validation import validate_schema
from gex_receiver.db import insert_raw_payload
from gex_receiver.dependencies import (
    CorrelationId,
    DbDep,
    PublisherDep,
    SettingsDep,
)
from gex_receiver.idempotency import check_idempotency

logger = structlog.get_logger()

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.post(
    "/{gateway}",
    response_model=ProcessingResult,
    status_code=status.HTTP_200_OK,
    responses={
        200: {"model": ProcessingResult, "description": "Accepted, duplicate, or discarded"},
        202: {"model": ProcessingResult, "description": "Decrypt or schema failure (acknowledged)"},
        422: {"description": "Invalid gateway"},
        503: {"description": "Service temporarily unavailable"},
    },
)
async def receive_webhook(
    gateway: str,
    request: Request,
    session: DbDep,
    publisher: PublisherDep,
    settings: SettingsDep,
    correlation_id: CorrelationId,
) -> ProcessingResult | JSONResponse:
    """Process incoming webhooks from grummer (encrypted) or lous (plaintext) gateways."""
    received_at = datetime.now(timezone.utc)

    # 1. Validate gateway
    if gateway not in VALID_GATEWAYS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid gateway: {gateway}",
        )

    # 2-3. Read body + headers
    body_bytes = await request.body()
    headers = dict(request.headers)

    try:
        raw_body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid JSON: {e}",
        ) from e

    body_decrypted: dict | None = None
    body_original: dict = raw_body

    try:
        # 4. Grummer + encrypted → decrypt
        if gateway == GATEWAY_GRUMMER and headers.get("x-gr-encrypted", "").lower() == "true":
            try:
                encrypted = GrummerEncryptedBody(**raw_body)
                plaintext = decrypt_grummer(
                    encrypted.iv, encrypted.ciphertext, settings.grummer_secret_hex
                )
                body_decrypted = json.loads(plaintext)
            except (DecryptionError, json.JSONDecodeError, TypeError, ValueError) as e:
                # Persist decrypt_failed raw payload
                await insert_raw_payload(
                    session,
                    gateway,
                    received_at,
                    headers,
                    body_original,
                    None,
                    STATUS_DECRYPT_FAILED,
                    str(e),
                    correlation_id,
                )
                await publisher.publish_dlq(
                    DLQMessage(
                        original_payload=body_original,
                        error_reason=str(e),
                        gateway=gateway,
                        correlation_id=correlation_id,
                        queue_origin=QUEUE_DLQ_DECRYPT_FAILED,
                    ),
                    QUEUE_DLQ_DECRYPT_FAILED,
                )
                return JSONResponse(
                    status_code=status.HTTP_202_ACCEPTED,
                    content=ProcessingResult(
                        status="decrypt_failed",
                        correlation_id=correlation_id,
                        error_detail=str(e),
                    ).model_dump(),
                )

        # 5. Lous or grummer-as-plaintext
        if body_decrypted is None:
            body_decrypted = raw_body

        # 6-7. Validate schema
        validation_result = validate_schema(body_decrypted)
        if not validation_result.is_valid:
            errors_str = "; ".join(f"{e.field}: {e.message}" for e in validation_result.errors)
            await insert_raw_payload(
                session,
                gateway,
                received_at,
                headers,
                body_original,
                body_decrypted,
                STATUS_SCHEMA_FAILED,
                errors_str,
                correlation_id,
            )
            await publisher.publish_dlq(
                DLQMessage(
                    original_payload=body_decrypted,
                    error_reason=errors_str,
                    gateway=gateway,
                    correlation_id=correlation_id,
                    queue_origin=QUEUE_DLQ_SCHEMA_FAILED,
                ),
                QUEUE_DLQ_SCHEMA_FAILED,
            )
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content=ProcessingResult(
                    status="schema_failed",
                    correlation_id=correlation_id,
                    error_detail=errors_str,
                ).model_dump(),
            )

        payload = validation_result.payload
        if payload is None:
            raise RuntimeError("Validation succeeded but payload is None")

        # 8. Idempotency check
        is_new = await check_idempotency(
            session,
            gateway,
            payload.transaction_id,
            payload.event,
            correlation_id,
        )

        if not is_new:
            await insert_raw_payload(
                session,
                gateway,
                received_at,
                headers,
                body_original,
                body_decrypted,
                STATUS_DUPLICATE,
                None,
                correlation_id,
            )
            return ProcessingResult(
                status="duplicate",
                correlation_id=correlation_id,
            )

        # 9. Route
        if payload.event == EVENT_ORDER_APPROVED and payload.payment.status == PAYMENT_APPROVED:
            await insert_raw_payload(
                session,
                gateway,
                received_at,
                headers,
                body_original,
                body_decrypted,
                STATUS_PROCESSED,
                None,
                correlation_id,
            )
            await publisher.publish_lead_received(
                LeadReceivedMessage(
                    transaction_id=payload.transaction_id,
                    transaction_time=payload.transaction_time,
                    event=payload.event,
                    customer=payload.customer,
                    product=payload.product,
                    payment=payload.payment,
                    gateway=gateway,
                    correlation_id=correlation_id,
                )
            )
            return ProcessingResult(
                status="accepted",
                correlation_id=correlation_id,
            )

        # Non-approved → discarded
        await insert_raw_payload(
            session,
            gateway,
            received_at,
            headers,
            body_original,
            body_decrypted,
            STATUS_DISCARDED_NON_APPROVED,
            None,
            correlation_id,
        )
        return ProcessingResult(
            status="discarded",
            correlation_id=correlation_id,
        )

    except HTTPException:
        raise
    except Exception as e:
        # 503: DB or RMQ unavailable
        logger.error("webhook_processing_failed", error=str(e))
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": "Service temporarily unavailable"},
        )
