import json
import uuid
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse

from gex_common.config import CONSTANTS
from gex_common.crypto import DecryptionError, decrypt_grummer
from gex_common.logging import anonymize_customer_id
from gex_common.models import (
    DLQMessage,
    GrummerEncryptedBody,
    LeadReceivedMessage,
    ProcessingResult,
)
from gex_common.validation import validate_schema
from gex_receiver.db import check_idempotency, insert_raw_payload
from gex_receiver.dependencies import (
    CorrelationId,
    DbDep,
    PublisherDep,
    SettingsDep,
)

logger = structlog.get_logger()

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


async def _fail_and_return(
    session,
    publisher,
    gateway,
    received_at,
    headers,
    body_original,
    body_decrypted,
    result_status,
    error_reason,
    dlq_payload,
    dlq_queue,
    correlation_id,
) -> JSONResponse:
    """Persist a failed raw payload, publish to DLQ, return 202 JSONResponse."""
    await insert_raw_payload(
        session,
        gateway,
        received_at,
        headers,
        body_original,
        body_decrypted,
        result_status,
        error_reason,
        correlation_id,
    )
    await publisher.publish_dlq(
        DLQMessage(
            original_payload=dlq_payload,
            error_reason=error_reason,
            gateway=gateway,
            correlation_id=correlation_id,
            queue_origin=dlq_queue,
        ),
        dlq_queue,
    )
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=ProcessingResult(
            status=result_status,
            correlation_id=correlation_id,
            error_detail=error_reason,
        ).model_dump(),
    )


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
    if gateway not in CONSTANTS.valid_gateways:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
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
        if (
            gateway == CONSTANTS.gateway_grummer
            and headers.get("x-gr-encrypted", "").lower() == "true"
        ):
            try:
                encrypted = GrummerEncryptedBody(**raw_body)
                plaintext = decrypt_grummer(
                    encrypted.iv, encrypted.ciphertext, settings.grummer_secret_hex
                )
                body_decrypted = json.loads(plaintext)
            except (DecryptionError, json.JSONDecodeError, TypeError, ValueError) as e:
                return await _fail_and_return(
                    session,
                    publisher,
                    gateway,
                    received_at,
                    headers,
                    body_original,
                    None,
                    CONSTANTS.status_decrypt_failed,
                    str(e),
                    body_original,
                    CONSTANTS.queue_dlq_decrypt_failed,
                    correlation_id,
                )

        # 5. Lous or grummer-as-plaintext
        if body_decrypted is None:
            body_decrypted = raw_body

        # 6-7. Validate schema
        validation_result = validate_schema(body_decrypted)
        if not validation_result.is_valid:
            errors_str = "; ".join(f"{e.field}: {e.message}" for e in validation_result.errors)
            return await _fail_and_return(
                session,
                publisher,
                gateway,
                received_at,
                headers,
                body_original,
                body_decrypted,
                CONSTANTS.status_schema_failed,
                errors_str,
                body_decrypted,
                CONSTANTS.queue_dlq_schema_failed,
                correlation_id,
            )

        payload = validation_result.payload
        if payload is None:
            raise RuntimeError("Validation succeeded but payload is None")

        structlog.contextvars.bind_contextvars(
            gateway=gateway,
            transaction_id=payload.transaction_id,
            event=payload.event,
            customer_id=anonymize_customer_id(payload.customer.email),
        )

        # 8. Idempotency check (natural key: transaction_id + event)
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
                CONSTANTS.status_duplicate,
                None,
                correlation_id,
            )
            return ProcessingResult(
                status=CONSTANTS.status_duplicate,
                correlation_id=correlation_id,
            )

        # 9. Route
        if (
            payload.event == CONSTANTS.event_order_approved
            and payload.payment.status == CONSTANTS.payment_approved
        ):
            await insert_raw_payload(
                session,
                gateway,
                received_at,
                headers,
                body_original,
                body_decrypted,
                CONSTANTS.status_accepted,
                None,
                correlation_id,
            )
            await publisher.publish_lead_received(
                LeadReceivedMessage(
                    event_id=str(uuid.uuid7()),
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
                status=CONSTANTS.status_accepted,
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
            CONSTANTS.status_discarded_non_approved,
            None,
            correlation_id,
        )
        return ProcessingResult(
            status=CONSTANTS.status_discarded_non_approved,
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
