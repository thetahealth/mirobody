"""The Apple Health ingest routes: what a phone app posts, signed in.

Their answers are not `server/envelope.py`'s: `/health` and `/cda` carry
`success` and `message`, the shape the mobile client reads, which this
repository cannot rebuild (envelope.py lists the exceptions).
"""

import logging
import json
import time
import zlib
from typing import Any

from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from mirobody.collect import AppleHealthRequest, AppleHealthStatisticsRequest
from mirobody.collect import process_apple_health_statistics
from mirobody.collect import platform_manager
from mirobody.kernel.ops import is_driver_exception
from mirobody.server.auth import verify_token

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/apple", tags=["apple_health"])

#: The most a gzip body may inflate to. Gzip reaches ~1000:1, so a 100 MB bomb
#: inflates to ~100 GB; a `decompressobj` with `max_length` never holds more
#: than this. 512 MB is far above any real export batch (the client chunks
#: uploads) and far below harm.
_MAX_DECOMPRESSED = 512 * 1024 * 1024


async def _process_request_data(request: Request, content_encoding: str | None) -> dict[str, Any]:
    """The JSON body, inflated first when it arrives gzip-compressed.

    Raises ValueError for a body that is neither; its message never quotes
    the body, as the decoders' own messages do, and the body is readings.
    """
    raw_body = await request.body()
    gzipped = bool(content_encoding) and content_encoding.lower() == "gzip"
    logger.info("Apple upload body: bytes=%d encoding=%s", len(raw_body), "gzip" if gzipped else "identity")

    body = raw_body
    if gzipped:
        decompressor = zlib.decompressobj(wbits=31)  # 31 = gzip container
        try:
            body = decompressor.decompress(raw_body, _MAX_DECOMPRESSED)
        except zlib.error:
            raise ValueError("the body is not valid gzip") from None
        if decompressor.unconsumed_tail:
            raise ValueError("the body inflates past the size limit")

    try:
        return json.loads(body.decode("utf-8"))
    except ValueError:
        # JSONDecodeError and UnicodeDecodeError are both ValueErrors.
        raise ValueError("the body is not JSON") from None


def _what_was_invalid(e: Exception) -> list[str]:
    """Why a body failed its model, without its values: pydantic's message
    quotes the input, and the input is readings."""
    if isinstance(e, ValidationError):
        return [x["type"] for x in e.errors()]
    return [type(e).__name__]


@router.post("/health")
async def process_apple_health_data(
        request: Request,
        current_user: str = Depends(verify_token),
        content_encoding: str | None = Header(None, alias="content-encoding"),
) -> JSONResponse:
    """
    Process Apple Health data

    Supported request formats:
    - Content-Type: application/json
    - Content-Encoding: gzip (optional, supports gzip compression)

    Request body format:
    {
        "request_id": "string",
        "metaInfo": {
            "timezone": "America/Los_Angeles",
            "taskId": "string (optional, for identifying data uploaded in the same batch)"
        },
        "healthData": [
            {
                "uuid": "unique_id",
                "type": "HKQuantityTypeIdentifierHeartRate",
                "startDate": 1705284600000,
                "endDate": 1705284600000,
                "value": 72,
                "unit": "count/min",
                "sourceName": "Apple Watch",
                ...
            }
        ]
    }
    """
    try:
        # Process request data (supports gzip compression)
        t1 = time.time()
        raw_data = await _process_request_data(request, content_encoding)
        t2 = time.time()

        # Validate data using Pydantic model
        try:
            validated_data = AppleHealthRequest(**raw_data)
        except Exception as e:
            logger.warning("Apple Health body refused: errors=%s", _what_was_invalid(e))  # phi: ok error types
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"success": False, "message": "Invalid request data."},
            )

        parse_ms = (t2 - t1) * 1e3
        logger.info("Validated data: request_id=%s timezone=%s parse_ms=%.1f record_count=%d",
                    validated_data.request_id, validated_data.metaInfo.timezone, parse_ms,
                    len(validated_data.healthData))

        # Get Apple Health platform
        apple_platform = platform_manager.get_platform("apple")
        if not apple_platform:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"success": False, "message": "Apple Health platform not initialized"},
            )

        # Construct data format that matches platform interface, pass Pydantic objects directly for better performance
        platform_data = {
            "user_id": current_user,
            "request_id": validated_data.request_id,
            "health_data": validated_data.healthData,  # Pass Pydantic object list directly
            "meta_info": validated_data.metaInfo,  # Pass Pydantic object directly
        }

        # Call platform to process data
        msg_id = f"apple_health_{current_user}_{int(time.time() * 1000)}"
        success = await apple_platform.post_data(provider_slug="apple_health", data=platform_data, msg_id=msg_id)

        process_ms = (time.time() - t2) * 1e3
        task_id = validated_data.metaInfo.taskId
        logger.info("Processing result: success=%s task_id=%s process_ms=%.1f", success, task_id, process_ms)

        if success:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "success": True,
                    "code": 0,
                    "data": {"request_id": validated_data.request_id},
                    "message": "Apple Health data processed successfully",
                    "msg": "Apple Health data processed successfully"
                },
            )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": False, 
                "code": 1,
                "message": "Apple Health data processing failed",
                "msg": "Apple Health data processing failed"
            },
        )

    except ValueError as e:
        logger.warning("Apple Health body unreadable: error_type=%s", type(e).__name__)
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "success": False,
                "code": 1,
                "message": "Request data parsing failed.",
                "msg": "Request data parsing failed."
            },
        )
    except Exception as e:
        logger.error("Apple Health upload failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": False,
                "code": 1,
                "message": "Processing failed.",
                "msg": "Processing failed."
            }
        )


@router.post("/statistics")
async def process_apple_health_statistics_data(
        request: Request,
        current_user: str = Depends(verify_token),
        content_encoding: str | None = Header(None, alias="content-encoding"),
) -> JSONResponse:
    """
    Process Apple Health pre-aggregated statistics data

    Accepts client-computed statistics (sum, average, min, max, mostRecent)
    and writes them directly as day-grained observations.

    Request body format:
    {
        "metaInfo": {
            "timezone": "Asia/Shanghai"
        },
        "statistics": [
            {
                "type": "HKQuantityTypeIdentifierStepCount",
                "dateFrom": 1710691200000,
                "dateTo": 1710777600000,
                "timezone": "Asia/Shanghai",
                "grouping": "day",
                "sum": 8523.0,
                "average": null,
                "minimum": null,
                "maximum": null,
                "mostRecent": null,
                "unit": "COUNT",
                "unitSymbol": "count"
            }
        ]
    }
    """
    try:
        raw_data = await _process_request_data(request, content_encoding)

        try:
            validated_data = AppleHealthStatisticsRequest(**raw_data)
        except Exception as e:
            logger.warning("Apple statistics body refused: errors=%s", _what_was_invalid(e))  # phi: ok error types
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"code": 1, "data": None, "msg": "Invalid request data."},
            )

        logger.info("Statistics request: user_id=%s statistics_count=%d timezone=%s",  # phi: ok an account id
                    current_user, len(validated_data.statistics), validated_data.metaInfo.timezone)

        accepted = await process_apple_health_statistics(validated_data, current_user)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"code": 0, "data": {"accepted": accepted}, "msg": "ok"},
        )

    except ValueError as e:
        logger.warning("Apple statistics body unreadable: error_type=%s", type(e).__name__)
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 1, "data": None, "msg": "Request data parsing failed."},
        )
    except Exception as e:
        logger.error("Apple statistics upload failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"code": 1, "data": None, "msg": "Processing failed."},
        )


@router.post("/cda")
async def process_apple_cda_data(
        request: Request,
        current_user: str = Depends(verify_token),
        content_encoding: str | None = Header(None, alias="content-encoding"),
) -> JSONResponse:
    """
    Process Apple Health CDA (Clinical Document Architecture) data

    Supported request formats:
    - Content-Type: application/json
    - Content-Encoding: gzip (optional, supports gzip compression)

    Request body format:
    {
        "request_id": "string",
        "metaInfo": {
            "userId": "string",
            "timezone": "America/Los_Angeles",
            "taskId": "string (optional, for identifying data uploaded in the same batch)"
        },
        "cdaData": [...]
    }
    """
    try:
        # Process request data (supports gzip compression)
        data = await _process_request_data(request, content_encoding)

        # Get Apple Health platform
        apple_platform = platform_manager.get_platform("apple")
        if not apple_platform:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"success": False, "message": "Apple Health platform not initialized"},
            )

        # Construct data format that matches platform interface
        platform_data = {
            "user_id": current_user,
            "request_id": data.get("request_id"),
            "cda_data": data.get("cdaData", []),
            "meta_info": data.get("metaInfo", {}),
        }

        # Call platform to process data
        msg_id = f"apple_cda_{current_user}_{int(time.time() * 1000)}"
        success = await apple_platform.post_data(provider_slug="cda", data=platform_data, msg_id=msg_id)

        if success:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "success": True,
                    "data": {"request_id": data.get("request_id")},
                    "message": "Apple CDA data processed successfully",
                },
            )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"success": False, "message": "Apple CDA data processing failed"},
        )

    except ValueError as e:
        logger.warning("Apple CDA body unreadable: error_type=%s", type(e).__name__)
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"success": False, "message": "Request data parsing failed."},
        )
    except Exception as e:
        logger.error("Apple CDA upload failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"success": False, "message": "Processing failed."}
        )
