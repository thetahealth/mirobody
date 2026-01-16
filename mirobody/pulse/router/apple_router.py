"""
Apple Health Platform Routes
"""

import logging
import gzip
import json
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import JSONResponse

from ..apple.models import AppleHealthRequest
from ..manager import platform_manager
from ...utils.utils_auth import verify_token

# Create router
router = APIRouter(prefix="/apple", tags=["apple_health"])
old_router = APIRouter(prefix="/api/v1/health", tags=["apple_health"])


async def _process_request_data(
        request: Request,
        content_encoding: Optional[str] = Header(None, alias="content-encoding"),
        content_type: Optional[str] = Header(None, alias="content-type"),
) -> Dict[str, Any]:
    """
    Process request data, supports gzip compression

    Args:
        request: FastAPI request object
        content_encoding: Content encoding type
        content_type: Content type

    Returns:
        Dict[str, Any]: Parsed JSON data

    Raises:
        ValueError: Raised when data parsing fails
    """
    try:
        # Read raw request body
        raw_body = await request.body()
        
        logging.info(f"Raw body size: {len(raw_body)} bytes, content_encoding: {content_encoding}, content_type: {content_type}")

        # If gzip compressed, decompress first
        if content_encoding and content_encoding.lower() == "gzip":
            try:
                decompressed_body = gzip.decompress(raw_body)
            except Exception as e:
                raise ValueError(f"Failed to decompress gzip data: {str(e)}")
        else:
            decompressed_body = raw_body

        # Parse JSON
        if content_type and "application/json" in content_type.lower():
            try:
                data = json.loads(decompressed_body.decode("utf-8"))
            except Exception as e:
                raise ValueError(f"Failed to parse JSON data: {str(e)}")
        else:
            # Try to parse directly as JSON (backward compatible)
            try:
                data = json.loads(decompressed_body.decode("utf-8"))
            except Exception as e:
                raise ValueError(f"Failed to parse data as JSON: {str(e)}")

        return data

    except Exception as e:
        logging.error(f"Failed to process request data: {str(e)}", stack_info=True)
        raise


@old_router.post("/apple-health")
async def old_process_apple_health_data(
        request: Request,
        current_user: str = Depends(verify_token),
        content_encoding: Optional[str] = Header(None, alias="content-encoding"),
        content_type: Optional[str] = Header(None, alias="content-type"),
) -> JSONResponse:
    return await process_apple_health_data(request, current_user, content_encoding, content_type)

@router.post("/health")
async def new_process_apple_health_data(
        request: Request,
        current_user: str = Depends(verify_token),
        content_encoding: Optional[str] = Header(None, alias="content-encoding"),
        content_type: Optional[str] = Header(None, alias="content-type"),
) -> JSONResponse:
    return await process_apple_health_data(request, current_user, content_encoding, content_type)

async def process_apple_health_data(
        request: Request,
        current_user: str = Depends(verify_token),
        content_encoding: Optional[str] = Header(None, alias="content-encoding"),
        content_type: Optional[str] = Header(None, alias="content-type"),
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
                "type": "HEART_RATE",
                "dateFrom": 1705284600000,
                "dateTo": 1705284600000,
                "value": {"numericValue": 72},
                "unitSymbol": "bpm",
                ...
            }
        ]
    }
    """
    try:
        # Process request data (supports gzip compression)
        t1 = time.time()
        raw_data = await _process_request_data(request, content_encoding, content_type)
        t2 = time.time()

        # Validate data using Pydantic model
        try:
            validated_data = AppleHealthRequest(**raw_data)
        except Exception as e:
            logging.error(f"Data validation failed: {str(e)}")
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"success": False, "message": f"Invalid request data: {str(e)}"},
            )

        # Add debug log
        logging.info(f"Validated data: request_id={validated_data.request_id}, "
            f"timezone={validated_data.metaInfo.timezone}, "
            f"cost_time={(t2 - t1) * 1e3}, "
            f"healthData_count={len(validated_data.healthData)}")

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

        t3 = time.time()

        # Add result log
        logging.info(f"Processing result: success={success}, taskId={validated_data.metaInfo.taskId}, cost_time={(t3 - t2) * 1e3}")

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
        else:
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
        # Data parsing error
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "success": False,
                "code": 1,
                "message": f"Request data parsing failed: {str(e)}",
                "msg": f"Request data parsing failed: {str(e)}"
            },
        )
    except Exception as e:
        logging.error(f"Service error occurred: {str(e)}", stack_info=True)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": False,
                "code": 1,
                "message": f"Processing failed: {str(e)}",
                "msg": f"Processing failed: {str(e)}"
            }
        )


@router.post("/cda")
async def process_apple_cda_data(
        request: Request,
        current_user: str = Depends(verify_token),
        content_encoding: Optional[str] = Header(None, alias="content-encoding"),
        content_type: Optional[str] = Header(None, alias="content-type"),
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
        data = await _process_request_data(request, content_encoding, content_type)

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
        else:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={"success": False, "message": "Apple CDA data processing failed"},
            )

    except ValueError as e:
        # Data parsing error
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"success": False, "message": f"Request data parsing failed: {str(e)}"},
        )
    except Exception as e:
        logging.error(f"Service error occurred: {str(e)}", stack_info=True)

        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"success": False, "message": f"Processing failed: {str(e)}"}
        )
