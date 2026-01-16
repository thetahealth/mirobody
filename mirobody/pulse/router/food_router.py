import asyncio
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from mirobody.utils.utils_auth import verify_token
from mirobody.utils import execute_query

from ..file_parser.services.food_recognizer import FoodImageRecognizer

router = APIRouter(prefix="/api/v1/food", tags=["food_analysis"])


# Pydantic models for request/response
class FoodSaveRequest(BaseModel):
    name: str
    category: str
    calories: int
    time: str
    date: str
    image_url: Optional[str] = None
    nutrition: Optional[List[Any]] = None  # Individual food components
    nut: Optional[List[Any]] = None  # Overall nutrition info
    advice: Optional[List[str]] = None  # Health advice
    # Removed analysis_result field as it's not used by frontend


class FoodHistoryResponse(BaseModel):
    success: bool
    data: List[dict]
    message: str


def get_content_type_from_filename(filename: str, original_content_type: str = None) -> tuple[str, bool]:
    """
    Get content_type from filename, supporting only specified image formats

    Args:
        filename: Filename
        original_content_type: Original content_type

    Returns:
        tuple[str, bool]: (content_type, is_valid) - content_type and whether it's a supported format
    """
    # Supported image format mapping (only these 5 formats supported)
    supported_extensions = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".heic": "image/heic",
        ".heif": "image/heif",
    }

    # Supported content_type list
    supported_content_types = {
        "image/png",
        "image/jpeg",
        "image/webp",
        "image/heic",
        "image/heif",
    }

    # If original content_type is a supported image type, use it first
    if original_content_type and original_content_type in supported_content_types:
        return original_content_type, True

    # Determine by file extension
    if filename:
        # Get file extension (convert to lowercase)
        file_ext = "." + filename.lower().split(".")[-1] if "." in filename else ""

        # Find corresponding content_type
        if file_ext in supported_extensions:
            return supported_extensions[file_ext], True

    # Unsupported format
    return "image/jpeg", False


@router.post("/analyze")
async def analyze_food_image(
    file: UploadFile = File(...),
    query: str = Form("Maintain healthy diet"),
    user_id: str = Depends(verify_token),
):
    """
    Analyze food image for nutritional information

    Args:
        file: Uploaded image file
        query: User's health goal or query
        user_id: Authenticated user ID

    Returns:
        StreamingResponse: Real-time analysis results
    """
    # Read file content
    try:
        file_content = await file.read()
        if not file_content:
            raise HTTPException(status_code=400, detail="File is empty")
    except Exception as e:
        logging.error(f"Error reading file {file.filename}: {str(e)}", stack_info=True)
        raise HTTPException(status_code=400, detail=f"Failed to read file: {str(e)}")
    finally:
        await file.close()

    # Get reliable content_type and validate format
    content_type, is_valid_format = get_content_type_from_filename(file.filename, file.content_type)
    logging.info(f"File type detection: original={file.content_type}, detected={content_type}, filename={file.filename}, format_supported={is_valid_format}")

    # If format is not supported, return error
    if not is_valid_format:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file format. Only PNG, JPEG, WEBP, HEIC, HEIF are supported. Your file: {file.filename}",
        )

    async def _stream():
        try:
            contents = []
            sid = str(uuid.uuid4())
            msg_id = f"food_analysis_{int(time.time())}_{user_id}"

            # Send message ID
            yield f"{json.dumps({'type': 'id', 'content': msg_id})}\n"

            # Define thinking messages
            thinking_messages = [
                "🔍 Analyzing your uploaded image... Looking at colors, textures, and food details!",
                "🥗 Identifying different food items... Processing ingredients through nutrition database!",
                "🧮 Calculating nutritional composition... Estimating calories, proteins, carbs, and fats!",
                "💡 Analyzing health impact... Cross-referencing with your health goals!",
                "📋 Generating personalized recommendations... Creating tailored nutrition advice!",
                "✨ Finalizing food analysis... Putting together insights and nutritional breakdown!",
                "🎯 Almost ready! Adding final touches to your nutrition report...",
            ]

            # Start food processing task
            food_image_recognizer = FoodImageRecognizer()

            processing_task = asyncio.create_task(
                food_image_recognizer.food_image_recognize(
                    file_content,
                    file.filename,
                    content_type,
                    user_id,
                    msg_id,
                    query,
                    sid,
                )
            )

            # Output thinking messages while waiting for processing results
            thinking_index = 0
            while not processing_task.done():
                if thinking_index < len(thinking_messages):
                    # Output thinking message
                    yield f"{json.dumps({'type': 'thinking', 'content': thinking_messages[thinking_index]})}\n"
                    thinking_index += 1

                # Wait for a while then check task status
                try:
                    await asyncio.wait_for(asyncio.shield(processing_task), timeout=0.5)
                    break  # Task completed
                except asyncio.TimeoutError:
                    # Task still running, continue outputting thinking messages
                    continue

            # Get processing results
            result = await processing_task

            # Send thinking end
            yield f"{json.dumps({'type': 'thinking', 'content': '🎉 Analysis complete! Now generating your detailed nutrition results...'})}\n"

            # Process results
            if isinstance(result, Exception):
                logging.error(f"File processing error: {str(result)}", stack_info=True)
                yield f"{json.dumps({'type': 'error', 'content': 'File processing failed'})}\n"
            elif not result.get("success", True):
                logging.error(f"File processing returned failure result: {result.get('message', 'Unknown error')}")
                error_content = json.dumps({"type": "error", "content": result.get("message", "Unknown error")})
                yield f"{error_content}\n"
            else:
                content = {
                    "type": "food",
                    "content": json.dumps(result["content"], ensure_ascii=False),
                }
                contents.append(content)
                yield f"{json.dumps(content)}\n"

                logging.info(f"user_id: {user_id}, Food recognition successful")

        except Exception:
            logging.error(f"user_id: {user_id}, Error occurred while processing results", stack_info=True)
            yield f"{json.dumps({'type': 'error', 'content': 'Error occurred while processing results'})}\n"

        # Send end marker
        yield '{"type": "end"}\n'

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/save")
async def save_food_analysis(
    food_data: FoodSaveRequest,
    user_id: str = Depends(verify_token),
):
    """
    Save food analysis result to database

    Args:
        food_data: Food analysis data to save
        user_id: Authenticated user ID

    Returns:
        Save result
    """
    try:
        # Generate unique IDs
        message_id = f"food_{int(time.time())}_{user_id}"
        session_id = f"food_session_{int(time.time())}_{user_id}"

        # Prepare content as JSON - removed only unused analysis_result field
        content_data = {
            "type": "food_analysis",
            "name": food_data.name,
            "category": food_data.category,
            "calories": food_data.calories,
            "time": food_data.time,
            "date": food_data.date,
            "image_url": food_data.image_url,
            "nutrition": food_data.nutrition,  # Individual food components
            "nut": food_data.nut,  # Overall nutrition info
            "advice": food_data.advice,  # Health advice
            # Removed analysis_result as it's not used by frontend
        }

        # Save to th_messages table
        save_sql = """
            INSERT INTO theta_ai.th_messages (
                id, user_id, session_id, role, content, message_type, 
                created_at, agent, provider
            ) VALUES (
                :id, :user_id, :session_id, :role, theta_ai.encrypt_content(:content), :message_type,
                :created_at, :agent, :provider
            )
        """

        await execute_query(
            save_sql,
            {
                "id": message_id,
                "user_id": user_id,
                "session_id": session_id,
                "role": "user",
                "content": json.dumps(content_data, ensure_ascii=False),
                "message_type": "food",  # Changed from food_analysis to food
                "created_at": datetime.now(),
                "agent": "food_analyzer",
                "provider": "food_analysis_service",
            },
        )

        logging.info(f"Food analysis saved for user {user_id}: {food_data.name}")

        return {
            "code": 0,
            "msg": "Food analysis saved successfully",
            "data": {
                "id": message_id
            }
        }

    except Exception as e:
        logging.error(f"Error saving food analysis for user {user_id}: {str(e)}", stack_info=True)
        # raise HTTPException(status_code=500, detail="Failed to save food analysis")
        return {
            "code": -1,
            "msg": f"Error saving food analysis for user {user_id}: {str(e)}"
        }


@router.get("/history")
async def get_food_analysis_history(
    user_id: str = Depends(verify_token),
    date: Optional[str] = None,
    limit: int = 50,
):
    """
    Get user's food analysis history

    Args:
        user_id: Authenticated user ID
        date: Specific date to filter (YYYY-MM-DD format)
        limit: Number of records to return

    Returns:
        List of food analysis records
    """
    try:
        # Build query with optional date filter
        base_sql = """
            SELECT id, theta_ai.decrypt_content(content) AS content, created_at
            FROM theta_ai.th_messages 
            WHERE user_id = :user_id 
                AND message_type = 'food'
                AND role = 'user'
                AND (is_del = false OR is_del IS NULL)
        """

        params = {"user_id": user_id, "limit": limit}

        if date:
            try:
                # Convert string date to datetime.date object
                date_obj = datetime.strptime(date, "%Y-%m-%d").date()
                base_sql += " AND DATE(created_at) = :date"
                params["date"] = date_obj
            except ValueError:
                logging.warning(f"Invalid date format: {date}")
                # Skip date filter if invalid format
                pass

        base_sql += " ORDER BY created_at DESC LIMIT :limit"

        results = await execute_query(base_sql, params)

        # Parse and format results
        food_items = []
        for row in results:
            try:
                content = json.loads(row["content"]) if isinstance(row["content"], str) else row["content"]

                # Extract food data from content
                food_item = {
                    "id": row["id"],
                    "name": content.get("name", "Unknown Food"),
                    "category": content.get("category", "snack"),
                    "calories": content.get("calories", 0),
                    "time": content.get("time", "00:00"),
                    "date": content.get(
                        "date",
                        row["created_at"].strftime("%Y-%m-%d") if row["created_at"] else "",
                    ),
                    "image_url": content.get("image_url"),
                    "nutrition": content.get("nutrition", []),  # Individual food components
                    "nut": content.get("nut", []),  # Overall nutrition info
                    "advice": content.get("advice", []),  # Health advice
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                }

                food_items.append(food_item)

            except Exception as parse_error:
                logging.warning(f"Failed to parse food record {row['id']}: {str(parse_error)}")
                continue

        logging.info(f"Retrieved {len(food_items)} food records for user {user_id}")

        return {
            # "success": True,
            # "message": f"Found {len(food_items)} food analysis records",
            "code"  : 0,
            "msg"   : "ok",
            "data"  : food_items,
        }

    except Exception as e:
        logging.error(f"Error retrieving food history for user {user_id}: {str(e)}", stack_info=True)
        # raise HTTPException(status_code=500, detail="Failed to retrieve food analysis history")

        return {
            # "success": False,
            # "message": f"Found {len(food_items)} food analysis records",
            "code"  : -1,
            "msg"   : str(e)
            # "data"  : food_items,
        }


@router.delete("/delete/{food_id}")
async def delete_food_analysis(
    food_id: str,
    user_id: str = Depends(verify_token),
):
    """
    Delete a food analysis record

    Args:
        food_id: ID of the food analysis record to delete
        user_id: Authenticated user ID

    Returns:
        Delete result
    """
    try:
        # Check if the record exists and belongs to the user
        check_sql = """
            SELECT id FROM theta_ai.th_messages 
            WHERE id = :food_id 
                AND user_id = :user_id 
                AND message_type = 'food'
        """

        existing_record = await execute_query(check_sql, {"food_id": food_id, "user_id": user_id})

        if not existing_record:
            raise HTTPException(
                status_code=404,
                detail="Food analysis record not found or access denied",
            )

        # Soft delete the record by setting is_del = true
        delete_sql = """
            UPDATE theta_ai.th_messages 
            SET is_del = true, updated_at = CURRENT_TIMESTAMP
            WHERE id = :food_id AND user_id = :user_id
        """

        await execute_query(delete_sql, {"food_id": food_id, "user_id": user_id})

        logging.info(f"Food analysis record {food_id} deleted for user {user_id}")

        return {"code": 0, "msg": "Food analysis record deleted successfully"}

    except HTTPException as e:
        return {"code": -1, "msg": str(e)}
    except Exception as e:
        logging.error(f"Error deleting food analysis {food_id} for user {user_id}: {str(e)}", stack_info=True)
        # raise HTTPException(status_code=500, detail="Failed to delete food analysis record")
        return {"code": -2, "msg": f"Error deleting food analysis {food_id} for user {user_id}: {str(e)}"}
