import asyncio
import json
import time
import logging
import uuid
from datetime import datetime
from typing import Any, Dict

from mirobody.pulse.file_parser.services.file_uploader import FileUploader
from mirobody.pulse.file_parser.services.temp_file_manager import TempFileManager
from mirobody.utils.i18n import t
from mirobody.utils import execute_query, get_req_ctx, safe_read_cfg

from mirobody.utils.llm import doubao_file_extract, gemini_file_extract

from .prompts.food_prompts import FOOD_RECOGNIZE_PROMPT, FOOD_RECOGNIZE_RESPONSE_SCHEMA, SIMPLE_FOOD_RECOGNIZE_PROMPT



def auto_determine_category() -> str:
    """
    Auto-determine food category based on current time
    """
    current_hour = datetime.now().hour

    if 5 <= current_hour < 11:
        return "breakfast"
    elif 11 <= current_hour < 16:
        return "lunch"
    elif 16 <= current_hour < 22:
        return "dinner"
    else:
        return "snack"


class FoodImageRecognizer:
    """Food image recognition service for data_server"""

    def __init__(self):
        self.temp_manager = TempFileManager()
        self.uploader = FileUploader()

    async def food_image_recognize(
        self,
        file_content: bytes,
        filename_original: str,
        content_type: str,
        user_id: str,
        msg_id: str,
        query: str,
        sid: str,
    ) -> Dict[str, Any]:
        """
        Recognize food in image and extract nutritional information

        Args:
            file_content: Image file content in bytes
            filename_original: Original filename
            content_type: MIME type of the file
            user_id: User ID
            msg_id: Message ID
            query: User's health query/goal
            sid: Session ID

        Returns:
            Dict containing recognition results
        """
        language = get_req_ctx("language", "en")
        start_time = datetime.now()  # Record start time for task flow
        ret = {
            "full_url": "",
            "content": "",
            "type": content_type,
            "filename": filename_original,
            "success": False,
            "message": "",
        }
        temp_file_path = None

        try:
            # Generate unique filename
            filetype = filename_original.split(".")[-1].lower() if filename_original else ""
            filename = f"health_food/{user_id}_{time.strftime('%Y%m%d%H%M%S')}_{str(uuid.uuid4())}.{filetype}"

            # Create temporary file
            temp_file_path, temp_file_path_str = self.temp_manager.create_temp_file_from_content(file_content, filename_original)

            # Start file upload and content extraction tasks in parallel
            upload_task = asyncio.create_task(
                self.uploader.upload_content_and_get_url(file_content, filename, content_type)
            )
            
            is_aliyun = safe_read_cfg("CLUSTER") == "ALIYUN"
            if is_aliyun:
                prompt = SIMPLE_FOOD_RECOGNIZE_PROMPT.format(query=query, language=language)
                extract_task = asyncio.create_task(
                    doubao_file_extract(
                        temp_file_path_str,
                        prompt,
                        model="doubao-1-5-ui-tars-250428",
                    )
                )
            else:
                # Use Gemini for content extraction
                prompt = FOOD_RECOGNIZE_PROMPT.format(query=query, language=language)
                from google.genai import types

                config = types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=FOOD_RECOGNIZE_RESPONSE_SCHEMA,
                    temperature=0.1,
                )
                extract_task = asyncio.create_task(
                    gemini_file_extract(
                        file_path=temp_file_path_str,
                        content_type=content_type,
                        prompt=prompt,
                        config=config,
                        model="gemini-2.5-flash",
                    )
                )


            # Wait for both tasks to complete
            full_url, llm_ret = await asyncio.gather(upload_task, extract_task)

            ret["full_url"] = full_url
            ret["file_type"] = content_type
            ret["filename"] = filename_original
            ret["message"] = t("file_processed_successfully", language, "file_processor")

            try:
                llm_ret_dict = json.loads(llm_ret)
                is_food = llm_ret_dict.get("is_food", "0")

                # Validate required fields if food is detected
                if is_food == "1":
                    required_keys = [
                        "name",
                        "nutrition",
                        "nut",
                        "advice",
                        "title",
                        "category",
                    ]
                    missing_keys = [key for key in required_keys if key not in llm_ret_dict]
                    if missing_keys:
                        logging.error(f"JSON missing required keys: {missing_keys}")
                        ret["message"] = "Recognition failed!"
                        return ret

            except Exception:
                logging.error("JSON parsing failed", stack_info=True)
                ret["message"] = "Recognition failed!"
                return ret

            # Save to database
            ret["success"] = True
            ret["title"] = llm_ret_dict.get("title", "No food detected in this image")
            ret["message"] = "No food detected!" if is_food == "0" else "Food recognition successful!"

            # Prepare content for th_messages table
            food_content = {
                "name": llm_ret_dict.get("name", "Non-food image"),
                "category": llm_ret_dict.get(
                    "category", auto_determine_category()
                ),  # Use AI category or auto-determine
                "nutrition": llm_ret_dict.get("nutrition", []),  # Individual food components
                "nut": llm_ret_dict.get("nut", []),  # Overall nutrition info
                "advice": llm_ret_dict.get("advice", []),  # Health advice
                "title": ret["title"],
                "image_url": full_url,
                "is_food": is_food,
                "filename": filename_original,
                "file_key": filename,
            }

            food_db_result = await self.save_food_message_result(
                user_id, sid, msg_id, json.dumps(food_content, ensure_ascii=False)
            )

            # Format timestamp fields to strings (keep original field names)
            if food_db_result.get("created_at"):
                food_db_result["created_at"] = food_db_result["created_at"].strftime("%Y-%m-%d %H:%M:%S")
            if food_db_result.get("updated_at"):
                food_db_result["updated_at"] = food_db_result["updated_at"].strftime("%Y-%m-%d %H:%M:%S")

                # Parse content and format response for frontend compatibility
            try:
                #content_data = json.loads(food_db_result.get("content", "{}"))
                content_data = food_content

                # Extract only the fields that frontend expects (FoodAnalysisData interface)
                frontend_data = {
                    "id": int(time.time()),  # Simple numeric ID for frontend
                    "name": content_data.get("name", "Unknown Food"),
                    "category": content_data.get("category", auto_determine_category()),  # Include category
                    "title": content_data.get("title", "Food Analysis Result"),
                    "is_food": content_data.get("is_food", "0"),
                    "image_url": content_data.get("image_url", ""),
                    "create_at": food_db_result.get("created_at", ""),  # Note: create_at not created_at
                    "nutrition": [],
                    "nut": [],  # Overall nutrition info - needed by frontend
                    "advice": [],
                }

                # Parse JSON fields properly - nutrition, nut, and advice
                for json_field in ["nutrition", "nut", "advice"]:
                    field_data = content_data.get(json_field)
                    if field_data:
                        if isinstance(field_data, str):
                            try:
                                frontend_data[json_field] = json.loads(field_data)
                            except (json.JSONDecodeError, TypeError):
                                frontend_data[json_field] = []
                        else:
                            frontend_data[json_field] = field_data
                    else:
                        frontend_data[json_field] = []

            except json.JSONDecodeError:
                # If parsing fails, return minimal structure
                frontend_data = {
                    "id": int(time.time()),
                    "name": "Analysis Failed",
                    "category": "snack",  # Default category for failed analysis
                    "title": "Unable to analyze food",
                    "is_food": "0",
                    "image_url": "",
                    "create_at": food_db_result.get("created_at", ""),
                    "nutrition": [],
                    "nut": [],  # Overall nutrition info - needed by frontend
                    "advice": [],
                }

            ret["content"] = frontend_data

            return ret

        except Exception as e:
            error_msg = str(e)
            logging.error("File processing failed", stack_info=True)

            # Check for specific error types and provide user-friendly messages
            if "location is not supported" in error_msg.lower():
                ret["message"] = "API region restriction: This service is not supported in your current region, please contact administrator"
            elif "failed_precondition" in error_msg.lower():
                ret["message"] = "API service restriction: Please try again later or contact administrator"
            elif "json" in error_msg.lower() and "decode" in error_msg.lower():
                ret["message"] = "Data parsing failed: AI service temporarily unavailable"
            else:
                ret["message"] = "Recognition failed!"

            return ret
        finally:
            # Clean up temp file
            if temp_file_path:
                try:
                    self.temp_manager.cleanup_temp_file(temp_file_path)
                except Exception:
                    pass

    @staticmethod
    async def save_food_message_result(user_id: str, session_id: str, msg_id: str, content: str) -> Dict[str, Any]:
        """Save food recognition result to th_messages table"""
        sql = """
            INSERT INTO th_messages (
                id, user_id, session_id, role, content, message_type,
                created_at, updated_at, is_del
            ) VALUES (
                :id, :user_id, :session_id, 'assistant', encrypt_content(:content), 'food',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, false
            ) RETURNING *
        """
        params = {
            "id": str(msg_id),
            "user_id": user_id,
            "session_id": session_id,
            "content": content,
        }

        result = await execute_query(sql, params)
        return result
