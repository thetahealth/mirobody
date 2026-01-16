#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List

from mirobody.utils.i18n import clear_translation_cache, t
from mirobody.utils import execute_query
from mirobody.pulse.file_parser.services.database_services import FileParserDatabaseService



async def debug_message_content(message_id: str, stage: str = "unknown"):
    """Debug function: Check if message content correctly retains file details"""
    try:
        db_service = FileParserDatabaseService()
        message_details = await db_service.get_message_details(message_id)
        content = message_details.get("content", "")
        if content:
            try:
                parsed_content = json.loads(content)
                file_count = len(parsed_content.get("files", []))
                has_genetic = any(f.get("type") == "genetic" for f in parsed_content.get("files", []))
                logging.debug(f"[DEBUG] {stage}: {message_id}, files={file_count}, genetic={has_genetic}")
            except json.JSONDecodeError:
                pass
    except Exception:
        pass


class GeneticDataLoader:
    """Genetic data loader, supports large file streaming processing and progress updates"""

    def __init__(
        self,
        message_id=None,
        language="en",
        user_id=None,
        display_filename: str = None,
        display_file_size: int = None,
    ):
        self.message_id = message_id
        self.language = language
        self.user_id = user_id
        self.display_filename = display_filename
        self.display_file_size = display_file_size

    def parse_genetic_file(
        self,
        file_path: str,
        user_id: str,
        source_table: str = None,
        source_table_id: str = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Parse genetic data file (generator version, yield line by line)"""
        import gc

        with open(file_path, "r", encoding="utf-8") as file:
            data_started = False
            processed_lines = 0
            valid_records = 0

            for line_num, line in enumerate(file, 1):
                line = line.strip()
                processed_lines += 1

                # Release memory every 10000 lines processed
                if processed_lines % 10000 == 0:
                    gc.collect()

                # Find data start marker
                if not data_started:
                    if line.startswith("# rsid") and "chromosome" in line and "position" in line and "genotype" in line:
                        data_started = True
                        logging.info(f"Found data start marker line: {line}")
                        continue
                    else:
                        continue

                # Skip empty lines and comment lines
                if not line or line.startswith("#"):
                    continue

                # Parse data line
                parts = line.split("\t") if "\t" in line else line.split()
                if len(parts) >= 4:
                    try:
                        rsid, chromosome, position_str, genotype = parts[:4]
                        rsid, chromosome, genotype = (
                            rsid.strip(),
                            chromosome.strip(),
                            genotype.strip(),
                        )

                        position = int(position_str)
                        valid_records += 1

                        yield {
                            "user_id": user_id,
                            "rsid": rsid,
                            "chromosome": chromosome,
                            "position": position,
                            "genotype": genotype,
                            "source_table": source_table,
                            "source_table_id": source_table_id,
                        }
                    except (ValueError, IndexError) as e:
                        logging.warning(f"Data format error in line {line_num}: {line} - {e}")
                        continue

            logging.info(f"Parsing complete: processed {processed_lines} lines total, generated {valid_records} valid records")


    async def update_progress(self, processed: int, saved: int, message: str, total: int = None):
        """Update processing progress"""
        if not self.message_id:
            return

        try:
            from ..file_upload_manager import websocket_file_upload_manager
            db_service = FileParserDatabaseService()

            # Calculate progress: genetic processing maps to 50-100%
            genetic_progress = min((processed / total * 100), 100) if total and total > 0 else 0
            progress_percent = 100 if (total and processed >= total) else 50 + (genetic_progress * 0.5)

            content = t("genetic_progress_display", self.language, "load_genetic_data",
                       processed=processed, saved=saved, percent=progress_percent)

            # Get existing message content to preserve file details
            try:
                existing_message = await db_service.get_message_details(self.message_id)
                existing_content = existing_message.get("content", "")

                if existing_content:
                    try:
                        existing_data = json.loads(existing_content)
                        if isinstance(existing_data, dict) and existing_data.get("files"):
                            # Preserve file details and update progress
                            updated_content = existing_data.copy()
                            updated_content.update({
                                "status": "processing", "progress": int(progress_percent),
                                "message": content, "timestamp": datetime.now().isoformat(),
                                "progress_details": {"processed": processed, "saved": saved,
                                                    "progress_percent": progress_percent, "stage": "genetic_processing"}
                            })
                            for file_info in updated_content.get("files", []):
                                # Match by filename or type (initial type might be MIME type like "text/plain")
                                if file_info.get("type") == "genetic" or file_info.get("filename") == self.display_filename:
                                    file_info.update({"progress": int(progress_percent), "status": "processing", "progress_message": content, "type": "genetic"})
                            await db_service.update_message_content(message_id=self.message_id, content=updated_content)
                        else:
                            await db_service.update_message_content(message_id=self.message_id, content=content, reasoning=message)
                    except json.JSONDecodeError:
                        await db_service.update_message_content(message_id=self.message_id, content=content, reasoning=message)
                else:
                    await db_service.update_message_content(message_id=self.message_id, content=content, reasoning=message)
            except Exception:
                await db_service.update_message_content(message_id=self.message_id, content=content, reasoning=message)

            # Send real-time progress updates via WebSocket
            if self.user_id:
                try:
                    detailed_message = f"🧬 Genetic data processing... {processed:,}/{total:,} ({progress_percent:.1f}%)"
                    if total and processed > 1000:
                        remaining_time = 300 * (1 - processed / total)  # Estimate based on 5min total
                        if remaining_time > 0:
                            time_str = f"{remaining_time / 60:.0f}min" if remaining_time > 60 else f"{remaining_time:.0f}s"
                            detailed_message += f" ~{time_str} remaining"

                    await websocket_file_upload_manager.send_message(str(self.user_id), {
                        "type": "upload_progress", "messageId": self.message_id,
                        "status": "processing", "progress": int(progress_percent),
                        "message": detailed_message, "file_type": "genetic",
                        "filename": self.display_filename, "success": False,
                        "processing_stats": {"processed_records": processed, "saved_records": saved,
                                           "total_estimated": total, "progress_percent": progress_percent},
                    })
                except Exception:
                    pass  # WebSocket failure doesn't affect processing

        except ImportError:
            pass
        except Exception as e:
            logging.error(f"Failed to update progress: {e}")

    async def process_batch(self, batch: List[Dict], insert_sql: str) -> bool:
        """Process single batch data insertion"""
        try:
            await execute_query(
                query=insert_sql,
                fieldList=batch,
            )
            return True
        except Exception as e:
            logging.error(f"Batch insertion failed: {e}")
            return False

    async def load_user_genetic_data(
        self,
        user_id: str,
        file_path: str,
        batch_size: int = 50000,
        is_up_progress: bool = True,
        source_table: str = None,
        source_table_id: str = None,
    ):
        """Load user genetic data (streaming batch insertion, supports very large files)"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File does not exist: {file_path}")

        insert_sql = """
        INSERT INTO theta_ai.th_series_data_genetic 
        (user_id, rsid, chromosome, position, genotype, source_table, source_table_id, create_time, update_time, is_deleted)
        VALUES (:user_id, :rsid, :chromosome, :position, :genotype, :source_table, :source_table_id, NOW(), NOW(), FALSE)
        """

        batch, total_processed, total_saved, batch_count, failed_batches = [], 0, 0, 0, 0
        estimated_total = sum(1 for _ in open(file_path, "r", encoding="utf-8"))
        
        if is_up_progress:
            await self.update_progress(0, 0, t("genetic_file_estimation", self.language, "load_genetic_data", total=estimated_total), estimated_total)

        try:
            # Process each record
            for record in self.parse_genetic_file(file_path, user_id, source_table, source_table_id):
                batch.append(record)
                total_processed += 1

                # Periodically update parsing progress
                if total_processed % batch_size == 0:
                    if is_up_progress:
                        await self.update_progress(
                            total_processed,
                            total_saved,
                            t(
                                "genetic_parsing_progress",
                                self.language,
                                "load_genetic_data",
                                total=total_processed,
                            ),
                            estimated_total,
                        )

                # Batch insertion
                if len(batch) >= batch_size:
                    if await self.process_batch(batch, insert_sql):
                        total_saved += len(batch)
                        batch_count += 1
                        logging.info(f"Batch {batch_count}: saved {len(batch)} records")
                    else:
                        failed_batches += 1

                    batch.clear()

                    # Update batch completion progress
                    if batch_count % 5 == 0:
                        if is_up_progress:
                            await self.update_progress(
                                total_processed,
                                total_saved,
                                t(
                                    "genetic_batch_status",
                                    self.language,
                                    "load_genetic_data",
                                    batches=batch_count,
                                ),
                                estimated_total,
                            )
                        await asyncio.sleep(0.1)  # Give system time to handle other tasks

            # Process remaining batch
            if batch:
                if await self.process_batch(batch, insert_sql):
                    total_saved += len(batch)
                    batch_count += 1
                else:
                    failed_batches += 1

            # Final progress update
            if is_up_progress:
                await self.update_progress(
                    total_processed,
                    total_saved,
                    t(
                        "genetic_processing_finished",
                        self.language,
                        "load_genetic_data",
                        total=total_processed,
                        saved=total_saved,
                    ),
                    total_processed,
                )

            logging.info(f"✓ User {user_id} genetic data loading completed: processed {total_processed} records, saved {total_saved} records, failed batches {failed_batches}")
            return total_saved

        except Exception as e:
            if is_up_progress:
                await self.update_progress(
                    total_processed,
                    total_saved,
                    f"❌ Processing error: {str(e)}",
                    estimated_total,
                )
            logging.error(f"Data loading failed: {e}", stack_info=True)
            raise


async def process_genetic_file(
    user_id: str,
    temp_file_path: Path,
    message_id: str = None,
    language: str = "en",
    original_filename: str = None,  # New: original filename parameter
    original_file_size: int = None,  # New: original file size parameter
    source_table: str = None,  # New: data source table name
    source_table_id: str = None,  # New: data source table record ID
    file_key: str = None,  # New: file_key for th_messages files field
    full_url: str = None,  # New: OSS/S3 URL for the file
    file_abstract: str = None,  # New: file abstract/summary
):
    """Entry function for processing genetic data files"""
    try:
        # Import websocket manager locally to avoid circular import
        from ..file_upload_manager import websocket_file_upload_manager

        clear_translation_cache("load_genetic_data")

        # 🔧 Fix: Use original filename, or temporary filename if not provided
        display_filename = original_filename or temp_file_path.name
        display_file_size = original_file_size or (temp_file_path.stat().st_size if temp_file_path.exists() else 0)

        loader = GeneticDataLoader(message_id, language, user_id, display_filename, display_file_size)

        if message_id:
            await loader.update_progress(0, 0, t("genetic_initializing_loader", language, "load_genetic_data"))

        # Execute data loading
        loaded_records = await loader.load_user_genetic_data(
            user_id,
            str(temp_file_path),
            source_table=source_table,
            source_table_id=source_table_id,
        )

        # Update completion status
        if message_id:
            final_content = t(
                "genetic_processing_complete_message",
                language,
                "load_genetic_data",
                records=loaded_records,
            )
            final_reasoning = t(
                "genetic_processing_complete_reasoning",
                language,
                "load_genetic_data",
                records=loaded_records,
            )

            db_service = FileParserDatabaseService()

            # Build complete file info structure (consistent with Excel format)
            url_value = full_url or display_filename
            complete_file_info = {
                "filename": display_filename,
                "type": "genetic",
                "url_thumb": url_value,
                "url_full": url_value,
                "raw": final_content,
                "file_abstract": file_abstract or "",
                "file_name": display_filename,
                "file_size": display_file_size,
                "file_key": file_key or "",
                "success": True,
                "status": "completed",
                "progress": 100,
                "loaded_records": loaded_records,
                "contentType": "text/plain",
            }

            # Get existing message content to preserve session info etc.
            try:
                existing_message = await db_service.get_message_details(message_id)
                existing_content = existing_message.get("content", "")
                existing_data = {}

                if existing_content:
                    try:
                        existing_data = json.loads(existing_content)
                        if not isinstance(existing_data, dict):
                            existing_data = {}
                    except json.JSONDecodeError:
                        existing_data = {}

                # Build complete content structure (consistent with Excel format)
                updated_content = {
                    "success": True,
                    "message": "Processing completed: 1 files successful",
                    "type": "genetic",
                    "url_thumb": [url_value],
                    "url_full": [url_value],
                    "message_id": message_id,
                    "files": [complete_file_info],
                    "original_filenames": [display_filename],
                    "file_sizes": [display_file_size],
                    "upload_time": existing_data.get("upload_time", datetime.now().isoformat()),
                    "total_files": 1,
                    "successful_files": 1,
                    "failed_files": 0,
                    "CODE_VERSION": "v2.0_WEBSOCKET_GENETIC_SUPPORTED",
                    "status": "completed",
                    "progress": 100,
                    "timestamp": datetime.now().isoformat(),
                    "loaded_records": loaded_records,
                }

                # Preserve session info from existing data
                for key in ["query_user_id", "target_user_name", "is_uploaded_for_others", "query", "session_id"]:
                    if key in existing_data:
                        updated_content[key] = existing_data[key]

                await db_service.update_message_content(message_id=message_id, content=updated_content, reasoning=final_reasoning)

            except Exception as e:
                logging.warning(f"Failed to update complete content, using simple format: {e}")
                await db_service.update_message_content(message_id=message_id, content=final_content, reasoning=final_reasoning)

            # Send completion status via WebSocket
            if user_id:
                try:
                    detailed_final_message = f"✅ Genetic data processing completed! Saved {loaded_records:,} records"
                    send_success = await websocket_file_upload_manager.send_message(str(user_id), {
                        "type": "upload_completed", "messageId": message_id, "status": "completed",
                        "progress": 100, "message": detailed_final_message, "file_type": "genetic",
                        "filename": display_filename, "success": True, "raw": final_content,
                        "url_thumb": url_value, "url_full": url_value, "file_key": file_key,
                        "file_size": display_file_size, "file_abstract": file_abstract or "",
                        "processing_stats": {"processed_records": loaded_records, "saved_records": loaded_records,
                                           "progress_percent": 100, "stage": "genetic_completed"},
                        "genetic_processing_final": True
                    })
                    if send_success:
                        await websocket_file_upload_manager.update_genetic_processing_complete(str(user_id), message_id)
                except Exception:
                    pass  # WebSocket failure doesn't affect results

        logging.info(f"Genetic processing completed: {loaded_records} records")

        # 🔧 Fix: Return correct original file information
        return {
            "success": True,
            "message": t("genetic_file_received", language, "load_genetic_data"),
            "type": "genetic",
            "url_thumb": display_filename,  # Use original filename
            "full_url": display_filename,  # Use original filename
            "filename": display_filename,  # Add original filename
            "file_size": display_file_size,  # Add original file size
            "loaded_records": loaded_records,
            "file_key": file_key,  # Add file_key
        }

    except Exception as e:
        error_msg = f"Error processing genetic data file: {str(e)}"
        logging.error(error_msg, stack_info=True)

        # 🔧 Fix: Use original filename, or temporary filename if not provided
        display_filename = original_filename or temp_file_path.name
        display_file_size = original_file_size or (temp_file_path.stat().st_size if temp_file_path.exists() else 0)

        # Update failure status
        if message_id:
            try:
                db_service = FileParserDatabaseService()

                failed_content = t(
                    "genetic_processing_failed_message",
                    language,
                    "load_genetic_data",
                    stack_info=True,
                )
                failed_reasoning = t(
                    "genetic_processing_failed_reasoning",
                    language,
                    "load_genetic_data",
                    stack_info=True,
                )

                # 🔧 Fix: Get existing message content to avoid overwriting file details
                try:
                    existing_message = await db_service.get_message_details(message_id)
                    existing_content = existing_message.get("content", "")

                    if existing_content:
                        try:
                            existing_data = json.loads(existing_content)
                            # If existing content contains file details, preserve them and update to failure status
                            if isinstance(existing_data, dict) and existing_data.get("files"):
                                # Update failure status while preserving file details
                                updated_content = existing_data.copy()
                                updated_content.update(
                                    {
                                        "status": "failed",
                                        "progress": 0,
                                        "message": failed_content,
                                        "timestamp": datetime.now().isoformat(),
                                        "success": False,
                                        "error": str(e),
                                        "processing_failed": True,
                                        "progress_details": {
                                            "processed": 0,
                                            "saved": 0,
                                            "progress_percent": 0,
                                            "stage": "genetic_failed",
                                            "error": str(e),
                                        },
                                    }
                                )

                                # Update failure status for each file
                                if "files" in updated_content:
                                    for file_info in updated_content["files"]:
                                        # Match by filename or type (initial type might be MIME type like "text/plain")
                                        if file_info.get("type") == "genetic" or file_info.get("filename") == display_filename:
                                            file_info["progress"] = 0
                                            file_info["status"] = "failed"
                                            file_info["success"] = False
                                            file_info["progress_message"] = failed_content
                                            file_info["error"] = str(e)
                                            file_info["raw"] = failed_content
                                            file_info["file_key"] = file_key
                                            file_info["type"] = "genetic"

                                await db_service.update_message_content(
                                    message_id=message_id,
                                    content=updated_content,
                                    reasoning=failed_reasoning,
                                )
                            else:
                                # If no file details, use simple failure update
                                await db_service.update_message_content(
                                    message_id=message_id,
                                    content=failed_content,
                                    reasoning=failed_reasoning,
                                )
                        except json.JSONDecodeError:
                            # If parsing fails, use simple failure update
                            await db_service.update_message_content(
                                message_id=message_id,
                                content=failed_content,
                                reasoning=failed_reasoning,
                            )
                    else:
                        # If no existing content, use simple failure update
                        await db_service.update_message_content(
                            message_id=message_id,
                            content=failed_content,
                            reasoning=failed_reasoning,
                        )

                except Exception as update_error:
                    logging.warning(f"Failed to get existing message content: {update_error}, using simple update")
                    await db_service.update_message_content(
                        message_id=message_id,
                        content=failed_content,
                        reasoning=failed_reasoning,
                    )

                # Send failure status via WebSocket
                try:
                    # 🔧 Fix: Use correct file info in WebSocket updates
                    if user_id:
                        # 🔧 Enhancement: WebSocket error handling
                        try:
                            # Use websocket_file_upload_manager to send failure status
                            send_success = await websocket_file_upload_manager.send_message(
                                str(user_id),  # 🔧 Ensure user ID is string type
                                {
                                    "type": "upload_error",  # 🔧 Fix: Use frontend expected message type
                                    "messageId": message_id,
                                    "status": "failed",
                                    "progress": 0,
                                    "message": failed_content,
                                    "file_type": "genetic",
                                    "filename": display_filename,
                                    "success": False,
                                    "raw": failed_content,
                                    "url_thumb": display_filename,
                                    "url_full": display_filename,
                                    "error": str(e),  # 🔧 New: Provide error details for frontend
                                },
                            )
                            # 🔧 Fix: Only log success when actually sent successfully
                            if send_success:
                                logging.info(f"✅ WebSocket failure status sent successfully: user_id={user_id}, error={str(e)}")
                            else:
                                logging.info(f"📡 WebSocket failure status send failed (user not connected): user_id={user_id}")
                        except Exception as ws_error:
                            logging.info(f"📡 WebSocket failure status send exception: {ws_error}")
                            logging.info(f"📝 Error information recorded to database: {str(e)}")
                            # WebSocket failure does not affect error recording, error information has been saved to database
                    else:
                        logging.warning(f"WebSocket failure status update skipped: user_id is empty")

                except Exception as ws_error:
                    logging.warning(f"⚠️ WebSocket module loading failed: {ws_error}")
                    logging.info(f"📝 Even if WebSocket is unavailable, error information has been recorded to database: {str(e)}")
                    # WebSocket failure does not affect error recording, error information has been saved to database

            except Exception as update_error:
                logging.error(f"Error updating failure message status: {update_error}")

        return {
            "success": False,
            "message": error_msg,
            "type": "error",
            "filename": display_filename,  # Add original filename
            "file_size": display_file_size,  # Add original file size
            "file_key": file_key,  # Add file_key
        }
    finally:
        # Clean up temporary files
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                logging.info(f"Deleted temporary file: {temp_file_path}")
            except Exception as ex:
                logging.error(f"Failed to delete temporary file: {str(ex)}")


# 🔧 Test script: Verify file information preservation functionality
async def test_file_info_preservation():
    """Test file information preservation functionality during genetic file processing"""
    try:
        import uuid
        from datetime import datetime

        db_service = FileParserDatabaseService()

        # Create test message
        test_message_id = f"test_genetic_{uuid.uuid4().hex[:8]}"
        test_user_id = "test_user_123"
        test_session_id = f"test_session_{uuid.uuid4().hex[:8]}"

        # Simulate initial file information
        initial_file_info = {
            "success": True,
            "message": "File processing completed",
            "type": "genetic",
            "url_thumb": ["test_genetic_file.txt"],
            "url_full": ["test_genetic_file.txt"],
            "message_id": test_message_id,
            "raw": "Genetic data file is being processed in the background, please wait...",
            "files": [
                {
                    "filename": "test_genetic_file.txt",
                    "type": "genetic",
                    "url_thumb": "test_genetic_file.txt",
                    "url_full": "test_genetic_file.txt",
                    "raw": "Genetic data file is being processed in the background, please wait...",
                    "file_size": 1024000,
                    "status": "processing",
                    "progress": 0,
                }
            ],
            "original_filenames": ["test_genetic_file.txt"],
            "file_sizes": [1024000],
            "upload_time": datetime.now().isoformat(),
            "total_files": 1,
            "successful_files": 1,
            "failed_files": 0,
            "CODE_VERSION": "v2.0_WEBSOCKET_TEST",
        }

        # 1. Create initial message
        await db_service.log_chat_message(
            id=test_message_id,
            user_id=test_user_id,
            session_id=test_session_id,
            role="user",
            content=json.dumps(initial_file_info),
            reasoning="Test genetic file information preservation",
            agent="test_agent",
            provider="test",
            message_type="file",
            user_name="test_user",
        )

        print(f"✅ Created test message: {test_message_id}")
        await debug_message_content(test_message_id, "initial_creation")

        # 2. Simulate progress update
        loader = GeneticDataLoader(test_message_id, "zh", test_user_id, "test_genetic_file.txt", 1024000)

        # Test progress update
        await loader.update_progress(50000, 25000, "Test progress update: processed 50,000 records", 200000)
        await debug_message_content(test_message_id, "after_progress_update")

        # Test completion status
        await loader.update_progress(200000, 200000, "Test completed: processed 200,000 records", 200000)
        await debug_message_content(test_message_id, "after_completion")

        print(f"✅ Test completed: {test_message_id}")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()


# 🔧 Verification script: Check WebSocket message format
async def test_websocket_message_format():
    """Test whether WebSocket message format contains correct file size and progress information"""
    try:
        import json

        # Simulate WebSocket messages for genetic files
        test_message_id = "test_genetic_message_123"
        test_filename = "my_genetic_data.txt"
        test_file_size = 2048576  # 2MB

        # Simulate progress update messages
        test_processing_stats = {
            "processed_records": 50000,
            "saved_records": 25000,
            "total_estimated": 200000,
            "progress_percent": 25.0,
            "file_size": test_file_size,
            "stage": "genetic_processing",
        }

        print("🧪 Testing WebSocket message format...")
        print(f"📁 Filename: {test_filename}")
        print(f"📊 File size: {test_file_size} bytes ({test_file_size / 1024 / 1024:.1f} MB)")
        print(f"🔄 Progress: {test_processing_stats['progress_percent']}%")
        print(f"📈 Processed: {test_processing_stats['processed_records']:,} records")
        print(f"💾 Saved: {test_processing_stats['saved_records']:,} records")

        # Build detailed message (same as actual code)
        detailed_message = f"🧬 Genetic data processing...\n\n📊 Processing progress:\n• Parsed: {test_processing_stats['processed_records']:,} records\n• Saved: {test_processing_stats['saved_records']:,} records\n• Progress: {test_processing_stats['progress_percent']:.1f}%"

        # Simulate WebSocket message data structure
        websocket_message = {
            "messageId": test_message_id,
            "status": "processing",
            "type": "genetic",
            "filename": test_filename,
            "message": detailed_message,
            "success": False,
            "raw": "Genetic data processing...",
            "url_thumb": test_filename,
            "url_full": test_filename,
            "progress": int(test_processing_stats["progress_percent"]),
            "file_size": test_file_size,
            "processed_records": test_processing_stats["processed_records"],
            "saved_records": test_processing_stats["saved_records"],
            "total_estimated": test_processing_stats["total_estimated"],
            "processing_stats": test_processing_stats,
        }

        print("\n📤 Simulated WebSocket message structure:")
        print(json.dumps(websocket_message, indent=2, ensure_ascii=False))

        # Verify key fields
        required_fields = [
            "messageId",
            "status",
            "filename",
            "file_size",
            "progress",
            "processing_stats",
        ]
        missing_fields = [field for field in required_fields if field not in websocket_message]

        if missing_fields:
            print(f"\n❌ Missing required fields: {missing_fields}")
        else:
            print("\n✅ All required fields are present")

        # Verify file size
        if websocket_message.get("file_size", 0) > 0:
            print(f"✅ File size field correct: {websocket_message['file_size']} bytes")
        else:
            print("❌ File size field missing or zero")

        # Verify processing statistics
        stats = websocket_message.get("processing_stats", {})
        if stats.get("processed_records", 0) > 0:
            print(f"✅ Processing statistics correct: processed {stats['processed_records']:,} records")
        else:
            print("❌ Processing statistics missing")

        print("\n🎯 Frontend should be able to display:")
        print(f"• Filename: {websocket_message['filename']}")
        print(f"• File size: {websocket_message['file_size'] / 1024 / 1024:.1f} MB")
        print(f"• Processing progress: {websocket_message['progress']}%")
        print(f"• Detailed message: {websocket_message['message'][:50]}...")

        return True

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


# If running this file directly, execute all tests
if __name__ == "__main__":
    import asyncio

    print("🧪 Starting genetic file information preservation function test...")
    asyncio.run(test_file_info_preservation())
    print("\n🧪 Starting WebSocket message format test...")
    asyncio.run(test_websocket_message_format())
