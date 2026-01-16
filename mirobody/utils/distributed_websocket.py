"""
Local WebSocket Manager - Single instance version without Redis dependency
"""

import json
import logging
import uuid
from typing import Dict

from fastapi import WebSocket


class LocalWebSocketManager:
    """Local WebSocket connection manager"""

    def __init__(self, namespace: str = "default"):
        self.namespace = namespace
        self.local_connections: Dict[int, WebSocket] = {}
        self.active_connections = self.local_connections
        self.instance_id = str(uuid.uuid4())[:8]
        self._started = False
        logging.info(f"🏭 [LocalWS:{namespace}] Instance started, ID: {self.instance_id}")

    async def startup(self):
        """Start local WebSocket manager"""
        if self._started:
            return
        try:
            self._started = True
            logging.info(f"✅ [LocalWS:{self.namespace}] Local WebSocket manager started")
        except Exception as e:
            logging.error(f"❌ [LocalWS:{self.namespace}] Startup failed: {str(e)}")
            raise

    async def shutdown(self):
        """Shutdown local WebSocket manager"""
        if not self._started:
            return
        try:
            await self.cleanup()
            self._started = False
            logging.info(f"✅ [LocalWS:{self.namespace}] Shutdown completed")
        except Exception as e:
            logging.error(f"❌ [LocalWS:{self.namespace}] Shutdown failed: {e}")

    async def start_redis_subscriber(self):
        """Empty method for interface compatibility"""
        pass

    async def connect(self, websocket: WebSocket, user_id: str):
        """Register WebSocket connection"""
        self.local_connections[user_id] = websocket
        self.active_connections = self.local_connections
        logging.info(f"🔗 [LocalWS:{self.namespace}] User {user_id} connected")
        logging.info(f"📊 [LocalWS:{self.namespace}] Current connections: {len(self.local_connections)}")

    async def disconnect(self, user_id: str):
        """Disconnect WebSocket connection"""
        if user_id in self.local_connections:
            del self.local_connections[user_id]
            self.active_connections = self.local_connections
            logging.info(f"🔌 [LocalWS:{self.namespace}] User {user_id} disconnected")
            logging.info(f"📊 [LocalWS:{self.namespace}] Current connections: {len(self.local_connections)}")

    async def _send_to_local_connection(self, user_id: int, message: str):
        """Send message to local WebSocket connection"""
        if user_id in self.local_connections:
            try:
                websocket = self.local_connections[user_id]
                await websocket.send_text(message)
                logging.info(f"✅ [LocalWS] Message sent to user {user_id}")
            except Exception as e:
                logging.warning(f"⚠️ [LocalWS] Send failed: {e}")
                await self.disconnect(user_id)

    async def send_message_to_user(self, user_id: str, message: dict):
        """Send message to specified user"""
        try:
            if user_id not in self.local_connections:
                logging.debug(f"⚠️ [LocalWS] User {user_id} has no active connection")
                return False

            message_json = json.dumps(message, ensure_ascii=False)
            logging.info(f"📤 [LocalWS] Sending message to user {user_id}")
            await self._send_to_local_connection(user_id, message_json)
            return True
        except Exception as e:
            logging.error(f"❌ [LocalWS] Send message failed: {e}")
            return False

    async def send_progress_update(
            self,
            user_id: int,
            message_id: str,
            status: str,
            progress: int = None,
            message: str = None,
            file_type: str = "genetic",
            filename: str = None,
            success: bool = False,
            raw: str = "",
            url_thumb: str = "",
            url_full: str = "",
            processing_stats: dict = None,
            file_size: int = None,
            processed_records: int = None,
            saved_records: int = None,
            total_estimated: int = None,
            **kwargs,
    ):
        """Send file processing progress update"""
        logging.info(
            f"🔍 [LocalWS] Sending progress: user_id={user_id}, status={status}, message_id={message_id}"
        )
        logging.info(
            f"🔍 [LocalWS] Active connections: {len(self.local_connections)}, users: {list(self.local_connections.keys())}"
        )

        if user_id not in self.local_connections:
            logging.info(f"⚠️ [LocalWS] User {user_id} has no active WebSocket connection")
            logging.info("📝 [LocalWS] File processing will continue, progress saved to database")
            return False

        update_data = {
            "messageId": message_id,
            "status": status,
            "type": file_type,
            "filename": filename,
            "message": message,
            "success": success,
            "raw": raw,
            "url_thumb": url_thumb,
            "url_full": url_full,
        }

        if progress is not None:
            update_data["progress"] = progress
        if file_size is not None:
            update_data["file_size"] = file_size
        if processed_records is not None:
            update_data["processed_records"] = processed_records
        if saved_records is not None:
            update_data["saved_records"] = saved_records
        if total_estimated is not None:
            update_data["total_estimated"] = total_estimated
        if processing_stats is not None:
            update_data["processing_stats"] = processing_stats

        for key, value in kwargs.items():
            if key not in update_data and value is not None:
                update_data[key] = value

        success = await self.send_message_to_user(user_id, update_data)

        if success:
            logging.info(f"✅ [LocalWS] Progress sent: user_id={user_id}, status={status}")
        else:
            logging.info(f"❌ [LocalWS] Progress failed: user_id={user_id}, status={status}")

        return success

    async def get_connection_stats(self):
        """Get connection statistics"""
        return {
            "current_instance": self.instance_id,
            "local_connections": len(self.local_connections),
            "total_connections": len(self.local_connections),
            "instance_distribution": {self.instance_id: len(self.local_connections)},
        }

    async def cleanup(self):
        """Clean up resources"""
        try:
            self.local_connections.clear()
            self.active_connections = self.local_connections
            logging.info(f"🧹 [LocalWS] Instance {self.instance_id} cleanup completed")
        except Exception as e:
            logging.error(f"❌ [LocalWS] Cleanup failed: {e}")


# Global WebSocket manager instance
distributed_ws_manager = None


def get_distributed_ws_manager():
    global distributed_ws_manager
    if distributed_ws_manager:
        return distributed_ws_manager
    distributed_ws_manager = LocalWebSocketManager(namespace="file_progress")
    return distributed_ws_manager
