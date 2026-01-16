"""
Theta utility functions

Time formatting, data formatting, and encryption utilities
"""

import base64
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


class ThetaEncryption:
    @staticmethod
    def encrypt(plaintext: str, key: str) -> str:
        try:
            key_bytes = key.encode("utf-8")
            if len(key_bytes) not in [16, 24, 32]:
                key_bytes = key_bytes[:16].ljust(16, b"\0")

            cipher = AES.new(key_bytes, AES.MODE_ECB)

            padded_data = pad(plaintext.encode("utf-8"), AES.block_size)
            encrypted_data = cipher.encrypt(padded_data)

            return base64.b64encode(encrypted_data).decode("utf-8")

        except Exception as e:
            logging.error(f"AES encryption failed: {str(e)}")
            raise

    @staticmethod
    def decrypt(ciphertext: str, key: str) -> str:
        try:
            # Validate input
            if not ciphertext:
                raise ValueError("Ciphertext is empty")
            if not key:
                raise ValueError("Encryption key is empty")

            key_bytes = key.encode("utf-8")
            if len(key_bytes) not in [16, 24, 32]:
                key_bytes = key_bytes[:16].ljust(16, b"\0")

            try:
                encrypted_data = base64.b64decode(ciphertext)
            except Exception as e:
                logging.error(f"Base64 decode failed: {str(e)}")
                raise ValueError(f"Invalid Base64 data: {str(e)}")

            cipher = AES.new(key_bytes, AES.MODE_ECB)

            decrypted_data = cipher.decrypt(encrypted_data)
            plaintext = unpad(decrypted_data, AES.block_size)

            return plaintext.decode("utf-8")

        except Exception as e:
            logging.error(f"AES decryption failed: {str(e)}")
            raise


class ThetaTimeUtils:

    @staticmethod
    def parse_time_to_timestamp(time_str: Optional[str]) -> int:
        try:
            if not time_str:
                return int(datetime.now(timezone.utc).timestamp() * 1000)
            try:
                num = float(time_str)
                return int(num * 1000) if num <= 1e10 else int(num)
            except (ValueError, TypeError):
                pass

            if isinstance(time_str, (int, float)):
                if time_str > 1e10:
                    return int(time_str)
                else:
                    return int(time_str * 1000)

            if isinstance(time_str, str):
                if time_str.endswith("Z"):
                    time_str = time_str[:-1] + "+00:00"

                dt = datetime.fromisoformat(time_str)
                return int(dt.timestamp() * 1000)

            return int(datetime.now(timezone.utc).timestamp() * 1000)

        except Exception as e:
            logging.error(f"Error parsing time {time_str}: {str(e)}")
            return int(datetime.now(timezone.utc).timestamp() * 1000)

    @staticmethod
    def parse_datetime_string(time_str: str) -> datetime:
        try:
            formats = [
                "%Y-%m-%dT%H:%M:%S%z",  # ISO 8601 with timezone
                "%Y-%m-%dT%H:%M:%S.%f%z",  # ISO 8601 with microseconds and timezone
                "%Y-%m-%dT%H:%M:%S",  # ISO 8601 without timezone
                "%Y-%m-%dT%H:%M:%S.%f",  # ISO 8601 with microseconds
                "%Y-%m-%d",  # Date only
                "%Y-%m-%d %H:%M:%S",  # Standard datetime
                "%Y-%m-%dT%H:%M:%S+00:00",  # ISO with explicit UTC
            ]

            for fmt in formats:
                try:
                    dt = datetime.strptime(time_str, fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except ValueError:
                    continue

            if "T" in time_str:
                if time_str.endswith("Z"):
                    time_str = time_str[:-1] + "+00:00"
                elif "+" in time_str[-6:] or time_str[-6:-3] == "-":
                    pass
                else:
                    time_str += "+00:00"

                try:
                    dt = datetime.fromisoformat(time_str)
                    return dt
                except:
                    pass

            logging.error(f"Could not parse datetime string '{time_str}', using current time")
            return datetime.now(timezone.utc)

        except Exception as e:
            logging.error(f"Error parsing datetime string '{time_str}': {str(e)}")
            return datetime.now(timezone.utc)

    @staticmethod
    def is_token_expired(expires_at: Optional[int]) -> bool:
        if not expires_at:
            return True

        current_time = int(time.time() * 1000)
        return current_time >= expires_at


class ThetaDataFormatter:

    @staticmethod
    def get_unit_for_indicator(indicator: str) -> str:
        try:
            if "(" in indicator and ")" in indicator:
                return indicator.split("(")[1].split(")")[0]
            return ""
        except Exception:
            return ""

    @staticmethod
    def format_source_name(provider_slug: str, device_info: Optional[str] = None) -> str:
        base_source = f"theta.{provider_slug}"
        if device_info:
            return f"{base_source}.{device_info}"
        return base_source

    @staticmethod
    def create_empty_health_data() -> List[Dict[str, Any]]:
        return []


class ThetaHttpUtils:

    @staticmethod
    def create_encrypted_request(data: Dict[str, Any], encryption_key: str) -> Dict[str, Any]:
        try:
            # Validate encryption key
            if not encryption_key:
                raise ValueError("Encryption key is empty")
            
            serialized_data = json.dumps(data, ensure_ascii=False)

            encrypted_data = ThetaEncryption.encrypt(serialized_data, encryption_key)

            return {"encryptData": encrypted_data}

        except Exception as e:
            logging.error(f"Failed to create encrypted request: {str(e)}")
            raise

    @staticmethod
    def parse_encrypted_response(response_data: Dict[str, Any], encryption_key: str) -> Dict[str, Any]:
        try:
            if not ThetaHttpUtils.is_response_success(response_data):
                error_msg = response_data.get("msg") or response_data.get("message", "Unknown error")
                code = response_data.get("code", 0)
                logging.error(f"API response indicates failure: code={code}, msg={error_msg}")
                raise ValueError(f"{error_msg}")

            encrypted_data = response_data.get("data", "")
            if not encrypted_data:
                logging.error("No encrypted data in response")
                raise ValueError("No encrypted data in response")

            # Validate encrypted_data type
            if not isinstance(encrypted_data, str):
                logging.error(f"Encrypted data is not a string: type={type(encrypted_data)}")
                raise ValueError(f"Encrypted data must be a string, got {type(encrypted_data)}")

            decrypted_data = ThetaEncryption.decrypt(encrypted_data, encryption_key)

            try:
                return json.loads(decrypted_data)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse decrypted JSON: {str(e)}")
                raise

        except Exception as e:
            logging.error(f"Failed to parse encrypted response: {str(e)}")
            raise

    @staticmethod
    def is_response_success(response_data: Dict[str, Any]) -> bool:
        msg = response_data.get("msg", "").lower()
        code = response_data.get("code", 0)
        return msg == "success" or code == 200

    @staticmethod
    def create_headers(
            token: Optional[str] = None,
            user_id: Optional[int] = None,
            app_version: str = "6.16.1",
            platform: str = "android",
    ) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "appVersion": app_version,
            "platform": platform,
        }

        if token:
            headers["token"] = token

        if user_id:
            headers["userId"] = str(user_id)

        return headers


class ThetaValidationUtils:

    @staticmethod
    def validate_credentials(username: str, password: str) -> None:
        if not username or not password:
            raise ValueError("Username and password are required")

        if not username.strip() or not password.strip():
            raise ValueError("Username and password cannot be empty")

        if "@" not in username:
            raise ValueError("Username should be an email address")

    @staticmethod
    def validate_api_response(response_data: Any, expected_fields: List[str]) -> None:
        if not isinstance(response_data, dict):
            raise ValueError("Response data must be a dictionary")

        missing_fields = []
        for field in expected_fields:
            if field not in response_data:
                missing_fields.append(field)

        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")


__all__ = [
    "ThetaEncryption",
    "ThetaTimeUtils",
    "ThetaDataFormatter",
    "ThetaHttpUtils",
    "ThetaValidationUtils",
]
