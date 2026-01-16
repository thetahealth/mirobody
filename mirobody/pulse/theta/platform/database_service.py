"""
Database service for Theta providers
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from mirobody.pulse.core import LinkType
from mirobody.utils import execute_query
from mirobody.utils.utils_encrypt import decrypt_string_aes_gcm, encrypt_string_aes_gcm


class ThetaDatabaseService:
    """
    Theta platform Database Service

    Handles Theta Provider connection-related database operations using AES-GCM encryption only
    """

    def __init__(self):
        """Initialize database service"""
        pass

    # Shared credentials payload for database operations
    @dataclass
    class ThetaCredentials:
        username: Optional[str] = None
        password: Optional[str] = None
        access_token: Optional[str] = None
        access_token_secret: Optional[str] = None
        refresh_token: Optional[str] = None
        expires_at: Optional[Union[int, Any]] = None
        connect_info: Optional[Dict[str, Any]] = None  # Additional connection information

    def _decrypt_password_aes_gcm(self, encrypted_password: str, user_id: str) -> Optional[str]:
        """
        Decrypt password using AES-GCM algorithm (matching Go implementation)

        Now using correct key handling method that matches Go's []byte(string) conversion.

        Args:
            encrypted_password: AES-GCM encrypted password string (base64 encoded)
            user_id: User ID for logging

        Returns:
            Decrypted password or None if decryption fails
        """
        if not encrypted_password:
            logging.info(f"Empty password for user {user_id}")
            return None

        logging.info(f"Decrypting password for user {user_id} using AES-GCM (length: {len(encrypted_password)})")

        try:
            decrypted = decrypt_string_aes_gcm(encrypted_password)
            if decrypted is not None:
                logging.info(f"AES-GCM decryption successful for user {user_id}")
                return decrypted
            else:
                logging.error(f"AES-GCM decryption failed for user {user_id}: returned None")
                return None
        except Exception as e:
            error_msg = str(e)
            if "InvalidTag" in error_msg:
                logging.error(
                    f"AES-GCM InvalidTag error for user {user_id}: Authentication tag verification failed. "
                    + f"Encrypted data: {encrypted_password[:20]}... "
                    + "This may indicate data corruption or encryption key mismatch."
                )
            else:
                logging.error(f"AES-GCM decryption error for user {user_id}: {error_msg}")
            return None

    async def get_all_user_credentials_for_provider(self, provider_slug: str, link_type: LinkType) -> List[Dict[str, Any]]:
        """
        Get all user credentials for a specified provider
        
        Only returns users with reconnect=0 (normal status), excluding users that need reconnection

        Args:
            provider_slug: provider identifier

        Returns:
            List of user credentials, including user_id, username, password (decrypted)
        """
        try:
            # Scope selected columns to the requested link_type to avoid unnecessary decrypts
            # Filter out users with reconnect=1 (need reconnection)
            if link_type == LinkType.PASSWORD:
                query = """
                SELECT user_id, username, password
                FROM theta_ai.health_user_provider
                WHERE provider = :provider AND is_del = FALSE AND reconnect = 0
                ORDER BY create_at DESC
                """
            elif link_type == LinkType.OAUTH1:
                query = """
                SELECT user_id, username, access_token, access_token_secret
                FROM theta_ai.health_user_provider
                WHERE provider = :provider AND is_del = FALSE AND reconnect = 0
                ORDER BY create_at DESC
                """
            elif link_type == LinkType.CUSTOMIZED:
                query = """
                SELECT user_id, connect_info
                FROM theta_ai.health_user_provider
                WHERE provider = :provider AND is_del = FALSE AND reconnect = 0
                ORDER BY create_at DESC
                """
            else:  # OAUTH2
                query = """
                SELECT user_id, access_token, refresh_token, expires_at
                FROM theta_ai.health_user_provider
                WHERE provider = :provider AND is_del = FALSE AND reconnect = 0
                ORDER BY create_at DESC
                """

            result = await execute_query(
                query=query,
                params={"provider": provider_slug},
            )

            if not result:
                logging.info(f"No users found for provider {provider_slug}")
                return []

            # Decrypt corresponding fields and return credentials list
            credentials = []
            for row in result:
                try:
                    user_id = row["user_id"]
                    entry: Dict[str, Any] = {"user_id": user_id, "link_type": link_type.value.lower()}
                    if link_type == LinkType.PASSWORD:
                        encrypted_password = row.get("password")
                        if encrypted_password:
                            decrypted_password = self._decrypt_password_aes_gcm(encrypted_password, user_id)
                            if decrypted_password is None:
                                logging.error(f"Failed to decrypt password for user {user_id}: password is None after decryption")
                                continue
                            entry["username"] = row.get("username")
                            entry["password"] = decrypted_password
                    elif link_type == LinkType.OAUTH1:
                        at = row.get("access_token")
                        ats = row.get("access_token_secret")
                        entry["access_token"] = self._decrypt_password_aes_gcm(at, user_id) if at else None
                        entry["access_token_secret"] = self._decrypt_password_aes_gcm(ats, user_id) if ats else None
                        if row.get("username"):
                            entry["username"] = row.get("username")
                    elif link_type == LinkType.CUSTOMIZED:
                        # For CUSTOMIZED type, return connect_info as-is (already stored as jsonb)
                        connect_info = row.get("connect_info")
                        if connect_info:
                            entry["connect_info"] = connect_info
                    else:  # OAUTH2
                        at = row.get("access_token")
                        rt = row.get("refresh_token")
                        entry["access_token"] = self._decrypt_password_aes_gcm(at, user_id) if at else None
                        entry["refresh_token"] = self._decrypt_password_aes_gcm(rt, user_id) if rt else None
                        entry["expires_at"] = row.get("expires_at")

                    credentials.append(entry)
                except Exception as e:
                    logging.error(f"Failed to decrypt password for user {row['user_id']}: {str(e)}")
                    continue

            logging.info(f"Found {len(credentials)} users with credentials for provider {provider_slug}")
            return credentials

        except Exception as e:
            logging.error(f"Error getting user credentials for provider {provider_slug}: {str(e)}")
            return []

    async def save_user_theta_provider(
            self,
            app_user_id: str,
            provider_slug: str,
            link_type: LinkType,
            credentials: "ThetaDatabaseService.ThetaCredentials",
    ) -> bool:
        """
        Unified save for PASSWORD / OAUTH1 / OAUTH2 credentials with strong type check by provider.
        Backward compatible entry; old wrappers should delegate here.
        
        Logic:
        - Always soft delete existing record (if any) and create new one
        - New record will have reconnect=0, create_at=now, is_del=FALSE
        - This ensures: only one active record, clean state
        """
        # 1. Soft delete existing record if any
        delete_query = """
        UPDATE theta_ai.health_user_provider
        SET is_del = TRUE, update_at = CURRENT_TIMESTAMP
        WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
        """

        await execute_query(
            query=delete_query,
            params={"user_id": app_user_id, "provider": provider_slug},
        )

        # 2. Insert new record
        # Build dynamic insert based on link_type
        # Always set reconnect=0 for new records (ensures clean state after reconnection)
        fields = ["user_id", "provider", "llm_access", "is_del", "reconnect", "create_at", "update_at"]
        values = [":user_id", ":provider", ":llm_access", ":is_del", ":reconnect", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"]
        params = {"user_id": app_user_id, "provider": provider_slug, "llm_access": 1, "is_del": False, "reconnect": 0}

        if link_type == LinkType.PASSWORD:
            if not credentials.username or not credentials.password:
                raise ValueError("Missing username/password for PASSWORD link type")
            fields += ["username", "password"]
            values += [":username", ":password"]
            params["username"] = credentials.username
            params["password"] = encrypt_string_aes_gcm(credentials.password)
            # Null other tokens implicitly by not setting them
        elif link_type == LinkType.OAUTH1:
            if not credentials.access_token or not credentials.access_token_secret:
                raise ValueError("Missing access_token/access_token_secret for OAUTH1 link type")
            fields += ["access_token", "access_token_secret", "password", "username"]
            values += [":access_token", ":access_token_secret", ":password", ":username"]
            params["access_token"] = encrypt_string_aes_gcm(credentials.access_token)
            params["access_token_secret"] = encrypt_string_aes_gcm(credentials.access_token_secret)
            params["password"] = ""  # Set empty password for OAUTH1
            params["username"] = credentials.username if credentials.username else ""  # Set username or empty string
        elif link_type == LinkType.OAUTH2:
            if not credentials.access_token or not credentials.refresh_token:
                raise ValueError("Missing access_token/refresh_token for OAUTH2 link type")
            fields += ["access_token", "refresh_token", "password", "username"]
            values += [":access_token", ":refresh_token", ":password", ":username"]
            params["access_token"] = encrypt_string_aes_gcm(credentials.access_token)
            params["refresh_token"] = encrypt_string_aes_gcm(credentials.refresh_token)
            params["password"] = ""  # Set empty password for OAUTH2
            params["username"] = credentials.username if credentials.username else ""  # Set username or empty string
            if credentials.expires_at is not None:
                fields.append("expires_at")
                values.append(":expires_at")
                # if int timestamp provided, convert in get_oauth2 save path elsewhere; here accept raw
                params["expires_at"] = credentials.expires_at
        elif link_type == LinkType.CUSTOMIZED:
            if not credentials.connect_info:
                raise ValueError("Missing connect_info for CUSTOMIZED link type")
            fields += ["username", "password"]
            values += [":username", ":password"]
            params["username"] = credentials.connect_info.get("username", "")
            params["password"] = encrypt_string_aes_gcm(credentials.connect_info.get("password", ""))
        else:
            raise ValueError(f"Unsupported link_type: {link_type}")

        # Add connect_info if provided (applicable to all link types)
        if credentials.connect_info is not None:
            import json
            fields.append("connect_info")
            values.append(":connect_info")
            # Store as JSON string, PostgreSQL will convert to jsonb automatically if column type is jsonb
            params["connect_info"] = json.dumps(credentials.connect_info)

        insert_query = f"""
        INSERT INTO theta_ai.health_user_provider ({', '.join(fields)})
        VALUES ({', '.join(values)})
        """

        await execute_query(query=insert_query, params=params)

        logging.info(f"Successfully saved theta provider for user {app_user_id}, provider {provider_slug}, link_type={link_type}")
        return True

    async def get_user_theta_providers(self, user_id: str) -> list[str]:
        try:
            query = """
            SELECT DISTINCT provider 
            FROM theta_ai.health_user_provider
            WHERE user_id = :user_id AND is_del = FALSE and provider like 'theta_%'
            """

            result = await execute_query(query=query, params={"user_id": user_id})

            return [row["provider"] for row in result] if result else []

        except Exception as e:
            logging.error(f"Error getting user theta providers for {user_id}: {str(e)}")
            return []

    async def delete_user_theta_provider(self, user_id: str, provider_slug: str) -> bool:
        query = """
        UPDATE theta_ai.health_user_provider
        SET is_del = TRUE, update_at = CURRENT_TIMESTAMP
        WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
        """

        await execute_query(
            query=query,
            params={"user_id": user_id, "provider": provider_slug},
        )

        logging.info(f"Successfully deleted theta provider {provider_slug} for user {user_id}")
        return True

    async def update_llm_access(self, user_id: str, provider_slug: str, llm_access: int) -> bool:
        """
        Update LLM access permission for a user's provider

        Args:
            user_id: User ID
            provider_slug: Provider identifier
            llm_access: Access level (0: no access, 1: limited access, 2: full access)

        Returns:
            Whether update was successful
        """
        try:
            query = """
            UPDATE theta_ai.health_user_provider
            SET llm_access = :llm_access, update_at = CURRENT_TIMESTAMP
            WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
            """

            result = await execute_query(
                query=query,
                params={
                    "user_id": user_id,
                    "provider": provider_slug,
                    "llm_access": llm_access,
                },
            )

            logging.info(f"Successfully updated LLM access to {llm_access} for user {user_id}, provider {provider_slug}")
            return True

        except Exception as e:
            logging.error(f"Error updating LLM access for user {user_id}, provider {provider_slug}: {str(e)}")
            return False

    async def get_user_theta_providers_with_llm_access(self, user_id: str) -> Dict[str, Dict[str, int]]:
        """
        Get user's theta providers with their LLM access permissions and reconnect status

        Args:
            user_id: User ID

        Returns:
            Dict mapping provider_slug to {"llm_access": int, "reconnect": int}
            Example: {"theta_renpho": {"llm_access": 1, "reconnect": 0}, "theta_libre": {"llm_access": 1, "reconnect": 1}}
        """
        try:
            query = """
            SELECT provider, llm_access, reconnect
            FROM theta_ai.health_user_provider
            WHERE user_id = :user_id AND is_del = FALSE
            """

            result = await execute_query(
                query=query,
                params={"user_id": user_id},
            )

            # Create mapping of provider to llm_access and reconnect
            provider_info_map = {}
            if result:
                for row in result:
                    provider_slug = row["provider"]
                    llm_access = row["llm_access"]
                    reconnect = row["reconnect"]

                    # Skip vital providers (they have vital_ prefix)
                    if not provider_slug.startswith("vital_"):
                        provider_info_map[provider_slug] = {
                            "llm_access": llm_access,
                            "reconnect": reconnect
                        }

            logging.info(f"Retrieved LLM access and reconnect status for {len(provider_info_map)} theta providers for user {user_id}")
            return provider_info_map

        except Exception as e:
            logging.error(f"Error getting theta providers info for user {user_id}: {str(e)}")
            return {}

    async def get_user_credentials(self, user_id: str, provider_slug: str, link_type: LinkType) -> Optional[Dict[str, Any]]:
        """Get user credentials for a given link_type (PASSWORD / OAUTH1 / OAUTH2 / CUSTOMIZED)."""
        try:
            if link_type == LinkType.CUSTOMIZED:
                # For CUSTOMIZED type, return connect_info
                query = """
                SELECT connect_info
                FROM theta_ai.health_user_provider
                WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
                ORDER BY create_at DESC LIMIT 1
                """
                result = await execute_query(query, {"user_id": user_id, "provider": provider_slug})
                if not result:
                    return None
                row = result[0]
                connect_info = row.get('connect_info')
                if not connect_info:
                    return None
                return {
                    "connect_info": connect_info,
                    "link_type": "customized"
                }

            elif link_type == LinkType.PASSWORD:
                # Query password fields AND token fields (access_token, refresh_token, expires_at)
                # Some PASSWORD providers (like FrontierX) also store OAuth2-style tokens
                query = """
                SELECT username, password, access_token, refresh_token, expires_at
                FROM theta_ai.health_user_provider
                WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
                ORDER BY create_at DESC LIMIT 1
                """
                result = await execute_query(query, {"user_id": user_id, "provider": provider_slug})
                if not result:
                    return None
                row = result[0]
                if not row.get('password'):
                    return None
                decrypted_password = self._decrypt_password_aes_gcm(row['password'], user_id)
                if decrypted_password is None:
                    return None

                # Build response with password and optional token fields
                response = {
                    "username": row.get("username"),
                    "password": decrypted_password,
                    "link_type": "password"
                }

                # If token fields exist, decrypt and include them
                if row.get('access_token'):
                    decrypted_access_token = self._decrypt_password_aes_gcm(row['access_token'], user_id)
                    if decrypted_access_token:
                        response["access_token"] = decrypted_access_token

                if row.get('refresh_token'):
                    decrypted_refresh_token = self._decrypt_password_aes_gcm(row['refresh_token'], user_id)
                    if decrypted_refresh_token:
                        response["refresh_token"] = decrypted_refresh_token

                if row.get('expires_at'):
                    response["expires_at"] = row.get('expires_at')

                return response

            elif link_type == LinkType.OAUTH1:
                query = """
                SELECT username, access_token, access_token_secret
                FROM theta_ai.health_user_provider
                WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
                ORDER BY create_at DESC LIMIT 1
                """
                result = await execute_query(query, {"user_id": user_id, "provider": provider_slug})
                if not result:
                    return None
                row = result[0]
                if not row.get('access_token') or not row.get('access_token_secret'):
                    return None
                access_token = self._decrypt_password_aes_gcm(row['access_token'], user_id)
                access_token_secret = self._decrypt_password_aes_gcm(row['access_token_secret'], user_id)
                if not access_token or not access_token_secret:
                    return None
                return {
                    "username": row.get("username"),
                    "access_token": access_token,
                    "access_token_secret": access_token_secret,
                    "link_type": "oauth1",
                }

            else:  # OAUTH2
                query = """
                SELECT access_token, refresh_token, expires_at, username
                FROM theta_ai.health_user_provider
                WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
                ORDER BY create_at DESC LIMIT 1
                """
                result = await execute_query(query, {"user_id": user_id, "provider": provider_slug})
                if not result:
                    return None
                row = result[0]
                if not row.get('access_token') or not row.get('refresh_token'):
                    return None
                access_token = self._decrypt_password_aes_gcm(row['access_token'], user_id)
                refresh_token = self._decrypt_password_aes_gcm(row['refresh_token'], user_id)
                if not access_token or not refresh_token:
                    return None

                return {
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "expires_at": row.get("expires_at"),
                    "username": row.get("username"),
                    "link_type": "oauth2",
                }

        except Exception as e:
            logging.error(f"Failed to get user credentials for {provider_slug}: {str(e)}")
            return None

    # _save_oauth_credentials_common removed after unification into save_user_theta_provider

    async def save_oauth1_credentials(
            self,
            user_id: str,
            provider_slug: str,
            access_token: str,
            access_token_secret: str,
            user_name: Optional[str] = None
    ) -> bool:
        """Backward-compatible wrapper → unified save_user_theta_provider"""
        creds = ThetaDatabaseService.ThetaCredentials(
            access_token=access_token,
            access_token_secret=access_token_secret,
            username=user_name,
        )
        return await self.save_user_theta_provider(user_id, provider_slug, LinkType.OAUTH1, creds)

    async def save_oauth2_credentials(
            self,
            user_id: str,
            provider_slug: str,
            access_token: str,
            refresh_token: str,
            expires_at: Optional[Any] = None,
            user_name: Optional[str] = None
    ) -> bool:
        """
        Backward-compatible wrapper → unified save_user_theta_provider
        
        Args:
            user_id: User ID
            provider_slug: Provider slug 
            access_token: OAuth2 access token
            refresh_token: OAuth2 refresh token
            expires_at: Token expiration timestamp
            user_name: Optional user name (can be used to store patient_id)
        """
        expires_at_value = None
        if expires_at is not None:
            try:
                # Use UTC time to match database CURRENT_TIMESTAMP behavior
                # timestamp without timezone should store UTC time consistently
                # utcfromtimestamp ensures no local timezone conversion
                expires_at_value = datetime.utcfromtimestamp(int(expires_at))
            except (ValueError, TypeError) as e:
                logging.warning(f"Invalid expires_at timestamp {expires_at}: {str(e)}")
                expires_at_value = None

        creds = ThetaDatabaseService.ThetaCredentials(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=expires_at_value,
            username=user_name,
        )
        return await self.save_user_theta_provider(user_id, provider_slug, LinkType.OAUTH2, creds)
