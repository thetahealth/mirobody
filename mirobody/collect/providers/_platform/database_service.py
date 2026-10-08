"""
Database service for providers
"""

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from mirobody.collect.core import LinkType
from mirobody.kernel.ops import is_driver_exception
from mirobody.utils import execute_query
from mirobody.utils.log import secret_fingerprint
from mirobody.utils.crypto import decrypt_string_aes_gcm, encrypt_string_aes_gcm

logger = logging.getLogger(__name__)


class ProviderDatabaseService:
    """
    provider platform Database Service

    Handles provider connection-related database operations using AES-GCM encryption only
    """

    def __init__(self):
        """Initialize database service"""

    # Shared credentials payload for database operations
    @dataclass
    class Credentials:
        username: str | None = None
        password: str | None = None
        access_token: str | None = None
        access_token_secret: str | None = None
        refresh_token: str | None = None
        expires_at: datetime | None = None
        connect_info: dict[str, Any] | None = None  # Additional connection information

    def _decrypt(self, ciphertext: str, user_id: str) -> str | None:
        """A stored secret in the clear, or None when it does not decrypt
        (a corrupted value or a changed key; `decrypt_string_aes_gcm` logs which)."""
        if not ciphertext:
            return None
        plain = decrypt_string_aes_gcm(ciphertext)
        if plain is None:
            # A fingerprint says whether it is the same stored value as last
            # time without carrying the value.
            logger.error("stored secret does not decrypt: user_id=%s fingerprint=%s",
                         user_id, secret_fingerprint(ciphertext))
        return plain

    def _decrypt_connect_info(self, stored: Any, user_id: str) -> dict[str, Any] | None:
        """`connect_info` as saved: one encrypted JSON string, since a
        CUSTOMIZED link keeps its secrets there. A value an earlier release
        stored in the clear is not read: that link has to be made again."""
        plain = self._decrypt(stored, user_id) if isinstance(stored, str) else None
        if plain is None:
            return None
        info = json.loads(plain)
        return info if isinstance(info, dict) else None

    async def get_all_user_credentials_for_provider(self, provider_slug: str, link_type: LinkType) -> list[dict[str, Any]]:
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
                FROM health_user_provider
                WHERE provider = :provider AND is_del = FALSE AND reconnect = 0
                ORDER BY create_at DESC
                """
            elif link_type == LinkType.OAUTH1:
                query = """
                SELECT user_id, username, access_token, access_token_secret
                FROM health_user_provider
                WHERE provider = :provider AND is_del = FALSE AND reconnect = 0
                ORDER BY create_at DESC
                """
            elif link_type == LinkType.CUSTOMIZED:
                query = """
                SELECT user_id, connect_info
                FROM health_user_provider
                WHERE provider = :provider AND is_del = FALSE AND reconnect = 0
                ORDER BY create_at DESC
                """
            else:  # OAUTH2
                query = """
                SELECT user_id, access_token, refresh_token, expires_at
                FROM health_user_provider
                WHERE provider = :provider AND is_del = FALSE AND reconnect = 0
                ORDER BY create_at DESC
                """

            result = await execute_query(
                query=query,
                params={"provider": provider_slug},
            )

            if not result:
                return []

            # Decrypt corresponding fields and return credentials list
            credentials = []
            for row in result:
                user_id = row["user_id"]
                try:
                    entry: dict[str, Any] = {"user_id": user_id, "link_type": link_type.value.lower()}
                    if link_type == LinkType.PASSWORD:
                        encrypted_password = row.get("password")
                        if encrypted_password:
                            decrypted_password = self._decrypt(encrypted_password, user_id)
                            if decrypted_password is None:
                                continue
                            entry["username"] = row.get("username")
                            entry["password"] = decrypted_password
                    elif link_type == LinkType.OAUTH1:
                        at = row.get("access_token")
                        ats = row.get("access_token_secret")
                        entry["access_token"] = self._decrypt(at, user_id) if at else None
                        entry["access_token_secret"] = self._decrypt(ats, user_id) if ats else None
                        if row.get("username"):
                            entry["username"] = row.get("username")
                    elif link_type == LinkType.CUSTOMIZED:
                        connect_info = self._decrypt_connect_info(row.get("connect_info"), user_id)
                        if connect_info is None:
                            continue
                        entry["connect_info"] = connect_info
                    else:  # OAUTH2
                        at = row.get("access_token")
                        rt = row.get("refresh_token")
                        entry["access_token"] = self._decrypt(at, user_id) if at else None
                        entry["refresh_token"] = self._decrypt(rt, user_id) if rt else None
                        entry["expires_at"] = row.get("expires_at")

                    credentials.append(entry)
                except Exception as e:
                    logger.error("credentials unreadable: provider=%s user_id=%s error_type=%s", provider_slug,
                                 user_id, type(e).__name__, exc_info=not is_driver_exception(e))
                    continue
            return credentials

        except Exception as e:
            logger.error("credentials lookup failed: provider=%s error_type=%s", provider_slug, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return []

    async def save_user_theta_provider(
            self,
            app_user_id: str,
            provider_slug: str,
            link_type: LinkType,
            credentials: "ProviderDatabaseService.Credentials",
    ) -> None:
        """
        Save one link's credentials, of any link type, replacing the live one.

        Logic:
        - Soft-delete the existing active row (if any) AND insert the new row in a
          single data-modifying CTE: both run inside the same statement / transaction.
          If anything in the statement fails, PostgreSQL rolls back the whole CTE,
          so we never end up with "old row soft-deleted but new row not inserted"
          which would silently drop the user's credentials.
        """
        # Build dynamic field list based on link_type.
        # reconnect=0 is forced so a successful save always clears any prior
        # "needs reconnect" flag, mirroring the OAuth callback's clean-state contract.
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

        # connect_info holds whatever fields a CUSTOMIZED provider declares,
        # passwords and keys among them, so it is stored encrypted: a jsonb
        # string holding the encrypted JSON object. It used to sit in the
        # clear beside the encrypted copy of its own password.
        if credentials.connect_info is not None:
            fields.append("connect_info")
            values.append(":connect_info")
            params["connect_info"] = json.dumps(encrypt_string_aes_gcm(json.dumps(credentials.connect_info)))

        # Atomic soft-delete + insert via data-modifying CTE.
        # The CTE's UPDATE may match 0 rows (first link), that's fine, the INSERT
        # still runs. Both statements share the same transaction; either both
        # commit or both roll back.
        atomic_query = f"""
        WITH deactivated AS (
            UPDATE health_user_provider
            SET is_del = TRUE, update_at = CURRENT_TIMESTAMP
            WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
            RETURNING 1
        )
        INSERT INTO health_user_provider ({', '.join(fields)})
        VALUES ({', '.join(values)})
        """

        await execute_query(query=atomic_query, params=params)
        logger.info("credentials saved: provider=%s user_id=%s link_type=%s",  # phi: ok LinkType enum value
                    provider_slug, app_user_id, link_type.value)

    async def mark_reconnect(self, user_id: str, provider_slug: str) -> None:
        """Flag a link whose credential the vendor refuses. Every credential
        query asks for `reconnect = 0`, so the pull leaves it alone; the app
        shows it as needing a reconnect; saving credentials again clears it."""
        await execute_query(
            query="""
            UPDATE health_user_provider
            SET reconnect = 1, update_at = CURRENT_TIMESTAMP
            WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
            """,
            params={"user_id": user_id, "provider": provider_slug},
        )

    async def delete_user_theta_provider(self, user_id: str, provider_slug: str) -> None:
        query = """
        UPDATE health_user_provider
        SET is_del = TRUE, update_at = CURRENT_TIMESTAMP
        WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
        """

        await execute_query(
            query=query,
            params={"user_id": user_id, "provider": provider_slug},
        )

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
            UPDATE health_user_provider
            SET llm_access = :llm_access, update_at = CURRENT_TIMESTAMP
            WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
            """

            await execute_query(
                query=query,
                params={
                    "user_id": user_id,
                    "provider": provider_slug,
                    "llm_access": llm_access,
                },
            )

            return True

        except Exception as e:
            logger.error("LLM access update failed: user_id=%s provider=%s error_type=%s", user_id, provider_slug,
                         type(e).__name__, exc_info=not is_driver_exception(e))
            return False

    async def get_user_theta_providers_with_llm_access(self, user_id: str) -> dict[str, dict[str, int]]:
        """
        Get user's theta providers with their LLM access permissions and reconnect status

        Args:
            user_id: User ID

        Returns:
            Dict mapping provider_slug to {"llm_access": int, "reconnect": int}
            Example: {"theta_oura": {"llm_access": 1, "reconnect": 0}, "theta_whoop": {"llm_access": 1, "reconnect": 1}}
        """
        try:
            query = """
            SELECT provider, llm_access, reconnect
            FROM health_user_provider
            WHERE user_id = :user_id AND is_del = FALSE
            """

            result = await execute_query(
                query=query,
                params={"user_id": user_id},
            )

            return {
                row["provider"]: {"llm_access": row["llm_access"], "reconnect": row["reconnect"]}
                for row in result or []
            }

        except Exception as e:
            logger.error("provider links lookup failed: user_id=%s error_type=%s", user_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return {}

    async def get_user_credentials(self, user_id: str, provider_slug: str, link_type: LinkType) -> dict[str, Any] | None:
        """Get user credentials for a given link_type (PASSWORD / OAUTH1 / OAUTH2 / CUSTOMIZED)."""
        try:
            if link_type == LinkType.CUSTOMIZED:
                # For CUSTOMIZED type, return connect_info
                query = """
                SELECT connect_info
                FROM health_user_provider
                WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
                ORDER BY create_at DESC LIMIT 1
                """
                result = await execute_query(query, {"user_id": user_id, "provider": provider_slug})
                if not result:
                    return None
                connect_info = self._decrypt_connect_info(result[0].get("connect_info"), user_id)
                if connect_info is None:
                    return None
                return {
                    "connect_info": connect_info,
                    "link_type": "customized"
                }

            if link_type == LinkType.PASSWORD:
                query = """
                SELECT username, password
                FROM health_user_provider
                WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
                ORDER BY create_at DESC LIMIT 1
                """
                result = await execute_query(query, {"user_id": user_id, "provider": provider_slug})
                if not result:
                    return None
                password = self._decrypt(result[0].get("password") or "", user_id)
                if password is None:
                    return None
                return {"username": result[0].get("username"), "password": password, "link_type": "password"}

            if link_type == LinkType.OAUTH1:
                query = """
                SELECT username, access_token, access_token_secret
                FROM health_user_provider
                WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
                ORDER BY create_at DESC LIMIT 1
                """
                result = await execute_query(query, {"user_id": user_id, "provider": provider_slug})
                if not result:
                    return None
                row = result[0]
                if not row.get('access_token') or not row.get('access_token_secret'):
                    return None
                access_token = self._decrypt(row['access_token'], user_id)
                access_token_secret = self._decrypt(row['access_token_secret'], user_id)
                if not access_token or not access_token_secret:
                    return None
                return {
                    "username": row.get("username"),
                    "access_token": access_token,
                    "access_token_secret": access_token_secret,
                    "link_type": "oauth1",
                }

            # OAUTH2
            query = """
                SELECT access_token, refresh_token, expires_at, username
                FROM health_user_provider
                WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
                ORDER BY create_at DESC LIMIT 1
                """
            result = await execute_query(query, {"user_id": user_id, "provider": provider_slug})
            if not result:
                return None
            row = result[0]
            if not row.get('access_token') or not row.get('refresh_token'):
                return None
            access_token = self._decrypt(row['access_token'], user_id)
            refresh_token = self._decrypt(row['refresh_token'], user_id)
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
            logger.error("credentials lookup failed: provider=%s user_id=%s error_type=%s", provider_slug, user_id,
                         type(e).__name__, exc_info=not is_driver_exception(e))
            return None

    async def save_oauth1_credentials(
            self,
            user_id: str,
            provider_slug: str,
            access_token: str,
            access_token_secret: str,
            user_name: str | None = None
    ) -> None:
        """An OAuth 1.0a token pair; `user_name` keeps the vendor's user id."""
        creds = ProviderDatabaseService.Credentials(
            access_token=access_token,
            access_token_secret=access_token_secret,
            username=user_name,
        )
        await self.save_user_theta_provider(user_id, provider_slug, LinkType.OAUTH1, creds)

    async def save_oauth2_credentials(
            self,
            user_id: str,
            provider_slug: str,
            access_token: str,
            refresh_token: str,
            expires_at: int | None = None,
            user_name: str | None = None
    ) -> None:
        """An OAuth 2.0 token pair; `expires_at` is epoch seconds.

        Stored as a naive UTC datetime: the column is a TIMESTAMP without a
        zone, and an aware value would be shifted by the session's TimeZone
        on the way in. `oauth2._to_epoch_seconds` reads it back as UTC.
        """
        expires_at_value = None
        if expires_at is not None:
            expires_at_value = datetime.fromtimestamp(int(expires_at), UTC).replace(tzinfo=None)
        creds = ProviderDatabaseService.Credentials(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=expires_at_value,
            username=user_name,
        )
        await self.save_user_theta_provider(user_id, provider_slug, LinkType.OAUTH2, creds)
