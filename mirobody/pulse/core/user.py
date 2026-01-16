"""
Theta Platform User Management Service

Provides core functions for user creation, authentication, and linking
"""

import hashlib
import logging
from mirobody.user import UserService as BaseUserService
from mirobody.user.jwt import JwtTokenValidator
from mirobody.utils.config import global_config
from mirobody.utils import execute_query
from typing import Optional, Dict, Any


class ThetaUserService:

    def __init__(self, jwt_secret_key: str = None, token_expires_in: int = 60 * 60 * 24 * 30):
        """
        Initialize Theta user service
        
        Args:
            jwt_secret_key: JWT secret key, if None will retrieve from config
            token_expires_in: Token expiration time (seconds), default 30 days
        """
        self._jwt_secret_key = jwt_secret_key  # Lazy initialization
        self.token_expires_in = token_expires_in
        self._jwt_validator = None  # Lazy initialization
        self._base_user_service = None  # Lazy initialization

    def _ensure_initialized(self):
        """Ensure JWT validator is initialized"""
        if self._jwt_validator is None:
            # Lazy retrieve JWT secret key
            jwt_secret_key = self._jwt_secret_key or global_config().get("JWT_KEY")

            if not jwt_secret_key:
                raise ValueError("JWT_SECRET_KEY is required but not provided")

            # Initialize JWT validator
            self._jwt_validator = JwtTokenValidator(
                key=jwt_secret_key,
                expires_in=self.token_expires_in
            )

            # Initialize base user service
            self._base_user_service = BaseUserService(token_validator=self._jwt_validator)

    @property
    def jwt_validator(self):
        self._ensure_initialized()
        return self._jwt_validator

    @property
    def base_user_service(self):
        self._ensure_initialized()
        return self._base_user_service

    async def generate_token(self, user_id: str, additional_claims: Dict[str, Any] = None) -> str:

        try:
            if additional_claims is None:
                additional_claims = {}

            # Add Theta platform specific claims
            theta_claims = {
                "platform": "theta",
                "token_type": "theta_access_token",
                "client_id": "theta_platform"
            }
            theta_claims.update(additional_claims)

            token = self.jwt_validator.generate_token(user_id, theta_claims)
            logging.info(f"Successfully generated token for user {user_id}")
            return token

        except Exception as e:
            logging.error(f"Failed to generate token for user {user_id}: {str(e)}")
            raise Exception(f"Token generation failed: {str(e)}")

    async def create_user(self, email: str, name: str = "", tz: str = "") -> str:

        if not email or not email.strip():
            raise ValueError("Email is required")

        email = email.strip().lower()
        name = name.strip() if name else ""

        # Prepare user data
        user_data = {
            "email": email,
            "name": name,
            "is_del": False,
            "tz": tz,
        }

        # Check if user already exists
        check_query = """
                SELECT id, name, is_del FROM theta_ai.health_app_user 
                WHERE email = :email
                ORDER BY create_at DESC
                LIMIT 1
            """

        existing_user = await execute_query(
            query=check_query,
            params={"email": email}
        )

        if existing_user and len(existing_user) > 0:
            user = existing_user[0]
            if not user["is_del"]:
                # User exists and not deleted
                logging.info(f"User already exists: {email}, user_id: {user['id']}")
                return str(user["id"])

        # Create new user
        create_query = """
                INSERT INTO theta_ai.health_app_user 
                (is_del, email, name, gender, birth, blood, tz, create_at, update_at)
                VALUES (:is_del, :email, :name, :gender, :birth, :blood, :tz, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING id, name
            """

        create_params = {
            "is_del": user_data["is_del"],
            "email": user_data["email"],
            "name": user_data["name"],
            "gender": user_data.get("gender"),
            "birth": user_data.get("birth", ""),
            "blood": user_data.get("blood", ""),
            "tz": user_data["tz"]
        }

        result = await execute_query(
            query=create_query,
            params=create_params,
            query_type="insert"
        )

        if result:
            # result is a dictionary, directly get ID
            if "id" in result:
                raw_id = result["id"]
                user_id = str(raw_id)
                logging.info(f"Successfully created user: {email}, user_id: {user_id}")

                return user_id
            else:
                logging.error(f"Cannot find 'id' in result: {result}")
                raise Exception("Cannot find user ID in database result")
        raise Exception("Failed to create user - no result returned")

    async def link_user_provider(self, user_id: str, provider_slug: str, provider_user_id: str = "") -> bool:
        try:
            if not user_id or not provider_slug:
                raise ValueError("user_id and provider_slug are required")

            # Check existing associations (including deleted ones)
            check_query = """
                SELECT id, is_del, reconnect, username, create_at
                FROM theta_ai.health_user_provider
                WHERE user_id = :user_id AND provider = :provider
                ORDER BY create_at DESC
                LIMIT 1
            """

            existing_link = await execute_query(
                query=check_query,
                params={"user_id": user_id, "provider": provider_slug}
            )

            if existing_link and len(existing_link) > 0:
                link = existing_link[0]
                active = not link["is_del"]
                reconnect_status = link.get("reconnect", 0)
                
                if active:
                    if reconnect_status == 0:
                        # Link is active and normal, keep it
                        logging.info(f"Existing valid link for user {user_id}, provider {provider_slug}")
                        return True
                    else:
                        # Link is active but needs reconnection, recreate it
                        logging.info(f"Recreating link for user {user_id}, provider {provider_slug} (reconnect={reconnect_status})")
                        delete_query = """
                            UPDATE theta_ai.health_user_provider
                            SET is_del = TRUE, update_at = CURRENT_TIMESTAMP
                            WHERE id = :link_id
                        """
                        await execute_query(
                            query=delete_query,
                            params={"link_id": link["id"]}
                        )
                        logging.info(f"Soft deleted old link for user {user_id}, provider {provider_slug}")
                else:
                    # Link is inactive (deleted), recreate it
                    logging.info(f"Recreating link for user {user_id}, provider {provider_slug} (inactive, active={active})")

            create_query = """
                INSERT INTO theta_ai.health_user_provider 
                (user_id, provider, username, password, llm_access, is_del, reconnect, create_at, update_at)
                VALUES (:user_id, :provider, :username, :password, :llm_access, :is_del, :reconnect, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """

            await execute_query(
                query=create_query,
                params={
                    "user_id": user_id,
                    "provider": provider_slug,
                    "username": provider_user_id,
                    "password": "",
                    "llm_access": 1,
                    "is_del": False,
                    "reconnect": 0
                }
            )

            logging.info(f"Successfully created link for user {user_id}, provider {provider_slug}")
            return True

        except Exception as e:
            error_msg = f"Failed to link user {user_id} to provider {provider_slug}: {str(e)}"
            logging.error(error_msg)
            raise Exception(error_msg)

    async def find_or_create_user_by_provider_id(self, provider_slug: str, provider_user_id: str, tz: str) -> str:
        if not provider_slug or not provider_user_id:
            raise ValueError("provider_slug and provider_user_id are required")

        # 1. Find existing association
        find_query = """
                SELECT user_id, is_del, reconnect, create_at
                FROM theta_ai.health_user_provider
                WHERE provider = :provider AND username = :provider_user_id
                ORDER BY create_at DESC
                LIMIT 1
            """

        existing_link = await execute_query(
            query=find_query,
            params={"provider": provider_slug, "provider_user_id": provider_user_id}
        )

        if existing_link and len(existing_link) > 0:
            link = existing_link[0]
            active = not link["is_del"]
            
            if active:
                # Found valid association, directly return user ID (regardless of reconnect status)
                # reconnect only affects Pull task, does not affect webhook data reception
                logging.info(
                    f"Found existing user for provider {provider_slug}, provider_user_id {provider_user_id}: {link['user_id']}")
                return str(link["user_id"])
            else:
                # Association deleted, restore and reset reconnect=0
                user_id = str(link["user_id"])
                restore_query = """
                    UPDATE theta_ai.health_user_provider
                    SET is_del = FALSE, reconnect = 0, update_at = CURRENT_TIMESTAMP
                    WHERE provider = :provider AND username = :provider_user_id
                """
                await execute_query(
                    query=restore_query,
                    params={"provider": provider_slug, "provider_user_id": provider_user_id}
                )
                logging.info(f"Restored deleted link for provider {provider_slug}, provider_user_id {provider_user_id}: {user_id}")
                return user_id

        user_info = {}
        # Generate default email (if not provided)
        email = user_info.get("email")
        if not email:
            # Use provider_slug and provider_user_id to generate unique email
            email_hash = hashlib.md5(f"{provider_slug}_{provider_user_id}".encode()).hexdigest()[:8]
            email = f"{provider_slug}_{email_hash}@theta.local"

        name = user_info.get("name", f"{provider_slug}_user_{provider_user_id}")

        # Create new user
        user_id = await self.create_user(
            email=email,
            name=name,
            tz=tz,
        )

        # 3. Establish association between user and Provider
        await self.link_user_provider(
            user_id=user_id,
            provider_slug=provider_slug,
            provider_user_id=provider_user_id,
        )

        logging.info(
            f"Successfully created and linked new user for provider {provider_slug}, provider_user_id {provider_user_id}: {user_id}")
        return user_id

    async def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get user information by user ID
        
        Args:
            user_id: User ID
            
        Returns:
            User information dictionary, None if not exists
        """
        try:
            query = """
                SELECT id, email, name, gender, birth, blood, tz, create_at, update_at
                FROM theta_ai.health_app_user
                WHERE id = :user_id AND is_del = FALSE
            """

            result = await execute_query(
                query=query,
                params={"user_id": int(user_id)}
            )

            if result and len(result) > 0:
                return dict(result[0])

            return None

        except Exception as e:
            logging.error(f"Failed to get user {user_id}: {str(e)}")
            return None

    async def get_user_providers(self, user_id: str, provider_prefix: str = None) -> list[Dict[str, Any]]:
        """
        Get user's Provider association list
        
        Args:
            user_id: User ID
            provider_prefix: Provider prefix filter (e.g., "theta_")
            
        Returns:
            Provider association list
        """
        try:
            base_query = """
                SELECT provider, username, llm_access, create_at, update_at
                FROM theta_ai.health_user_provider
                WHERE user_id = :user_id AND is_del = FALSE
            """

            params = {"user_id": user_id}

            if provider_prefix:
                base_query += " AND provider LIKE :provider_prefix"
                params["provider_prefix"] = f"{provider_prefix}%"

            base_query += " ORDER BY create_at DESC"

            result = await execute_query(query=base_query, params=params)

            return [dict(row) for row in result] if result else []

        except Exception as e:
            logging.error(f"Failed to get user providers for {user_id}: {str(e)}")
            return []

    def _generate_unique_email(self, provider_slug: str, provider_user_id: str) -> str:
        provider_hash = hashlib.md5(f"{provider_slug}_{provider_user_id}".encode()).hexdigest()[:8]
        return f"{provider_slug}_{provider_hash}@theta.local"


# Global instance that can be imported and used in other modules
_theta_user_service = None


def get_theta_user_service(jwt_secret_key: str = None, token_expires_in: int = None) -> ThetaUserService:
    """
    Get Theta User Service instance (Singleton pattern)
    
    Args:
        jwt_secret_key: JWT secret key
        token_expires_in: Token expiration time
        
    Returns:
        ThetaUserService instance
    """
    global _theta_user_service

    if _theta_user_service is None:
        # Only pass non-None arguments, let ThetaUserService use default values
        kwargs = {}
        if jwt_secret_key is not None:
            kwargs['jwt_secret_key'] = jwt_secret_key
        if token_expires_in is not None:
            kwargs['token_expires_in'] = token_expires_in

        _theta_user_service = ThetaUserService(**kwargs)

    return _theta_user_service
