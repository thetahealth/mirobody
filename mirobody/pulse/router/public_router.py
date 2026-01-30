"""
Public API Router for Pulse System

Provides user-facing APIs for interacting with the Pulse health data platform
"""

import json
import logging
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from ..core import LinkType
from ..core import ProviderStatus
from ..core.user import get_theta_user_service
# Import platform manager
from ..manager import platform_manager
from ...utils.utils_auth import verify_token, verify_token_optional

# Create router
router = APIRouter(prefix="/api/v1/pulse", tags=["pulse"])


class AuthType(str, Enum):
    """Authentication type enumeration"""

    PASSWORD = "password"
    OAUTH2 = "oauth2"
    TOKEN = "token"
    CUSTOMIZED = "customized"


class LinkProviderRequest(BaseModel):
    """Connect Provider request model"""

    provider_slug: str = Field(..., description="Provider slug")
    platform: str = Field(..., description="Platform name")
    auth_type: AuthType = Field(..., description="Authentication type")

    # Authentication credentials (optional)
    username: Optional[str] = Field(None, description="Username for auth")
    password: Optional[str] = Field(None, description="Password for auth")
    token: Optional[str] = Field(None, description="Token for auth")
    email: Optional[str] = Field(None, description="Email for auth")
    connect_info: Dict[str, Any] = Field(default_factory=dict, description="Authentication credentials for customized")

    # Additional options (optional)
    redirect_url: Optional[str] = Field(None, description="Redirect URL for OAuth")
    return_url: Optional[str] = Field(None, description="Frontend return URL after OAuth completes")
    owner_user_id: Optional[str] = Field(None, description="if sharing device,help link")

class UnlinkProviderRequest(BaseModel):
    """Disconnect Provider request model"""

    provider_slug: str = Field(..., description="Provider slug")
    platform: str = Field(..., description="Platform name")
    owner_user_id: Optional[str] = Field(None, description="if sharing device, help unlink")


class GetLlmAccessRequest(BaseModel):
    """Get LLM access permission request model"""

    provider_slug: str = Field(..., description="Provider identifier")
    platform: str = Field(..., description="Platform name (vital, theta, cgm)")


class UpdateLlmAccessRequest(BaseModel):
    """Update LLM access permission request model"""

    provider_slug: str = Field(..., description="Provider identifier")
    platform: str = Field(..., description="Platform name (vital, theta, cgm)")
    llm_access: bool = Field(..., description="Whether to allow LLM access")


class ThetaTokenRequest(BaseModel):
    """Theta token request model"""

    provider_slug: str = Field(default="", description="Provider slug")
    user_id: str = Field(..., description="User identifier from device manufacturer")
    certification: str = Field(..., description="Authentication credentials from device manufacturer")


class ThetaWebhookData(BaseModel):
    """Theta webhook data model"""

    type: str = Field(..., description="Indicator type")
    value: float = Field(..., description="Measurement value")
    unit: str = Field(..., description="Unit of measurement")
    timestamp: int = Field(..., description="Measurement timestamp in milliseconds")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Optional metadata")


class ThetaWebhookRequest(BaseModel):
    """Theta webhook request model"""

    user_id: str = Field(..., description="User identifier")
    source: Optional[str] = Field(default="", description="Data source")
    timestamp: int = Field(..., description="Request timestamp in milliseconds")
    timezone: Optional[str] = Field(default="", description="Timezone")
    data: List[ThetaWebhookData] = Field(..., description="Health data list")


# ===== Unified Response Models =====


class StandardResponse(BaseModel):
    """Standard response format"""

    code: int = Field(default=0, description="Response code, 0 indicates success")
    msg: str = Field(default="ok", description="Response message")
    data: Dict[str, Any] = Field(default_factory=dict, description="Response data")


class ErrorResponse(BaseModel):
    """Error response format"""

    code: int = Field(default=500, description="Error code")
    msg: str = Field(..., description="Error details")


# Import ConnectInfoField for type hints
from mirobody.pulse.core.models import ConnectInfoField as CoreConnectInfoField


# ProviderInfo model - Unified definition
class ProviderInfo(BaseModel):
    """Provider information - API response format"""

    slug: str = Field(..., description="Provider slug")
    name: str = Field(..., description="Provider name")
    description: str = Field(..., description="Provider description")
    logo: Optional[str] = Field(None, description="Provider logo URL")
    supported: bool = Field(default=True, description="Whether supported")
    auth_type: str = Field(default="oauth", description="Authentication type")
    status: str = Field(default=ProviderStatus.AVAILABLE.value, description="Connection status")
    platform: str = Field(..., description="Platform name")
    connected_at: Optional[str] = Field(default=None, description="Connection time")
    last_sync_at: Optional[str] = Field(default=None, description="Last sync time")
    record_count: Optional[int] = Field(default=0, description="Data record count")
    allow_llm_access: Optional[bool] = Field(default=False, description="AI access permission")
    connect_info_fields: Optional[List[CoreConnectInfoField]] = Field(
        default=None,
        description="Extra connection fields (e.g., host, port for database providers)"
    )


class UserProviderConnection(BaseModel):
    """User Provider connection information - API response format"""

    slug: str = Field(..., description="Provider slug")
    status: str = Field(..., description="Connection status")
    connected_at: Optional[str] = Field(None, description="Connection time")
    last_sync_at: Optional[str] = Field(None, description="Last sync time")


# User-facing interfaces


def _sort_providers_by_priority(providers: List[ProviderInfo]) -> List[ProviderInfo]:
    """
    Sort providers list by priority slugs
    
    Priority order: theta_fitbit, fitbit, theta_whoop, whoop, theta_garmin, garmin
    Others keep original order
    
    Args:
        providers: List of ProviderInfo objects
        
    Returns:
        Sorted list with priority providers first
    """
    priority_slugs = [
        "theta_fitbit", "fitbit",
        "theta_whoop", "whoop",
        "theta_garmin", "garmin"
    ]

    # Separate priority and non-priority providers
    priority_providers = []
    other_providers = []

    # Create a map for priority providers to maintain order
    priority_map = {slug: [] for slug in priority_slugs}

    for provider in providers:
        if provider.slug in priority_slugs:
            priority_map[provider.slug].append(provider)
        else:
            other_providers.append(provider)

    # Build priority list in specified order
    for slug in priority_slugs:
        priority_providers.extend(priority_map[slug])

    # Combine: priority first, then others in original order
    return priority_providers + other_providers


def handle_redirect(request: Request, return_url: str, success: bool, platform: str, provider: str, error_msg: str = None):
    """Build a redirect response for OAuth callback with minimal safe params."""
    from urllib.parse import urlencode
    from fastapi.responses import RedirectResponse

    code = 0
    if not success:
        code = 1
    qs_params = {
        "code": code,
        "success": str(success).lower(),
        "platform": platform,
        "provider": provider,
        "provider_slug": provider,
    }
    if error_msg:
        qs_params["error"] = error_msg

    qs = urlencode(qs_params)
    sep = "?" if "?" not in return_url else "&"
    redirect_url = f"{return_url}{sep}{qs}"

    return RedirectResponse(url=redirect_url, status_code=302)


@router.get("/providers", response_model=Union[StandardResponse, ErrorResponse])
async def get_providers(
        current_user: Optional[str] = Depends(verify_token_optional),
        owner_user_id: Optional[str] = None,
        nocache: bool = False,
        platform: Optional[str] = None,
        status: Optional[str] = None
):
    """
    Get available providers list with optional sharing support
    
    Args:
        owner_user_id: If provided, returns the providers of this shared user (requires authorization)
    """
    try:
        # Determine which user's providers to query
        query_user_id = current_user

        # If owner_user_id is provided, verify sharing permissions
        if owner_user_id and current_user and owner_user_id != current_user:
            from mirobody.utils.utils_user import get_query_user_id

            permission_check = await get_query_user_id(
                user_id=owner_user_id,  # Data owner
                query_user_id=current_user,  # Querier (current user)
                permission=[]  # No specific permission needed for providers list
            )

            if not permission_check.get("success", False):
                return ErrorResponse(
                    code=-1,
                    msg=f"No permission to query providers for user {owner_user_id}"
                )

            query_user_id = owner_user_id
            logging.info(f"User {current_user} querying providers for shared user {owner_user_id}")

        platform_filter = platform  # Rename to avoid variable shadowing
        all_providers = []
        logging.info(f"get_providers platform: {len(platform_manager._platforms.items())}, filter: {platform_filter}")

        for platform_name, platform_obj in platform_manager._platforms.items():
            if platform_filter and platform_filter != platform_name:
                continue
            try:
                if platform_obj.solo and not platform_filter:
                    # For solo platforms (without filter), add only a single virtual provider
                    virtual_provider = ProviderInfo(
                        slug=platform_name,
                        name=platform_name.upper(),
                        description=getattr(platform_obj, 'description', platform_name),
                        logo=getattr(platform_obj, 'logo', ""),
                        supported=True,
                        auth_type=LinkType.PLATFORM.value,
                        status=ProviderStatus.AVAILABLE.value,
                        platform=platform_name,
                    )
                    all_providers.append(virtual_provider)
                    logging.info(f"Added virtual provider for solo platform {platform_name}")
                else:
                    platform_providers = await platform_obj.get_providers(nocache=nocache)
                    for provider in platform_providers:
                        if provider.auth_type.value == provider.auth_type.SERVICE:
                            continue
                        provider_info = ProviderInfo(
                            slug=provider.slug,
                            name=provider.name,
                            description=provider.description,
                            logo=provider.logo,
                            supported=provider.supported,
                            auth_type=provider.auth_type.value,
                            status=ProviderStatus.AVAILABLE.value,
                            platform=platform_name,
                            connect_info_fields=provider.connect_info_fields,
                        )
                        all_providers.append(provider_info)
            except Exception as e:
                logging.error(f"Error getting providers from platform {platform_name}: {str(e)}")
                continue

        connected_providers = []
        unconnected_providers = []
        unsupported_providers = []
        user_providers = []
        if query_user_id:
            try:
                user_providers = await platform_manager.get_user_providers(query_user_id)
                logging.info(f"Updated connection status for user {query_user_id}")
            except Exception as e:
                logging.error(f"Error getting user providers for {query_user_id}: {str(e)}")

        # Create connection info mapping
        user_provider_map = {up.slug: up for up in user_providers}

        for provider in all_providers:
            if provider.slug in user_provider_map:
                user_provider = user_provider_map[provider.slug]
                provider.status = user_provider.status.value
                provider.connected_at = user_provider.connected_at
                provider.last_sync_at = user_provider.last_sync_at
                provider.record_count = user_provider.record_count
                provider.allow_llm_access = user_provider.llm_access > 0
                connected_providers.append(provider)
            else:
                if provider.supported:
                    unconnected_providers.append(provider)
                else:
                    unsupported_providers.append(provider)

        # Sort unconnected_providers by priority
        unconnected_providers = _sort_providers_by_priority(unconnected_providers)

        if status == "connected":
            all_providers = connected_providers
        elif status == "unconnected":
            all_providers = unconnected_providers
        elif status == "unsupported":
            all_providers = unsupported_providers
        else:
            all_providers = connected_providers + unconnected_providers + unsupported_providers

        # Populate provider statistics
        if query_user_id:
            await platform_manager.populate_provider_stats(query_user_id, all_providers)

        user_info = f" for user {query_user_id}" if query_user_id else " (no user context)"
        logging.info(f"Retrieved {len(all_providers)} providers{user_info}")
        return StandardResponse(
            data={"providers": all_providers, "total": len(all_providers)},
        )

    except Exception as e:
        logging.error(f"Error getting providers: {str(e)}")
        return ErrorResponse(code=500, msg=f"Failed to get providers: {str(e)}")


@router.get("/user/providers", response_model=Union[StandardResponse, ErrorResponse])
async def get_user_providers(
        current_user: str = Depends(verify_token),
        owner_user_id: Optional[str] = None
):
    """
    Get user's connected Provider list with optional sharing support

    Get user connections across all Platforms through PlatformManager

    Args:
        current_user: User ID obtained from token
        owner_user_id: If provided, returns the providers of this shared user (requires authorization)
    """
    try:
        # Determine which user's providers to query
        query_user_id = current_user

        # If owner_user_id is provided, verify sharing permissions
        if owner_user_id and owner_user_id != current_user:
            from mirobody.utils.utils_user import get_query_user_id

            permission_check = await get_query_user_id(
                user_id=owner_user_id,  # Data owner
                query_user_id=current_user,  # Querier (current user)
                permission=[]  # No specific permission needed for providers list
            )

            if not permission_check.get("success", False):
                return ErrorResponse(
                    code=-1,
                    msg=f"No permission to query providers for user {owner_user_id}"
                )

            query_user_id = owner_user_id
            logging.info(f"User {current_user} querying user providers for shared user {owner_user_id}")

        # Get user connections across all Platforms through PlatformManager
        provider_list = await platform_manager.get_user_providers(query_user_id)
        return StandardResponse(
            code=0,
            msg="ok",
            data={"providers": provider_list, "total": len(provider_list)},
        )

    except Exception as e:
        logging.error(f"Error getting user providers: {str(e)}")
        return ErrorResponse(code=500, msg=f"Failed to get user providers: {str(e)}")


@router.post("/user/providers/link", response_model=Union[StandardResponse, ErrorResponse])
async def link_provider(request: LinkProviderRequest, req: Request, current_user: str = Depends(verify_token)):
    """
    Connect Provider

    Args:
        request: Connection request
        current_user: User ID obtained from token
    """
    try:
        query_user_id = current_user

        # If owner_user_id is provided, verify sharing permissions
        if request.owner_user_id and request.owner_user_id != current_user:
            from mirobody.utils.utils_user import get_query_user_id

            permission_check = await get_query_user_id(
                user_id=request.owner_user_id,  # Data owner
                query_user_id=current_user,  # Querier (current user)
                permission=["device"]  # Check 'all' permission for provider linking
            )

            if not permission_check.get("success", False):
                return ErrorResponse(
                    code=-1,
                    msg=f"No permission to link provider for user {request.owner_user_id}"
                )

            all_permission = permission_check.get("permissions", {}).get("device", 0)
            if all_permission < 2:
                return ErrorResponse(
                    code=-1,
                    msg="Insufficient permission to link provider. Write access required."
                )

            query_user_id = request.owner_user_id
            logging.info(f"User {current_user} linking provider for shared user {request.owner_user_id}")

        # Auto-detect correct platform (based on provider_slug prefix)
        actual_platform = request.platform
        provider_slug = request.provider_slug

        # Theta platform: theta_ prefix
        if provider_slug.startswith("theta_"):
            actual_platform = "theta"
            logging.info(f"Auto-detected platform 'theta' from provider_slug '{provider_slug}'")

        # Build credentials dictionary
        credentials = {}
        if request.username:
            credentials["username"] = request.username
        if request.password:
            credentials["password"] = request.password
        if request.token:
            credentials["token"] = request.token
        if request.email:
            credentials["email"] = request.email
        if request.connect_info:
            credentials["connect_info"] = request.connect_info


        options = {}
        if request.redirect_url:
            options["redirect_url"] = request.redirect_url
        if hasattr(request, "return_url") and request.return_url:
            options["return_url"] = request.return_url

        host = req.headers.get("Host", "unknown")
        scheme = req.url.scheme if req.url.scheme else "https"
        options["default_return_url"] = f"{scheme}://{host}/api/v1/pulse/{actual_platform}/{provider_slug}/callback"

        # Call PlatformManager's simplified interface (business logic has been delegated)
        result_data = await platform_manager.link_provider(
            user_id=query_user_id,  # Use query_user_id instead of current_user to support sharing
            provider_slug=provider_slug,
            platform=actual_platform,
            auth_type=request.auth_type,
            credentials=credentials,
            options=options,
        )
        logging.info(f"Link successful for provider {provider_slug}")
        return StandardResponse(code=0, msg="ok", data=result_data)

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return ErrorResponse(code=400, msg=f"{str(e)}")


def _generate_oauth_completion_html(platform: str, provider: str, success: bool, result_data: Any = None,
                                    error_message: str = None) -> str:
    """Generate OAuth completion HTML with postMessage to parent window"""

    # Prepare data for JavaScript
    data_js = json.dumps(result_data) if result_data else 'null'
    error_js = json.dumps(error_message) if error_message else 'null'

    return f"""
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8" />
        <meta http-equiv="Cache-Control" content="no-store" />
      </head>
      <body style="margin:0;padding:0;background:#fff;">
        <script>
          (function() {{
            try {{
              if (window.opener) {{
                var messageType;
                if ('{platform}' === 'theta') {{
                  // For theta providers: use provider name (e.g., GARMIN_OAUTH_COMPLETE)
                  var providerName = '{provider}'.replace('theta_', '').toUpperCase();
                  messageType = providerName + '_OAUTH_COMPLETE';
                }} else {{
                  // For other platforms: use platform-specific message
                  messageType = '{platform.upper()}_OAUTH_COMPLETE';
                }}
                
                var message = {{
                  type: messageType,
                  success: {str(success).lower()},
                  provider: '{provider}',
                  platform: '{platform}'
                }};
                
                if ({str(success).lower()}) {{
                  message.data = {data_js};
                  console.log('OAuth complete signal sent to parent window');
                }} else {{
                  message.error = {error_js};
                  console.log('OAuth error signal sent to parent window');
                }}
                
                window.opener.postMessage(message, '*');
              }} else {{
                console.warn('No window.opener found, cannot send completion signal');
              }}
            }} catch (e) {{ 
              console.error('Error sending OAuth completion signal:', e); 
            }}
            setTimeout(function() {{ 
              console.log('Closing OAuth popup window');
              window.close(); 
            }}, 100);
          }})();
        </script>
      </body>
    </html>
    """


@router.get("/{platform}/{provider}/callback")
async def oauth_callback(platform: str, provider: str, request: Request):
    """
    OAuth callback endpoint: {host}/api/v1/pulse/{platform}/{provider}/callback

    Query params expected (OAuth 1.0a typical): oauth_token, oauth_verifier
    """
    try:
        params = dict(request.query_params)

        logging.info(f"OAuth callback received - platform: {platform}, provider: {provider}")

        # Validation delegated to provider

        # Get platform and provider instances
        platform_instance = platform_manager.get_platform(platform)
        if not platform_instance:
            return ErrorResponse(code=500, msg=f"{platform.title()} platform not available")

        provider_instance = platform_instance.get_provider(provider)
        if not provider_instance:
            return ErrorResponse(code=500, msg=f"{provider} provider not available")

        # Call provider's callback method with appropriate parameters based on OAuth version
        auth_type = provider_instance.info.auth_type

        result = None
        if auth_type == LinkType.OAUTH2 or auth_type == LinkType.OAUTH:
            # OAuth2 callback parameters (OAuth2 or legacy OAUTH defaults to OAuth2)
            code = params.get("code")
            state = params.get("state")
            return_url = params.get("return_url")
            if state == "success" and return_url:
                return handle_redirect(request, return_url, True, platform, provider)
            if not code:
                return ErrorResponse(code=400, msg="Missing OAuth2 authorization code")
            result = await provider_instance.callback(code, state)
            if isinstance(result, dict) and result.get("return_url"):
                return handle_redirect(request, result["return_url"], True, platform, provider)
        elif auth_type == LinkType.OAUTH1:
            # OAuth1 callback parameters
            oauth_token = params.get("oauth_token")
            oauth_verifier = params.get("oauth_verifier")
            if not oauth_token or not oauth_verifier:
                return ErrorResponse(code=400, msg="Missing OAuth1 parameters")
            result = await provider_instance.callback(oauth_token, oauth_verifier)
            return_url = request.query_params.get("return_url")
            if return_url:
                return handle_redirect(request, return_url, True, platform, provider)
        else:
            return ErrorResponse(code=400, msg=f"Unsupported auth type: {auth_type}")

        return HTMLResponse(content=_generate_oauth_completion_html(platform, provider, True, result, None))

    except Exception as e:
        logging.error(f"OAuth callback error for {platform}/{provider}: {str(e)}")

        # Return OAuth error HTML
        return HTMLResponse(content=_generate_oauth_completion_html(platform, provider, False, None, str(e)))


@router.post("/user/providers/unlink", response_model=Union[StandardResponse, ErrorResponse])
async def unlink_provider(request: UnlinkProviderRequest, current_user: str = Depends(verify_token)):
    """
    Unlink Provider connection with optional sharing support

    Args:
        request: Unlink request
        current_user: User ID obtained from token
    """
    try:
        query_user_id = current_user

        # If owner_user_id is provided, verify sharing permissions
        if request.owner_user_id and request.owner_user_id != current_user:
            from mirobody.utils.utils_user import get_query_user_id

            permission_check = await get_query_user_id(
                user_id=request.owner_user_id,  # Data owner
                query_user_id=current_user,  # Querier (current user)
                permission=["device"]  # Check 'device' permission for provider unlinking
            )

            if not permission_check.get("success", False):
                return ErrorResponse(
                    code=-1,
                    msg=f"No permission to unlink provider for user {request.owner_user_id}"
                )

            # Check if user has write permission (level 2) required for unlinking
            device_permission = permission_check.get("permissions", {}).get("device", 0)
            if device_permission < 2:
                return ErrorResponse(
                    code=-1,
                    msg="Insufficient permission to unlink provider. Write access required."
                )

            query_user_id = request.owner_user_id
            logging.info(f"User {current_user} unlinking provider for shared user {request.owner_user_id}")

        # Auto-detect correct platform (based on provider_slug prefix)
        actual_platform = request.platform
        provider_slug = request.provider_slug

        # Theta platform: theta_ prefix
        if provider_slug.startswith("theta_"):
            actual_platform = "theta"
            logging.info(f"Auto-detected platform 'theta' from provider_slug '{provider_slug}'")
        # Other cases use platform passed from frontend
        elif actual_platform != request.platform:
            logging.info(f"Using platform '{actual_platform}' from request")

        # Call PlatformManager interface
        result_data = await platform_manager.unlink_provider(
            user_id=query_user_id,  # Use query_user_id instead of current_user to support sharing
            provider_slug=provider_slug,
            platform=actual_platform,
        )

        logging.info(f"Unlink successful for provider {provider_slug}")
        return StandardResponse(code=0, msg="ok", data=result_data)

    except ValueError as e:
        # Parameter validation error (400)
        logging.error(f"Validation error: {str(e)}")
        return ErrorResponse(code=400, msg=str(e))
    except RuntimeError as e:
        # Business logic error (500)
        logging.error(f"Runtime error: {str(e)}")
        return ErrorResponse(code=500, msg=str(e))
    except Exception as e:
        # Other unknown errors (500)
        logging.error(f"Unexpected error: {str(e)}")
        return ErrorResponse(code=500, msg=f"Failed to unlink provider: {str(e)}")


@router.post(
    "/user/providers/update-llm-access",
    response_model=Union[StandardResponse, ErrorResponse],
)
async def update_llm_access(request: UpdateLlmAccessRequest, current_user: str = Depends(verify_token)):
    """
    Update Provider LLM access permission

    Args:
        request: Update request
        current_user: User ID obtained from token
    """

    try:
        # Auto-detect correct platform (based on provider_slug prefix)
        actual_platform = request.platform
        provider_slug = request.provider_slug

        # Theta platform: theta_ prefix
        if provider_slug.startswith("theta_"):
            actual_platform = "theta"
            logging.info(f"Auto-detected platform 'theta' from provider_slug '{provider_slug}'")

        llm_access = 0
        if request.llm_access:
            llm_access = 1
        # Call PlatformManager to delegate to appropriate platform
        result_data = await platform_manager.update_llm_access(
            user_id=current_user,
            provider_slug=provider_slug,
            platform=actual_platform,
            llm_access=llm_access,
        )

        logging.info(
            f"Updated LLM access for provider {provider_slug} to {request.llm_access} for user {current_user}"
        )
        return StandardResponse(code=0, msg="ok", data=result_data)

    except ValueError as e:
        # Parameter validation error (400)
        logging.error(f"Validation error: {str(e)}")
        return ErrorResponse(code=400, msg=str(e))
    except RuntimeError as e:
        # Business logic error (500)
        logging.error(f"Runtime error: {str(e)}")
        return ErrorResponse(code=500, msg=str(e))
    except Exception as e:
        # Other unknown errors (500)
        logging.error(f"Unexpected error: {str(e)}")
        return ErrorResponse(code=500, msg=f"Failed to update LLM access: {str(e)}")


@router.post("/vital/generate-sign-in-token", response_model=Union[StandardResponse, ErrorResponse])
async def generate_vital_sign_in_token(
        current_user: str = Depends(verify_token)
):
    """
    Generate Vital sign-in token for client

    According to Junction docs, this is for mobile SDK to sign in to Vital system, not for connecting specific devices

    Reference: https://docs.junction.com/wearables/sdks/authentication#vital-sign-in-token

    Args:
        current_user: Current user ID

    Returns:
        StandardResponse: Response containing sign_in_token
        ErrorResponse: Error response
    """
    try:
        # 1. Get vital platform
        vital_platform = platform_manager.get_platform("vital")
        if not vital_platform:
            return ErrorResponse(code=503, msg="Vital platform not available")

        # 2. Ensure user exists in Vital system
        vital_user = await vital_platform.db_service.get_user_by_app_user_id(current_user)
        if not vital_user:
            # Create new user
            await vital_platform.user_service.create_new_user(current_user)
            vital_user = await vital_platform.db_service.get_user_by_app_user_id(current_user)

            if not vital_user:
                return ErrorResponse(code=500, msg="Failed to create vital user")

        # 3. Call Vital API to generate Sign-In Token
        # According to Junction docs, should call POST /v2/user/{user_id}/sign_in_token

        token_result = vital_platform.vital_client.generate_sign_in_token(vital_user.vital_user_id)

        if not token_result.get("success"):
            error_msg = token_result.get("error", "Unknown error")
            logging.error(f"Failed to generate vital sign-in token: {error_msg}")
            return ErrorResponse(code=500, msg=f"Failed to generate sign-in token: {error_msg}")

        # 4. Build response data matching Junction docs format
        # Reference: https://docs.junction.com/wearables/sdks/authentication#vital-sign-in-token
        token_data = token_result.get("data", {})
        response_data = {
            "user_id": vital_user.vital_user_id,
            "sign_in_token": token_data.get("sign_in_token")
        }

        logging.info(f"Generated vital sign-in token for user {current_user}")

        return StandardResponse(
            code=0,
            msg="ok",
            data=response_data
        )

    except ValueError as e:
        # Parameter validation error
        logging.error(f"Validation error generating vital sign-in token: {str(e)}")
        return ErrorResponse(code=400, msg=str(e))
    except RuntimeError as e:
        # Business logic error
        logging.error(f"Runtime error generating vital sign-in token: {str(e)}")
        return ErrorResponse(code=500, msg=str(e))
    except Exception as e:
        # Other unknown errors
        logging.error(f"Unexpected error generating vital sign-in token: {str(e)}")
        return ErrorResponse(code=500, msg=f"Internal error: {str(e)}")


@router.post("/{platform}/webhook", response_model=Union[StandardResponse, ErrorResponse])
async def universal_webhook(platform: str, request: Request):
    """
    Universal Platform Webhook Interface

    Receive data pushes from various Platform Providers
    Provider identification is extracted from request data, each Platform has its own extraction logic

    Args:
        platform: Platform identifier
        request: Raw request object
    """

    try:
        raw_body = await request.body()
        raw_body_str = raw_body.decode("utf-8")
        msg_id = await get_msg_id(request)

        # Parse JSON data
        event_data = json.loads(raw_body_str)
        provider_slug = await get_provider_slug(platform, event_data)

        # Log request
        logging.info(f"Universal webhook received - platform: {platform}, provider_slug: {provider_slug}, msg_id: {msg_id}")
        if not provider_slug:
            logging.warning("provider_slug is None")
        # Call PlatformManager to process data (built-in idempotency based on msg_id)
        success = await platform_manager.post_data(platform, provider_slug, event_data, msg_id)
        if success:
            return StandardResponse(
                data={
                    "message": "Webhook processed successfully",
                    "platform": platform,
                    "provider_slug": provider_slug,
                    "msg_id": msg_id,
                }
            )
        else:
            return ErrorResponse(code=500, msg="Failed to process webhook data")

    except json.JSONDecodeError as e:
        error_msg = f"JSON parse error: {str(e)}"
        logging.error(error_msg)
        return ErrorResponse(code=400, msg=error_msg)

    except Exception as e:
        error_msg = f"Error processing webhook: {str(e)}"
        logging.error(error_msg)
        return ErrorResponse(code=500, msg=error_msg)


@router.post("/{platform}/{provider}/webhook", response_model=Union[StandardResponse, ErrorResponse])
async def provider_specific_webhook(platform: str, provider: str, request: Request):
    """
    Provider-Specific Platform Webhook Interface

    Receive data pushes from specific platform providers with explicit provider identification
    This route is more explicit than the universal webhook and supports better provider isolation

    Args:
        platform: Platform identifier (e.g., "theta", "vital")
        provider: Provider identifier (e.g., "theta_garmin", "theta_whoop", "garmin", "oura")
        request: Raw request object
    """

    try:
        raw_body = await request.body()
        raw_body_str = raw_body.decode("utf-8")
        msg_id = await get_msg_id(request)

        # Parse JSON data
        event_data = json.loads(raw_body_str)

        # Log request with explicit provider info and complete raw data
        logging.info(f"Provider-specific webhook received - platform: {platform}, provider: {provider}, msg_id: {msg_id}")

        # Call PlatformManager to process data with explicit provider
        success = await platform_manager.post_data(platform, provider, event_data, msg_id)
        if success:
            return StandardResponse(
                code=0,
                data={
                    "message": "Provider webhook processed successfully",
                    "platform": platform,
                    "provider": provider,
                    "msg_id": msg_id,
                }
            )
        else:
            return ErrorResponse(code=500, msg="Failed to process provider webhook data")

    except json.JSONDecodeError as e:
        error_msg = f"JSON parse error: {str(e)}"
        logging.error(error_msg)
        return ErrorResponse(code=400, msg=error_msg)

    except Exception as e:
        error_msg = f"Error processing provider webhook: {str(e)}"
        logging.error(error_msg)
        return ErrorResponse(code=500, msg=error_msg)


async def get_provider_slug(platform: str, event_data: Dict[str, Any]) -> Optional[str]:
    try:
        if event_data.get("source") != "":
            return event_data.get("source")
        else:
            # For other platforms, try the nested structure
            return event_data.get("data", {}).get("source", {}).get("provider")
    except Exception as e:
        logging.error(f"Error extracting provider_slug for platform {platform}: {str(e)}")
        return None


async def get_msg_id(request: Request) -> str:
    msg_id = request.headers.get("Svix-Id", "")
    if not msg_id:
        msg_id = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    return msg_id


@router.post("/{platform}/token", response_model=Union[StandardResponse, ErrorResponse])
async def get_theta_token(platform: str, request: ThetaTokenRequest):
    """
    Get Theta User Token API

    Args:
        platform: "theta" only now
        request: include provider_slug、user_id、certification

    Returns:
        StandardResponse: token info
        ErrorResponse: errors
    """
    try:
        if platform != "theta":
            return StandardResponse(data={})  # theta only now

        provider_slug = request.provider_slug
        user_id = request.user_id
        certification = request.certification

        logging.info(f"Get theta token request - provider_slug: {provider_slug}, user_id: {user_id}")

        platform_entity = platform_manager.get_platform(platform)
        if not platform_entity:
            logging.error("platform not available")
            return ErrorResponse(code=503, msg="Theta platform not available")

        provider = platform_entity.get_provider(provider_slug)
        if not provider:
            logging.error(f"Provider {provider_slug} not found")
            return ErrorResponse(code=404, msg=f"Provider {provider_slug} not found")

        await provider._validate_credentials(user_id, certification)
        user = get_theta_user_service()
        app_user_id = await user.find_or_create_user_by_provider_id(provider_slug, user_id, "America/Los_Angeles")
        token = await user.generate_token(app_user_id)

        # 5. Build response data
        response_data = {
            "user_id": user_id,
            "token": token,
            "expires_in": 60 * 60 * 24 * 30,
            "token_type": "Bearer",
            "provider_slug": provider_slug
        }

        logging.info(f"Successfully generated token for user {user_id} with provider {provider_slug}")

        return StandardResponse(
            code=0,
            msg="ok",
            data=response_data
        )

    except ValueError as e:
        logging.error(f"Validation error in get_theta_token: {str(e)}")
        return ErrorResponse(code=400, msg=str(e))
    except Exception as e:
        logging.error(f"Unexpected error in get_theta_token: {str(e)}")
        return ErrorResponse(code=500, msg=f"Internal error: {str(e)}")


@router.get("/theta/indicators", response_model=Union[StandardResponse, ErrorResponse])
async def get_theta_indicators():
    """
    Get supported indicators information for Theta platform

    Provides standard health indicators, units and description information for device manufacturers
    Now uses the same data source as manage interface (get_all_indicators_info), dynamically filters indicators by category
    
    Supported categories: vital signs, body composition, activity, metabolic, sleep, performance
    When new indicators are added to manage, theta interface will automatically include new indicators under these categories

    Returns:
        StandardResponse: Contains indicator information list
        ErrorResponse: Get failure error
    """
    try:
        # Use manage data source but maintain theta filtering logic
        from ..core import get_all_indicators_info

        # Get complete manage data
        manage_data = get_all_indicators_info()

        indicators_info = []
        categories_info = {}

        # Define theta supported indicator categories (dynamic filtering, automatically includes all indicators in these categories)
        theta_supported_categories = {
            "vital_signs",  # Vital signs
            "body_composition",  # Body composition
            "activity",  # Activity indicators
            "metabolic",  # Metabolic indicators
            "sleep",  # Sleep indicators
            "performance"  # Performance
        }

        # Define supported units for theta interface (temporary solution)
        theta_supported_units = {
            "heartRates": ["count/min", "bpm"],
            "bodyMasss": ["kg", "lb", "g"],
            "bloodGlucoses": ["mg/dL", "mmol/L"],
            "bodyTemperatures": ["°C", "°F"],
            "systolicPressures": ["mmHg", "kPa"],
            "diastolicPressures": ["mmHg", "kPa"],
        }

        # Filter theta supported indicators by category from manage data
        if "categories" in manage_data:
            for category_key, category_data in manage_data["categories"].items():
                # Only process theta supported categories
                if category_key not in theta_supported_categories:
                    continue

                # Add category information
                categories_info[category_key] = {
                    "name": category_key,
                    "display_name": category_data["name"],
                    "display_name_en": category_data["name_en"]
                }

                # Process all indicators under this category
                if "indicators" in category_data:
                    for indicator_data in category_data["indicators"]:
                        try:
                            indicator_key = indicator_data["key"]

                            # Convert manage data to theta format
                            indicator_info = {
                                "name": indicator_data["key"],
                                "display_name": indicator_data["name"],
                                "display_name_en": indicator_data["name_en"],
                                "description": indicator_data["description"],
                                "description_en": indicator_data.get("description_en", indicator_data["description"]),
                                "standard_unit": indicator_data["standard_unit"],
                                "supported_units": theta_supported_units.get(indicator_key, indicator_data.get("supported_units", [indicator_data["standard_unit"]])),
                                "data_type": "numeric",
                                "category": category_key
                            }

                            indicators_info.append(indicator_info)

                        except Exception as e:
                            logging.error(f"Error processing indicator {indicator_data.get('key', 'unknown')}: {str(e)}")
                            continue

        # Build response data
        response_data = {
            "indicators": indicators_info,
            "categories": list(categories_info.values()),
            "total": len(indicators_info)
        }

        logging.info(f"Successfully retrieved {len(indicators_info)} indicators information using manage data source")

        return StandardResponse(
            code=0,
            msg="ok",
            data=response_data
        )

    except Exception as e:
        logging.error(f"Unexpected error in get_theta_indicators: {str(e)}")
        return ErrorResponse(code=500, msg=f"Failed to get indicators information: {str(e)}")

# Export router
# pulse_public_router = router
