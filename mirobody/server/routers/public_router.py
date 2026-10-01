"""
Public API Router for Pulse System

Provides user-facing APIs for interacting with the Pulse health data platform

SECTION INDEX (line numbers are approximate):
    ~28   Request/Response Models (AuthType, LinkProviderRequest, StandardResponse, ProviderInfo, etc.)
    ~164  Helper: _sort_providers_by_priority()
    ~204  Helper: handle_redirect()
    ~229  GET  /providers              list all available providers
    ~365  GET  /user/providers         list user's connected providers
    ~415  POST /user/providers/link    link a provider (OAuth/password/custom)
    ~502  Helper: _generate_oauth_completion_html()
    ~565  GET  /{platform}/{provider}/callback (OAuth callback handler
    ~626  POST /user/providers/unlink  unlink a provider
    ~701  POST /user/providers/llm-access) update LLM access permission
    ~828  POST /{platform}/webhook     universal webhook receiver
    ~879  POST /{platform}/{provider}/webhook: provider-specific webhook
    ~930  Helper: get_provider_slug(), get_msg_id()
    ~959  POST /{platform}/token       get theta token
    ~1022 GET  /theta/indicators       list theta indicators
"""

import hmac
import json
import logging
import os
from datetime import datetime
from enum import Enum
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from mirobody.collect import LinkType
from mirobody.collect import ProviderStatus
from mirobody.user.platform import get_platform_user_service
# Import platform manager
from mirobody.collect import platform_manager
from mirobody.server.auth import verify_token, verify_token_optional
from mirobody.utils.config import global_config
from mirobody.utils.http import request_origin, safe_return_url

logger = logging.getLogger(__name__)

#: Where these routes answer. The path says `pulse` because that was the
#: package's name when deployments first registered their OAuth redirect URIs
#: with Garmin, Oura and Whoop: those are registered in the vendor's console,
#: against the deployment's own host, and we cannot change them from here.
#: A deployment that has registered nothing yet, or is willing to re-register,
#: can set COLLECT_API_PREFIX. It is read from the environment rather than the
#: config object because the router is built at import time, before
#: `Config.init` has run.
API_PREFIX = os.environ.get("COLLECT_API_PREFIX", "/api/v1/pulse").rstrip("/")

router = APIRouter(prefix=API_PREFIX, tags=["collect"])


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
    username: str | None = Field(None, description="Username for auth")
    password: str | None = Field(None, description="Password for auth")
    token: str | None = Field(None, description="Token for auth")
    email: str | None = Field(None, description="Email for auth")
    connect_info: dict[str, Any] = Field(default_factory=dict, description="Authentication credentials for customized")

    # Additional options (optional)
    redirect_url: str | None = Field(None, description="Redirect URL for OAuth")
    return_url: str | None = Field(None, description="Frontend return URL after OAuth completes")
    owner_user_id: str | None = Field(None, description="if sharing device,help link")


class UnlinkProviderRequest(BaseModel):
    """Disconnect Provider request model"""

    provider_slug: str = Field(..., description="Provider slug")
    platform: str = Field(..., description="Platform name")
    owner_user_id: str | None = Field(None, description="if sharing device, help unlink")




class UpdateLlmAccessRequest(BaseModel):
    """Update LLM access permission request model"""

    provider_slug: str = Field(..., description="Provider identifier")
    platform: str = Field(..., description="Platform name (vital, theta, cgm)")
    llm_access: bool = Field(..., description="Whether to allow LLM access")


class ProviderTokenRequest(BaseModel):
    """Provider token request model"""

    provider_slug: str = Field(default="", description="Provider slug")
    user_id: str = Field(..., description="User identifier from device manufacturer")
    certification: str = Field(..., description="Authentication credentials from device manufacturer")


from mirobody.server.envelope import ErrorResponse, StandardResponse

# Import ConnectInfoField for type hints
from mirobody.user.care_circle import CareCircleDenied, resolve_subject
from mirobody.collect import ConnectInfoField as CoreConnectInfoField


# ProviderInfo model - Unified definition
class ProviderInfo(BaseModel):
    """Provider information - API response format"""

    slug: str = Field(..., description="Provider slug")
    name: str = Field(..., description="Provider name")
    description: str = Field(..., description="Provider description")
    logo: str | None = Field(None, description="Provider logo URL")
    supported: bool = Field(default=True, description="Whether supported")
    auth_type: str = Field(default="oauth", description="Authentication type")
    status: str = Field(default=ProviderStatus.AVAILABLE.value, description="Connection status")
    platform: str = Field(..., description="Platform name")
    connected_at: str | None = Field(default=None, description="Connection time")
    last_sync_at: str | None = Field(default=None, description="Last sync time")
    record_count: int | None = Field(default=0, description="Data record count")
    allow_llm_access: bool | None = Field(default=False, description="AI access permission")
    connect_info_fields: list[CoreConnectInfoField] | None = Field(
        default=None,
        description="Extra connection fields (e.g., host, port for database providers)"
    )




# User-facing interfaces


def _sort_providers_by_priority(providers: list[ProviderInfo]) -> list[ProviderInfo]:
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
    """Build a redirect response for OAuth callback with minimal safe params.

    Only to where `safe_return_url` keeps it: this origin, the CORS origin the
    deployment allows, and `OAUTH_RETURN_ORIGINS`. Anywhere else gets the
    completion page this callback shows when there is no `return_url`.
    """
    from urllib.parse import urlencode
    from fastapi.responses import RedirectResponse

    cfg = global_config()
    allowed = list(cfg.get_list("OAUTH_RETURN_ORIGINS") or []) if cfg else []
    # A list of (name, value) pairs, as `middleware_stack` reads it.
    cors_origin = dict((cfg.http.headers or []) if cfg else []).get("Access-Control-Allow-Origin", "")
    if cors_origin and cors_origin != "*":
        allowed.append(cors_origin)
    return_url = safe_return_url(return_url, request_origin(request), allowed)
    if return_url is None:
        provider_slug = provider
        logger.warning("refused an OAuth return_url outside this deployment: provider_slug=%s", provider_slug)
        return HTMLResponse(content=_generate_oauth_completion_html(platform, provider, success, None, error_msg))

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


@router.get("/providers", response_model=StandardResponse | ErrorResponse)
async def get_providers(
        current_user: str | None = Depends(verify_token_optional),
        owner_user_id: str | None = None,
        nocache: bool = False,
        platform: str | None = None,
        status: str | None = None
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
            try:
                await resolve_subject(current_user, owner_user_id)
            except CareCircleDenied:
                return ErrorResponse(
                    code=-1,
                    msg=f"No permission to query providers for user {owner_user_id}"
                )

            query_user_id = owner_user_id
            logger.info(f"User {current_user} querying providers for shared user {owner_user_id}")

        platform_filter = platform  # Rename to avoid variable shadowing
        all_providers = []
        logger.info(f"get_providers platform: {len(platform_manager._platforms.items())}, filter: {platform_filter}")

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
                    logger.info(f"Added virtual provider for solo platform {platform_name}")
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
                logger.error(f"Error getting providers from platform {platform_name}: {str(e)}")
                continue

        connected_providers = []
        unconnected_providers = []
        unsupported_providers = []
        user_providers = []
        if query_user_id:
            try:
                user_providers = await platform_manager.get_user_providers(query_user_id)
                logger.info(f"Updated connection status for user {query_user_id}")
            except Exception as e:
                logger.error(f"Error getting user providers for {query_user_id}: {str(e)}")

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

        user_info = f" for user {query_user_id}" if query_user_id else " (no user context)"
        logger.info(f"Retrieved {len(all_providers)} providers{user_info}")
        return StandardResponse(
            data={"providers": all_providers, "total": len(all_providers)},
        )

    except Exception as e:
        logger.error(f"Error getting providers: {str(e)}")
        return ErrorResponse(code=500, msg=f"Failed to get providers: {str(e)}")


@router.get("/user/providers", response_model=StandardResponse | ErrorResponse)
async def get_user_providers(
        current_user: str = Depends(verify_token),
        owner_user_id: str | None = None
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
            try:
                await resolve_subject(current_user, owner_user_id)
            except CareCircleDenied:
                return ErrorResponse(
                    code=-1,
                    msg=f"No permission to query providers for user {owner_user_id}"
                )

            query_user_id = owner_user_id
            logger.info(f"User {current_user} querying user providers for shared user {owner_user_id}")

        # Get user connections across all Platforms through PlatformManager
        provider_list = await platform_manager.get_user_providers(query_user_id)
        return StandardResponse(
            code=0,
            msg="ok",
            data={"providers": provider_list, "total": len(provider_list)},
        )

    except Exception as e:
        logger.error(f"Error getting user providers: {str(e)}")
        return ErrorResponse(code=500, msg=f"Failed to get user providers: {str(e)}")


@router.post("/user/providers/link", response_model=StandardResponse | ErrorResponse)
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
            # Linking a device writes to someone's record, so the request asks
            # for write and gets it only from a read-write grant. Two checks
            # collapsed into one: the old code fetched the level and then
            # compared it to 2 itself, which is the comparison every caller had
            # to remember to write.
            try:
                await resolve_subject(current_user, request.owner_user_id, require_write=True)
            except CareCircleDenied as denied:
                return ErrorResponse(
                    code=-1,
                    msg=f"No permission to link provider for user {request.owner_user_id}: {denied}"
                )

            query_user_id = request.owner_user_id
            logger.info(f"User {current_user} linking provider for shared user {request.owner_user_id}")

        # Auto-detect correct platform (based on provider_slug prefix)
        actual_platform = request.platform
        provider_slug = request.provider_slug

        # provider platform: theta_ prefix
        if provider_slug.startswith("theta_"):
            actual_platform = "theta"
            logger.info(f"Auto-detected platform 'theta' from provider_slug '{provider_slug}'")

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
        options["default_return_url"] = f"{scheme}://{host}{API_PREFIX}/{actual_platform}/{provider_slug}/callback"

        # Call PlatformManager's simplified interface (business logic has been delegated)
        result_data = await platform_manager.link_provider(
            user_id=query_user_id,  # Use query_user_id instead of current_user to support sharing
            provider_slug=provider_slug,
            platform=actual_platform,
            auth_type=request.auth_type,
            credentials=credentials,
            options=options,
        )
        logger.info(f"Link successful for provider {provider_slug}")
        return StandardResponse(code=0, msg="ok", data=result_data)

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
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

        logger.info(f"OAuth callback received - platform: {platform}, provider: {provider}")

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
        logger.error(f"OAuth callback error for {platform}/{provider}: {str(e)}")

        # Return OAuth error HTML
        return HTMLResponse(content=_generate_oauth_completion_html(platform, provider, False, None, str(e)))


@router.post("/user/providers/unlink", response_model=StandardResponse | ErrorResponse)
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
            # Unlinking writes, so the request asks for write.
            try:
                await resolve_subject(current_user, request.owner_user_id, require_write=True)
            except CareCircleDenied as denied:
                return ErrorResponse(
                    code=-1,
                    msg=f"No permission to unlink provider for user {request.owner_user_id}: {denied}"
                )

            query_user_id = request.owner_user_id
            logger.info(f"User {current_user} unlinking provider for shared user {request.owner_user_id}")

        # Auto-detect correct platform (based on provider_slug prefix)
        actual_platform = request.platform
        provider_slug = request.provider_slug

        # provider platform: theta_ prefix
        if provider_slug.startswith("theta_"):
            actual_platform = "theta"
            logger.info(f"Auto-detected platform 'theta' from provider_slug '{provider_slug}'")
        # Other cases use platform passed from frontend
        elif actual_platform != request.platform:
            logger.info(f"Using platform '{actual_platform}' from request")

        # Call PlatformManager interface
        result_data = await platform_manager.unlink_provider(
            user_id=query_user_id,  # Use query_user_id instead of current_user to support sharing
            provider_slug=provider_slug,
            platform=actual_platform,
        )

        logger.info(f"Unlink successful for provider {provider_slug}")
        return StandardResponse(code=0, msg="ok", data=result_data)

    except ValueError as e:
        # Parameter validation error (400)
        logger.error(f"Validation error: {str(e)}")
        return ErrorResponse(code=400, msg=str(e))
    except RuntimeError as e:
        # Business logic error (500)
        logger.error(f"Runtime error: {str(e)}")
        return ErrorResponse(code=500, msg=str(e))
    except Exception as e:
        # Other unknown errors (500)
        logger.error(f"Unexpected error: {str(e)}")
        return ErrorResponse(code=500, msg=f"Failed to unlink provider: {str(e)}")


@router.post(
    "/user/providers/update-llm-access",
    response_model=StandardResponse | ErrorResponse,
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

        # provider platform: theta_ prefix
        if provider_slug.startswith("theta_"):
            actual_platform = "theta"
            logger.info(f"Auto-detected platform 'theta' from provider_slug '{provider_slug}'")

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

        logger.info(
            f"Updated LLM access for provider {provider_slug} to {request.llm_access} for user {current_user}"
        )
        return StandardResponse(code=0, msg="ok", data=result_data)

    except ValueError as e:
        # Parameter validation error (400)
        logger.error(f"Validation error: {str(e)}")
        return ErrorResponse(code=400, msg=str(e))
    except RuntimeError as e:
        # Business logic error (500)
        logger.error(f"Runtime error: {str(e)}")
        return ErrorResponse(code=500, msg=str(e))
    except Exception as e:
        # Other unknown errors (500)
        logger.error(f"Unexpected error: {str(e)}")
        return ErrorResponse(code=500, msg=f"Failed to update LLM access: {str(e)}")


#: A vendor push carries no user token, so the webhook routes take a shared
#: secret instead, from `X-Webhook-Secret` or `?secret=` (some vendor consoles
#: only take a URL). Unset means the routes answer 404: an open webhook let any
#: caller write readings into any account by naming it in the body.
WEBHOOK_SECRET_KEY = "COLLECT_WEBHOOK_SECRET"

#: Internal account ids a payload may name. A push identifies its person by the
#: vendor's own user id, which the provider maps through the linked account;
#: an internal id in the body is dropped rather than trusted.
_INTERNAL_ID_KEYS = ("theta_user_id", "app_user_id")


def _check_webhook(platform: str, request: Request) -> None:
    cfg = global_config()
    expected = str(cfg.get(WEBHOOK_SECRET_KEY) or "") if cfg else ""
    if not expected:
        raise HTTPException(status_code=404, detail=f"Webhooks are off. Set {WEBHOOK_SECRET_KEY} to enable them.")
    # Apple data arrives signed in, on /apple/health and /apple/cda. Its
    # platform takes the account from the payload, so it is never a push target.
    if platform == "apple":
        raise HTTPException(status_code=404, detail="Apple data is posted to /apple/*, signed in.")
    given = request.headers.get("X-Webhook-Secret") or request.query_params.get("secret") or ""
    if not hmac.compare_digest(given.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Webhook secret missing or wrong.")


def _without_internal_ids(event_data: Any) -> Any:
    if isinstance(event_data, dict):
        for key in _INTERNAL_ID_KEYS:
            event_data.pop(key, None)
    return event_data


@router.post("/{platform}/webhook", response_model=StandardResponse | ErrorResponse)
async def universal_webhook(platform: str, request: Request):
    """
    Universal Platform Webhook Interface

    Receive data pushes from various Platform Providers
    Provider identification is extracted from request data, each Platform has its own extraction logic

    Args:
        platform: Platform identifier
        request: Raw request object
    """

    _check_webhook(platform, request)
    try:
        raw_body = await request.body()
        raw_body_str = raw_body.decode("utf-8")
        msg_id = await get_msg_id(request)

        # Parse JSON data
        event_data = _without_internal_ids(json.loads(raw_body_str))
        provider_slug = await get_provider_slug(platform, event_data)

        # Log request
        logger.info(f"Universal webhook received - platform: {platform}, provider_slug: {provider_slug}, msg_id: {msg_id}")
        if not provider_slug:
            logger.warning("provider_slug is None")
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
        return ErrorResponse(code=500, msg="Failed to process webhook data")

    except json.JSONDecodeError as e:
        error_msg = f"JSON parse error: {str(e)}"
        logger.error(error_msg)
        return ErrorResponse(code=400, msg=error_msg)

    except Exception as e:
        error_msg = f"Error processing webhook: {str(e)}"
        logger.error(error_msg)
        return ErrorResponse(code=500, msg=error_msg)


@router.post("/{platform}/{provider}/webhook", response_model=StandardResponse | ErrorResponse)
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

    _check_webhook(platform, request)
    try:
        raw_body = await request.body()
        raw_body_str = raw_body.decode("utf-8")
        msg_id = await get_msg_id(request)

        # Parse JSON data
        event_data = _without_internal_ids(json.loads(raw_body_str))

        # Log request with explicit provider info and complete raw data
        logger.info(f"Provider-specific webhook received - platform: {platform}, provider: {provider}, msg_id: {msg_id}")

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
        return ErrorResponse(code=500, msg="Failed to process provider webhook data")

    except json.JSONDecodeError as e:
        error_msg = f"JSON parse error: {str(e)}"
        logger.error(error_msg)
        return ErrorResponse(code=400, msg=error_msg)

    except Exception as e:
        error_msg = f"Error processing provider webhook: {str(e)}"
        logger.error(error_msg)
        return ErrorResponse(code=500, msg=error_msg)


async def get_provider_slug(platform: str, event_data: dict[str, Any]) -> str | None:
    try:
        # Top-level `source`: some platforms send it as a bare string.
        top_level_source = event_data.get("source")
        if top_level_source and isinstance(top_level_source, str):
            return top_level_source

        # Vital nests it: data.source.slug
        source_info = event_data.get("data", {}).get("source", {})
        if source_info and isinstance(source_info, dict):
            s = source_info.get("slug")
            p = source_info.get("provider", s)
            if s and p and s != p:
                logger.error(f"{platform} slug not equal provider {s} != {p}")
            return p

        return None
    except Exception as e:
        logger.error(f"Error extracting provider_slug for platform {platform}: {str(e)}")
        return None


async def get_msg_id(request: Request) -> str:
    msg_id = request.headers.get("Svix-Id", "")
    if not msg_id:
        msg_id = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    return msg_id


@router.post("/{platform}/token", response_model=StandardResponse | ErrorResponse)
async def get_theta_token(platform: str, request: ProviderTokenRequest):
    """
    Get Provider user token API

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

        logger.info(f"Get theta token request - provider_slug: {provider_slug}, user_id: {user_id}")

        platform_entity = platform_manager.get_platform(platform)
        if not platform_entity:
            logger.error("platform not available")
            return ErrorResponse(code=503, msg="provider platform not available")

        provider = platform_entity.get_provider(provider_slug)
        if not provider:
            logger.error(f"Provider {provider_slug} not found")
            return ErrorResponse(code=404, msg=f"Provider {provider_slug} not found")


        # `certification` is the only proof this unauthenticated route gets, so
        # a provider that does not verify it cannot issue a token. The default
        # `_validate_credentials` accepts everything (it is a link-time sanity
        # hook for a caller who is already signed in), and every shipped
        # provider inherits it: with it, anyone who knew an account's vendor
        # user id got a 30-day token for that account.
        from mirobody.collect import BasePullProvider
        if type(provider)._validate_credentials is BasePullProvider._validate_credentials:
            return ErrorResponse(code=401, msg=f"Provider {provider_slug} cannot verify credentials")
        await provider._validate_credentials({"username": user_id, "password": certification})
        user = get_platform_user_service()
        from mirobody.utils.config import get_default_timezone
        app_user_id = await user.find_or_create_user_by_provider_id(provider_slug, user_id, get_default_timezone())
        token = await user.generate_token(app_user_id)

        # 5. Build response data
        response_data = {
            "user_id": user_id,
            "token": token,
            "expires_in": 60 * 60 * 24 * 30,
            "token_type": "Bearer",
            "provider_slug": provider_slug
        }

        logger.info(f"Successfully generated token for user {user_id} with provider {provider_slug}")

        return StandardResponse(
            code=0,
            msg="ok",
            data=response_data
        )

    except ValueError as e:
        logger.error(f"Validation error in get_theta_token: {str(e)}")
        return ErrorResponse(code=400, msg=str(e))
    except Exception as e:
        logger.error(
            f"Unexpected error in get_theta_token: {type(e).__name__}: {str(e)}",
            exc_info=True,
        )
        return ErrorResponse(code=500, msg=f"Internal error: {type(e).__name__}: {str(e)}")


@router.get("/theta/indicators", response_model=StandardResponse | ErrorResponse)
async def get_theta_indicators():
    """
    Get supported indicators information for provider platform

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
        from mirobody.translate import get_all_indicators_info

        # Get complete manage data
        manage_data = get_all_indicators_info()

        indicators_info = []
        categories_info = {}

        # The categories this endpoint publishes, spelled the way
        # `get_all_indicators_info()` spells them: WITH SPACES. Written with
        # underscores they intersect the real labels nowhere, so the filter
        # dropped all 296 indicators and the route answered
        # `{"indicators": [], "total": 0}` with HTTP 200: an empty catalogue
        # that looks like a successful one. The six below select 195 of the 296;
        # tests/server/routers/test_theta_indicators.py pins the intersection.
        theta_supported_categories = {
            "vital signs",
            "body composition",
            "activity metrics",
            "metabolic metrics",
            "sleep metrics",
            "performance metrics",
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
                            logger.error(f"Error processing indicator {indicator_data.get('key', 'unknown')}: {str(e)}")
                            continue

        # Build response data
        response_data = {
            "indicators": indicators_info,
            "categories": list(categories_info.values()),
            "total": len(indicators_info)
        }

        logger.info(f"Successfully retrieved {len(indicators_info)} indicators information using manage data source")

        return StandardResponse(
            code=0,
            msg="ok",
            data=response_data
        )

    except Exception as e:
        logger.error(f"Unexpected error in get_theta_indicators: {str(e)}")
        return ErrorResponse(code=500, msg=f"Failed to get indicators information: {str(e)}")

