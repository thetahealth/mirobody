import base64
import hashlib
import hmac
import json
import logging
import secrets
import time
import urllib.parse

from collections.abc import Callable
from mirobody.utils.ephemeral import EphemeralStore

from .bearer import MCP_CLIENT_PREFIX, audience_matches, bearer_subject, mcp_resource
from .jwt import REFRESH_TOKEN_TYPE, AbstractTokenValidator, minted_at

from mirobody.kernel.ops import is_driver_exception

from mirobody.utils import request_origin, secret_fingerprint, json_response, json_response_with_code, redirect, get_jwt_token, Request, Response, Route

logger = logging.getLogger(__name__)


def _pkce_matches(challenge: str, verifier: object) -> bool:
    """RFC 7636 §4.6, S256: BASE64URL(SHA256(verifier)) without padding."""
    if not isinstance(verifier, str) or not 43 <= len(verifier) <= 128:
        return False
    digest = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii", "ignore")).digest()).rstrip(b"=").decode()
    return hmac.compare_digest(digest, challenge)

#-----------------------------------------------------------------------------

# HTTP/1.1 200 OK
# Content-Type: application/json
# Cache-Control: no-store

# {
#   "access_token":"MTQ0NjJkZmQ5OTM2NDE1ZTZjNGZmZjI3",
#   "token_type":"Bearer",
#   "expires_in":3600,
#   "refresh_token":"IwOGYzYTlmM2YxOTQ5MGE3YmNmMDFkNTVk",
#   "scope":"create"
# }



# HTTP/1.1 400 Bad Request
# Content-Type: application/json
# Cache-Control: no-store 

# {
#   "error": "invalid_request",
#   "error_description": "Request was missing the 'redirect_uri' parameter.",
#   "error_uri": "See the full API docs at https://authorization-server.com/docs/access_token"
# }

# invalid_request
# invalid_client
# invalid_grant
# invalid_scope
# unauthorized_client
# unsupported_grant_type

#-----------------------------------------------------------------------------

# RFC 6749 §4.1.2: an authorization code "MUST be short lived", 10 minutes
# maximum recommended. This used to be `token_validator.get_expires_in()`: the
# ACCESS TOKEN lifetime, which defaults to 30 days when JWT_EXPIRES_IN is unset.
# The comment beside the state token already said "will expire in 10 minutes";
# the code had drifted from its own documented intent.
_AUTH_CODE_TTL_SECONDS = 600


class OAuthService:
    def __init__(
        self,
        token_validator : AbstractTokenValidator,
        gen_jwt_claims_func : Callable[[str, str], dict] | None = None,
        uri_prefix      : str = "",
        routes          : list | None = None,
        ephemeral       : EphemeralStore | None = None,
        **kwargs
    ):
        self._token_validator   = token_validator
        self._get_jwt_token     = get_jwt_token
        self._gen_jwt_claims    = gen_jwt_claims_func if callable(gen_jwt_claims_func) else None

        #-------------------------------------------------

        self._ephemeral = ephemeral

        self._uri_prefix = uri_prefix

        if self._ephemeral:
            self._auth_code_keyprefix   = "mirobody:user:auth:code:"
            self._client_keyprefix      = "mirobody:user:client:"

        else:
            # Local memory when the server has no shared store.
            self._auth_codes    = {}
            self._clients       = {}

        #-------------------------------------------------

        if routes is not None:
            self.routes = routes
        else:
            self.routes = []

        self.routes.append(Route("/.well-known/oauth-authorization-server/mcp", endpoint=self.metadata_handler, methods=["GET"]))
        self.routes.append(Route("/.well-known/oauth-authorization-server", endpoint=self.metadata_handler, methods=["GET"]))
        self.routes.append(Route("/.well-known/mcp-configuration", endpoint=self.metadata_handler, methods=["GET"]))

        self.routes.append(Route(f"{uri_prefix}/oauth2/authorize", endpoint=self.authorize_handler, methods=["POST", "GET", "OPTIONS"]))

        self.routes.append(Route(f"{uri_prefix}/oauth/register", endpoint=self.register_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/oauth/authorize", endpoint=self.authorize_handler, methods=["POST", "GET", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/oauth/token", endpoint=self.token_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/oauth/introspect", endpoint=self.introspect_handler, methods=["POST", "OPTIONS"]))

    #-------------------------------------------------------------------------

    def _mcp_resource(self, request: Request) -> str:
        return mcp_resource(request_origin(request), self._uri_prefix)

    #-------------------------------------------------------------------------

    async def metadata_handler(self, request: Request) -> Response:
        url_prefix = request_origin(request)
        metadata = {
            "issuer": url_prefix,
            "authorization_endpoint": f"{url_prefix}/oauth/authorize",
            "token_endpoint": f"{url_prefix}/oauth/token",
            "registration_endpoint": f"{url_prefix}/oauth/register",
            "introspection_endpoint": f"{url_prefix}/oauth/introspect",
            "scopes_supported": [
                "openid",
                "profile",
                "email",
                "offline_access",
                "mcp:read",
                "mcp:write",
                "mcp:tools",
                "mcp:admin",
                "mcp:connect",
            ],
            "response_types_supported": ["code"],
            "response_modes_supported": ["query"],
            "grant_types_supported": [
                "authorization_code",
                "refresh_token",
            ],
            "token_endpoint_auth_methods_supported": [
                "client_secret_post",
                "client_secret_basic",
                "none",
            ],
            "code_challenge_methods_supported": ["S256"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
            "token_endpoint_auth_signing_alg_values_supported": ["RS256"],
            "claims_supported": ["sub", "aud", "exp", "iat", "iss", "jti"],
            "request_object_signing_alg_values_supported": ["none"],
            "request_parameter_supported": True,
            "request_uri_parameter_supported": False,
            "registration_endpoint_auth_methods_supported": ["none"],
            "resource_indicators_supported": True,  # RFC 8707 for 2025-06-18
            "authorization_response_iss_parameter_supported": True,  # RFC 9207
            "mcp": {
                "version": "2025-06-18",
                "auth_type": "oauth2",
                "capabilities": {
                    "supports_refresh_tokens": True,
                    "supports_state_verification": True,
                    "supports_pkce": True,
                    "supports_resource_indicators": True,
                },
            },
        }

        return json_response(metadata, request=request)

    #-------------------------------------------------------------------------

    async def _is_registered_redirect_uri(self, client_id: str, redirect_uri: str) -> bool:
        """Is `redirect_uri` one this client registered? Exact match only.

        A client with no registered URIs cannot use the redirect flow at all.
        RFC 7591 requires `redirect_uris` for the authorization_code grant, and
        treating "none registered" as "anything allowed" would reinstate the
        vulnerability for every client that simply omits the field.
        """
        if not redirect_uri or not client_id:
            return False

        raw = None
        if self._ephemeral:
            try:
                raw = await self._ephemeral.hget(self._client_keyprefix + client_id, "redirect_uris")
            except Exception as e:
                logger.warning("OAuth client lookup failed: error_type=%s", type(e).__name__)
        else:
            raw = (self._clients.get(client_id) or {}).get("redirect_uris")

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")
        try:
            registered = json.loads(raw) if raw else []
        except Exception:
            registered = []

        return isinstance(registered, list) and redirect_uri in registered

    #-------------------------------------------------------------------------

    async def register_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response(disable_log=True)

        try:
            data = await request.json()
            client_id = f"{MCP_CLIENT_PREFIX}{secrets.token_hex(16)}"
            client_secret = secrets.token_hex(32)

            # `redirect_uris` must be PERSISTED, not merely echoed back. It was
            # only ever put in the response body, so the authorize handler had
            # nothing to compare against and trusted whatever `redirect_uri` the
            # request carried: an attacker could send a logged-in victim to
            # /authorize with `redirect_uri=https://evil.example/cb` and receive
            # a code exchangeable for that victim's tokens (RFC 6749 §10.6).
            # JSON because each field of a stored hash is one string.
            registered_redirect_uris = data.get("redirect_uris", [])
            if not isinstance(registered_redirect_uris, list):
                registered_redirect_uris = []

            cached_client = {
                "secret": client_secret,
                "redirect_uris": json.dumps(registered_redirect_uris),
            }

            if self._ephemeral:
                try:
                    await self._ephemeral.hset(self._client_keyprefix + client_id, mapping=cached_client)

                except Exception as e:
                    logger.warning("OAuth client registration failed: error_type=%s", type(e).__name__)
            else:
                if client_id in self._clients:
                    self._clients[client_id].update(cached_client)
                else:
                    self._clients[client_id] = cached_client

            requested_auth_method = data.get(
                "token_endpoint_auth_method", "client_secret_post"
            )

            supported_auth_methods = ["client_secret_post", "none"]
            if requested_auth_method not in supported_auth_methods:
                requested_auth_method = "client_secret_post"

            client_info = {
                "client_id": client_id,
                "client_secret": client_secret,
                "client_name": data.get("client_name", "MCP Client"),
                "redirect_uris": registered_redirect_uris,
                "scope": data.get("scope", "mcp:read mcp:write"),
                "response_types": data.get("response_types", ["code"]),
                "grant_types": data.get(
                    "grant_types", ["authorization_code", "refresh_token"]
                ),
                "token_endpoint_auth_method": requested_auth_method,
                "client_id_issued_at": int(time.time()),
                "client_secret_expires_at": 0,
                "created_at": time.time(),
            }
            logger.info("Client registered: client_id=%s method=%s", client_id, requested_auth_method)

            return json_response(
                content = client_info,
                status_code = 201,
                request = request
            )
        
        except Exception as e:
            logger.error("OAuth client registration failed: error_type=%s", type(e).__name__,
                         exc_info=not is_driver_exception(e))

            return json_response(
                content = {"error": "registration_failed", "message": "The registration request could not be read."},
                status_code = 400,
                request = request
            )

    #-------------------------------------------------------------------------

    async def authorize_handler(self, request: Request) -> Response:
        if request.method == "GET":
            url_prefix = request_origin(request)

            # The login page posts every one of these back (PKCE included) with
            # its own session token. A signed-in caller used to be redirected
            # with `access_token=<the session token>` in the URL, into browser
            # history and proxy logs, and without the PKCE parameters; the page
            # reads only `oauth_params`, so that branch never completed anyway.
            oauth_params = urllib.parse.urlencode(dict(request.query_params))
            return redirect(f"{url_prefix}/mcplogin?oauth_params={urllib.parse.quote(oauth_params)}")

        if request.method != "POST":
            return json_response_with_code()

        # Posted by the login page, with the signed-in user's session token.
        form_data = await request.form()
        client_id = str(form_data.get("client_id") or "")
        redirect_uri = form_data.get("redirect_uri")
        redirect_uri = redirect_uri if isinstance(redirect_uri, str) else ""
        state = form_data.get("state")
        state = state if isinstance(state, str) else ""
        # RFC 9207: the metadata advertises `iss` on every authorization
        # response, and a client that reads that (the MCP SDK does) rejects a
        # response without it.
        suffix = (f"&state={urllib.parse.quote(state)}" if state else "") + \
            f"&iss={urllib.parse.quote(request_origin(request), safe='')}"

        # The redirect target must be one the client registered, before any
        # answer is sent there, a refusal included. Exact string match, per
        # RFC 6749 §3.1.2.3 and RFC 8252 §7.1: no prefix or host matching,
        # both routinely bypassed (`https://good.example.evil.com`).
        if not await self._is_registered_redirect_uri(client_id, redirect_uri):
            logger.warning("rejected unregistered redirect_uri for client %s", client_id)
            return json_response(content={"code": -1, "msg": "invalid redirect_uri"}, status_code=400, request=request)

        def go(query: str) -> Response:
            return json_response(content={"code": 0, "msg": "ok", "data": {}, "location": f"{redirect_uri}?{query}{suffix}"},
                                 request=request)

        if form_data.get("action") != "allow":
            return go("error=access_denied")

        # A signed-in person's session, not an MCP client's token and not a
        # refresh token: only the person may grant a client access.
        payload, err = self._token_validator.verify_token(self._get_jwt_token(request))
        if err or not await bearer_subject(payload):
            return go("error=access_denied&error_description=authentication_failed")

        # PKCE (RFC 7636), S256 only, as the metadata advertises: an
        # intercepted code is otherwise as good as a token.
        code_challenge = str(form_data.get("code_challenge") or "")
        if not code_challenge or form_data.get("code_challenge_method") != "S256":
            return json_response(content={"code": -1, "msg": "PKCE with code_challenge_method=S256 is required"},
                                 status_code=400, request=request)

        auth_code = f"auth_code_{secrets.token_urlsafe(32)}"
        cached_auth_code = {
            "client_id"     : client_id,
            "user_id"       : str(payload["sub"]),
            "scope"         : str(form_data.get("scope", "mcp:read mcp:write")),
            "expires_at"    : int(time.time()) + _AUTH_CODE_TTL_SECONDS,
            # §4.1.3: the token request must name the same redirect_uri.
            "redirect_uri"  : redirect_uri,
            "code_challenge": code_challenge,
            # RFC 8707: checked against this server's MCP endpoint at /oauth/token.
            "resource"      : str(form_data.get("resource") or ""),
            # What the authorising session proved. The tokens this code
            # mints carry it, so an AAL1 session cannot mint an MCP token
            # the AAL2 gate would let through.
            "aal"           : str(int(payload.get("aal") or 0)),
        }

        if self._ephemeral:
            code_key = self._auth_code_keyprefix + auth_code
            try:
                await self._ephemeral.hset(code_key, mapping=cached_auth_code)
                await self._ephemeral.expire(code_key, _AUTH_CODE_TTL_SECONDS)
            except Exception as e:
                logger.warning("OAuth code store failed: error_type=%s", type(e).__name__)
        else:
            self._auth_codes[auth_code] = cached_auth_code

        return go(f"code={auth_code}")

    #-------------------------------------------------------------------------

    async def token_handler(self, request: Request) -> Response:
        try:
            data = await request.form()

            grant_type = data.get("grant_type")
            if grant_type is None or not isinstance(grant_type, str):
                grant_type = ""

            client_id = data.get("client_id")
            if client_id is None or not isinstance(client_id, str):
                client_id = ""

            client_secret = data.get("client_secret")
            if client_secret is None or not isinstance(client_secret, str):
                client_secret = ""

            # Check authorization header (client_secret_basic)
            if not client_id or not client_secret:
                auth_header = request.headers.get("Authorization", "")
                if auth_header.startswith("Basic "):
                    try:
                        encoded = auth_header[6:]
                        decoded = base64.b64decode(encoded).decode("utf-8")
                        client_id, client_secret = decoded.split(":", 1)
                    
                    except Exception as e:
                        logger.warning("OAuth Basic credentials unreadable: error_type=%s", type(e).__name__)

            # `client_secret` was in this line, at INFO, in cleartext. It is a
            # long-lived credential: anyone with log read access could
            # impersonate the client. The fingerprint still answers the only
            # question this log line was ever used for: "did the client send
            # the secret we expect?".
            logger.info(
                "Token request - grant_type: %s, client_id: %s, client_secret: %s",
                grant_type, client_id, secret_fingerprint(client_secret),
            )

            if grant_type == "authorization_code":
                code = data.get("code")
                if code is None or not isinstance(code, str):
                    code = ""

                # Read AND consume in one step. RFC 6749 §4.1.2 requires an
                # authorization code to be single-use; the shared-store branch
                # (Redis, then) used to read it and leave it in place behind a
                # `# TODO: pass`, so a leaked code could be exchanged for fresh
                # token pairs for its whole lifetime. `take_hash` is one DELETE
                # ... RETURNING: an empty answer means another request already
                # redeemed it, that is a replay, and it is refused.
                consumed = True
                if self._ephemeral:
                    key = self._auth_code_keyprefix + code
                    try:
                        stored_code = await self._ephemeral.take_hash(key)
                        consumed = bool(stored_code)
                    except Exception as e:
                        logger.warning("OAuth code redeem failed: error_type=%s", type(e).__name__)
                        stored_code = {}
                        consumed = False

                else:
                    stored_code = self._auth_codes.pop(code, {})

                # These three checks were present but commented out. Without
                # them the grant only required that the stored record carry a
                # user_id, so an expired code, a replayed code, or a code
                # issued to a different client all minted tokens.
                try:
                    expires_at = int(stored_code.get("expires_at", 0))
                except (TypeError, ValueError):
                    expires_at = 0

                if not stored_code or not consumed or expires_at < time.time():
                    return json_response(
                        {
                            "error": "invalid_grant",
                            "error_description": "Authorization code is invalid, expired or already used.",
                        },
                        status_code=400,
                        request=request,
                    )

                if not client_id:
                    return json_response(
                        {
                            "error": "invalid_client",
                            "error_description": "Client authentication failed.",
                        },
                        status_code=401,
                        request=request,
                    )

                # §4.1.3: the code must have been issued to the client
                # presenting it, or one client can redeem another's code.
                if stored_code.get("client_id") != client_id:
                    return json_response(
                        {
                            "error": "invalid_grant",
                            "error_description": "Client ID mismatch.",
                        },
                        status_code=400,
                        request=request,
                    )

                if stored_code.get("redirect_uri") and data.get("redirect_uri") != stored_code.get("redirect_uri"):
                    return json_response(
                        {"error": "invalid_grant", "error_description": "redirect_uri does not match the authorization request."},
                        status_code=400,
                        request=request,
                    )

                challenge = stored_code.get("code_challenge")
                if challenge and not _pkce_matches(challenge, data.get("code_verifier")):
                    return json_response(
                        {"error": "invalid_grant", "error_description": "code_verifier does not match the code_challenge."},
                        status_code=400,
                        request=request,
                    )

                # RFC 8707: a client may only ask for a token to this server's
                # MCP endpoint, and the token names it as its audience, which is
                # the one place it is accepted (`bearer.bearer_subject`).
                audience = self._mcp_resource(request)
                requested = data.get("resource") or stored_code.get("resource")
                if requested and not audience_matches(requested, audience):
                    return json_response(
                        {"error": "invalid_target", "error_description": "resource is not this server's MCP endpoint."},
                        status_code=400,
                        request=request,
                    )

                user_id = stored_code.get("user_id")
                scope   = stored_code.get("scope", "mcp:read mcp:write")
                aal     = int(stored_code.get("aal") or 0)

                if not user_id:
                    return json_response(
                        {
                            "error": "server_error",
                            "error_description": "User information not found.",
                        },
                        status_code=500,
                        request=request
                    )

                access_token, refresh_token, err = await self._token_validator.generate_tokens(
                    user_id, "", "mcp", client_id=client_id, scope=scope,
                    gen_claims_func=lambda _uid, _em: {"aud": audience, **({"aal": aal} if aal else {})},
                )
                if err:
                    logger.error(err)

                    return json_response(
                        {
                            "error": "server_error",
                            "error_description": err,
                        },
                        status_code=500,
                        request=request
                    )

                return json_response(
                    content={
                        "access_token": access_token,
                        "token_type": "Bearer",
                        "expires_in": self._token_validator.get_expires_in(),
                        "refresh_token": refresh_token,
                        "scope": scope,
                    },
                    request=request
                )

            if grant_type == "refresh_token":
                if not client_id:
                    return json_response(
                        {
                            "error": "invalid_client",
                            "error_description": "Client authentication required.",
                        },
                        status_code=401,
                        request=request
                    )

                refresh_token = data.get("refresh_token")
                if not refresh_token or not isinstance(refresh_token, str):
                    return json_response(
                        {
                            "error": "invalid_request",
                            "error_description": "Refresh token required.",
                        },
                        status_code=400,
                        request=request
                    )
                
                payload, err = self._token_validator.verify_token(refresh_token)
                if err or not payload:
                    logger.error(err, extra={"refresh_token": secret_fingerprint(refresh_token), "client_id": client_id})

                    return json_response(
                        {
                            "error": "expired_token",
                            "error_description": err,
                        },
                        status_code=401,
                        request=request
                    )
                
                # A refresh token, issued to this client. Any valid token used to
                # do: an access token, or a web session's, came back as a fresh
                # 30-day MCP token.
                if (not isinstance(payload, dict) or "sub" not in payload
                        or payload.get("token_type") != REFRESH_TOKEN_TYPE
                        or payload.get("client_id") != client_id):
                    err = "Not a refresh token issued to this client."
                    logger.error(err, extra={"refresh_token": secret_fingerprint(refresh_token), "client_id": client_id})

                    return json_response(
                        {
                            "error": "expired_token",
                            "error_description": err,
                        },
                        status_code=401,
                        request=request
                    )

                # A closed account, or one whose sessions were revoked after this
                # refresh token was minted, gets nothing new from it.
                from mirobody.user.user import is_active_account

                if not await is_active_account(payload["sub"], minted_at(payload)):
                    return json_response(
                        {"error": "invalid_grant", "error_description": "This session was signed out."},
                        status_code=400,
                        request=request
                    )

                aal = int(payload.get("aal") or 0)
                audience = self._mcp_resource(request)
                new_access_token, new_refresh_token, err = await self._token_validator.generate_tokens(
                    payload["sub"], "", "mcp", client_id=client_id, scope=str(payload.get("scope") or ""),
                    gen_claims_func=lambda _uid, _em: {"aud": audience, **({"aal": aal} if aal else {})},
                )
                if err:
                    logger.error(err, extra={"refresh_token": secret_fingerprint(refresh_token), "client_id": client_id})

                    return json_response(
                        {
                            "error": "server_error",
                            "error_description": err,
                        },
                        status_code=500,
                        request=request
                    )

                if not new_access_token:
                    return json_response(
                        {
                            "error": "invalid_grant",
                            "error_description": "Invalid refresh token.",
                        },
                        status_code=401,
                        request=request
                    )

                scope = payload["scope"] if "scope" in payload and len(payload["scope"]) > 0 else "mcp:read mcp:write"

                return json_response(
                    content={
                        "access_token": new_access_token,
                        "token_type": "Bearer",
                        "expires_in": self._token_validator.get_expires_in(),
                        "refresh_token": new_refresh_token,
                        "scope": scope,
                    },
                    request=request
                )

            return json_response(
                content     = {
                    "error": "unsupported_grant_type",
                    "error_description": f"Grant type '{grant_type}' is not supported.",
                },
                status_code = 400,
                request     = request
            )
            
        except Exception as e:
            logger.error("OAuth token request failed: error_type=%s", type(e).__name__,
                         exc_info=not is_driver_exception(e))

            return json_response(
                content     = {
                    "error": "server_error",
                    "error_description": "The token request failed."
                },
                status_code = 500,
                request     = request
            )

    #-------------------------------------------------------------------------

    async def introspect_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        data    = await request.form()
        token   = data.get("token")

        if not token or not isinstance(token, str):
            logger.error("No token found.")
            return json_response({"active": False}, request=request)
        
        payload, err = self._token_validator.verify_token(token)
        if err:
            logger.error(err, extra={"token": secret_fingerprint(token)})
            return json_response({"active": False}, request=request)

        from mirobody.user.user import is_active_account

        if not await is_active_account(payload.get("sub"), minted_at(payload)):
            return json_response({"active": False}, request=request)
        
        return json_response(
            content = {
                "active"    : True,
                "scope"     : payload.get("scope", ""),
                "client_id" : payload.get("client_id"),
                "username"  : payload.get("sub"),
                "exp"       : payload.get("exp"),
                "iat"       : payload.get("iat"),
                "sub"       : payload.get("sub"),
                "aud"       : payload.get("aud"),
                "iss"       : payload.get("iss"),
                "jti"       : payload.get("jti"),
                "token_type": payload.get("token_type"),
            },
            request = request
        )

#-----------------------------------------------------------------------------
