import base64, logging, secrets, time, urllib.parse

from typing import Callable
from redis.asyncio import Redis

from .jwt import AbstractTokenValidator

from ..utils import (
    json_response,
    json_response_with_code,
    redirect,
    get_jwt_token,
    
    Request,
    Response,
    Route
)

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

class OAuthService:
    def __init__(
        self,
        token_validator : AbstractTokenValidator,
        gen_jwt_claims_func : Callable[[str, str], dict] | None = None,
        uri_prefix      : str = "",
        routes          : list | None = None,
        web_server_url  : str = "",
        mcp_server_url  : str = "",
        redis           : Redis | None = None
    ):
        self._token_validator   = token_validator
        self._get_jwt_token     = get_jwt_token
        self._gen_jwt_claims    = gen_jwt_claims_func if callable(gen_jwt_claims_func) else None

        #-------------------------------------------------

        self._redis = redis

        if self._redis:
            self._state_token_keyprefix = "mirobody:user:state:"
            self._auth_code_keyprefix   = "mirobody:user:auth:code:"
            self._client_keyprefix      = "mirobody:user:client:"

        else:
            # Use local memory when no redis connection is available.
            self._state_tokens  = {}
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
        self.routes.append(Route(f"{uri_prefix}/oauth2/check_state/{{state:str}}", endpoint=self.check_state_handler, methods=["GET"]))

        self.routes.append(Route(f"{uri_prefix}/oauth/register", endpoint=self.register_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/oauth/authorize", endpoint=self.authorize_handler, methods=["POST", "GET", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/oauth/token", endpoint=self.token_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/oauth/introspect", endpoint=self.introspect_handler, methods=["POST", "OPTIONS"]))

    #-------------------------------------------------------------------------

    async def metadata_handler(self, request: Request) -> Response:
        url_prefix = f"{"http" if request.url.hostname == "localhost" else "https"}://{request.url.hostname}"
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
                "client_credentials",
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

    async def register_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response(disable_log=True)

        try:
            data = await request.json()
            logging.debug(f"request.json: {data}")
            client_id = f"mcp_client_{secrets.token_hex(16)}"
            client_secret = secrets.token_hex(32)

            cached_client = {
                "secret": client_secret
            }

            if self._redis:
                try:
                    await self._redis.hset(self._client_keyprefix + client_id, mapping=cached_client)

                except Exception as e:
                    logging.warning(str(e))
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
                "redirect_uris": data.get("redirect_uris", []),
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
            logging.info(f"Client registered: {client_id} with auth method: {requested_auth_method}")

            return json_response(
                content = client_info,
                status_code = 201,
                request = request
            )
        
        except Exception as e:
            logging.error(f"Client registration failed: {e}")

            return json_response(
                content = {"error": "registration_failed", "message": str(e)},
                status_code = 400,
                request = request
            )

    #-------------------------------------------------------------------------

    async def authorize_handler(self, request: Request) -> Response:
        token = self._get_jwt_token(request)

        payload, err = self._token_validator.verify_token(token)
        
        if request.method == "GET":
            url_prefix = f"{"http" if request.url.hostname == "localhost" else "https"}://{request.url.hostname}"

            if err:
                # Invalid jwt token, redirect to the login url.
                oauth_params = urllib.parse.urlencode(dict(request.query_params))
                redirect_url = f"{url_prefix}/mcplogin?oauth_params={urllib.parse.quote(oauth_params)}"

                return redirect(redirect_url)

            # Redirect to the device login url.
            device_auth_params = {
                "client_id"     : request.query_params.get("client_id", ""),
                "state"         : request.query_params.get("state", ""),
                "redirect_uri"  : request.query_params.get("redirect_uri", ""),
                "scope"         : request.query_params.get("scope", "mcp:read mcp:write"),
                "oauth_callback": f"{url_prefix}{request.url.path}",
                "access_token"  : token
            }
            query_string = urllib.parse.urlencode(device_auth_params)

            return redirect(f"{url_prefix}/mcplogin?{query_string}")

        elif request.method == "POST":
            # Post from the device login url.
            form_data = await request.form()

            redirect_uri = form_data.get("redirect_uri")
            if redirect_uri is None or not isinstance(redirect_uri, str):
                redirect_uri = ""

            state = form_data.get("state")
            if state is None or not isinstance(state, str):
                state = ""

            if form_data.get("action") != "allow":
                if redirect_uri and redirect_uri != "urn:ietf:wg:oauth:2.0:oob":
                    return json_response(
                        content = {
                            "code"      : 0,
                            "msg"       : "ok",
                            "data"      : {},
                            "location"  : f"{redirect_uri}?error=access_denied" + (f"&state={state}" if state else "")
                        },
                        request = request
                    )
                
                else:
                    # For OOB or no redirect_uri, return JSON response
                    return json_response_with_code(-1, "access_denied", request=request)

            if err or not payload:
                # For OOB, return JSON error
                if redirect_uri == "urn:ietf:wg:oauth:2.0:oob":
                    return json_response_with_code(-2, "Authentication failed", request=request)
                
                else:
                    return json_response(
                        content = {
                            "code"      : 0,
                            "msg"       : "ok",
                            "data"      : {},
                            "location"  : f"{redirect_uri}?error=authentication_failed" + (f"&state={state}" if state else "")
                        },
                        request = request
                    )

            #---------------------------------------------

            auth_code   = f"auth_code_{secrets.token_urlsafe(32)}"
            client_id   = str(form_data.get("client_id"))
            user_id     = str(payload["sub"])

            cached_auth_code = {
                "client_id" : client_id,
                "user_id"   : user_id,
                "scope"     : str(form_data.get("scope", "mcp:read mcp:write")),
                "expires_at": int(time.time()) + self._token_validator.get_expires_in(),
            }
            cached_client = {
                "user_id"   : user_id
            }

            if self._redis:
                redis_key = self._auth_code_keyprefix + auth_code
                try:
                    await self._redis.hset(redis_key, mapping=cached_auth_code)
                    await self._redis.expire(redis_key, self._token_validator.get_expires_in())
                except Exception as e:
                    logging.warning(str(e))

                redis_key = self._client_keyprefix + client_id
                try:
                    await self._redis.hset(redis_key, mapping=cached_client)
                except Exception as e:
                    logging.warning(str(e))

            else:
                self._auth_codes[auth_code] = cached_auth_code
                self._clients[client_id].update(cached_client)

            # Initial state,
            #   will expire in 10 minutes.
            if state:
                initial_state = {
                    "status"    : "pending",
                    "auth_code" : auth_code,
                    "expires_at": int(time.time()) + self._token_validator.get_expires_in(),
                }

                if self._redis:
                    redis_key = self._state_token_keyprefix + state

                    try:
                        await self._redis.hset(redis_key, mapping=initial_state)
                        await self._redis.expire(redis_key, self._token_validator.get_expires_in())

                    except Exception as e:
                        logging.warning(str(e))

                else:
                    self._state_tokens[state] = initial_state

            if redirect_uri == "urn:ietf:wg:oauth:2.0:oob":
                if not token:
                    return json_response_with_code(-3, "No valid access token found", request=request)
                
                # Completed state,
                completed_state = {
                    "status"    : "completed",
                    "token"     : token,
                    "token_type": "Bearer",
                    "expires_at": int(time.time()) + self._token_validator.get_expires_in(),
                }

                if self._redis:
                    redis_key = self._state_token_keyprefix + state

                    try:
                        await self._redis.hset(redis_key, mapping=completed_state)
                        await self._redis.expire(redis_key, self._token_validator.get_expires_in())
                    
                    except Exception as e:
                        logging.warning(str(e))
                
                else:
                    if state and state in self._state_tokens:
                        self._state_tokens[state].update(completed_state)


                return json_response(
                    content = {
                        # Response in code/msg style. 
                        "code"      : 0,
                        "msg"       : "ok",
                        "data"      : {
                            "access_token"  : token,
                            "token_type"    : "Bearer",
                            "expires_in"    : self._token_validator.get_expires_in() if self._token_validator else 60*60*24*7,
                            "state"         : state
                        },

                        # Standard OAuth response.
                        "success"       : True,
                        "access_token"  : token,
                        "token_type"    : "Bearer",
                        "expires_in"    : self._token_validator.get_expires_in() if self._token_validator else 60*60*24*7,
                        "state"         : state,
                        "message"       : "Authorization successful",
                    },
                    request = request
                )
            
            else:
                return json_response(
                    content = {
                        "code"      : 0,
                        "msg"       : "ok",
                        "data"      : {},
                        "location"  : f"{redirect_uri}?code={auth_code}" + (f"&state={state}" if state else "")
                    },
                    request = request
                )
            
        else:
            return json_response_with_code()

    #-------------------------------------------------------------------------

    async def check_state_handler(self, request: Request) -> Response:
        state = request.path_params["state"]
        if not state or not isinstance(state, str):
            return json_response(
                content     = {
                    "error"     : "missing_state",
                    "message"   : "State parameter is required"
                },
                status_code = 400,
                request     = request
            )

        
        #-------------------------------------------------

        if self._redis:
            try:
                state_info = await self._redis.hgetall(self._state_token_keyprefix + state)

                if state_info and "expires_at" in state_info:
                    n = int(state_info["expires_at"])
                    state_info["expires_at"] = n
            
            except Exception as e:
                logging.warning(str(e))
                state_info = {}

        else:
            state_info = self._state_tokens.get(state)

        #-------------------------------------------------

        if not state_info:
            return json_response(
                {
                    "status"    : "not_found",
                    "message"   : "State not found or authentication not started",
                },
                status_code = 404,
                request     = request
            )
        
        elif state_info.get("expires_at", 0) < time.time():
            if not self._redis:
                del self._state_tokens[state]

            return json_response(
                {
                    "status"    : "expired", 
                    "message"   : "Authentication session expired"
                },
                status_code = 410,
                request     = request
            )
        
        elif state_info["status"] == "pending":
            return json_response(
                {
                    "status"    : "pending",
                    "message"   : "Authentication in progress, please continue in browser",
                },
                request     = request
            )
        
        elif state_info["status"] == "completed":
            return json_response(
                {
                    "status"    : "completed",
                    "token"     : state_info.get("token"),
                    "token_type": state_info.get("token_type", "Bearer"),
                    "message"   : "Authentication completed successfully",
                },
                request     = request
            )

        else:
            return json_response(
                {
                    "status"    : "unknown", 
                    "message"   : "Unknown authentication status"
                },
                status_code = 500,
                request     = request
            )

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
                        logging.warning(str(e))

            logging.info(f"Token request - grant_type: {grant_type}, client_id: {client_id}, client_secret: {client_secret}")

            if grant_type == "authorization_code":
                code = data.get("code")
                if code is None or not isinstance(code, str):
                    code = ""

                if self._redis:
                    try:
                        stored_code = await self._redis.hgetall(self._auth_code_keyprefix + code)
                    except Exception as e:
                        logging.warning(str(e))
                        stored_code = {}

                else:
                    stored_code = self._auth_codes.get(code, {})

                # if not stored_code or stored_code["expires_at"] < time.time():
                #     return JSONResponse(
                #         {
                #             "error": "invalid_grant",
                #             "error_description": "Authorization code is invalid or expired.",
                #         },
                #         status_code=400,
                #     )
                # if stored_code["client_id"] != client_id:
                #     return JSONResponse(
                #         {
                #             "error": "invalid_grant",
                #             "error_description": "Client ID mismatch.",
                #         },
                #         status_code=400,
                #     )
                # if not client_id:
                #     return JSONResponse(
                #         {
                #             "error": "invalid_client",
                #             "error_description": "Client authentication failed.",
                #         },
                #         status_code=401,
                #     )

                user_id = stored_code.get("user_id")
                scope   = stored_code.get("scope", "mcp:read mcp:write")

                if not user_id:
                    return json_response(
                        {
                            "error": "server_error",
                            "error_description": "User information not found.",
                        },
                        status_code=500,
                        request=request
                    )

                if self._redis:
                    # TODO:
                    pass
                else:
                    del self._auth_codes[code]

                access_token, refresh_token, err = await self._token_validator.generate_tokens(user_id, "", "mcp", client_id=client_id, scope=scope)
                if err:
                    logging.error(err)

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

            elif grant_type == "refresh_token":
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
                    logging.error(err, extra={"refresh_token": refresh_token, "client_id": client_id})

                    return json_response(
                        {
                            "error": "expired_token",
                            "error_description": err,
                        },
                        status_code=401,
                        request=request
                    )
                
                if not isinstance(payload, dict) or "sub" not in payload:
                    err = "No subject in refresh token."
                    logging.error(err, extra={"refresh_token": refresh_token, "client_id": client_id})

                    return json_response(
                        {
                            "error": "expired_token",
                            "error_description": err,
                        },
                        status_code=401,
                        request=request
                    )

                new_access_token, new_refresh_token, err = await self._token_validator.generate_tokens(payload["sub"], "", "mcp", client_id=client_id)
                if err:
                    logging.error(err, extra={"refresh_token": refresh_token, "client_id": client_id})

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

            elif grant_type == "credentials":
                cached_client = None
                if self._redis:
                    cached_client = await self._redis.hgetall(self._client_keyprefix + client_id)
                else:
                    cached_client = self._clients[client_id]

                if not cached_client or not isinstance(cached_client, dict) or \
                    "client_secret" not in cached_client or not isinstance(cached_client["client_secret"], str) or \
                    "user_id" not in cached_client or not isinstance(cached_client["user_id"], str) or \
                    client_secret != cached_client["client_secret"] or \
                    len(cached_client["user_id"]) == 0:

                    return json_response(
                        {
                            "error": "invalid_client",
                            "error_description": "Invalid client id or secret.",
                        },
                        status_code=401,
                        request=request
                    )
                
                new_access_token, new_refresh_token, err = await self._token_validator.generate_tokens(cached_client["user_id"], "", "mcp", client_id=client_id)
                if err:
                    logging.error(err, extra={"refresh_token": refresh_token, "client_id": client_id})

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
                            "error": "server_error",
                            "error_description": "Failed to generate access token.",
                        },
                        status_code=500,
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

            else:
                return json_response(
                    content     = {
                        "error": "unsupported_grant_type",
                        "error_description": f"Grant type '{grant_type}' is not supported.",
                    },
                    status_code = 400,
                    request     = request
                )
            
        except Exception as e:
            logging.error(str(e))

            return json_response(
                content     = {
                    "error": "server_error",
                    "error_description": str(e)
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
            logging.error("No token found.")
            return json_response({"active": False}, request=request)
        
        payload, err = self._token_validator.verify_token(token)
        if err:
            logging.error(err, extra={"token": token})
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
