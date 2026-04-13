import jwt, logging, secrets, urllib.parse

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from .jwt import AbstractTokenValidator
from .email import create_email_validator
from .apple import AppleTokenValidator
from .google import GoogleTokenValidator
from .firebase import FirebaseTokenValidator
from .webauthn import WebAuthnService

from .user import (
    add_or_get_user,
    del_user,
    get_user_via_apple_subject
)

from ..utils import (
    json_response_with_code,
    json_response,

    get_jwt_token,

    Request,
    Response,
    Route
)

#-----------------------------------------------------------------------------

class UserService:
    def __init__(
        self,
        token_validator : AbstractTokenValidator,

        uri_prefix      : str = "",
        routes          : list | None = None,
        
        db_pool         : AsyncConnectionPool | None = None,
        redis           : Redis | None = None,

        email_smtp_host : str = "",
        email_smtp_port : int = 0,
        email_smtp_user : str = "",
        email_from      : str = "",
        email_from_name : str = "",
        email_template  : str = "",
        email_password  : str = "",
        email_predefined: str | bytes | bytearray | dict[str, str] | None = None,

        apple_client_id : str = "",
        apple_team_id   : str = "",
        apple_key_id    : str = "",
        apple_private_key   : str = "",
        apple_auth_client_id: str = "",

        google_client_id    : str = "",
        firebase_project_id : str = "",

        qr_login_url    : str = "",

        # WebAuthn (AAL2).
        webauthn_rp_id      : str = "",
        webauthn_rp_name    : str = "",
        webauthn_origin     : str = "",
        webauthn_mfa_ticket_ttl : int = 300,
    ):
        self._token_validator   = token_validator
        self._get_jwt_token     = get_jwt_token
        self._qr_login_url      = qr_login_url

        self._email_validator = create_email_validator(
            smtp_host       = email_smtp_host,
            smtp_port       = email_smtp_port,
            smtp_user       = email_smtp_user if email_smtp_user else email_from,
            smtp_pass       = email_password,
            mandrill_api_key= email_password,
            from_email      = email_from,
            from_name       = email_from_name if email_from_name else "Theta Wellness",
            template        = email_template,
            predefined_codes= email_predefined,
            redis           = redis
        )
        
        if apple_client_id and apple_team_id and apple_key_id and apple_private_key:
            self._apple_validator = AppleTokenValidator(
                apple_client_id,
                apple_team_id,
                apple_key_id,
                apple_private_key,
                auth_client_id = apple_auth_client_id
            )
        else:
            self._apple_validator = None

        if google_client_id:
            self._google_validator = GoogleTokenValidator(google_client_id)
        else:
            self._google_validator = None

        if firebase_project_id:
            self._firebase_validator = FirebaseTokenValidator(firebase_project_id)
        else:
            self._firebase_validator = None

         #-------------------------------------------------

        self._db_pool = db_pool

        self._redis = redis
        if self._redis:
            self._qr_state_keyprefix = "mirobody:user:qr:state:"
        else:
            # Use local memory when no redis connection is available.
            self._qr_states = {}

        # WebAuthn service (enabled only when rp_id is configured).
        self._webauthn_service = WebAuthnService(
            token_validator = token_validator,
            uri_prefix      = uri_prefix,
            routes          = routes,
            db_pool         = db_pool,
            redis           = redis,
            rp_id           = webauthn_rp_id,
            rp_name         = webauthn_rp_name,
            origin          = webauthn_origin,
            mfa_ticket_ttl  = webauthn_mfa_ticket_ttl,
        ) if webauthn_rp_id else None

         #-------------------------------------------------

        if routes is not None:
            self.routes = routes
        else:
            self.routes = []

        self.routes.append(Route(f"{uri_prefix}/email/login", endpoint=self.email_login_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/email/verify", endpoint=self.email_verify_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/email/register", endpoint=self.email_register_handler, methods=["POST", "OPTIONS"]))

        self.routes.append(Route(f"{uri_prefix}/user/del", endpoint=self.user_unregister_handler, methods=["POST", "OPTIONS"]))

        if self._apple_validator:
            self.routes.append(Route(f"{uri_prefix}/apple/verify", endpoint=self.apple_verify_handler, methods=["POST", "OPTIONS"]))

        if self._google_validator or self._firebase_validator:
            self.routes.append(Route(f"{uri_prefix}/google/verify", endpoint=self.google_verify_handler, methods=["POST", "OPTIONS"]))

        if self._qr_login_url:
            self.routes.append(Route(f"{uri_prefix}/qr/login", endpoint=self.qr_login_handler, methods=["GET", "POST", "OPTIONS"]))
            self.routes.append(Route(f"{uri_prefix}/qr/verify", endpoint=self.qr_verify_handler, methods=["POST", "OPTIONS"]))
            self.routes.append(Route(f"{uri_prefix}/qr/check", endpoint=self.qr_check_handler, methods=["POST", "OPTIONS"]))

    #-------------------------------------------------------------------------

    async def email_login_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        if not self._email_validator:
            return json_response_with_code(-1, "Invalid email validator.", request=request)

        try:
            data = await request.json()
            email = data.get("email")

            err = await self._email_validator.send(email)
            if err:
                return json_response_with_code(-2, err, request=request)

        except Exception as e:
            return json_response_with_code(-3, str(e), request=request)

        return json_response_with_code(data={"email": email}, request=request)
    
    #-------------------------------------------------------------------------

    async def email_verify_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        if not self._email_validator:
            return json_response_with_code(-1, "Invalid email validator.", request=request)

        try:
            data = await request.json()
            email = data.get("email")
            code = data.get("code")

            err = await self._email_validator.verify(email, code)
            if err:
                return json_response_with_code(-2, err, request=request)
        
        except Exception as e:
            return json_response_with_code(-3, str(e), request=request)
        
        #-------------------------------------------------

        id, err = await add_or_get_user(self._db_pool, email)
        if err:
            return json_response_with_code(-4, err, request=request)

        #-------------------------------------------------

        return await self._generate_auth_response(id, email, "email", request)

    #-------------------------------------------------------------------------

    async def email_register_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        #-------------------------------------------------

        token = self._get_jwt_token(request)

        payload, err = self._token_validator.verify_token(token)
        if err:
            return json_response_with_code(-1, err, request=request)
        
        #-------------------------------------------------
        # Generate a Mirobody JWT token.

        email = payload.get("email")
        if not email:
            return json_response_with_code(-2, "No email found", request=request)
        
        id, name, err = await add_or_get_user(self._db_pool, email)
        if err:
            return json_response_with_code(-3, err, request=request)
        
        access_token, refresh_token, err = await self._token_validator.generate_tokens(str(id), email)
        if err:
            return json_response_with_code(-4, err, request=request)

        return json_response_with_code(data={"token": access_token}, request=request)

    #-------------------------------------------------------------------------

    async def user_unregister_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        #-------------------------------------------------

        if not request.state.user_id or \
            not isinstance(request.state.user_id, int) or \
            request.state.user_id <= 0:

            return json_response(status_code=401, request=request)

        user_id = request.state.user_id

        #-------------------------------------------------

        err = del_user(self._db_pool, user_id)
        if err:
            return json_response_with_code(-1, err, request=request)
        
        return json_response_with_code(request=request)

    #-------------------------------------------------------------------------

    async def apple_verify_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        try:
            request_json = await request.json()

            token   = request_json.get("token")
            code    = request_json.get("code")
            if not token and not code:
                return json_response_with_code(-1, "Apple ID token or authorization code is required.", request=request)

            email   = request_json.get("email")
            name    = request_json.get("name")

            payload = None

            #---------------------------------------------

            if code:
                logging.debug(f"Apple authorization code: {code}")

                payload, err = await self._apple_validator.verify_authorization_code(code)
                if err:
                    logging.error(err, extra={"code": code, "email": email})

                    if not token:
                        return json_response_with_code(-2, err, request=request)

            #---------------------------------------------

            if not payload:
                if not token:
                    return json_response_with_code(-3, "Apple ID token is required.", request=request)

                else:
                    logging.debug(f"Apple JWT token: {token}")

                    payload, err = await self._apple_validator.verify_token(token)
                    if err:
                        return json_response_with_code(-4, err, request=request)
                    if not payload:
                        return json_response_with_code(-5, "Empty payload.", request=request)

            #---------------------------------------------

            apple_subject = payload.get("sub")      # Apple user ID.
            if not apple_subject:
                return json_response_with_code(-6, "Empty Apple subject.", request=request)

            id, email, err = await get_user_via_apple_subject(self._db_pool, apple_subject)
            if err:
                logging.warning(err, extra={"apple_subject": apple_subject})

                email = payload.get("email")
                if not email:
                    email = f"{apple_subject}@apple-private.com"

                id, err = await add_or_get_user(self._db_pool, email, name, apple_subject)
                if err:
                    return json_response_with_code(-7, err, request=request)
                if not id:
                    return json_response_with_code(-8, "Empty user ID.", request=request)

            #---------------------------------------------

            return await self._generate_auth_response(id, email, "apple", request)

        except Exception as e:
            return json_response_with_code(-10, str(e), request=request)

    #-------------------------------------------------------------------------

    async def google_verify_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        try:
            request_json = await request.json()
            
            token = request_json.get("token")
            if not token:
                return json_response_with_code(-1, "Google ID token is required", request=request)

            #---------------------------------------------

            try:
                unverified_payload = jwt.decode(
                    token,
                    options = {
                        "verify_signature" : False
                    }
                )
                token_issuer = unverified_payload.get("iss", "")
            except:
                token_issuer = ""

            payload = None

            if "securetoken.google.com" in token_issuer:
                # Firebase validator first.
                if self._firebase_validator:
                    payload, err = await self._firebase_validator.verify_token(token)
                    if err:
                        logging.warning(err, extra={"token": token})

                if not payload and self._google_validator:
                    payload, err = await self._google_validator.verify_token(token)
                    if err:
                        logging.warning(err, extra={"token": token})
            
            else:
                # Google validator first.
                if self._google_validator:
                    payload, err = await self._google_validator.verify_token(token)
                    if err:
                        logging.warning(err, extra={"token": token})

                if not payload and self._firebase_validator:
                    payload, err = await self._firebase_validator.verify_token(token)
                    if err:
                        logging.warning(err, extra={"token": token})

            if not payload:
                return json_response_with_code(-2, "Invalid Google/Firebase ID token.", request=request)

            #---------------------------------------------

            verified_email = payload.get("email")
            if not verified_email:
                return json_response_with_code(-3, "No email in verified token.", request=request)

            id, err = await add_or_get_user(self._db_pool, verified_email)
            if err:
                return json_response_with_code(-4, err, request=request)
            
            #-------------------------------------------------

            return await self._generate_auth_response(id, verified_email, "google", request)

        except Exception as e:
            return json_response_with_code(-6, str(e), request=request)

    #-------------------------------------------------------------------------

    async def qr_login_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        state = secrets.token_urlsafe(32)
        
        #-------------------------------------------------

        if self._redis:
            try:
                await self._redis.set(self._qr_state_keyprefix + state, "", 10 * 60)
            
            except Exception as e:
                logging.warning(str(e))

                return json_response_with_code(-1, str(e), request=request)

        else:
            self._qr_states[state] = ""

        check_url = urllib.parse.quote_plus(
            f"https://{request.url.hostname}/qr/verify?state={state}"
        )

        return json_response_with_code(data={"qrCode": f"{self._qr_login_url}?check={check_url}"})

    #-------------------------------------------------------------------------

    async def qr_verify_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        state = request.query_params["state"]
        if not state:
            return json_response_with_code(-1, "No state found.", request=request)
        
        #-------------------------------------------------
        # Get 3rd JWT token.

        jwt_token = request.headers.get("Authorization")
        if jwt_token and isinstance(jwt_token, str):
            while jwt_token.startswith("Bearer "):
                jwt_token = jwt_token[7:]

        if not jwt_token:
            return json_response_with_code(-2, "Invalid JWT token.", request=request)
        
        #-------------------------------------------------
        # Check the 3rd JWT token.

        payload, err = self._token_validator.verify_token(token=jwt_token)
        if err:
            return json_response_with_code(-3, err, request=request)
        
        if not isinstance(payload, dict) or "email" not in payload:
            return json_response_with_code(-4, "Invalid JWT payload.", request=request)
        
        #-------------------------------------------------
        # Generate a Mirobody JWT token.

        email   = payload.get("email")
        name    = email.split("@")[0].strip()

        id, err = await add_or_get_user(self._db_pool, email, name)
        if err:
            return json_response_with_code(-5, err, request=request)
        
        access_token, refresh_token, err = await self._token_validator.generate_tokens(str(id), email)
        if err:
            return json_response_with_code(-6, err, request=request)
        
        #-------------------------------------------------
        # Save the Mirobody JWT token.
        
        if self._redis:
            try:
                await self._redis.set(self._qr_state_keyprefix + state, access_token, 10 * 60)

            except Exception as e:
                logging.warning(str(e))

                return json_response_with_code(-7, str(e), request=request)

        else:
            self._qr_states[state] = access_token

        #-------------------------------------------------

        return json_response_with_code()

    #-------------------------------------------------------------------------

    async def qr_check_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        state = request.query_params["state"]
        if not state:
            return json_response_with_code(-1, "No state found.", request=request)
        
        #-------------------------------------------------

        if self._redis:
            try:
                jwt_token = await self._redis.get(self._qr_state_keyprefix + state)

            except Exception as e:
                logging.warning(str(e))

                jwt_token = None

        else:
            jwt_token = self._qr_states.get(state)

        #-------------------------------------------------

        if not jwt_token:
            return json_response_with_code(-2, "Authentication not started.", request=request, disable_log=True)
        
        else:
            if self._redis:
                try:
                    await self._redis.delete(self._qr_state_keyprefix + state)

                except Exception as e:
                    logging.warning(str(e))

            else:
                del self._qr_states[state]

        #-------------------------------------------------
        
        return json_response_with_code(data={"accessToken": jwt_token}, request=request)

    #-------------------------------------------------------------------------

    async def _generate_auth_response(
        self,
        user_id: int,
        email: str,
        auth_method: str = "",
        request: Request | None = None,
    ) -> Response:
        """Generate final auth response, with MFA challenge if required."""
        # Check if WebAuthn MFA is required.
        if self._webauthn_service:
            mfa_challenge = await self._webauthn_service.check_mfa_required(user_id, email)
            if mfa_challenge:
                # Include a fallback AAL1 token so frontend can degrade gracefully
                # if user cancels WebAuthn (e.g. Touch ID dismissed).
                fallback_token, _, _ = await self._token_validator.generate_tokens(
                    str(user_id), email, auth_method,
                    gen_claims_func=lambda uid, em: {"aal": 1},
                )
                if fallback_token:
                    mfa_challenge["fallback_token"] = fallback_token
                return json_response_with_code(data=mfa_challenge, request=request)

        # No MFA required — issue AAL1 token directly.
        aal_level = 1 if self._webauthn_service else None

        access_token, refresh_token, err = await self._token_validator.generate_tokens(
            str(user_id),
            email,
            auth_method,
            gen_claims_func=(lambda uid, em: {"aal": aal_level}) if aal_level else None,
        )
        if err:
            return json_response_with_code(-100, err, request=request)

        result = self._generate_verification_response(
            access_token, refresh_token, user_id=user_id, email=email
        )

        # Tell frontend WebAuthn is not yet registered for this user.
        if self._webauthn_service:
            result["webauthn_registered"] = False

        return json_response_with_code(data=result, request=request)

    #-------------------------------------------------------------------------

    def _generate_verification_response(
        self,
        access_token    : str,
        refresh_token   : str | None = None,
        scope           : str | None = None,
        user_id         : int = 0,
        email           : str = ""
    ) -> dict:

        result = {
            "access_token"  : access_token,
            "token_type"    : "Bearer",
            "expires_in"    : self._token_validator.get_expires_in() if self._token_validator else 60*60*24*7,
            # "user_id"       : str(user_id),
            # "email"         : email,
            # "name"          : email.split("@")[0]
        }

        if refresh_token:
            result["refresh_token"] = refresh_token
        if scope:
            result["scope"] = scope

        return result

#-----------------------------------------------------------------------------
