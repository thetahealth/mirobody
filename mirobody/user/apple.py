import aiohttp, base64, jwt, logging, time

#-----------------------------------------------------------------------------

class AppleTokenValidator:

    APPLE_JWK_URL   = "https://appleid.apple.com/auth/keys"
    APPLE_ISSUER    = "https://appleid.apple.com"
    APPLE_TOKEN_URL = "https://appleid.apple.com/auth/token"
    
    def __init__(
            self,
            client_id: str,
            team_id: str,
            key_id: str,
            private_key: str,
            expires_in: int = 60*60*24*180,
            auth_client_id: str = ""
        ):

        self.client_id  = client_id
        self.team_id    = team_id
        self.key_id     = key_id
        self.private_key= private_key
        self._expires_in= expires_in

        self.auth_client_id = auth_client_id if auth_client_id and isinstance(auth_client_id, str) else client_id

        self.jwk_client = jwt.PyJWKClient(self.APPLE_JWK_URL)

        if not self.private_key.startswith("-----BEGIN PRIVATE KEY-----"):
            # Base64 string, deocde it.
            try:
                b = base64.b64decode(self.private_key)
                s = b.decode()
                self.private_key = s

            except Exception as e:
                logging.warning(e)
                pass


    #-------------------------------------------------------------------------
    
    async def verify_token(self, id_token: str) -> tuple[dict | None, str | None]:
        try:
            signing_key = self.jwk_client.get_signing_key_from_jwt(id_token)
            
            payload = jwt.decode(
                id_token,
                signing_key.key,
                algorithms  = ["RS256"],
                audience    = self.client_id,
                issuer      = self.APPLE_ISSUER,
                options     = {
                    "verify_exp": True,
                    "verify_aud": False # TODO: DO NOT CHECK AUD TEMPORARILY.
                }
            )
            
            if not payload.get("email"):
                # No email found, initiate a virtual email via sub field
                payload["email"] = f"{payload.get('sub')}@apple-private.com"
            
            return payload, None
            
        except jwt.ExpiredSignatureError:
            return None, "Apple token expired."
        
        except jwt.InvalidAudienceError:
            return None, f"Apple token invalid audience, expected: {self.client_id}"
        
        except Exception as e:
            return None, f"Apple token verification failed: {e}"
    
    #-------------------------------------------------------------------------

    async def verify_authorization_code(self, code: str) -> tuple[dict | None, str | None]:
        try:
            client_secret = self._generate_client_secret()
            
            data = {
                "client_id"     : self.auth_client_id,
                "client_secret" : client_secret,
                "code"          : code,
                "grant_type"    : "authorization_code"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(self.APPLE_TOKEN_URL, data=data) as response:
                    if response.status != 200:
                        error_text = await response.text()

                        return None, f"Apple token exchange failed: {error_text}"
                    
                    result = await response.json()
                    if result.get("id_token"):
                        return await self.verify_token(result["id_token"])
                    
                    return None, "No id_token found"
                    
        except Exception as e:
            return None, f"Apple authorization code verification failed: {e}"
    
    #-------------------------------------------------------------------------

    def _generate_client_secret(self) -> str:
        headers = {
            "alg": "ES256",
            "kid": self.key_id
        }
        
        payload = {
            "iss": self.team_id,
            "iat": int(time.time()),
            "exp": int(time.time()) + self._expires_in,
            "aud": "https://appleid.apple.com",
            "sub": self.auth_client_id
        }
        
        return jwt.encode(
            payload,
            self.private_key,
            algorithm   = "ES256",
            headers     = headers
        )

#-----------------------------------------------------------------------------
