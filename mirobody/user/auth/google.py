import jwt

#-----------------------------------------------------------------------------

class GoogleTokenValidator:
    
    GOOGLE_JWK_URL  = "https://www.googleapis.com/oauth2/v3/certs"
    GOOGLE_ISSUER   = "https://accounts.google.com"

    #-----------------------------------------------------

    def __init__(self, client_id: str):
        self._client_id = client_id
        self.jwk_client = jwt.PyJWKClient(self.GOOGLE_JWK_URL)

    #-----------------------------------------------------

    async def verify_token(self, id_token: str) -> tuple[dict | None, str | None]:
        try:
            signing_key = self.jwk_client.get_signing_key_from_jwt(id_token)
            print(signing_key.key)
            
            payload = jwt.decode(
                id_token,
                signing_key.key,
                algorithms  = ["RS256"],
                audience    = self._client_id,
                issuer      = self.GOOGLE_ISSUER,
                options     = {"verify_exp": True}
            )
            
            if not payload.get("email"):
                return None, "Google token missing email claim."
            
            return payload, None
            
        except jwt.ExpiredSignatureError:
            return None, "Google token expired."
        
        except jwt.InvalidAudienceError:
            return None, f"Google token invalid audience, expected: {self._client_id}"
        
        except Exception as e:
            return None, f"Google token verification failed: {e}"

#-----------------------------------------------------------------------------
