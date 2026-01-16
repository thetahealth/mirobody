import aiohttp, jwt, logging, time

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

#-----------------------------------------------------------------------------

class FirebaseTokenValidator:

    def __init__(self, project_id: str):

        self.project_id         = project_id
        self.firebase_jwk_url   = "https://www.googleapis.com/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com"
        self._public_keys_cache = {}
        self._cache_expiry      = 0

    #-----------------------------------------------------
    
    async def verify_token(self, id_token: str) -> tuple[dict | None, str | None]:
        try:
            unverified_header = jwt.get_unverified_header(id_token)
            kid = unverified_header.get("kid")
            
            if not kid:
                return None, "No kid in token header."
            
            public_key, err = await self._get_public_key(kid)
            if err:
                return None, f"Could not get public key for kid {kid}: {err}"
            if not public_key:
                return None, "Empty public key"
            
            payload = jwt.decode(
                id_token,
                public_key,
                algorithms  = ["RS256"],
                audience    = self.project_id,
                issuer      = f"https://securetoken.google.com/{self.project_id}",
                options     = {"verify_exp": True}
            )
            
            if not payload.get("email"):
                return None, "Firebase token missing email claim."
            
            return payload, None
            
        except jwt.ExpiredSignatureError:
            return None, "Firebase token expired."
        
        except jwt.InvalidAudienceError:
            return None, f"Firebase token invalid audience, expected: {self.project_id}"
        
        except Exception as e:
            return None, f"Firebase token verification failed: {e}"
    
    #-----------------------------------------------------

    async def _get_public_key(self, kid: str) -> tuple[str | None, str | None]:
        try:
            if self._cache_expiry > time.time() and kid in self._public_keys_cache:
                return self._public_keys_cache[kid], None
        
            # Get Firebase public key.
            async with aiohttp.ClientSession() as session:
                async with session.get(self.firebase_jwk_url) as response:
                    if response.status != 200:
                        return None, f"Failed to fetch Firebase public keys: {response.status}"
                    
                    cache_control = response.headers.get("Cache-Control", "")
                    max_age = 60*60
                    if "max-age=" in cache_control:
                        try:
                            max_age = int(cache_control.split("max-age=")[1].split(",")[0])
                        except Exception as e:
                            logging.warning(str(e))
                    
                    # Get certificates.
                    certs = await response.json()
                    
                    # Update public keys.
                    self._public_keys_cache = {}
                    for cert_kid, cert_data in certs.items():
                        # Transform X.509 certificate to public key.
                        try:
                            cert_bytes  = cert_data.encode("utf-8")
                            cert        = x509.load_pem_x509_certificate(cert_bytes, default_backend())
                            public_key  = cert.public_key()
                            
                            # Transform public key to PEM.
                            pem = public_key.public_bytes(
                                encoding= serialization.Encoding.PEM,
                                format  = serialization.PublicFormat.SubjectPublicKeyInfo
                            ).decode("utf-8")
                            
                            self._public_keys_cache[cert_kid] = pem

                        except Exception as e:
                            logging.error(f"Failed to parse certificate for kid {cert_kid}: {e}")

                    self._cache_expiry = time.time() + max_age
                    
                    return self._public_keys_cache.get(kid), None
                    
        except Exception as e:
            return None, f"Failed to get public key: {e}"

#-----------------------------------------------------------------------------
