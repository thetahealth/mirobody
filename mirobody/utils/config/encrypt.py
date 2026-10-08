import logging

from cryptography.fernet import Fernet

from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

class FernetEncrypter:
    def __init__(self, key: str):
        self._key = key.strip()

        try:
            self._fernet = Fernet(self._key)
        except Exception as e:
            logger.error("unusable CONFIG_ENCRYPTION_KEY: error_type=%s", type(e).__name__,
                         exc_info=not is_driver_exception(e))
            self._fernet = None

    #-----------------------------------------------------

    def decrypt(self, s: str, name: str = "") -> str:
        if not s or not self._fernet:
            return ""

        if not self.is_encrypted(s):
            return s

        try:
            decrypted = self._fernet.decrypt(s.encode()).decode()
            return decrypted

        except Exception as e:
            # InvalidToken carries no message, so `str(e)` alone logged an
            # ERROR with an empty msg: the operator saw red and nothing else.
            # Name the exception, the key, and the fix, because the return
            # below hands the CIPHERTEXT back as if it were the value: the
            # service then signs JWTs with it and every token issued before
            # silently stops verifying.
            key_id = name or "an encrypted config value"
            logger.error(
                "cannot decrypt %s (%s): CONFIG_ENCRYPTION_KEY in .env does not"
                " match the ciphertext in config.${ENV}.yaml. Restore the old"
                " key, or delete that file and let deploy.sh regenerate it."
                " Using the ciphertext as the value until then.",
                key_id, type(e).__name__,
            )

            return s

    #-----------------------------------------------------

    def encrypt(self, s: str) -> str:
        if not s or not self._fernet:
            return ""
        
        try:
            encrypted = self._fernet.encrypt(s.encode()).decode()
            return encrypted

        except Exception as e:
            logger.error("cannot encrypt a config value (%s)",
                         type(e).__name__)

            return s

    #-----------------------------------------------------

    def is_encrypted(self, s: str) -> bool:
        return s.startswith("gAAAA")

#-----------------------------------------------------------------------------
