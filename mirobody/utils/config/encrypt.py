import base64, logging

from cryptography.fernet import Fernet

#-----------------------------------------------------------------------------

class AbstractEncrypter:
    def decrypt(self, s: str) -> str: ...
    def encrypt(self, s: str) -> str: ...
    def is_encrypted(self, s: str) -> bool: ...

#-----------------------------------------------------------------------------

class FernetEncrypter:
    def __init__(self, key: str):
        self._key = key.strip()

        try:
            self._fernet = Fernet(self._key)
        except Exception as e:
            logging.error(str(e), exc_info=True)
            self._fernet = None

    #-----------------------------------------------------

    def decrypt(self, s: str) -> str:
        if not s or not self._fernet:
            return ""
        
        if not self.is_encrypted(s):
            return s
        
        try:
            decrypted = self._fernet.decrypt(s.encode()).decode()
            return decrypted

        except Exception as e:
            logging.error(str(e), extra={"s": s})
            
            return s

    #-----------------------------------------------------

    def encrypt(self, s: str) -> str:
        if not s or not self._fernet:
            return ""
        
        try:
            encrypted = self._fernet.encrypt(s.encode()).decode()
            return encrypted

        except Exception as e:
            logging.error(str(e), extra={"s": s})

            return s

    #-----------------------------------------------------

    def is_encrypted(self, s: str) -> bool:
        return s.startswith("gAAAA")

#-----------------------------------------------------------------------------
