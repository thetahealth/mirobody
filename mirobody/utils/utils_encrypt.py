"""
Encryption utility module

Provides string encryption/decryption functions, corresponding to utils.EncryptString and utils.DecryptString in Go code.
"""

import base64, logging
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from typing import Optional

from .config import global_config, safe_read_cfg


class EncryptionService:
    def __init__(self):
        self.fernet = Fernet(
            global_config().get_fernet_key("CONFIG_ENCRYPTION_KEY")
        )

    def encrypt_string(self, plaintext: str, key: Optional[str] = None) -> Optional[str]:
        try:
            if not plaintext:
                return None

            if key:
                fernet = self._get_fernet_from_key(key)
            else:
                fernet = self.fernet

            if not fernet:
                logging.error("Fernet encryptor not available")
                return None

            encrypted_bytes = fernet.encrypt(plaintext.encode("utf-8"))
            return encrypted_bytes.decode("utf-8")

        except Exception as e:
            logging.error(f"Error encrypting string: {str(e)}")
            return None

    def decrypt_string(self, encrypted_text: str, key: Optional[str] = None) -> Optional[str]:
        try:
            if not encrypted_text:
                return None

            if key:
                fernet = self._get_fernet_from_key(key)
            else:
                fernet = self.fernet

            if not fernet:
                logging.error("Fernet decryptor not available")
                return None

            decrypted_bytes = fernet.decrypt(encrypted_text.encode("utf-8"))
            return decrypted_bytes.decode("utf-8")

        except Exception as e:
            logging.error(f"Error decrypting string: {str(e)}")
            return None

    def _get_fernet_from_key(self, key: str) -> Optional[Fernet]:
        try:
            if len(key) == 44 and key.endswith("="):
                return Fernet(key.encode())

            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=b"holywell_salt",
                iterations=100000,
            )
            key_bytes = kdf.derive(key.encode())
            fernet_key = base64.urlsafe_b64encode(key_bytes)
            return Fernet(fernet_key)

        except Exception as e:
            logging.error(f"Error creating Fernet from key: {str(e)}")
            return None


_encryption_service = None


def encrypt_string(plaintext: str, key: Optional[str] = None) -> Optional[str]:
    global _encryption_service
    if not _encryption_service:
        _encryption_service = EncryptionService()

    return _encryption_service.encrypt_string(plaintext, key)


def decrypt_string(encrypted_text: str, key: Optional[str] = None) -> Optional[str]:
    global _encryption_service
    if not _encryption_service:
        _encryption_service = EncryptionService()

    return _encryption_service.decrypt_string(encrypted_text, key)


def decrypt_string_aes_gcm(ciphertext_base64: str, key_hex: Optional[str] = None) -> Optional[str]:
    try:
        if not ciphertext_base64:
            return None

        if key_hex is None:
            key_hex = safe_read_cfg("DATABASE_DECRYPTION_KEY")

        key_bytes = key_hex.encode("utf-8")

        try:
            ciphertext = base64.b64decode(ciphertext_base64)
        except Exception as e:
            logging.error(f"Error decoding base64 ciphertext: {str(e)}")
            return None

        # Go: nonceSize := gcm.NonceSize()
        nonce_size = 12
        if len(ciphertext) < nonce_size:
            logging.error(f"Ciphertext too short: {len(ciphertext)} < {nonce_size}")
            return None

        # Go: nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
        nonce = ciphertext[:nonce_size]
        ciphertext_with_tag = ciphertext[nonce_size:]

        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        aesgcm = AESGCM(key_bytes)

        try:
            plaintext = aesgcm.decrypt(nonce, ciphertext_with_tag, None)
            return plaintext.decode("utf-8")
        except Exception as e:
            logging.error(
                f"AESGCM decrypt failed: {type(e).__name__}: {str(e)} (total_len={len(ciphertext)}, nonce_len={len(nonce)}, ciphertext_with_tag_len={len(ciphertext_with_tag)})"
            )
            return None

    except Exception as e:
        logging.error(f"Unexpected error in AES-GCM decryption: {str(e)}")
        return None


def encrypt_string_aes_gcm(plaintext: str, key_hex: Optional[str] = None) -> Optional[str]:
    try:
        if not plaintext:
            return None

        if key_hex is None:
            key_hex = safe_read_cfg("DATABASE_DECRYPTION_KEY")

        key_bytes = key_hex.encode("utf-8")

        import os

        nonce = os.urandom(12)

        algorithm = algorithms.AES(key_bytes)
        mode = modes.GCM(nonce)
        cipher = Cipher(algorithm, mode, backend=default_backend())
        encryptor = cipher.encryptor()

        try:
            ciphertext = encryptor.update(plaintext.encode("utf-8")) + encryptor.finalize()
            tag = encryptor.tag

            combined = nonce + ciphertext + tag

            return base64.b64encode(combined).decode("utf-8")
        except Exception as e:
            logging.error(f"Error during AES-GCM encryption: {str(e)}")
            return None

    except Exception as e:
        logging.error(f"Unexpected error in AES-GCM encryption: {str(e)}")
        return None
