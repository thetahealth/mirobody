"""AES-GCM string encryption for values stored in the database.

The byte layout — 12-byte nonce ‖ ciphertext ‖ tag — must match the format
already used for values stored in the database, which is why the parsing
below is spelled out step by step rather than left to a higher-level API.
The one consumer is `pulse/providers/platform/database_service.py`.

Not to be confused with `utils/config/encrypt.py`, which is the Fernet
encrypter the log pipeline uses for its `encrypted_info` field. Two different
ciphers for two different jobs; the previous name (`utils_encrypt.py`) sat one
directory away from `config/encrypt.py` and told you nothing about which was
which.

A THIRD cipher used to live here too: a Fernet `EncryptionService` class plus
`encrypt_string`/`decrypt_string` wrappers and a PBKDF2 key-derivation path,
keyed on `CONFIG_ENCRYPTION_KEY`. Nothing in the project ever called any of it —
which is also why nobody noticed that it duplicated `config/encrypt.py`'s job
with a different salt. Deleted.
"""

import base64
import logging
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from .config import safe_read_cfg

logger = logging.getLogger(__name__)


def decrypt_string_aes_gcm(ciphertext_base64: str, key_hex: str | None = None) -> str | None:
    try:
        if not ciphertext_base64:
            return None

        if key_hex is None:
            key_hex = safe_read_cfg("DATABASE_DECRYPTION_KEY")

        key_bytes = key_hex.encode("utf-8")

        try:
            ciphertext = base64.b64decode(ciphertext_base64)
        except Exception as e:
            logger.error(f"Error decoding base64 ciphertext: {str(e)}")
            return None

        # The nonce is fixed at 12 bytes, matching the layout it was written with.
        nonce_size = 12
        if len(ciphertext) < nonce_size:
            logger.error(f"Ciphertext too short: {len(ciphertext)} < {nonce_size}")
            return None

        # Split the leading nonce from the remaining ciphertext + tag.
        nonce = ciphertext[:nonce_size]
        ciphertext_with_tag = ciphertext[nonce_size:]

        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        aesgcm = AESGCM(key_bytes)

        try:
            plaintext = aesgcm.decrypt(nonce, ciphertext_with_tag, None)
            return plaintext.decode("utf-8")
        except Exception as e:
            logger.error(
                f"AESGCM decrypt failed: {type(e).__name__}: {str(e)} (total_len={len(ciphertext)}, nonce_len={len(nonce)}, ciphertext_with_tag_len={len(ciphertext_with_tag)})"
            )
            return None

    except Exception as e:
        logger.error(f"Unexpected error in AES-GCM decryption: {str(e)}")
        return None


def encrypt_string_aes_gcm(plaintext: str, key_hex: str | None = None) -> str | None:
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
            logger.error(f"Error during AES-GCM encryption: {str(e)}")
            return None

    except Exception as e:
        logger.error(f"Unexpected error in AES-GCM encryption: {str(e)}")
        return None
