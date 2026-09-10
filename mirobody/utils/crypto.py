"""AES-GCM string encryption for values stored in the database.

Two things are fixed by the rows already written and must not change: the byte
layout — 12-byte nonce ‖ ciphertext ‖ 16-byte tag, then base64 — and the key
handling — the configured `DATABASE_DECRYPTION_KEY` string's UTF-8 bytes ARE the
AES key (not hex-decoded, despite the parameter name). The one consumer is
`pulse/providers/platform/database_service.py` (device credentials and OAuth
tokens).

Not to be confused with `utils/config/encrypt.py`, which is the Fernet encrypter
the log pipeline uses for its `encrypted_info` field.

The two directions used to be written at two different API levels (the low-level
`Cipher`/`modes.GCM` for encrypt, `AESGCM` for decrypt), each under three nested
try/except blocks. `AESGCM` does both, and one guard per function keeps the
contract callers rely on: any failure is logged and returns None.
"""

import base64
import logging
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .config import safe_read_cfg

logger = logging.getLogger(__name__)

_NONCE_BYTES = 12


def _key(key_hex: str | None) -> bytes:
    if key_hex is None:
        key_hex = safe_read_cfg("DATABASE_DECRYPTION_KEY")
    return key_hex.encode("utf-8")


def encrypt_string_aes_gcm(plaintext: str, key_hex: str | None = None) -> str | None:
    if not plaintext:
        return None
    try:
        nonce = os.urandom(_NONCE_BYTES)
        sealed = AESGCM(_key(key_hex)).encrypt(nonce, plaintext.encode("utf-8"), None)
        return base64.b64encode(nonce + sealed).decode("utf-8")
    except Exception as e:
        logger.error(f"AES-GCM encryption failed: {type(e).__name__}: {e}")
        return None


def decrypt_string_aes_gcm(ciphertext_base64: str, key_hex: str | None = None) -> str | None:
    if not ciphertext_base64:
        return None
    try:
        blob = base64.b64decode(ciphertext_base64)
        if len(blob) < _NONCE_BYTES:
            logger.error(f"Ciphertext too short: {len(blob)} < {_NONCE_BYTES}")
            return None
        plain = AESGCM(_key(key_hex)).decrypt(blob[:_NONCE_BYTES], blob[_NONCE_BYTES:], None)
        return plain.decode("utf-8")
    except Exception as e:
        logger.error(f"AES-GCM decryption failed: {type(e).__name__}: {e} (b64_len={len(ciphertext_base64)})")
        return None
