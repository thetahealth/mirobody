"""Deriving the config encryption key, and refusing to destroy a secret.

Two bugs met here and the second one ate data.

`get_fernet_key` truncated to 32 CHARACTERS and padded to 32 BYTES — the same
operation only for ASCII. A passphrase with any CJK character produced 33-96
bytes, `Fernet()` rejected it, and `FernetEncrypter` silently set
`self._fernet = None`, making `encrypt()` return `""`.

`_load_data` then wrote that `""` back over the real secret in the YAML file,
while `self._raw` kept the plaintext in memory — so the process kept working
and the key was simply gone at the next restart, with nothing to point at.
"""

from __future__ import annotations

import base64

import pytest
from cryptography.fernet import Fernet

from mirobody.utils.config.config import Config


class _Probe(Config):
    """A Config whose only job is to answer `get_str` with a fixed value."""

    def __init__(self, value: str):
        self._v = value

    def get_str(self, key: str, default: str = "") -> str:
        return self._v


def _legacy_derivation(s: str) -> str:
    """What the code did before, kept as the compatibility oracle."""
    s = s.strip()
    if len(s) > 32:
        s = s[:32]
    return base64.urlsafe_b64encode(s.encode().ljust(32, b"0")).decode()


@pytest.mark.parametrize("passphrase", [
    "correct-horse-battery-staple",
    "我的密钥非常安全请勿泄露",          # the failing case: 12 chars, 36 bytes
    "keyed-with-🔐-emoji",
    "Paßwort-mit-Umlauten-äöü",
    "a" * 40,                            # longer than 32
    "a" * 32,                            # exactly 32
    "short",
    "",                                  # unset
])
def test_every_passphrase_yields_a_usable_fernet_key(passphrase):
    key = _Probe(passphrase).get_fernet_key("CONFIG_ENCRYPTION_KEY")
    f = Fernet(key)                                  # raised for the CJK case
    assert f.decrypt(f.encrypt(b"secret")) == b"secret"


@pytest.mark.parametrize("passphrase", ["correct-horse", "a" * 40, "", "x" * 32])
def test_ascii_derivation_is_unchanged(passphrase):
    """Anything already encrypted must still decrypt: an ASCII passphrase has
    to derive the exact key it derived before, or every existing deployment's
    config becomes unreadable on upgrade."""
    assert _Probe(passphrase).get_fernet_key("K") == _legacy_derivation(passphrase)


def test_a_multibyte_passphrase_is_cut_on_a_byte_boundary():
    """32 bytes, not 32 characters — the actual fix."""
    key = _Probe("我" * 20).get_fernet_key("K")       # 20 chars, 60 bytes
    assert len(base64.urlsafe_b64decode(key)) == 32


def _config_with(encrypter, tmp_path):
    """A Config wired to `encrypter`, plus a YAML file holding one secret."""
    cfg = Config.__new__(Config)
    cfg._raw = {}
    cfg._encrypter = encrypter
    path = tmp_path / "config.yaml"
    path.write_text("OPENAI_API_KEY: sk-a-real-key-that-must-survive\n", encoding="utf-8")
    return cfg, path


class _DeadEncrypter:
    """What an unusable key produces: encrypt() returns the empty string."""

    def is_encrypted(self, s): return False
    def decrypt(self, s): return s
    def encrypt(self, s): return ""


class _RealEncrypter:
    """Stands in for Fernet. Actually transforms the value — a fake that only
    prefixed the plaintext would let the "not on disk" assertion pass
    vacuously."""

    def is_encrypted(self, s): return s.startswith("gAAAAA")
    def decrypt(self, s):
        return base64.urlsafe_b64decode(s.removeprefix("gAAAAA")).decode()
    def encrypt(self, s):
        return "gAAAAA" + base64.urlsafe_b64encode(s.encode()).decode()


def test_an_unusable_encrypter_does_not_overwrite_the_secret_on_disk(tmp_path):
    """The data-loss half, tested against a real file.

    This wrote `OPENAI_API_KEY: ''` over the user's key. `_raw` kept the
    plaintext, so the running process was fine and the loss only appeared at
    the next restart, with nothing in the logs to point at.
    """
    cfg, path = _config_with(_DeadEncrypter(), tmp_path)
    cfg.load_yaml(str(path))

    assert "sk-a-real-key-that-must-survive" in path.read_text(encoding="utf-8")
    assert cfg._raw["OPENAI_API_KEY"] == "sk-a-real-key-that-must-survive"


def test_a_working_encrypter_still_encrypts_on_disk(tmp_path):
    """The guard must not disable encryption for everyone else."""
    cfg, path = _config_with(_RealEncrypter(), tmp_path)
    cfg.load_yaml(str(path))

    on_disk = path.read_text(encoding="utf-8")
    assert "gAAAAA" in on_disk
    assert "sk-a-real-key-that-must-survive" not in on_disk
