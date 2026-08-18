"""Credentials must never reach a log line or a response body in the clear.

Six call sites were logging whole bearer tokens, Google/Firebase ID tokens and
an OAuth `client_secret`, and one of them returned the rejected token in the
HTTP 401 body — to the caller, and onward into their proxy logs and error
tracker. None of it was malicious; each site was added to answer a reasonable
debugging question ("which token failed?"), which is exactly why a rule is
worth more here than six individual fixes.

The `encrypted_info` field was the subtler half. It Fernet-encrypts its payload
when `LOG_ENCRYPT_KEY` is set and used to fall back to writing the plaintext
when it is not — under a field name that promises the opposite, so a reader
scanning logs would trust it least where it protected them least.
"""

from __future__ import annotations

import ast
import io
import logging
import pathlib

from .log import JsonFormatter, secret_fingerprint

# A real-shaped JWT: the claims sit in the middle segment, which is why
# truncating to `token[:50]` is not redaction.
_JWT = "eyJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6ImFsaWNlQGV4YW1wbGUuY29tIn0.SIGNATURE"


def _emit(**extra) -> str:
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("test_log_redaction")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger.warning("probe", extra=extra)
    return buf.getvalue()


def test_fingerprint_is_stable_and_distinguishing():
    """It has to survive as a correlation handle, or people go back to raw tokens."""
    assert secret_fingerprint(_JWT) == secret_fingerprint(_JWT)
    assert secret_fingerprint(_JWT) != secret_fingerprint(_JWT + "x")
    assert secret_fingerprint("") == "<none>"
    assert secret_fingerprint(None) == "<none>"


def test_fingerprint_reveals_no_claims():
    out = _emit(token=secret_fingerprint(_JWT))
    assert _JWT not in out
    assert "alice" not in out, "the payload segment carries the email"
    assert "eyJ" not in out


def test_encrypted_info_fails_closed_without_a_key():
    """No key configured must mean no payload — not a plaintext payload."""
    out = _emit(encrypted_info={"token": _JWT})
    assert _JWT not in out
    assert "alice" not in out
    assert "LOG_ENCRYPT_KEY not configured" in out


def test_no_raw_credential_interpolated_into_a_log_call():
    """Reject `logging.x(f"... {token} ...")` and `extra={"token": token}`.

    Matches on the *variable name* rather than the value, since the value is
    only known at runtime. That makes it a naming rule as much as a security
    one: if a variable is called `token`, `client_secret` or `password`, it may
    not be interpolated into a log record un-fingerprinted.
    """
    SECRET_NAMES = {"token", "client_secret", "secret", "password", "id_token",
                    "access_token", "refresh_token", "jwt_key", "api_key"}
    LOG_CALLS = {"debug", "info", "warning", "error", "critical", "exception"}
    offenders: list[str] = []
    root = pathlib.Path(__file__).parent.parent

    def bare_secret(node: ast.AST) -> str | None:
        """A secret-named variable used directly, not wrapped in a call."""
        if isinstance(node, ast.Name) and node.id in SECRET_NAMES:
            return node.id
        if isinstance(node, ast.FormattedValue):
            return bare_secret(node.value)
        if isinstance(node, ast.JoinedStr):
            for v in node.values:
                if (hit := bare_secret(v)):
                    return hit
        return None

    for path in root.rglob("*.py"):
        if "__pycache__" in str(path) or path.name.startswith("test_"):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - the import walk catches these first
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
            if name not in LOG_CALLS:
                continue

            checked = list(node.args)
            for kw in node.keywords:
                # `extra={"token": token}` — inspect the dict's values.
                if kw.arg == "extra" and isinstance(kw.value, ast.Dict):
                    checked.extend(kw.value.values)
                else:
                    checked.append(kw.value)

            for arg in checked:
                if (hit := bare_secret(arg)):
                    offenders.append(f"{path.relative_to(root.parent)}:{node.lineno} ({hit})")

    assert not offenders, (
        "raw credential interpolated into a log call; wrap it in "
        "mirobody.utils.log.secret_fingerprint. Offenders: " + ", ".join(offenders)
    )
