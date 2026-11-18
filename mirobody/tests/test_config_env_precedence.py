"""Issue #299: `Config.placeholder_keys()` must judge the EFFECTIVE value a
server would use (environment first, then YAML — `get()`'s own precedence),
not `self._raw` alone.

`deploy.sh` writes every secret it generates to `.env` and never edits the
YAML, so a Docker stack ships `config.yaml` and `config.devices.yaml` with
`REPLACE_THIS_VALUE_IN_PRODUCTION` for `PG_PASSWORD`, `PG_ENCRYPTION_KEY`,
`JWT_KEY` and `DATABASE_DECRYPTION_KEY` for good. Before this fix,
`placeholder_keys()` scanned `self._raw` (the YAML alone), so every one of
those kept failing `enforce_production_auth_safety()` even though `get_str()`
already returned the real secret from `.env` — `PRODUCTION: true` refused to
start on every such stack.
"""
import io

import pytest

# The config module needs the app's dependencies, which CI's minimal install
# (`.[test]` only) does not have; every case here builds a real `Config`.
pytest.importorskip("dotenv")
pytest.importorskip("ruamel.yaml")
pytest.importorskip("cryptography")

import mirobody.utils.config.config as cfg_mod
from mirobody.utils.config.config import PLACEHOLDER_SENTINEL, Config


def _config(yaml_text: str, monkeypatch) -> Config:
    # `Config.__init__` sets the process-wide `_global_config`. Snapshot it
    # with `monkeypatch.setattr` (not `None`) so pytest's teardown puts back
    # whatever this process already had, and a Config built here cannot
    # change what a later test's `global_config()` returns.
    monkeypatch.setattr(cfg_mod, "_global_config", cfg_mod._global_config)
    return Config(io.StringIO(yaml_text))


def test_a_real_env_secret_over_a_yaml_placeholder_starts(monkeypatch):
    """The exact repro in #299: `.env` carries the real `PG_PASSWORD`, the
    shipped YAML still carries the placeholder. `get_str()` already returned
    the real value; `placeholder_keys()` must agree so the production guard
    does not refuse to start."""
    monkeypatch.setenv("PG_PASSWORD", "pw-from-dotenv")
    config = _config("PG_PASSWORD: " + PLACEHOLDER_SENTINEL + "\n", monkeypatch)

    assert config.get_str("PG_PASSWORD") == "pw-from-dotenv"
    assert "PG_PASSWORD" not in config.placeholder_keys()


def test_an_env_value_that_is_the_placeholder_is_still_refused(monkeypatch):
    """Setting the sentinel itself in the environment does not excuse it,
    even when the YAML underneath holds a real value (an overlay a previous
    run wrote, say)."""
    monkeypatch.setenv("JWT_KEY", PLACEHOLDER_SENTINEL)
    config = _config("JWT_KEY: real-overlay-value\n", monkeypatch)

    assert config.get_str("JWT_KEY") == PLACEHOLDER_SENTINEL
    assert "JWT_KEY" in config.placeholder_keys()


def test_a_yaml_placeholder_with_no_env_override_is_still_refused(monkeypatch):
    """The common case, unchanged: nothing in `.env` for this key, so the
    YAML placeholder is what a server would actually read, and the guard
    must still catch it."""
    monkeypatch.delenv("DATABASE_DECRYPTION_KEY", raising=False)
    config = _config(
        "DATABASE_DECRYPTION_KEY: " + PLACEHOLDER_SENTINEL + "\n", monkeypatch
    )

    assert config.get_str("DATABASE_DECRYPTION_KEY") == PLACEHOLDER_SENTINEL
    assert "DATABASE_DECRYPTION_KEY" in config.placeholder_keys()


def test_get_still_checks_the_key_as_given_before_its_upper_case(monkeypatch):
    """`get()`/`get_str()` check the environment for the key exactly as
    passed before falling back to its upper-cased name (config.py L292-L309
    pre-fix). `placeholder_keys()` now shares that lookup through
    `_env_value`; this pins the refactor did not reorder or drop a step."""
    monkeypatch.delenv("Mixed_Case_Key", raising=False)
    monkeypatch.setenv("Mixed_Case_Key", "exact-case-value")
    config = _config("MIXED_CASE_KEY: " + PLACEHOLDER_SENTINEL + "\n", monkeypatch)

    assert config.get_str("Mixed_Case_Key") == "exact-case-value"


def test_api_keys_report_the_value_get_str_returns(monkeypatch):
    """`api_keys` (the startup banner's list) must agree with `get_str()`: the
    environment's value when it has one, and a key set only there included."""
    monkeypatch.setenv("SYNTH_A_API_KEY", "from-env")
    monkeypatch.setenv("SYNTH_B_API_KEY", "env-only")
    cfg = _config("SYNTH_A_API_KEY: from-yaml\nSYNTH_C_API_KEY: yaml-only\n", monkeypatch)
    monkeypatch.delenv("SYNTH_C_API_KEY", raising=False)  # exported by setdefault; keep the process clean
    for key in ("SYNTH_A_API_KEY", "SYNTH_B_API_KEY", "SYNTH_C_API_KEY"):
        assert cfg.api_keys.get(key) == cfg.get_str(key), key
    assert cfg.api_keys["SYNTH_A_API_KEY"] == "from-env"

