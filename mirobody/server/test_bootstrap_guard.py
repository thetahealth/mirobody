"""`PRODUCTION: true` must strip the demo affordances; nothing else may.

The switch is explicit rather than inferred from the environment NAME because
a list of recognised names is an allowlist doing a safety gate's job:
`ENV=production`, `ENV=live`, any name an operator picks that the list did not
anticipate, silently keeps the demo posture — anonymous login codes accepted,
placeholder secrets tolerated, demo data seeded.

What `PRODUCTION: true` buys, each pinned here:
* refuse to start while `EMAIL_PREDEFINE_CODES` is non-empty (a predefined
  code is a login bypass — the short-circuit works in the production
  Mandrill/SMTP verifiers too);
* refuse to start while any config value still reads the shipped
  `REPLACE_THIS_VALUE_IN_PRODUCTION` placeholder;
* never seed synthetic patients.

And the two non-behaviors that keep the demo alive: the switch off means all
demo affordances work, and environment names mean nothing.
"""

import pytest

from mirobody.server.bootstrap import (
    enforce_production_auth_safety,
    is_production,
    seed_demo_data,
    should_bootstrap,
)


class _FakeConfig:
    def __init__(self, *, production=False, codes=None, placeholders=(), bootstrap=None):
        self._production = production
        self._codes = codes or {}
        self._placeholders = list(placeholders)
        self._bootstrap = bootstrap

    def get_bool(self, key, default=False):
        if key == "PRODUCTION":
            return self._production
        if key == "BOOTSTRAP_SCHEMA":
            return self._bootstrap if self._bootstrap is not None else default
        return default

    def get_dict(self, key, default=None):
        assert key == "EMAIL_PREDEFINE_CODES"
        return self._codes

    def placeholder_keys(self):
        return self._placeholders


def test_production_with_codes_refuses_to_start():
    with pytest.raises(RuntimeError, match="EMAIL_PREDEFINE_CODES"):
        enforce_production_auth_safety(
            _FakeConfig(production=True, codes={"demo1@mirobody.ai": "777777"}))


def test_production_with_placeholder_secrets_refuses_to_start():
    with pytest.raises(RuntimeError, match="REPLACE_THIS_VALUE_IN_PRODUCTION"):
        enforce_production_auth_safety(
            _FakeConfig(production=True, placeholders=["PG_PASSWORD", "JWT_KEY"]))


def test_production_with_clean_config_starts():
    enforce_production_auth_safety(_FakeConfig(production=True))  # must not raise


def test_demo_posture_keeps_the_demo_codes():
    # The local one-command demo is the product's front door; the guard must
    # never break it.
    enforce_production_auth_safety(
        _FakeConfig(production=False, codes={"caregiver@mirobody.ai": "111111"},
                    placeholders=["PG_PASSWORD"]))


@pytest.mark.parametrize("env", ["PROD", "production", "live", "staging", "prod-eu"])
def test_environment_names_carry_no_behavior(env, monkeypatch, caplog):
    """No environment name gates anything by itself — only the switch does."""
    monkeypatch.setenv("ENV", env)
    cfg = _FakeConfig(production=False, codes={"caregiver@mirobody.ai": "111111"})
    enforce_production_auth_safety(cfg)          # no refusal without the switch
    assert should_bootstrap(cfg) is True         # and no bootstrap skip either
    if "prod" in env.lower():
        # ...but the likely foot-gun gets a warning pointing at the switch.
        assert any("PRODUCTION is not" in r.message for r in caplog.records)


def test_bootstrap_schema_is_the_only_bootstrap_gate(monkeypatch):
    monkeypatch.setenv("ENV", "PROD")
    assert should_bootstrap(_FakeConfig(bootstrap=True)) is True
    assert should_bootstrap(_FakeConfig(bootstrap=False)) is False


@pytest.mark.asyncio
async def test_seed_demo_data_skips_under_production(monkeypatch):
    monkeypatch.setenv("SEED_DEMO_DATA", "true")

    async def _must_not_run(members):
        raise AssertionError("demo seed ran under PRODUCTION")

    import mirobody.server.demo as demo
    monkeypatch.setattr(demo, "seed", _must_not_run)

    await seed_demo_data(
        _FakeConfig(production=True, codes={"caregiver@mirobody.ai": "111111"}))


def test_is_production_reads_the_switch_not_the_env(monkeypatch):
    monkeypatch.setenv("ENV", "PROD")
    assert is_production(_FakeConfig(production=False)) is False
    monkeypatch.setenv("ENV", "anything-at-all")
    assert is_production(_FakeConfig(production=True)) is True
