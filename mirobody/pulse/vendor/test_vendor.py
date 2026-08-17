"""Contract tests for the vendor transport layer.

Everything here runs with zero credentials and zero network — that is itself the
contract under test: metadata must work bare, and unimplemented operations must
fail loudly and legibly, never silently.
"""

import asyncio
import inspect

import pytest

from mirobody.pulse.vendor import (
    DataDomain,
    TokenSet,
    Vendor,
    VendorConfig,
    VendorError,
    VendorInfo,
    VendorStatus,
    all_vendor_info,
    open_vendor,
    vendor_ids,
)

EXPECTED_IDS = [
    # Matrix order from the archived comparison table.
    "rook", "spike", "terra", "junction", "wefitter", "lexisnexis", "thryve",
    "validic", "human_api", "vitalera", "open_wearables", "redox",
    "particle_health", "healthconnect", "metriport", "huawei", "fitbit",
    "withings", "garmin", "dexcom", "oura", "whoop", "polar", "ehr",
]


def test_registry_matches_matrix_order():
    assert vendor_ids() == EXPECTED_IDS


def test_metadata_needs_no_credentials():
    infos = all_vendor_info()
    assert len(infos) == 24
    for info in infos:
        assert isinstance(info, VendorInfo)
        # The non-negotiable minimum for a profile to be useful in listings.
        assert info.id and info.display_name and info.positioning, info.id
        assert info.category in ("platform", "device", "phone", "ehr"), info.id
        # First batch ships as honest metadata across the board.
        assert info.status is VendorStatus.METADATA, info.id


def test_every_vendor_declares_domains_and_integrations():
    for info in all_vendor_info():
        assert info.domains, f"{info.id} declares no data domains"
        assert info.integrations, f"{info.id} declares no integrations"


def test_open_vendor_unknown_id_lists_known():
    with pytest.raises(VendorError) as exc:
        open_vendor("nope")
    assert "unknown vendor id" in str(exc.value)
    assert "terra" in str(exc.value)  # the error names the known ids


def test_open_vendor_reads_env(monkeypatch):
    monkeypatch.setenv("MIROBODY_VENDOR_TERRA_API_KEY", "k-test")
    monkeypatch.setenv("MIROBODY_VENDOR_TERRA_CLIENT_ID", "dev-id")
    v = open_vendor("terra")
    assert v.config.api_key == "k-test"
    assert v.config.client_id == "dev-id"
    assert v.config.configured()


def test_explicit_config_wins_over_env(monkeypatch):
    monkeypatch.setenv("MIROBODY_VENDOR_TERRA_API_KEY", "env-key")
    v = open_vendor("terra", VendorConfig(api_key="explicit"))
    assert v.config.api_key == "explicit"


@pytest.mark.parametrize("vendor_id", EXPECTED_IDS)
def test_stubs_raise_vendor_error_naming_vendor_and_op(vendor_id):
    """Every unimplemented operation must raise VendorError that names the
    vendor and the operation — the honest-stub rule from the C++ side."""
    v = open_vendor(vendor_id, VendorConfig())

    async def run():
        ops = [
            v.authorize_url("https://cb"),
            v.list_providers(),
            v.fetch("u1", DataDomain.ACTIVITY),
            v.handle_webhook("", "{}"),
            v.revoke("u1"),
            v.exchange_code("c", "https://cb"),
            v.refresh("r"),
        ]
        names = ["authorize_url", "list_providers", "fetch", "handle_webhook",
                 "revoke", "exchange_code", "refresh"]
        for coro, op in zip(ops, names):
            with pytest.raises(VendorError) as exc:
                await coro
            msg = str(exc.value)
            assert vendor_id in msg, f"{op}: error does not name the vendor"
            assert op in msg, f"{op}: error does not name the operation"

    asyncio.run(run())


def test_contract_is_fully_async():
    """The pulse pipeline is async; a sync override would block the loop."""
    for name in ("authorize_url", "list_providers", "fetch", "handle_webhook",
                 "revoke", "exchange_code", "refresh"):
        assert inspect.iscoroutinefunction(getattr(Vendor, name)), name


def test_tokenset_defaults():
    t = TokenSet()
    assert t.access_token == "" and t.refresh_token == "" and t.expires_in == 0


def test_config_from_env_convention(monkeypatch):
    monkeypatch.setenv("MIROBODY_VENDOR_HUMAN_API_BASE_URL", "https://sandbox")
    cfg = VendorConfig.from_env("human_api")
    assert cfg.base_url == "https://sandbox"
    assert not cfg.configured()  # base_url alone is not credentials
