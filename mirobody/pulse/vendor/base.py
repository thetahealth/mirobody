"""Health-data vendor abstraction — the transport layer under the pulse pipeline.

Faithful Python port of the archived C++ contract (``src/health/vendor/vendor.hpp``
in the private ``mirobody-on-device`` repo). A *vendor* is a platform that brokers
wearable, lab-diagnostic, and/or clinical-EHR data on a consumer's behalf — a
B2B aggregator (Terra, Validic, …), a device brand's own API (Fitbit, Withings, …),
a phone vendor's cloud (Huawei), or a SMART-on-FHIR EHR facade.

This layer deliberately does ONE thing: talk to the vendor's wire API and hand
back the vendor's own JSON. It does not normalize, persist, or schedule — that is
the job of the existing pulse provider pipeline (``pulse/theta``), which consumes
this layer through an adapter. The three sources that already have deep Python
providers (garmin / oura / whoop) keep them; this package fills in the sources
Python never had.

Design rules carried over from the C++ side, verbatim in spirit:

* **Honest stubs.** An operation whose wire contract could not be confirmed from
  public docs raises ``VendorError("<id>: <op> not implemented (stub)")`` and the
  module docstring records WHY (gated/undocumented, contract mismatch, or no such
  endpoint). Fabricating a contract is worse than an honest "not implemented".
* **Metadata needs no credentials.** ``Vendor.info`` is a static literal —
  ``registry.all_vendor_info()`` can describe the whole field with zero config.
* **Async-first.** The pulse pipeline is fully async; every network operation
  here is an ``async def``.

One deliberate Python-side extension: ``VendorInfo.status`` grades each vendor as
``metadata`` / ``implemented`` / ``verified`` so docs and the registry can tell
the truth about how far each integration has been exercised (see VENDORS.md).
"""

from __future__ import annotations

import os
from abc import ABC
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class VendorError(RuntimeError):
    """Any non-success vendor outcome: an unimplemented stub operation, a
    transport failure, a non-2xx response, or misconfiguration."""


# ── metadata enums (values match the C++ to_string() spellings) ──────────────

class Region(str, Enum):
    """Primary regulatory / hosting orientation of the platform."""
    GLOBAL = "global"            # markets globally; no single jurisdiction emphasized
    US = "us"                    # U.S.-first (HIPAA framework, QHIN access)
    EU = "eu"                    # Europe-first (GDPR, local hosting)
    SELF_HOSTED = "self-hosted"  # open-source self-hosted; compliance is the deployer's job


class DataDomain(str, Enum):
    """Categories of health data a vendor brokers."""
    ACTIVITY = "activity"
    SLEEP = "sleep"
    HEART_RATE = "heart_rate"
    GLUCOSE = "glucose"
    NUTRITION = "nutrition"
    BODY_METRICS = "body_metrics"  # weight, BMI, blood pressure, SpO2, …
    LABS = "labs"                  # diagnostic lab panels
    CLINICAL = "clinical"          # clinical EHR records (encounters, conditions, meds)


class Integration(str, Enum):
    """How a vendor exposes its API. A platform may support several at once."""
    REST = "rest"
    SDK = "sdk"
    WEBHOOK = "webhook"
    WEBSOCKET = "websocket"
    FHIR = "fhir"
    HL7 = "hl7"
    WHITE_LABEL = "white_label"


class VendorStatus(str, Enum):
    """How far this integration has been exercised (Python-side extension).

    * ``metadata``    — profile only; every operation is an honest stub.
    * ``implemented`` — coded against the public wire docs, not yet run against
      a live account (most aggregators gate keys behind a sales contract).
    * ``verified``    — exercised end-to-end with real credentials.
    """
    METADATA = "metadata"
    IMPLEMENTED = "implemented"
    VERIFIED = "verified"


# ── data records ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class VendorInfo:
    """Static facts about a vendor, lifted from the archived market-comparison
    matrix. Available without credentials or a network call, so the registry can
    describe the whole field for selection / comparison."""

    id: str                        # stable lowercase key, e.g. "terra"
    display_name: str              # "Terra API"
    positioning: str               # core positioning
    target_customers: str = ""
    data_source_coverage: str = ""
    integration_method: str = ""   # human-readable integration summary
    compliance_summary: str = ""   # human-readable compliance summary
    differentiator: str = ""       # key differentiator
    docs_url: str = ""             # official developer docs
    region: Region = Region.GLOBAL
    open_source: bool = False
    # Structured, queryable view of the same posture. `compliance` holds short
    # credential tags ("HIPAA", "GDPR", "SOC2_TYPE_II", …); empty means none
    # publicly disclosed in the source report.
    compliance: tuple[str, ...] = ()
    domains: tuple[DataDomain, ...] = ()
    integrations: tuple[Integration, ...] = ()
    category: str = "platform"     # platform | device | phone | ehr
    status: VendorStatus = VendorStatus.METADATA


@dataclass
class VendorConfig:
    """Per-vendor credentials and connection settings.

    Most platforms use a bearer/API key or an OAuth client pair; ``base_url``
    overrides the default API host for sandbox / region endpoints.
    """

    api_key: str = ""        # bearer / API key (most REST vendors)
    client_id: str = ""      # OAuth client id (consumer-mediated consent)
    client_secret: str = ""  # OAuth client secret
    base_url: str = ""       # API host override; empty => vendor default

    def configured(self) -> bool:
        return bool(self.api_key) or bool(self.client_id and self.client_secret)

    @classmethod
    def from_env(cls, vendor_id: str) -> "VendorConfig":
        """Build a config from environment variables, by convention:
        ``MIROBODY_VENDOR_<ID>_API_KEY / _CLIENT_ID / _CLIENT_SECRET / _BASE_URL``
        where ``<ID>`` is the vendor id upper-cased (e.g. ``HUMAN_API``).
        Missing vars are left empty."""
        prefix = f"MIROBODY_VENDOR_{vendor_id.upper()}"
        return cls(
            api_key=os.environ.get(f"{prefix}_API_KEY", ""),
            client_id=os.environ.get(f"{prefix}_CLIENT_ID", ""),
            client_secret=os.environ.get(f"{prefix}_CLIENT_SECRET", ""),
            base_url=os.environ.get(f"{prefix}_BASE_URL", ""),
        )


@dataclass
class TokenSet:
    """Result of an OAuth2 token endpoint call (exchange_code / refresh).

    ``expires_in`` is the access-token lifetime in seconds (0 = unknown /
    non-expiring, e.g. Polar). ``refresh_token`` may be empty when the vendor
    does not issue or rotate one."""

    access_token: str = ""
    refresh_token: str = ""
    expires_in: int = 0


# ── the vendor contract ───────────────────────────────────────────────────────

class Vendor(ABC):
    """A health-data vendor client.

    ``user_id`` is the platform's identifier for the end user whose data is being
    brokered (the value returned by the consent flow). Time bounds are ISO-8601
    strings. Data-returning operations hand back the vendor's JSON as a string
    for now; normalization happens downstream in the pulse pipeline.

    Every concrete vendor subclasses this base, which stores the profile + config
    and defaults every network operation to an honest ``VendorError`` stub. A
    vendor overrides only the operations whose wire contract it has confirmed.
    """

    INFO: VendorInfo  # each concrete module defines its literal

    def __init__(self, config: Optional[VendorConfig] = None):
        self._config = config or VendorConfig.from_env(self.INFO.id)

    # -- metadata ------------------------------------------------------------

    @property
    def info(self) -> VendorInfo:
        """Static metadata for this vendor (never raises, no network)."""
        return self.INFO

    @property
    def config(self) -> VendorConfig:
        return self._config

    # -- consent / connection -------------------------------------------------

    async def authorize_url(self, redirect_uri: str, state: str = "",
                            user_id: str = "", provider: str = "") -> str:
        """Begin consumer-mediated consent and return the entry point the caller
        hands the user — usually a redirect URL, but for vendors whose consent is
        per-provider it may be the vendor's connect-URLs JSON or a hosted-widget
        payload. A vendor ignores the arguments it has no use for."""
        self._not_implemented("authorize_url")

    async def list_providers(self, user_id: str = "") -> str:
        """The data sources (device brands / labs / health systems) a connected
        user can link, as the vendor's JSON. ``user_id`` is for vendors that only
        expose a user-scoped connections list; empty for vendors with a global
        provider catalogue."""
        self._not_implemented("list_providers")

    # -- data ------------------------------------------------------------------

    async def fetch(self, user_id: str, domain: DataDomain,
                    start_iso: str = "", end_iso: str = "") -> str:
        """Fetch ``domain`` data for ``user_id`` over [start_iso, end_iso], as the
        vendor's JSON. Empty bounds mean the vendor's default window."""
        self._not_implemented("fetch")

    async def handle_webhook(self, raw_headers: str, body: str) -> str:
        """Validate and parse an inbound webhook delivery; returns the parsed
        event as JSON. ``raw_headers`` carries the signature header(s) the vendor
        signs with."""
        self._not_implemented("handle_webhook")

    # -- lifecycle --------------------------------------------------------------

    async def revoke(self, user_id: str, provider: str = "") -> None:
        """Revoke a user's authorization / disconnect them. ``provider``
        disconnects a single data source for vendors that revoke per-source;
        empty revokes the whole user."""
        self._not_implemented("revoke")

    async def exchange_code(self, code: str, redirect_uri: str) -> TokenSet:
        """Exchange an OAuth2 authorization ``code`` (from authorize_url's
        redirect) for a TokenSet at the vendor's token endpoint."""
        self._not_implemented("exchange_code")

    async def refresh(self, refresh_token: str) -> TokenSet:
        """Exchange a ``refresh_token`` for a fresh TokenSet. Raises for vendors
        whose tokens do not expire / have no refresh grant (e.g. Polar)."""
        self._not_implemented("refresh")

    # -- internals ---------------------------------------------------------------

    def _not_implemented(self, op: str) -> None:
        """Uniform honest stub: name the vendor, the operation, and where the
        per-operation rationale is recorded."""
        raise VendorError(
            f"{self.INFO.id}: {op} not implemented (stub) — see the module "
            f"docstring of mirobody.pulse.vendor and the vendor's own module "
            f"for why this operation is stubbed"
        )
