"""Health-data vendor transport layer — 24 sources behind one contract.

Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/``). See ``base`` for the contract and the porting rules
(honest stubs, credential-free metadata, async-first), ``registry`` for lookup.

Quick taste (no credentials needed)::

    from mirobody.pulse.vendor import all_vendor_info
    for v in all_vendor_info():
        print(f"{v.id:16s} {v.status.value:12s} {v.display_name}")

Layering: this package only talks to vendor wire APIs and returns their JSON.
Normalization into standard indicators is the pulse pipeline's job; the three
sources with deep existing Python providers (garmin / oura / whoop under
``pulse/theta``) keep those — their entries here are profile metadata until the
transport consolidation lands.
"""

from .base import (
    DataDomain,
    Integration,
    Region,
    TokenSet,
    Vendor,
    VendorConfig,
    VendorError,
    VendorInfo,
    VendorStatus,
)
from .oauth2 import oauth2_token_request
from .registry import all_vendor_info, open_vendor, vendor_ids

__all__ = [
    "DataDomain",
    "Integration",
    "Region",
    "TokenSet",
    "Vendor",
    "VendorConfig",
    "VendorError",
    "VendorInfo",
    "VendorStatus",
    "all_vendor_info",
    "oauth2_token_request",
    "open_vendor",
    "vendor_ids",
]
