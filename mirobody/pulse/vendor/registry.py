"""Runtime lookup of health-data vendors by id.

Port of the archived C++ ``src/health/vendor/registry.{hpp,cpp}``. The concrete
vendor classes live one-per-module under ``platform/``, ``device/``, ``phone/``
and ``ehr/``; callers reach them only through :func:`open_vendor`. This module
plus ``base`` is the whole public surface of the vendor package.

Registry order matches the archived market-comparison matrix, so listings render
in the same order as the source report.
"""

from __future__ import annotations

from typing import Optional, Type

from .base import Vendor, VendorConfig, VendorError, VendorInfo

from .platform.rook import RookVendor
from .platform.spike import SpikeVendor
from .platform.terra import TerraVendor
from .platform.junction import JunctionVendor
from .platform.wefitter import WefitterVendor
from .platform.lexisnexis import LexisnexisVendor
from .platform.thryve import ThryveVendor
from .platform.validic import ValidicVendor
from .platform.human_api import HumanApiVendor
from .platform.vitalera import VitaleraVendor
from .platform.open_wearables import OpenWearablesVendor
from .platform.redox import RedoxVendor
from .platform.particle_health import ParticleHealthVendor
from .platform.healthconnect import HealthconnectVendor
from .platform.metriport import MetriportVendor
from .phone.huawei import HuaweiVendor
from .device.fitbit import FitbitVendor
from .device.withings import WithingsVendor
from .device.garmin import GarminVendor
from .device.dexcom import DexcomVendor
from .device.oura import OuraVendor
from .device.whoop import WhoopVendor
from .device.polar import PolarVendor
from .ehr.ehr import EhrVendor

# Matrix order matches the archived comparison table.
_VENDORS: tuple[Type[Vendor], ...] = (
    RookVendor,
    SpikeVendor,
    TerraVendor,
    JunctionVendor,
    WefitterVendor,
    LexisnexisVendor,
    ThryveVendor,
    ValidicVendor,
    HumanApiVendor,
    VitaleraVendor,
    OpenWearablesVendor,
    RedoxVendor,
    ParticleHealthVendor,
    HealthconnectVendor,
    MetriportVendor,
    HuaweiVendor,
    FitbitVendor,
    WithingsVendor,
    GarminVendor,
    DexcomVendor,
    OuraVendor,
    WhoopVendor,
    PolarVendor,
    EhrVendor,
)

_BY_ID: dict[str, Type[Vendor]] = {cls.INFO.id: cls for cls in _VENDORS}

assert len(_BY_ID) == len(_VENDORS), "duplicate vendor id in registry"


def vendor_ids() -> list[str]:
    """The id of every registered vendor, in matrix order."""
    return [cls.INFO.id for cls in _VENDORS]


def all_vendor_info() -> list[VendorInfo]:
    """Static metadata for all registered vendors — no credentials, no network."""
    return [cls.INFO for cls in _VENDORS]


def open_vendor(vendor_id: str, config: Optional[VendorConfig] = None) -> Vendor:
    """Construct the vendor with the given id.

    Raises :class:`VendorError` if ``vendor_id`` is unknown. The returned client
    is a stub until its module implements the transport; metadata via ``info``
    works regardless. With ``config=None`` credentials are read from the
    environment (:meth:`VendorConfig.from_env`).
    """
    cls = _BY_ID.get(vendor_id)
    if cls is None:
        known = ", ".join(vendor_ids())
        raise VendorError(f"unknown vendor id '{vendor_id}' (known: {known})")
    return cls(config)
