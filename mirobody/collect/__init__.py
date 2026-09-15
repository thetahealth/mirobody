"""① Collect: every way a reading gets into Mirobody, and what happens next.

Two source shapes, one convergence point, then meaning:

    providers/   devices and health platforms: Garmin, Oura, WHOOP pulled on a
                 schedule, Apple Health and CDA documents pushed
    files/       a file is a source too: lab PDFs, photos, CSV, genetic raw data
         ↓
    ingest/      all three converge on StandardPulseData → observations.py
         ↓

What a value MEANS is ② Translate's, not this stage's: the indicator
catalogue, units, ranges and fhir_id are `mirobody.translate`.

`core/` is what those stand on, not a stage: the provider contract types, the
scheduler, the DB base classes, the distributed lock. Sub-package sizes and
entry points are in README.md, ordered the same way: the directory listing
cannot show this order, since `core/` sorts before `providers/`.

Providers are discovered by file scan, so deleting one takes it offline.

Exports resolve lazily (PEP 562). ``import mirobody.collect`` is the ENGINE,
it must not eagerly construct the platform singletons, and it carries no HTTP
routers at all: those were platform assembly, not engine logic, and now live
where they always belonged: ``mirobody/server/routers/``. Every existing
``from mirobody.collect import X`` keeps working unchanged; each export simply
pays its own import cost at first use.
"""

from typing import TYPE_CHECKING

# name -> submodule that defines it
_EXPORTS = {
    # Base classes and models
    "Platform": "base",
    "Provider": "base",
    "ProviderInfo": "base",
    "UserProvider": "base",
    "LinkRequest": "base",
    # Managers
    "PlatformManager": "manager",
    "platform_manager": "manager",
    # Setup functions
    "setup_platform_system": "setup",
    "setup_platform_system_async": "setup",
    "get_platform_manager": "setup",
    # Concrete implementations
    "ProviderPlatform": "providers",
    "BasePullProvider": "providers",
    # The provider-plugin contract. A third party writing a provider needs
    # exactly these names, and used to need four internal paths to find them
    # (`collect.base`, `collect.core`, `collect.ingest.models.requests`,
    # `collect.providers._platform.base`). They are one import now, so moving
    # any of those modules stops being a breaking change for plugins.
    "LinkType": "core",
    "ProviderStatus": "core",
    "push_service": "core.push_service",
    "FormatDataContext": "ingest.models.requests",
    "FormatDataInput": "ingest.models.requests",
    "StandardPulseData": "ingest.models.requests",
    "StandardPulseMetaInfo": "ingest.models.requests",
    "StandardPulseRecord": "ingest.models.requests",
    "DataFormatter": "providers._platform.normalize",
    "TimeUtils": "providers._platform.normalize",
    "records_from_facts": "providers._platform.normalize",
    "StandardIndicator": "mirobody.translate.indicators_info",
    "UNIT_CONVERSIONS": "mirobody.translate.canonical_units",
    # Apple Health implementations
    "AppleHealthPlatform": "providers.apple",
    "AppleHealthProvider": "providers.apple",
    "CDAProvider": "providers.apple",
    "AppleHealthRequest": "providers.apple.models",
    "AppleHealthStatisticsRequest": "providers.apple.models",
    "process_apple_health_statistics": "providers.apple.statistics_service",
    # What `mirobody.server` needs. Named here so the modules behind them can
    # move without a router edit, which is the whole point: `db_utils`,
    # `database_services` and `providers/platform/` all moved in 1.4.4.
    "start_theta_pull_scheduler": "providers._platform.startup",
    "ConnectInfoField": "core.models",
    "installed_provider_slugs": "providers.installed",
    "PostgresHealthQuery": "query",
    "REST_CATALOG_MAX": "query",
    # Files, shared by server and agent.
    "get_websocket_file_upload_manager": "files.file_upload_manager",
    "FileDbService": "files.services.file_db_service",
    "FileUploadData": "files.services.file_processing_service",
    "delete_all_files_from_message": "files.services.file_processing_service",
    "delete_files_from_message": "files.services.file_processing_service",
    "upload_files_to_storage": "files.services.file_processing_service",
    "process_files_async": "files.services.file_processing_service",
    "get_uploaded_files_paginated": "files.services.drive_listing",
    "regenerate_file_url": "files.services.drive_listing",
    "get_user_data_distribution": "files.services.list_my_data",
    "set_file_report_date": "files.services.report_date",
    "FileAbstractExtractor": "files.services.file_abstract_extractor",
    "lookup_extracted_text": "files.services.file_abstract_extractor",
    "GeneticHandler": "files.handlers.genetic",
    # What `mirobody.agent` needs beyond the above.
    "PostgresDoseLogStore": "meds",
    "PostgresMedicationStore": "meds",
    # Note: Specific providers (GarminProvider, etc.) are auto-loaded
    # and can be imported from .providers if needed
}
__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .providers.apple import AppleHealthPlatform, AppleHealthProvider, CDAProvider
    from .base import LinkRequest, Platform, Provider, ProviderInfo, UserProvider
    from .manager import PlatformManager, platform_manager
    from .setup import get_platform_manager, setup_platform_system, setup_platform_system_async
    from .core import LinkType, ProviderStatus
    from .core.push_service import push_service
    from .ingest.models.requests import (
        FormatDataContext,
        FormatDataInput,
        StandardPulseData,
        StandardPulseMetaInfo,
        StandardPulseRecord,
    )
    from .providers._platform.normalize import DataFormatter, TimeUtils, records_from_facts
    from mirobody.translate import StandardIndicator
    from mirobody.translate import UNIT_CONVERSIONS
    from .providers import BasePullProvider, ProviderPlatform
    from .providers.apple.models import AppleHealthRequest, AppleHealthStatisticsRequest
    from .providers.apple.statistics_service import process_apple_health_statistics
    
    from .providers._platform.startup import start_theta_pull_scheduler
    from .core.models import ConnectInfoField
    from .providers.installed import installed_provider_slugs
    from .query import PostgresHealthQuery, REST_CATALOG_MAX
    from .meds import PostgresDoseLogStore, PostgresMedicationStore
    from .files.file_upload_manager import get_websocket_file_upload_manager
    from .files.handlers.genetic import GeneticHandler
    from .files.services.drive_listing import get_uploaded_files_paginated, regenerate_file_url
    from .files.services.file_abstract_extractor import FileAbstractExtractor, lookup_extracted_text
    from .files.services.file_db_service import FileDbService
    from .files.services.file_processing_service import (
        FileUploadData,
        delete_all_files_from_message,
        delete_files_from_message,
        process_files_async,
        upload_files_to_storage,
    )
    from .files.services.list_my_data import get_user_data_distribution
    from .files.services.report_date import set_file_report_date


def __getattr__(name: str):
    import importlib

    if name in _EXPORTS:
        where = _EXPORTS[name]
        # An absolute path means the symbol left this package. Two did, when
        # `standardize/` became `mirobody.translate`: they keep resolving here
        # because a provider plugin names its metrics with `StandardIndicator`
        # and declares units with `UNIT_CONVERSIONS`, and the plugin contract
        # is one import path, not two. The catalogue reads the server does went
        # straight to `mirobody.translate` instead.
        module = importlib.import_module(where if "." in where and where.startswith("mirobody.")
                                         else f".{where}", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
