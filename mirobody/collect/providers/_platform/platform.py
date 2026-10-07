"""
Provider platform implementation
"""

import importlib
import importlib.util
import logging
import sys
from pathlib import Path
from typing import Any

from mirobody.collect.base import LinkRequest, Platform, ProviderInfo, UserProvider
from mirobody.collect.core import ProviderStatus
from mirobody.utils.scheduler import scheduler
from mirobody.collect.ingest import FormatDataInput
from mirobody.collect.ingest import StandardHealthService
from mirobody.collect.providers._platform.database_service import ProviderDatabaseService
from mirobody.utils.config import Config
from .base import BasePullProvider
from .pull_task import ProviderPullTask

logger = logging.getLogger(__name__)


class ProviderPlatform(Platform):

    def __init__(self, config: Config):
        """Initialize Provider platform"""
        super().__init__()
        self.config = config
        self.db_service = ProviderDatabaseService()

    @property
    def name(self) -> str:
        """Platform name: deliberately still `theta`, not `providers`.

        The package, this class and every provider class were renamed off
        their original name because that name explained nothing. This string
        was NOT, and must not be: it is persisted and client-visible data, not
        a label. It keys `platform_manager.get_platform("theta")`, it is stored in
        `health_user_provider`, it pairs with the `theta_*` provider slugs and
        the `theta_user_id` columns, and clients send it as the `platform` field
        (`Platform name (vital, theta, cgm)`) and on `GET /theta/indicators`.

        Changing it is a data migration plus a breaking API change, not a
        rename. Until someone does that deliberately, the mismatch between the
        code names and the wire names is the honest state of things.
        """
        return "theta"

    @property
    def supports_registration(self) -> bool:
        """Whether provider registration is supported (this platform does)"""
        return True

    def get_provider(self, provider_slug: str) -> BasePullProvider | None:
        """Narrowed return type: ProviderPlatform only registers BasePullProvider."""
        return self._providers.get(provider_slug)  # type: ignore[return-value]

    def register_provider(self, provider: BasePullProvider) -> None:
        super().register_provider(provider)

        if provider.register_pull_task():
            scheduler.register_task(ProviderPullTask(provider))

    def _load_providers_from_directory(self, directory: Path) -> list[BasePullProvider]:
        providers = []

        if not directory.exists():
            logger.debug(f"Provider directory does not exist: {directory}")
            return providers

        provider_files = sorted(directory.glob("mirobody_*/provider_*.py"))

        if not provider_files:
            logger.debug(f"No provider files found in {directory}")
            return providers

        # Providers shipped INSIDE this package must be imported by their real
        # dotted path. The sys.path branch below makes `mirobody_oura` a
        # top-level package, which has no parent, so `provider_oura.py`'s
        # `from ....utils.tasks import spawn` died with "attempted relative
        # import beyond top-level package" and the loader swallowed it as a
        # warning: all three packaged providers carry that import, so the
        # platform logged "loaded 0 providers" on every boot. sys.path is still
        # right for EXTERNAL `PROVIDER_DIRS`, which are inside no package.
        packaged_dir = Path(__file__).resolve().parent.parent
        packaged_pkg = __package__.rsplit(".", 1)[0]  # mirobody.collect.providers
        is_packaged = directory.resolve() == packaged_dir

        for provider_file in provider_files:
            provider_name = provider_file.stem  # e.g., "provider_garmin"
            provider_dir = provider_file.parent.name  # e.g., "mirobody_garmin"

            try:
                if is_packaged:
                    module = importlib.import_module(
                        f"{packaged_pkg}.{provider_dir}.{provider_name}"
                    )
                    provider = self._provider_from_module(module, provider_file)
                    if provider:
                        providers.append(provider)
                    continue

                module_name = f"{provider_dir}.{provider_name}"

                parent_dir = str(directory)
                add_to_path = parent_dir not in sys.path
                if add_to_path:
                    sys.path.insert(0, parent_dir)

                try:
                    module = importlib.import_module(module_name)
                except ModuleNotFoundError:
                    spec = importlib.util.spec_from_file_location(module_name, provider_file)
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        sys.modules[module_name] = module
                        spec.loader.exec_module(module)
                    else:
                        raise ImportError(f"Cannot load module from {provider_file}") from None
                finally:
                    if add_to_path and parent_dir in sys.path:
                        sys.path.remove(parent_dir)

                provider = self._provider_from_module(module, provider_file)
                if provider:
                    providers.append(provider)

            except Exception as e:
                logger.warning(f"Failed to load provider {provider_name} from {directory}: {e}")
                continue

        return providers

    def _provider_from_module(self, module, provider_file: Path) -> BasePullProvider | None:
        """The provider instance a loaded module offers, or None.

        Discovery is by SUBCLASS, not by name. This used to require
        `attr_name.startswith(...)` against a fixed class-name prefix, which
        silently skipped any provider not sharing that old naming scheme: a
        trap for the next contributor, and the reason renaming the classes had
        to touch this line.
        `endswith("Provider")` stays as a cheap pre-filter; the issubclass test
        is the authority, and the `!=` guard keeps the imported base class from
        matching itself.

        Returning None is normal, not a failure: `create_provider` is where a
        provider declines because its credentials are absent (Oura without
        `OURA_CLIENT_ID`).
        """
        provider_class = None
        for attr_name in dir(module):
            if attr_name.endswith("Provider"):
                attr = getattr(module, attr_name)
                if isinstance(attr, type) and issubclass(attr, BasePullProvider) and attr != BasePullProvider:
                    provider_class = attr
                    break

        if provider_class is None:
            logger.debug(f"No provider class found in {provider_file.stem}, skipping")
            return None

        if not hasattr(provider_class, "create_provider"):
            logger.warning(f"Provider class {provider_class.__name__} missing create_provider method, skipping")
            return None

        provider_instance = provider_class.create_provider(self.config)
        if provider_instance is None:
            logger.info(f"Provider {provider_class.__name__} declined to start (not configured)")
            return None

        logger.info(f"Loaded provider from {provider_file}")
        return provider_instance

    def load_providers(self) -> list[BasePullProvider]:
        providers = []

        provider_dirs = self.config.get("PROVIDER_DIRS", [])

        # Always include the packaged provider directory
        default_provider_dir = Path(__file__).parent.parent.resolve()

        # Collect all directories and deduplicate (compare after converting to absolute paths with resolve())
        seen_dirs = set()
        all_dirs = []

        # Add default directory first
        if default_provider_dir not in seen_dirs:
            all_dirs.append(default_provider_dir)
            seen_dirs.add(default_provider_dir)

        # Add configured directories
        import os
        for dir_str in provider_dirs:
            if not dir_str:
                continue
            dir_path = Path(dir_str)
            if not dir_path.is_absolute():
                dir_path = (Path(os.getcwd()) / dir_path).resolve()
            else:
                dir_path = dir_path.resolve()

            # Deduplicate: only add unseen directories
            if dir_path not in seen_dirs:
                all_dirs.append(dir_path)
                seen_dirs.add(dir_path)

        for directory in all_dirs:
            logger.info(f"Scanning for providers in: {directory}")
            dir_providers = self._load_providers_from_directory(directory)
            providers.extend(dir_providers)

        # Installed plugins: a distribution that declares a `mirobody.providers`
        # entry point pointing at a module with a BasePullProvider subclass.
        from mirobody.utils.plugin_dirs import GROUP_PROVIDERS, entry_point_modules
        for module in entry_point_modules(GROUP_PROVIDERS):
            try:
                provider = self._provider_from_module(module, Path(getattr(module, "__file__", None) or module.__name__))
            except Exception as e:
                logger.warning("failed to load a provider plugin: module=%s error_type=%s", module.__name__, type(e).__name__)
                continue
            if provider:
                providers.append(provider)

        return providers

    async def get_providers(self, nocache: bool = False) -> list[ProviderInfo]:
        providers = []

        for provider in self._providers.values():
            providers.append(provider.info)

        logger.info(f"Got {len(providers)} providers from theta platform")
        return providers

    async def get_user_providers(self, user_id: str) -> list[UserProvider]:
        connections = []

        try:
            # Get provider info (llm_access and reconnect) from database service
            provider_info_map = await self.db_service.get_user_theta_providers_with_llm_access(user_id)

            for provider_slug, info in provider_info_map.items():
                llm_access = info["llm_access"]
                reconnect = info["reconnect"]

                # Determine status based on reconnect flag
                status = ProviderStatus.RECONNECT if reconnect == 1 else ProviderStatus.CONNECTED

                connections.append(
                    UserProvider(
                        slug=provider_slug,
                        status=status,
                        platform="theta",
                        connected_at=None,
                        last_sync_at=None,  # Will be filled by _populate_provider_stats
                        record_count=0,  # Will be filled by _populate_provider_stats
                        llm_access=llm_access,
                    )
                )

            logger.info(f"Got {len(connections)} connections for user {user_id} from theta platform")

        except Exception as e:
            logger.error(f"Error getting user providers for user {user_id}: {str(e)}")

        return connections

    async def link(self, request: LinkRequest) -> dict[str, Any]:
        provider_slug = request.provider_slug

        provider = self.get_provider(provider_slug)
        if not provider:
            return {"provider_slug": provider_slug,
                    "username": request.credentials.get("username", ""),
                    "msg": f"Provider {provider_slug} not found in theta platform"
                    }
        return await provider.link(request)

    async def unlink(self, user_id: str, provider_slug: str) -> dict[str, Any]:
        provider = self.get_provider(provider_slug)
        if not provider:
            raise ValueError(f"Provider {provider_slug} not found in theta platform")

        try:
            result_data = await provider.unlink(user_id)
            logger.info(f"Unlink successful for theta provider {provider_slug}")
            return result_data
        except Exception as e:
            logger.error(f"Error unlinking theta provider {provider_slug}: {str(e)}")
            raise RuntimeError(f"Failed to unlink provider: {str(e)}") from e

    async def post_data(self, provider_slug: str, data: dict[str, Any], msg_id: str) -> bool:
        provider = self.get_provider(provider_slug)
        if not provider:
            logger.error(f"Provider {provider_slug} not found in theta platform")
            return False

        try:
            data["msg_id"] = msg_id
            saved_data_list = await provider.save_raw_data_to_db(data)
            if not saved_data_list:
                logger.warning(f"Raw data save failed for provider {provider_slug}, msg_id={msg_id}")

            standard_health_service = StandardHealthService()
            total_records = 0
            success_count = 0
            error_count = 0

            for saved_data in saved_data_list:
                try:
                    ctx = await provider.build_format_context(saved_data)
                    fmt_input = FormatDataInput(context=ctx, payload=saved_data)
                    standard_pulse_data = await provider.format_data(fmt_input)
                    if not standard_pulse_data or not standard_pulse_data.healthData:
                        logger.info(f"No data formatted by theta provider {provider_slug}")
                        continue

                    user_id = standard_pulse_data.metaInfo.userId
                    if not user_id:
                        logger.error(f"No user ID found in formatted data from provider {provider_slug}")
                        error_count += 1
                        continue

                    success = await standard_health_service.process_standard_data(standard_pulse_data, user_id)
                    records_count = len(standard_pulse_data.healthData)

                    if success:
                        success_count += 1
                        total_records += records_count
                        logger.info(f"Processed {records_count} records for user {user_id}")
                    else:
                        error_count += 1
                        logger.error(f"Failed to process {records_count} records for user {user_id}")

                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing saved_data item: {str(e)}")
                    continue

            logger.info(
                f"provider platform completed: {total_records} total records, {success_count} success, {error_count} errors")
            return error_count == 0

        except Exception as e:
            logger.error(f"Error posting data to theta provider {provider_slug}: {str(e)}")
            return False

    async def start_pull_scheduler(self) -> None:
        try:
            await scheduler.start()
        except Exception as e:
            logger.error(f"Failed to start theta pull scheduler: {str(e)}")

    # ===== LLM Access Management =====

    async def update_llm_access(self, user_id: str, provider_slug: str, llm_access: int) -> dict[str, Any]:
        """
        Update LLM access permission for a theta provider

        Args:
            user_id: User ID
            provider_slug: Provider identifier
            llm_access: Access level (0: no access, 1: limited access, 2: full access)

        Returns:
            Update result data
        """
        success = await self.db_service.update_llm_access(user_id, provider_slug, llm_access)

        if not success:
            raise RuntimeError(f"Failed to update LLM access for provider {provider_slug}")

        return {
            "provider_slug": provider_slug,
            "platform": "theta",
            "llm_access": llm_access,
            "updated": True,
        }
