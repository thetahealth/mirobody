"""
Theta platform implementation
"""

import importlib
import importlib.util
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

from mirobody.pulse.base import LinkRequest, Platform, ProviderInfo, UserProvider
from mirobody.pulse.core import ProviderStatus
from mirobody.pulse.core.scheduler import scheduler
from mirobody.pulse.data_upload.services.upload_health import StandardHealthService
from mirobody.pulse.theta.platform.database_service import ThetaDatabaseService
from mirobody.utils.config import Config
from .base import BaseThetaProvider
from .pull_task import create_pull_task_for_provider


class ThetaPlatform(Platform):

    def __init__(self, config: Config):
        """Initialize Theta Platform"""
        super().__init__()
        self.config = config
        self.db_service = ThetaDatabaseService()

    @property
    def name(self) -> str:
        """Platform name"""
        return "theta"

    @property
    def supports_registration(self) -> bool:
        """Whether provider registration is supported (Theta supports)"""
        return True

    def register_provider(self, provider: BaseThetaProvider) -> None:
        super().register_provider(provider)

        if not provider.register_pull_task():
            logging.info(f"Do not register pull task for provider {provider.info.slug}")
            return

        try:
            pull_task = create_pull_task_for_provider(provider)
            scheduler.register_task(pull_task)
        except Exception as e:
            logging.error(f"Failed to register pull task for provider {provider.info.slug}: {str(e)}")

    def _load_providers_from_directory(self, directory: Path) -> List[BaseThetaProvider]:
        providers = []

        if not directory.exists():
            logging.debug(f"Provider directory does not exist: {directory}")
            return providers

        provider_files = sorted(directory.glob("mirobody_*/provider_*.py"))

        if not provider_files:
            logging.debug(f"No provider files found in {directory}")
            return providers

        for provider_file in provider_files:
            provider_name = provider_file.stem  # e.g., "provider_garmin"
            provider_dir = provider_file.parent.name  # e.g., "mirobody_garmin"

            try:
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
                        raise ImportError(f"Cannot load module from {provider_file}")
                finally:
                    if add_to_path and parent_dir in sys.path:
                        sys.path.remove(parent_dir)

                provider_class = None
                for attr_name in dir(module):
                    if attr_name.startswith("Theta") and attr_name.endswith("Provider"):
                        attr = getattr(module, attr_name)
                        if isinstance(attr, type) and issubclass(attr, BaseThetaProvider) and attr != BaseThetaProvider:
                            provider_class = attr
                            break

                if provider_class is None:
                    logging.debug(f"No provider class found in {provider_name}, skipping")
                    continue

                if not hasattr(provider_class, "create_provider"):
                    logging.warning(f"Provider class {provider_class.__name__} missing create_provider method, skipping")
                    continue

                provider_instance = provider_class.create_provider(self.config)
                if provider_instance is not None:
                    providers.append(provider_instance)
                    logging.info(f"Loaded provider from {directory}/{provider_dir}/{provider_name}.py")

            except Exception as e:
                logging.warning(f"Failed to load provider {provider_name} from {directory}: {e}")
                continue

        return providers

    def load_providers(self) -> List[BaseThetaProvider]:
        providers = []

        provider_dirs = self.config.get("PROVIDER_DIRS", [])

        # Always include default theta directory
        default_theta_dir = Path(__file__).parent.parent.resolve()

        # Collect all directories and deduplicate (compare after converting to absolute paths with resolve())
        seen_dirs = set()
        all_dirs = []

        # Add default directory first
        if default_theta_dir not in seen_dirs:
            all_dirs.append(default_theta_dir)
            seen_dirs.add(default_theta_dir)

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
            logging.info(f"Scanning for theta providers in: {directory}")
            dir_providers = self._load_providers_from_directory(directory)
            providers.extend(dir_providers)

        return providers

    async def get_providers(self, nocache: bool = False) -> List[ProviderInfo]:
        providers = []

        for provider in self._providers.values():
            providers.append(provider.info)

        logging.info(f"Got {len(providers)} providers from theta platform")
        return providers

    async def get_user_providers(self, user_id: str) -> List[UserProvider]:
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

            logging.info(f"Got {len(connections)} connections for user {user_id} from theta platform")

        except Exception as e:
            logging.error(f"Error getting user providers for user {user_id}: {str(e)}")

        return connections

    async def link(self, request: LinkRequest) -> Dict[str, Any]:
        provider_slug = request.provider_slug

        provider = self.get_provider(provider_slug)
        if not provider:
            return {"provider_slug": provider_slug,
                    "username": request.credentials.get("username", ""),
                    "msg": f"Provider {provider_slug} not found in theta platform"
                    }
        return await provider.link(request)

    async def unlink(self, user_id: str, provider_slug: str) -> Dict[str, Any]:
        provider = self.get_provider(provider_slug)
        if not provider:
            raise ValueError(f"Provider {provider_slug} not found in theta platform")

        try:
            result_data = await provider.unlink(user_id)
            logging.info(f"Unlink successful for theta provider {provider_slug}")
            return result_data
        except Exception as e:
            logging.error(f"Error unlinking theta provider {provider_slug}: {str(e)}")
            raise RuntimeError(f"Failed to unlink provider: {str(e)}")

    async def post_data(self, provider_slug: str, data: Dict[str, Any], msg_id: str) -> bool:
        provider = self.get_provider(provider_slug)
        if not provider:
            logging.error(f"Provider {provider_slug} not found in theta platform")
            return False

        try:
            data["msg_id"] = msg_id
            saved_data_list = await provider.save_raw_data_to_db(data)
            if not saved_data_list:
                logging.error(f"Raw data save failed for provider {provider_slug}, msg_id={msg_id}")

            standard_health_service = StandardHealthService()
            total_records = 0
            success_count = 0
            error_count = 0

            for saved_data in saved_data_list:
                try:
                    standard_pulse_data = await provider.format_data(saved_data)
                    if not standard_pulse_data or not standard_pulse_data.healthData:
                        logging.info(f"No data formatted by theta provider {provider_slug}")
                        continue

                    user_id = standard_pulse_data.metaInfo.userId
                    if not user_id:
                        logging.error(f"No user ID found in formatted data from provider {provider_slug}")
                        error_count += 1
                        continue

                    success = await standard_health_service.process_standard_data(standard_pulse_data, user_id)
                    records_count = len(standard_pulse_data.healthData)

                    if success:
                        success_count += 1
                        total_records += records_count
                        logging.info(f"Processed {records_count} records for user {user_id}")
                    else:
                        error_count += 1
                        logging.error(f"Failed to process {records_count} records for user {user_id}")

                except Exception as e:
                    error_count += 1
                    logging.error(f"Error processing saved_data item: {str(e)}")
                    continue

            logging.info(
                f"Theta platform completed: {total_records} total records, {success_count} success, {error_count} errors")
            return error_count == 0

        except Exception as e:
            logging.error(f"Error posting data to theta provider {provider_slug}: {str(e)}")
            return False

    async def start_pull_scheduler(self) -> None:
        try:
            await scheduler.start()
        except Exception as e:
            logging.error(f"Failed to start theta pull scheduler: {str(e)}")

    async def stop_pull_scheduler(self) -> None:
        try:
            await scheduler.stop()
            logging.info("Theta pull scheduler stopped successfully")
        except Exception as e:
            logging.error(f"Failed to stop theta pull scheduler: {str(e)}")

    def get_pull_task_status(self, provider_slug: str) -> Dict[str, Any]:
        return scheduler.get_task_status(provider_slug) or {}

    def get_all_pull_task_status(self) -> Dict[str, Any]:
        return scheduler.get_tasks_status()

    # ===== LLM Access Management =====

    async def update_llm_access(self, user_id: str, provider_slug: str, llm_access: int) -> Dict[str, Any]:
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

    async def sync_user_devices(self, user_id: str) -> bool:
        """
        Sync user devices to health_user_provider table

        For theta platform, this is mainly a no-op since theta providers
        are already stored in health_user_provider when linked.
        But we can use this to ensure consistency.

        Args:
            user_id: User ID

        Returns:
            Whether sync was successful
        """
        try:
            # Get all connected theta providers for the user
            connected_providers = await self.db_service.get_user_theta_providers(user_id)

            logging.info(
                f"Theta platform sync_user_devices: found {len(connected_providers)} connected providers for user {user_id}"
            )

            # For theta platform, devices are already in health_user_provider table
            # So this is essentially a verification step
            return True

        except Exception as e:
            logging.error(f"Error syncing theta devices for user {user_id}: {str(e)}")
            return False
