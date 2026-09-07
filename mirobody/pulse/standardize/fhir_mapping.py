"""
FHIR Indicator Mapping

Loads fhir_indicators table at startup and provides fhir_id lookup for
th_series_data writes. Hot path is read-only (memory cache).

Configuration (read via safe_read_cfg — environment > overlay > config.yaml):
    FHIR_TABLE_AUTO_R: "true"/"false" - Whether to load fhir_indicators mapping
    FHIR_TABLE_AUTO_W: "true"/"false" - Whether to auto-register missing indicators
"""

import logging
import threading
from typing import Optional

from ...utils import execute_query
from ...utils.config import safe_read_cfg

logger = logging.getLogger(__name__)

# Fixed constants
FHIR_INDICATOR_STANDARD = "THETA"
FHIR_TABLE_NAME = "fhir_indicators"


class FhirMapping:
    """
    FHIR indicator ID mapping with in-memory cache.

    - Startup: load code -> fhir_id from fhir_indicators table
    - Write time: lookup fhir_id by indicator name (read-only, no DB call)
    - Background: auto-register missing indicators if configured
    """

    _instance: Optional['FhirMapping'] = None

    def __init__(self, auto_register: bool = False):
        self._auto_register = auto_register
        self._cache: dict[str, int] = {}  # code -> fhir_id
        self._pending: set[str] = set()   # indicators not found in cache
        self._lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> Optional['FhirMapping']:
        """Get singleton instance. Returns None if not initialized."""
        return cls._instance

    @classmethod
    async def initialize(cls) -> Optional['FhirMapping']:
        """
        Initialize FhirMapping from configuration. Call once at startup.

        Config keys:
            FHIR_TABLE_AUTO_R: "true" to enable reading fhir_indicators mapping
            FHIR_TABLE_AUTO_W: "true" to enable auto-registering missing indicators

        Returns None if FHIR_TABLE_AUTO_R is not "true" (feature disabled).
        If config keys are missing, logs how to enable the feature.
        """
        auto_read = safe_read_cfg("FHIR_TABLE_AUTO_R")
        if not auto_read:
            logger.info(
                "[FhirMapping] FHIR_TABLE_AUTO_R not configured; fhir_id "
                "mapping stays off. Set `FHIR_TABLE_AUTO_R: true` in "
                "config.yaml (or the environment) to enable it."
            )
            cls._instance = None
            return None

        if auto_read.lower() != "true":
            logger.info("[FhirMapping] FHIR_TABLE_AUTO_R is not 'true', skipping")
            cls._instance = None
            return None

        auto_write = safe_read_cfg("FHIR_TABLE_AUTO_W")
        if not auto_write:
            logger.info(
                "[FhirMapping] FHIR_TABLE_AUTO_W not configured. "
                "Auto-registration disabled. Set `FHIR_TABLE_AUTO_W: true` in config.yaml (or the environment) to enable it."
            )
        auto_register = auto_write.lower() == "true" if auto_write else False

        instance = cls(auto_register=auto_register)
        await instance.load()

        cls._instance = instance
        return instance

    async def load(self):
        """Load code -> fhir_id mapping from database."""
        try:
            query = f"""
                SELECT code, id FROM {FHIR_TABLE_NAME}
                WHERE indicator_standard = :standard
            """
            results = await execute_query(query, {"standard": FHIR_INDICATOR_STANDARD})

            new_cache: dict[str, int] = {}
            for row in results:
                code = row.get("code")
                fhir_id = row.get("id")
                if code and fhir_id:
                    # If same code has multiple entries, keep the largest id (newest)
                    if code not in new_cache or fhir_id > new_cache[code]:
                        new_cache[code] = fhir_id

            with self._lock:
                self._cache = new_cache

            logger.info(
                f"[FhirMapping] Loaded {len(new_cache)} indicators "
                f"from {FHIR_TABLE_NAME} (standard={FHIR_INDICATOR_STANDARD}), "
                f"auto_register={'enabled' if self._auto_register else 'disabled'}"
            )

        except Exception as e:
            logger.error(f"[FhirMapping] Failed to load from {FHIR_TABLE_NAME}: {e}")

    def get_fhir_id(self, indicator: str) -> int | None:
        """
        Lookup fhir_id for an indicator name.

        Matching strategy:
        1. Strip source suffix: "dailyAvgHeartRates.apple_health" -> "dailyAvgHeartRates"
        2. Exact match against cache

        Returns fhir_id if found, None if not (adds to pending set).
        """
        base_indicator = self._strip_source_suffix(indicator)

        fhir_id = self._cache.get(base_indicator)
        if fhir_id:
            return fhir_id

        # Cache miss — record for background processing
        if base_indicator not in self._pending:
            self._pending.add(base_indicator)
            logger.debug(f"[FhirMapping] Cache miss: {base_indicator}")

        return None

    def get_pending(self) -> set[str]:
        """Get indicators that were not found in cache."""
        return self._pending.copy()

    def clear_pending(self, indicators: set[str]):
        """Remove resolved indicators from pending set."""
        self._pending -= indicators

    async def register_missing(self, indicator_info_map: dict[str, dict]):
        """
        Register missing indicators in fhir_indicators table.
        Only called if auto_register is enabled (FHIR_TABLE_AUTO_W=true).

        Args:
            indicator_info_map: {code: {short_name, description, unit}} from IndicatorInfo
        """
        if not self._auto_register:
            if self._pending:
                logger.info(
                    f"[FhirMapping] FHIR_TABLE_AUTO_W is off, unregistered indicators: "
                    f"{', '.join(sorted(self._pending))}"
                )
            return

        registered = set()
        for code in list(self._pending):
            info = indicator_info_map.get(code, {})
            try:
                new_id = await self._insert_indicator(
                    code=code,
                    short_name=info.get("short_name", code),
                    description=info.get("description", ""),
                    unit=info.get("unit", ""),
                )
                if new_id:
                    with self._lock:
                        self._cache[code] = new_id
                    registered.add(code)
                    logger.info(f"[FhirMapping] Auto-registered: {code} -> {new_id}")
                else:
                    # ON CONFLICT DO NOTHING — already exists, fetch its id into cache
                    existing_id = await self._lookup_fhir_id(code)
                    if existing_id:
                        with self._lock:
                            self._cache[code] = existing_id
                        logger.info(f"[FhirMapping] Loaded existing: {code} -> {existing_id}")
                    registered.add(code)
            except Exception as e:
                logger.error(f"[FhirMapping] Failed to register {code}: {e}")

        if registered:
            self.clear_pending(registered)

    async def _lookup_fhir_id(self, code: str) -> int | None:
        """Lookup fhir_id for a single code from database."""
        query = f"""
            SELECT id FROM {FHIR_TABLE_NAME}
            WHERE indicator_standard = :standard AND code = :code
            ORDER BY id DESC LIMIT 1
        """
        result = await execute_query(query, {"standard": FHIR_INDICATOR_STANDARD, "code": code})
        if result and len(result) > 0:
            return result[0].get("id")
        return None

    async def _insert_indicator(self, code: str, short_name: str,
                                description: str, unit: str) -> int | None:
        """Insert a new indicator into fhir_indicators and return its id.

        The column list used to end in ``update_time``, which
        ``schema/01_basedata.sql`` does not define — so this INSERT raised
        ``column "update_time" of relation "fhir_indicators" does not exist``
        against our own DDL, every time. It went unnoticed because the write
        path is gated behind ``FHIR_TABLE_AUTO_W`` and because the table used
        to be populated by an external mapper instead. That mapper is retired,
        which makes this the only thing that can put a row in the table, so it
        has to actually work.
        """
        query = f"""
            INSERT INTO {FHIR_TABLE_NAME} (
                indicator_standard, code, full_name, short_name,
                description, unit, llm_unit, rank
            ) VALUES (
                :standard, :code, :full_name, :short_name,
                :description, :unit, :llm_unit, 0
            )
            ON CONFLICT (indicator_standard, code) DO NOTHING
            RETURNING id
        """
        params = {
            "standard": FHIR_INDICATOR_STANDARD,
            "code": code,
            "full_name": code,
            "short_name": short_name,
            "description": description,
            "unit": unit,
            "llm_unit": unit,
        }
        result = await execute_query(query, params)
        if result:
            return result[0].get("id")
        return None

    @staticmethod
    def _strip_source_suffix(indicator: str) -> str:
        """
        Strip source suffix from indicator name.
        "dailyAvgHeartRates.apple_health" -> "dailyAvgHeartRates"
        "dailyAvgHeartRates" -> "dailyAvgHeartRates"
        """
        dot_pos = indicator.find(".")
        if dot_pos > 0:
            return indicator[:dot_pos]
        return indicator


def get_fhir_id(indicator: str) -> int | None:
    """
    Convenience function for hot path usage.
    Returns fhir_id or None (if FhirMapping not initialized or indicator not found).
    """
    instance = FhirMapping.get_instance()
    if instance is None:
        return None
    return instance.get_fhir_id(indicator)
