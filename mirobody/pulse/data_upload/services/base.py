"""
Base service class for health data processing
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Optional, Tuple, Union

from ..models.requests import StandardPulseData
from ..repositories.health_data import HealthDataRepository, health_data_repository
from ...core import convert_to_standard
from ...core.indicators_info import get_indicator_by_str


class BaseHealthService(ABC):
    """Health data service base class"""

    def __init__(self, repository: Optional[HealthDataRepository] = None):
        """
        Initialize service

        Args:
            repository: Data repository instance for dependency injection
        """
        self.repository = repository or health_data_repository

    @abstractmethod
    async def process_standard_data(self, standard_data: StandardPulseData, current_user: str) -> bool:
        """
        Abstract method for processing data

        Args:
            standard_data: Data to be processed
            current_user: Authenticated current user ID

        Returns:
            bool: Whether processing succeeded
        """
        pass

    @abstractmethod
    def get_service_name(self) -> str:
        """Get service name"""
        pass

    def normalize_health_data_unit(self, record_type: Union[str, Any], value: float, unit: Optional[str], percentage_handling: bool = False, ) -> Tuple[float, str]:
        """
        Generic health data unit normalization method

        Args:
            record_type: Health record type (StandardIndicator value)
            value: Original value
            unit: Original unit
            percentage_handling: Whether to handle percentage conversion (multiply by 100)

        Returns:
            Tuple[float, str]: Normalized value and standard unit
        """
        # DEPRECATED: percentage_handling should be handled in Provider layer
        if percentage_handling and unit == "%":
            value = value * 100

        # Get indicator enum
        indicator = get_indicator_by_str(record_type) if isinstance(record_type, str) else record_type
        
        if not indicator:
            logging.warning(f"Unknown indicator: {record_type}, keeping original value")
            return value, unit or ""
        
        # Convert to standard unit using new implementation
        try:
            converted_value, standard_unit = convert_to_standard(indicator, value, unit or "")
            # logging.debug(f"Unit converted: {value} {unit} -> {converted_value} {standard_unit} for {record_type}")
            return converted_value, standard_unit
        except Exception as e:
            # Conversion failed - keep original value and unit (fail gracefully)
            logging.debug(f"Conversion not available for {record_type} {unit}: {e}, keeping original")
            return value, unit or indicator.value.standard_unit
