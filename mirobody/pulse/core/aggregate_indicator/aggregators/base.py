"""
Base protocol for aggregators

Defines the interface that all aggregator implementations must follow.
Uses Protocol (PEP 544) for duck typing instead of ABC for more flexibility.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Set

from ..models import CalculationTask


class AggregatorProtocol(Protocol):
    """
    Protocol for aggregation implementations
    
    Any class implementing this protocol can be injected into AggregateIndicatorService.
    This allows for flexible implementations (SQL, Python, custom) without inheritance.
    """

    async def get_trigger_tasks(
            self,
            since_timestamp: int,
            user_id: Optional[str] = None
    ) -> List[CalculationTask]:
        """
        Get trigger tasks based on time range
        
        This replaces the previous get_updated_series_data functionality.
        Returns CalculationTask objects ready for aggregation.
        
        Args:
            since_timestamp: Unix timestamp (seconds) to fetch updates after
            user_id: Optional user ID filter (None = all users)
            
        Returns:
            List of CalculationTask objects
        """
        ...

    async def calculate_batch_aggregations(
            self,
            tasks: List[CalculationTask]
    ) -> List[Dict[str, Any]]:
        """
        Calculate aggregations for a batch of tasks
        
        This method handles all the complex grouping logic:
        1. Group tasks by date
        2. For each date, group by user batches (max 100 users per batch)
        3. For each batch, decide whether to use single SQL or split by indicator
        4. Execute aggregation and return summary records
        
        Args:
            tasks: List of CalculationTask objects
            
        Returns:
            List of summary record dicts ready for database insertion
        """
        ...

    async def calculate_time_range_aggregations(
            self,
            start_date: datetime,
            end_date: datetime,
            user_id: str
    ) -> List[Dict[str, Any]]:
        """
        Calculate aggregations for a time range
        
        This method handles historical data processing:
        1. If time range > 30 days, split by 30-day chunks and recurse
        2. If time range <= 30 days, execute direct aggregation for the entire range
        
        Args:
            start_date: Start date for aggregation
            end_date: End date for aggregation
            user_id: Single user ID to process
            
        Returns:
            List of summary record dicts ready for database insertion
        """
        ...
