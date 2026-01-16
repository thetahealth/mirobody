"""
Aggregators module

Provides aggregation implementations using dependency injection pattern.
"""

from .base import AggregatorProtocol
from .sql_aggregator import SQLAggregator

__all__ = [
    "AggregatorProtocol",
    "SQLAggregator",
]
