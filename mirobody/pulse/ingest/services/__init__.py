"""
Health data services package

Pipeline: StandardPulseData → StandardHealthService.process_standard_data() → series_data table.
All platforms (pull providers, Apple) converge here after formatting raw data into StandardPulseData.

Key class:
    StandardHealthService — validates, deduplicates, and inserts health records into the database.
"""

from .upload_health import StandardHealthService

__all__ = [
    "StandardHealthService",
]
