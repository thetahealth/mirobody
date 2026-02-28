"""
Health data services package

Pipeline: StandardPulseData → StandardHealthService.process_standard_data() → series_data table.
All platforms (Theta, Apple, Vital) converge here after formatting raw data into StandardPulseData.

Key class:
    StandardHealthService — validates, deduplicates, and inserts health records into the database.
"""

from .upload_health import StandardHealthService
# Keep backward compatibility
VitalHealthService = StandardHealthService

__all__ = [
    "StandardHealthService",
    "VitalHealthService",  # For backward compatibility
]
