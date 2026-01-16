"""
Health data services package
"""

from .upload_health import StandardHealthService
# Keep backward compatibility
VitalHealthService = StandardHealthService

__all__ = [
    "StandardHealthService",
    "VitalHealthService",  # For backward compatibility
]
