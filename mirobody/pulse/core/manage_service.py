"""
Management Service Module - Core business logic for indicator and unit management

Provides core functionality for indicator statistics and indicator updates.
"""

import logging

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from .database import ManageDatabaseService
from .indicators_info import StandardIndicator, VALID_INDICATORS


class ManageService:
    """Management Service Class - Handles core business logic for indicator management"""

    def __init__(self):
        """Initialize management service"""
        self.db_service = ManageDatabaseService()

    def extract_base_indicator(self, indicator: str) -> str:
        """
        Extract base indicator name without source suffix
        
        Args:
            indicator: Full indicator name, e.g.:
                - "dailyMaxHeartRates.oura" -> "dailyMaxHeartRates"
                - "dailyMinHeartRates.apple_health" -> "dailyMinHeartRates"
                - "heartRates" -> "heartRates"
            
        Returns:
            Base indicator name without source suffix
        """
        if not indicator:
            return indicator
        
        # Check for format: indicator.source
        # Split by dot and take the first part (base indicator)
        if '.' in indicator:
            return indicator.split('.')[0]
        
        # No source suffix found
        return indicator

    def normalize_source_name(self, source: str) -> str:
        """
        Normalize data source names for better categorization display
        
        Args:
            source: Original data source name
            
        Returns:
            Normalized data source name
        """
        if not source:
            return "unknown"

        # Apple Health data source normalization
        if "apple_health" in source.lower() or "com.apple.health" in source.lower():
            return "apple_health"

        # Theta related data source normalization
        if source.startswith("theta."):
            # Keep theta. prefix but simplify suffix
            parts = source.split(".")
            if len(parts) >= 2:
                return f"theta.{parts[1]}"

        # Vital data source normalization
        if "vital" in source.lower():
            return "vital"

        # ResmedMyAir data source normalization
        if "resmed" in source.lower():
            return "resmed"

        # Return original name if no pattern matches
        return source

    async def get_yearly_indicator_stats(self) -> Dict[str, Any]:
        """
        Get yearly indicator statistics data
        
        Returns:
            Dictionary containing indicator statistics information
        """
        try:
            logging.info("Starting to get yearly indicator statistics")

            # Get standard indicators set
            standard_indicators = {indicator.identifier for indicator in StandardIndicator}

            # Calculate date one year ago
            one_year_ago = datetime.now() - timedelta(days=365)

            # Get statistics data from database
            results = await self.db_service.get_yearly_stats(one_year_ago)

            # If no real data, return empty result
            if not results or len(results) == 0:
                return {
                    "sources": {},
                    "overview": {
                        "total_indicators": 0,
                        "total_users": 0,
                        "total_records": 0,
                        "defined_indicators": 0,
                        "undefined_indicators": 0,
                        "total_sources": 0
                    },
                    "generated_at": datetime.now().isoformat(),
                    "is_sample_data": False
                }

            # Process results and normalize data source names
            stats_data = []
            for row in results:
                # Normalize data source names
                original_source = row["source"]
                normalized_source = self.normalize_source_name(original_source)

                # Extract base indicator name without source suffix for defined check
                base_indicator = self.extract_base_indicator(row["indicator"])
                is_defined = base_indicator in standard_indicators

                stats_data.append({
                    "source": normalized_source,
                    "original_source": original_source,
                    "indicator": row["indicator"],
                    "unique_users": row["unique_users"],
                    "total_records": row["total_records"],
                    "latest_time": row["latest_time"],
                    "is_defined_in_constants": is_defined,
                    "indicator_type": row.get("indicator_type", "series"),  # series, summary, or aggregate
                })

            # Group by data source
            sources_data = {}
            total_indicators = 0
            total_users = set()
            total_records = 0
            defined_count = 0
            undefined_count = 0

            for stat in stats_data:
                source = stat["source"]
                if source not in sources_data:
                    sources_data[source] = {
                        "source_name": source,
                        "indicators": [],
                        "summary": {
                            "total_indicators": 0,
                            "total_users": set(),
                            "total_records": 0,
                            "defined_indicators": 0,
                            "undefined_indicators": 0,
                        }
                    }

                # Add indicator data
                indicator_data = {
                    "indicator": stat["indicator"],
                    "user_count": stat["unique_users"],
                    "record_count": stat["total_records"],
                    "latest_time": stat["latest_time"],
                    "is_defined_in_constants": stat["is_defined_in_constants"],
                    "source": stat["original_source"],  # Keep original source for API calls
                    "indicator_type": stat["indicator_type"],  # series, summary, or aggregate
                }

                sources_data[source]["indicators"].append(indicator_data)

                # Update summary statistics
                sources_data[source]["summary"]["total_indicators"] += 1
                sources_data[source]["summary"]["total_records"] += stat["total_records"]

                # Add users to both source-specific and global sets
                sources_data[source]["summary"]["total_users"].add(stat["unique_users"])

                if stat["is_defined_in_constants"]:
                    sources_data[source]["summary"]["defined_indicators"] += 1
                    defined_count += 1
                else:
                    sources_data[source]["summary"]["undefined_indicators"] += 1
                    undefined_count += 1

                # Global statistics
                total_indicators += 1
                total_records += stat["total_records"]
                total_users.add(f"{source}_{stat['unique_users']}")  # Simple user deduplication

            # Convert user set to number (simplified processing)
            for source_data in sources_data.values():
                source_data["summary"]["total_users"] = len(source_data["summary"]["total_users"])

            # Build final response
            response_data = {
                "sources": sources_data,
                "summary": {
                    "total_indicators": total_indicators,
                    "total_unique_users": len(total_users),
                    "total_records": total_records,
                    "defined_in_constants": defined_count,
                    "undefined_in_constants": undefined_count,
                    "total_sources": len(sources_data),
                },
                "generated_at": datetime.now().isoformat(),
                "is_sample_data": False
            }

            logging.info(
                f"Successfully got yearly indicator statistics: {total_indicators} indicators, {len(sources_data)} data sources")
            return response_data

        except Exception as e:
            logging.error(f"Failed to get yearly indicator statistics: {str(e)}")
            raise

    async def update_indicator(
            self,
            old_indicator: str,
            new_indicator: str,
            source: str,
            indicator_type: str,
            dry_run: bool = True
    ) -> Dict[str, Any]:
        """
        Update indicator name
        
        Args:
            old_indicator: Original indicator name
            new_indicator: New indicator name  
            source: Data source
            indicator_type: Indicator type ('series' or 'summary')
            dry_run: Whether to run in dry-run mode
            
        Returns:
            Update operation result
        """
        try:
            logging.info(
                f"Starting to update indicator: {old_indicator} -> {new_indicator} (source: {source}, type: {indicator_type}, dry_run: {dry_run})")

            # Validate indicator type
            if indicator_type not in ['series', 'summary']:
                raise ValueError(f"Invalid indicator type '{indicator_type}'. Must be 'series' or 'summary'")

            # Validate new indicator is a standard indicator
            if new_indicator not in VALID_INDICATORS:
                raise ValueError(f"New indicator '{new_indicator}' is not a standard indicator constant")

            # 1. Query current data situation
            record_info = await self.db_service.get_indicator_record_info(old_indicator, source, indicator_type)
            if not record_info or record_info["record_count"] == 0:
                raise ValueError(f"No data found for indicator '{old_indicator}' in data source '{source}' with type '{indicator_type}'")

            # 2. Check if target indicator already exists
            existing_count = await self.db_service.get_existing_indicator_count(new_indicator, source, indicator_type)

            # 3. Execute update operation (if not dry-run)
            update_result = None
            if not dry_run:
                updated_count = await self.db_service.update_indicator_name(
                    old_indicator, new_indicator, source, indicator_type
                )

                update_result = {
                    "status": "success",
                    "updated_records": updated_count,
                    "expected_records": record_info["record_count"] + existing_count
                }

                logging.info(
                    f"Successfully updated indicator: {old_indicator} -> {new_indicator}, updated {updated_count} records")

            # 4. Build response data
            response_data = {
                "dry_run": dry_run,
                "old_indicator": old_indicator,
                "new_indicator": new_indicator,
                "source": source,
                "current_data": record_info,
                "existing_target_records": existing_count,
                "update_result": update_result,
                "operation_time": datetime.now().isoformat()
            }

            message = f"Indicator update {'preview' if dry_run else 'completed'}: {old_indicator} -> {new_indicator}"
            if dry_run:
                message += f" (will affect {record_info['record_count']} records)"

            return {"message": message, "data": response_data}

        except Exception as e:
            logging.error(f"Update indicator failed: {str(e)}")
            raise

    async def get_theta_pull_configuration(self) -> Dict[str, Any]:
        """
        Get complete theta pull configuration information
        
        Returns:
            Dictionary containing complete configuration information
        """
        try:
            logging.info("Starting to get theta pull configuration")

            # Import configuration constants
            from mirobody.pulse.theta.platform.pull_task import PROVIDER_EXECUTION_INTERVALS, PROVIDER_LOCK_DURATIONS
            from ..manager import platform_manager

            # Get theta platform
            theta_platform = platform_manager.get_platform("theta")
            if not theta_platform:
                logging.warning("Theta platform not available")
                return {
                    "platform": "theta",
                    "scheduler_enabled": False,
                    "message": "Theta platform not available",
                    "static_config": {},
                    "current_tasks": {},
                    "system_info": {}
                }

            # Build complete configuration data
            config_data = {
                "platform": "theta",
                "scheduler_enabled": True,
                "pull_interval": "hourly",
                "static_config": {
                    "execution_intervals": PROVIDER_EXECUTION_INTERVALS,
                    "lock_durations": PROVIDER_LOCK_DURATIONS,
                    "description": {
                        "execution_intervals": "Provider execution interval configuration (hours)",
                        "lock_durations": "Provider lock duration configuration (hours)"
                    }
                },
                "current_tasks": {},
                "system_info": {
                    "total_providers": len(PROVIDER_EXECUTION_INTERVALS) - 1,  # Exclude 'default'
                    "default_execution_interval": PROVIDER_EXECUTION_INTERVALS.get("default", 1.0),
                    "default_lock_duration": PROVIDER_LOCK_DURATIONS.get("default", 0.5),
                    "supported_schedule_types": ["hourly", "interval", "manual"]
                }
            }

            # Try to get current task information
            try:
                from mirobody.pulse.theta.platform.startup import get_theta_pull_task_status
                task_status = get_theta_pull_task_status()
                if "tasks" in task_status:
                    config_data["current_tasks"] = task_status["tasks"]
                    config_data["system_info"]["active_tasks"] = len(task_status["tasks"])
                else:
                    config_data["system_info"]["active_tasks"] = 0
            except Exception as task_error:
                logging.warning(f"Could not get current task status: {str(task_error)}")
                config_data["current_tasks"] = {}
                config_data["system_info"]["active_tasks"] = 0

            # Add generation timestamp
            config_data["generated_at"] = datetime.now().isoformat()

            logging.info(
                f"Successfully got theta pull configuration: {len(config_data['static_config']['execution_intervals'])} providers configured")
            return config_data

        except Exception as e:
            logging.error(f"Failed to get theta pull configuration: {str(e)}")
            raise

