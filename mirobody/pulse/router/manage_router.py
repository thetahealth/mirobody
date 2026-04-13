"""
Management router for Pulse system
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional, Union

from fastapi import APIRouter, Query, Depends
from pydantic import BaseModel, Field

from ..core.manage_service import ManageService
from ..core.auth import verify_manage_key
from ..core.user_health_service import UserHealthDataService

class StandardResponse(BaseModel):
    """Standard response format"""

    code: int = Field(default=0, description="Response code, 0 indicates success")
    msg: str = Field(default="ok", description="Response message")
    data: Dict[str, Any] = Field(default_factory=dict, description="Response data")


class ErrorResponse(BaseModel):
    """Error response format"""

    code: int = Field(description="Error code")
    detail: str = Field(description="Error details")


class UpdateIndicatorRequest(BaseModel):
    """Update indicator request model"""

    old_indicator: str = Field(..., description="Original indicator name")
    new_indicator: str = Field(..., description="New indicator name")
    source: str = Field(..., description="Data source")
    indicator_type: str = Field(..., description="Indicator type: 'series' or 'summary'")
    dry_run: bool = Field(default=True, description="Whether to run in dry-run mode")


class TriggerTaskRequest(BaseModel):
    """Request model for triggering theta pull tasks"""

    provider_slug: str = Field(description="The provider slug to trigger")
    force: bool = Field(default=False, description="Whether to force trigger even if conditions are not met")


# Import PlatformManager
from ..manager import platform_manager

# Create router - Management interfaces use different prefix
router = APIRouter(prefix="/api/v1/manage", tags=["management"])

# Create management service instance
manage_service = ManageService()


# ===== Theta Pull Scheduler Management Interfaces =====


@router.get("/theta/pull/status", response_model=Union[StandardResponse, ErrorResponse])
async def get_theta_pull_status(authorized: bool = Depends(verify_manage_key)):
    """
    Get the status of theta pull scheduler

    Returns:
        Current status of all theta pull tasks
    """
    try:
        # Import the function from startup module
        from mirobody.pulse.theta.platform.startup import get_theta_pull_task_status

        # Get status from theta startup function
        status_data = get_theta_pull_task_status()

        return StandardResponse(code=0, msg="ok", data=status_data)

    except Exception as e:
        logging.error(f"Failed to get theta pull status: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to get theta pull status: {str(e)}")


@router.post("/theta/pull/trigger", response_model=Union[StandardResponse, ErrorResponse])
async def trigger_theta_pull(request: TriggerTaskRequest, authorized: bool = Depends(verify_manage_key)):
    """
    Manually trigger theta pull task for a specific provider

    Args:
        request: Request containing provider_slug and force parameters

    Returns:
        Trigger result
    """
    try:
        # Get theta platform and trigger task
        theta_platform = platform_manager.get_platform("theta")
        if not theta_platform:
            return ErrorResponse(code=404, detail="Theta platform not found")

        # Import scheduler to trigger task
        from ..core.scheduler import scheduler
        result = await scheduler.trigger_task(request.provider_slug, request.force)

        if result:
            return StandardResponse(code=0, msg="ok",
                                    data={"triggered": True, "provider": request.provider_slug, "force": request.force})
        else:
            return ErrorResponse(code=400, detail=f"Failed to trigger task for provider: {request.provider_slug}")

    except Exception as e:
        logging.error(f"Failed to trigger theta pull: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to trigger theta pull: {str(e)}")


@router.get("/theta/pull/lock-status", response_model=Union[StandardResponse, ErrorResponse])
async def get_theta_pull_lock_status(authorized: bool = Depends(verify_manage_key)):
    """
    Get the distributed lock status for theta pull tasks

    Returns:
        Lock status information
    """
    try:
        # Get theta platform and check lock status
        theta_platform = platform_manager.get_platform("theta")
        if not theta_platform:
            return StandardResponse(code=0, msg="ok", data={"locks": [], "message": "Theta platform not available"})

        # For now, return basic lock status - this can be enhanced later
        lock_status = {
            "locks": [],
            "total_locks": 0,
            "active_locks": 0,
            "message": "Lock status feature not fully implemented yet"
        }

        return StandardResponse(code=0, msg="ok", data=lock_status)

    except Exception as e:
        logging.error(f"Failed to get theta pull lock status: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to get theta pull lock status: {str(e)}")


@router.get("/theta/pull/config", response_model=Union[StandardResponse, ErrorResponse])
async def get_theta_pull_config(authorized: bool = Depends(verify_manage_key)):
    """
    Get theta pull configuration

    Returns:
        Complete configuration information including execution intervals and lock durations
    """
    try:
        # Call management service to get configuration
        config_data = await manage_service.get_theta_pull_configuration()

        return StandardResponse(code=0, msg="ok", data=config_data)

    except Exception as e:
        logging.error(f"Failed to get theta pull config: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to get theta pull config: {str(e)}")


@router.post("/theta/pull/start", response_model=Union[StandardResponse, ErrorResponse])
async def start_theta_pull_scheduler(authorized: bool = Depends(verify_manage_key)):
    """
    Start theta pull scheduler

    Returns:
        Start operation result
    """
    try:
        # Import startup function to start scheduler
        from mirobody.pulse.theta.platform.startup import start_theta_pull_scheduler as start_scheduler

        await start_scheduler()

        return StandardResponse(code=0, msg="ok",
                                data={"status": "started", "message": "Theta pull scheduler started successfully"})

    except Exception as e:
        logging.error(f"Failed to start theta pull scheduler: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to start theta pull scheduler: {str(e)}")


@router.post("/theta/pull/stop", response_model=Union[StandardResponse, ErrorResponse])
async def stop_theta_pull_scheduler(authorized: bool = Depends(verify_manage_key)):
    """
    Stop theta pull scheduler

    Returns:
        Stop operation result
    """
    try:
        # Import startup function to stop scheduler
        from mirobody.pulse.theta.platform.startup import stop_theta_pull_scheduler as stop_scheduler

        await stop_scheduler()

        return StandardResponse(code=0, msg="ok",
                                data={"status": "stopped", "message": "Theta pull scheduler stopped successfully"})

    except Exception as e:
        logging.error(f"Failed to stop theta pull scheduler: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to stop theta pull scheduler: {str(e)}")


# ===== Indicators and Units Management Interfaces =====


@router.get("/pulse/indicators", response_model=Union[StandardResponse, ErrorResponse])
async def get_indicators_info(authorized: bool = Depends(verify_manage_key)):
    """
    Get all standard indicators information

    Returns:
        Classification and detailed information of standard indicators
    """
    try:
        from ..core import get_all_indicators_info

        indicators_data = get_all_indicators_info()

        return StandardResponse(code=0, msg="ok", data=indicators_data)
    except Exception as e:
        logging.error(f"Failed to get indicators info: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to get indicators info: {str(e)}")


@router.get("/pulse/units", response_model=Union[StandardResponse, ErrorResponse])
async def get_units_info(authorized: bool = Depends(verify_manage_key)):
    """
    Get all standard units information

    Returns:
        Classification and detailed information of standard units
    """
    try:
        from ..core import get_all_units_info

        units_data = get_all_units_info()

        return StandardResponse(code=0, msg="ok", data=units_data)
    except Exception as e:
        logging.error(f"Failed to get units info: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to get units info: {str(e)}")


@router.get("/pulse/indicators-and-units", response_model=Union[StandardResponse, ErrorResponse])
async def get_indicators_and_units(authorized: bool = Depends(verify_manage_key)):
    """
    Get both indicators and units information in one request

    Returns:
        Combined indicators and units data
    """
    try:
        from ..core import get_all_indicators_info, get_all_units_info

        indicators_data = get_all_indicators_info()
        units_data = get_all_units_info()

        # Calculate summary statistics
        total_indicators = 0
        indicator_categories = 0
        if indicators_data and "categories" in indicators_data:
            indicator_categories = len(indicators_data["categories"])
            # Use deduplicated total indicator count instead of duplicate counts in categories
            if "total_indicators" in indicators_data:
                total_indicators = indicators_data["total_indicators"]
            else:
                # Fallback calculation method: calculate from indicators dictionary
                if "indicators" in indicators_data:
                    total_indicators = len(indicators_data["indicators"])

        total_units = 0
        unit_categories = 0
        if units_data and "categories" in units_data:
            unit_categories = len(units_data["categories"])
            for category in units_data["categories"].values():
                if "units" in category and isinstance(category["units"], list):
                    total_units += len(category["units"])

        generated_at = datetime.now().isoformat()

        combined_data = {
            "indicators": indicators_data,
            "units": units_data,
            "generated_at": generated_at,
            "summary": {
                "total_indicators": total_indicators,
                "total_units": total_units,
                "indicator_categories": indicator_categories,
                "unit_categories": unit_categories,
                "generated_at": generated_at,
            }
        }

        return StandardResponse(code=0, msg="ok", data=combined_data)
    except Exception as e:
        logging.error(f"Failed to get indicators and units info: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to get indicators and units info: {str(e)}")


@router.get("/pulse/indicators-yearly-stats", response_model=Union[StandardResponse, ErrorResponse])
async def get_indicators_yearly_stats(authorized: bool = Depends(verify_manage_key)):
    """
    Get yearly statistics for health indicators

    Returns:
        Yearly statistics grouped by data source with indicator details
    """
    try:
        logging.info("Starting to get yearly indicator statistics")
        stats_data = await manage_service.get_yearly_indicator_stats()
        return StandardResponse(code=0, msg="ok", data=stats_data)
    except Exception as e:
        logging.error(f"Failed to get yearly indicator stats: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to get yearly indicator stats: {str(e)}")


@router.post("/pulse/update-indicator")
async def update_indicator(request: UpdateIndicatorRequest, authorized: bool = Depends(verify_manage_key)):
    """
    Update indicator name
    
    Args:
        request: Update indicator request containing old_indicator, new_indicator, source, and dry_run
        
    Returns:
        Update operation result
    """
    try:
        logging.info(f"Starting to update indicator: {request.old_indicator} -> {request.new_indicator} (source: {request.source}, type: {request.indicator_type}")
        result = await manage_service.update_indicator(request.old_indicator, request.new_indicator, request.source, request.indicator_type, request.dry_run)
        return StandardResponse(code=0, msg=result["message"], data=result["data"])

    except ValueError as ve:
        logging.error(f"Parameter validation failed: {str(ve)}")
        return ErrorResponse(code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Update indicator failed: {str(e)}")
        return ErrorResponse(code=500, detail=f"Update indicator failed: {str(e)}")


# ===== Aggregate Indicator Management Interfaces =====
# Only keep historical data recalculation interface, other functions managed uniformly through scheduler

@router.post("/aggregate/recalculate-range", response_model=Union[StandardResponse, ErrorResponse])
async def recalculate_date_range(
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    user_id: Optional[str] = Query(None, description="Specific user ID (optional)"),
    authorized: bool = Depends(verify_manage_key)
):
    """
    Recalculate aggregations for a date range (for data repair)
    
    This is a special operation, not part of the regular scheduler flow.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        user_id: Optional user ID filter
        
    Returns:
        Recalculation result
    """
    try:
        from ..core.aggregate_indicator.service import AggregateIndicatorService
        from datetime import datetime
        
        service = AggregateIndicatorService()
        
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
        
        # Execute historical data recalculation
        result = await service.recalculate_date_range(
            start_date=start,
            end_date=end,
            user_id=user_id
        )
        
        return StandardResponse(
            code=0,
            msg="ok",
            data={
                "start_date": start_date,
                "end_date": end_date,
                "user_id": user_id or "all",
                "result": result
            }
        )
        
    except Exception as e:
        logging.error(f"Failed to recalculate date range: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to recalculate date range: {str(e)}")


# ===== Platform Webhook Management Interfaces =====


@router.get("/pulse/{platform}/webhooks", response_model=Union[StandardResponse, ErrorResponse])
async def get_platform_webhooks(
        platform: str,
        page: int = Query(1, description="Page number (starting from 1)"),
        page_size: int = Query(20, description="Number of records per page"),
        provider: Optional[str] = Query(None, description="Provider slug (for Theta platform)"),
        event_type: Optional[str] = Query(None, description="Filter by event type"),
        user_id: Optional[str] = Query(None, description="Filter by user ID"),
        status: Optional[str] = Query(None, description="Filter by status (e.g., 'success', 'pending', 'error')"),
        authorized: bool = Depends(verify_manage_key)
):
    """
    Get platform webhook data with pagination and filters
    
    Args:
        platform: Platform name (e.g., 'vital', 'theta')
        page: Page number (starting from 1)
        page_size: Number of records per page
        provider: Optional provider slug (required for Theta, ignored for Vital)
        event_type: Optional filter for event type
        user_id: Optional filter for user ID
        status: Optional filter for status
        
    Returns:
        Paginated webhook data
    """
    try:
        logging.info(f"Getting {platform} webhooks: page={page}, page_size={page_size}, provider={provider}, filters: event_type={event_type}, user_id={user_id}, status={status}")

        # Get platform instance
        platform_instance = platform_manager.get_platform(platform)
        if not platform_instance:
            return ErrorResponse(code=404, detail=f"Platform '{platform}' not found")

        # Strip whitespace from filter parameters
        provider_clean = provider.strip() if provider else None
        event_type_clean = event_type.strip() if event_type else None
        user_id_clean = user_id.strip() if user_id else None
        status_clean = status.strip() if status else None

        # Call platform's get_webhooks method
        # Only pass provider parameter if it's not None (for platforms that support it like Theta)
        # Vital platform doesn't accept provider parameter
        webhook_params = {
            "page": page,
            "page_size": page_size,
            "event_type": event_type_clean,
            "user_id": user_id_clean,
            "status": status_clean
        }
        
        # Only add provider if it's specified (for Theta platform)
        if provider_clean is not None:
            webhook_params["provider"] = provider_clean
            
        webhook_data = await platform_instance.get_webhooks(**webhook_params)

        return StandardResponse(code=0, msg="ok", data=webhook_data)

    except NotImplementedError as e:
        logging.warning(f"Platform {platform} does not support webhook management: {str(e)}")
        return ErrorResponse(code=501, detail=f"Platform '{platform}' does not support webhook management")
    except Exception as e:
        logging.error(f"Failed to get {platform} webhooks: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to get {platform} webhooks: {str(e)}")


@router.get("/pulse/{platform}/check_format", response_model=Union[StandardResponse, ErrorResponse])
async def check_platform_webhook_format(
        platform: str,
        id: int = Query(..., description="Webhook ID"),
        provider: Optional[str] = Query(None, description="Provider slug (for Theta platform)"),
        authorized: bool = Depends(verify_manage_key)
):
    """
    Check platform webhook format by simulating event provider processing
    
    Args:
        platform: Platform name (e.g., 'vital', 'theta')
        id: Webhook ID from database
        provider: Optional provider slug (required for Theta, ignored for Vital)
        
    Returns:
        Original webhook data and formatted result
    """
    try:
        logging.info(f"Checking {platform} webhook format for ID: {id}, provider: {provider}")

        # Get platform instance
        platform_instance = platform_manager.get_platform(platform)
        if not platform_instance:
            return ErrorResponse(code=404, detail=f"Platform '{platform}' not found")

        # Strip whitespace from provider parameter
        provider_clean = provider.strip() if provider else None

        # Call platform's check_format method
        # Note: Only pass provider parameter if it's not None (for Theta platform)
        # Vital platform's check_format doesn't accept provider parameter
        if provider_clean:
            format_result = await platform_instance.check_format(id, provider=provider_clean)
        else:
            format_result = await platform_instance.check_format(id)

        return StandardResponse(code=0, msg="ok", data=format_result)

    except NotImplementedError as e:
        logging.warning(f"Platform {platform} does not support format checking: {str(e)}")
        return ErrorResponse(code=501, detail=f"Platform '{platform}' does not support format checking")
    except Exception as e:
        logging.error(f"Failed to check {platform} webhook format: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to check {platform} webhook format: {str(e)}")


# ===== User Health Data Query Interfaces (Management) =====


@router.get("/pulse/user-health-data", response_model=Union[StandardResponse, ErrorResponse])
async def get_user_health_data(
    user_id: str = Query(..., description="User ID"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD), max 7 days from start"),
    indicators: Optional[str] = Query(None, description="Comma-separated indicator names"),
    data_sources: Optional[str] = Query(None, description="Comma-separated data source names"),
    include_series: bool = Query(True, description="Include series_data (fine-grained)"),
    include_summary: bool = Query(True, description="Include th_series_data (aggregated)"),
    authorized: bool = Depends(verify_manage_key)
):
    """
    Get user health data for a specific time range (Management API)
    
    Query parameters:
        - user_id: User ID (required)
        - start_date: Start date in YYYY-MM-DD format (required)
        - end_date: End date in YYYY-MM-DD format (required, max 7 days)
        - indicators: Comma-separated list of indicators to filter (optional)
        - data_sources: Comma-separated list of data sources to filter (optional)
        - include_series: Whether to include fine-grained data from series_data table
        - include_summary: Whether to include aggregated data from th_series_data table
        - sk: Management secret key (required, passed as query parameter)
        
    Example:
        GET /api/v1/manage/pulse/user-health-data?user_id=505&start_date=2025-11-10&end_date=2025-11-10&sk=***
    
    Returns:
        User health data with statistics
    """
    try:
        # Parse dates
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            # Set end time to end of day
            end = end.replace(hour=23, minute=59, second=59)
        except ValueError as e:
            return ErrorResponse(code=400, detail=f"Invalid date format: {str(e)}. Use YYYY-MM-DD")
        
        # Validate time range
        if (end - start).days > 7:
            return ErrorResponse(code=400, detail="Time range cannot exceed 7 days")
        
        if start > end:
            return ErrorResponse(code=400, detail="Start date must be before or equal to end date")
        
        # Parse optional parameters
        indicator_list = [ind.strip() for ind in indicators.split(",")] if indicators else None
        source_list = [src.strip() for src in data_sources.split(",")] if data_sources else None
        
        # Query data
        service = UserHealthDataService()
        data = await service.get_user_health_data(
            user_id=user_id,
            start_date=start,
            end_date=end,
            indicators=indicator_list,
            data_sources=source_list,
            include_series=include_series,
            include_summary=include_summary,
        )
        
        return StandardResponse(code=0, msg="ok", data=data)
        
    except ValueError as e:
        logging.warning(f"Validation error in user health data query: {str(e)}")
        return ErrorResponse(code=400, detail=str(e))
    except Exception as e:
        logging.error(f"Failed to get user health data: {str(e)}")
        return ErrorResponse(code=500, detail=f"Failed to get user health data: {str(e)}")


@router.get("/pulse/user-data-sources", response_model=Union[StandardResponse, ErrorResponse])
async def get_user_data_sources(
    user_id: str = Query(..., description="User ID"),
    authorized: bool = Depends(verify_manage_key)
):
    """
    Get all data sources for a specific user (Management API)
    
    Query parameters:
        - user_id: User ID (required)
        - sk: Management secret key (required, passed as query parameter)
    
    Returns:
        List of data sources with record counts and date ranges
    """
    try:
        # Query data sources
        service = UserHealthDataService()
        sources = await service.get_user_data_sources(user_id)
        
        return StandardResponse(
            code=0,
            msg="ok",
            data={
                "user_id": user_id,
                "sources": sources,
                "total": len(sources)
            }
        )
        
    except Exception as e:
        logging.error(f"Failed to get user data sources: {str(e)}")
        return ErrorResponse(code=500, detail=str(e))


@router.get("/pulse/user-indicators", response_model=Union[StandardResponse, ErrorResponse])
async def get_user_indicators(
    user_id: str = Query(..., description="User ID"),
    limit: int = Query(100, description="Maximum number of indicators to return"),
    authorized: bool = Depends(verify_manage_key)
):
    """
    Get all indicators for a specific user (Management API)
    
    Query parameters:
        - user_id: User ID (required)
        - limit: Maximum number of indicators to return (default: 100)
        - sk: Management secret key (required, passed as query parameter)
    
    Returns:
        List of indicators with metadata and record counts
    """
    try:
        # Query indicators
        service = UserHealthDataService()
        indicators = await service.get_user_indicators(user_id, limit)
        
        return StandardResponse(
            code=0,
            msg="ok",
            data={
                "user_id": user_id,
                "indicators": indicators,
                "total": len(indicators),
                "limit": limit
            }
        )
        
    except Exception as e:
        logging.error(f"Failed to get user indicators: {str(e)}")
        return ErrorResponse(code=500, detail=str(e))


# ===== Theta Provider Raw Data Query Interfaces =====


@router.get("/pulse/theta/providers", response_model=Union[StandardResponse, ErrorResponse])
async def get_theta_providers(
    authorized: bool = Depends(verify_manage_key)
):
    """
    Get all registered Theta providers (Management API)
    
    Query parameters:
        - sk: Management secret key (required, passed as query parameter)
    
    Returns:
        List of provider information with slug, name, description, logo, etc.
    """
    try:
        # Get theta platform from platform_manager
        platform_instance = platform_manager.get_platform("theta")
        if not platform_instance:
            return ErrorResponse(code=404, detail="Theta platform not found")
        
        # Get all registered providers
        providers = await platform_instance.get_providers()
        
        # Format response
        provider_list = [
            {
                "slug": p.slug,
                "name": p.name,
                "description": p.description,
                "logo": p.logo,
                "supported": p.supported,
                "auth_type": p.auth_type.value if p.auth_type else None,
                "status": p.status.value if p.status else None,
            }
            for p in providers
        ]
        
        return StandardResponse(
            code=0,
            msg="ok",
            data={
                "providers": provider_list,
                "total": len(provider_list)
            }
        )
    except Exception as e:
        logging.error(f"Error getting theta providers: {str(e)}")
        return ErrorResponse(
            code=500,
            msg="Failed to get theta providers",
            detail=str(e)
        )


# ===== Data Health Monitoring (W3.1 / W3.2, TH-141) =====


@router.get("/pulse/monitor/ingestion", response_model=Union[StandardResponse, ErrorResponse])
async def get_ingestion_stats(
    period: int = Query(24, description="Lookback period in hours (default 24)"),
    platform: Optional[str] = Query(None, description="Filter by platform (optional)"),
    authorized: bool = Depends(verify_manage_key),
):
    """
    W3.1 ingestion monitoring API.

    Reads pre-computed platform_hourly_profile for fast response.
    Returns by-platform summary and hourly trend.
    """
    try:
        from ..core.monitor.query_service import MonitorQueryService
        service = MonitorQueryService()
        data = await service.get_ingestion_stats(period_hours=period, platform=platform)
        return StandardResponse(data=data)
    except Exception as e:
        logging.error(f"Ingestion stats query failed: {str(e)}")
        return ErrorResponse(code=500, detail=f"Ingestion stats query failed: {str(e)}")


@router.get("/pulse/data-quality", response_model=Union[StandardResponse, ErrorResponse])
async def get_data_quality(
    date: Optional[str] = Query(None, description="Date to analyze (YYYY-MM-DD, default=yesterday)"),
    indicator: Optional[str] = Query(None, description="Specific indicator (optional, default=all)"),
    authorized: bool = Depends(verify_manage_key),
):
    """
    W3.2 data quality monitoring API.

    Reads pre-computed indicator_daily_profile for <100ms response.
    Returns value distribution, anomaly detection, and cross-source comparison.
    """
    try:
        from ..core.monitor.query_service import MonitorQueryService
        service = MonitorQueryService()
        data = await service.get_data_quality(target_date=date, indicator=indicator)
        return StandardResponse(data=data)
    except Exception as e:
        logging.error(f"Data quality query failed: {str(e)}")
        return ErrorResponse(code=500, detail=f"Data quality query failed: {str(e)}")


@router.post("/pulse/monitor/backfill", response_model=Union[StandardResponse, ErrorResponse])
async def trigger_backfill(
    hourly_days: int = Query(7, description="Days to backfill for hourly stats"),
    daily_days: int = Query(30, description="Days to backfill for daily profiles"),
    authorized: bool = Depends(verify_manage_key),
):
    """
    Trigger historical data backfill for report tables.

    WARNING: This is a long-running operation. Hourly backfill (7 days = 168 hours)
    can take several minutes.
    """
    try:
        from ..core.monitor.collector_service import MonitorCollectorService
        service = MonitorCollectorService()

        hourly_result = await service.backfill_hourly(days=hourly_days)
        daily_result = await service.backfill_daily(days=daily_days)

        return StandardResponse(
            data={
                "hourly_backfill": hourly_result,
                "daily_backfill": daily_result,
            }
        )
    except Exception as e:
        logging.error(f"Backfill failed: {str(e)}")
        return ErrorResponse(code=500, detail=f"Backfill failed: {str(e)}")


@router.get("/pulse/monitor/aggregation", response_model=Union[StandardResponse, ErrorResponse])
async def get_aggregation_status(
    authorized: bool = Depends(verify_manage_key),
):
    """
    W3.3 aggregation pipeline monitoring API (TH-158).

    Returns task status, last execution stats, and monitor collector status.
    """
    try:
        from ..core.aggregate_indicator.startup import get_aggregate_task_full_status
        from ..core.monitor.startup import get_hourly_task_full_status, get_daily_task_full_status

        agg_status = await get_aggregate_task_full_status()
        hourly_status = await get_hourly_task_full_status()
        daily_status = await get_daily_task_full_status()

        return StandardResponse(
            data={
                "aggregate_indicator": agg_status,
                "monitor_hourly_collector": hourly_status,
                "monitor_daily_profile": daily_status,
            }
        )
    except Exception as e:
        logging.error(f"Aggregation status query failed: {str(e)}")
        return ErrorResponse(code=500, detail=f"Aggregation status query failed: {str(e)}")


@router.get("/pulse/monitor/trends", response_model=Union[StandardResponse, ErrorResponse])
async def get_trends_and_alerts(
    authorized: bool = Depends(verify_manage_key),
):
    """
    W3.8 trends and alerts API (TH-159).

    Returns week-over-week ingestion trends and active alerts
    (provider silent, filter rate spike, aggregation stuck).
    """
    try:
        from ..core.monitor.query_service import MonitorQueryService
        service = MonitorQueryService()
        data = await service.get_trends_and_alerts()
        return StandardResponse(data=data)
    except Exception as e:
        logging.error(f"Trends query failed: {str(e)}")
        return ErrorResponse(code=500, detail=f"Trends query failed: {str(e)}")


@router.get("/pulse/monitor/sources", response_model=Union[StandardResponse, ErrorResponse])
async def get_source_status(
    authorized: bool = Depends(verify_manage_key),
):
    """
    Source-level monitoring API (TH-157 W3.6).

    Returns per-source health status: last active time, daily average,
    active/warning/stopped classification.
    """
    try:
        from ..core.monitor.query_service import MonitorQueryService
        service = MonitorQueryService()
        data = await service.get_source_status()
        return StandardResponse(data=data)
    except Exception as e:
        logging.error(f"Source status query failed: {str(e)}")
        return ErrorResponse(code=500, detail=f"Source status query failed: {str(e)}")


@router.get("/pulse/monitor/source-detail", response_model=Union[StandardResponse, ErrorResponse])
async def get_source_detail(
    source: str = Query(..., description="Source name (e.g. vital.garmin)"),
    days: int = Query(7, description="Lookback days"),
    authorized: bool = Depends(verify_manage_key),
):
    """
    Source detail API: hourly trend + indicator breakdown for a single source.
    """
    try:
        from ..core.monitor.query_service import MonitorQueryService
        service = MonitorQueryService()
        data = await service.get_source_detail(source=source, days=days)
        return StandardResponse(data=data)
    except Exception as e:
        logging.error(f"Source detail query failed: {str(e)}")
        return ErrorResponse(code=500, detail=f"Source detail query failed: {str(e)}")


@router.get("/pulse/monitor/aggregation-coverage", response_model=Union[StandardResponse, ErrorResponse])
async def get_aggregation_coverage(
    days: int = Query(90, description="Lookback days for data scan"),
    authorized: bool = Depends(verify_manage_key),
):
    """
    Aggregation coverage report (W2.8 TH-175).
    Cross-references StandardIndicator config with actual DB data.
    """
    try:
        from ..core.monitor.coverage_service import CoverageService
        service = CoverageService()
        data = await service.get_coverage_report(lookback_days=days)
        return StandardResponse(data=data)
    except Exception as e:
        logging.error(f"Coverage report failed: {str(e)}")
        return ErrorResponse(code=500, detail=f"Coverage report failed: {str(e)}")


@router.get("/pulse/monitor/derived-trend", response_model=Union[StandardResponse, ErrorResponse])
async def get_derived_trend(
    indicator: str = Query(..., description="Derived indicator name (e.g. derivedHrRange)"),
    days: int = Query(30, description="Lookback days"),
    authorized: bool = Depends(verify_manage_key),
):
    """
    Derived indicator daily production trend (W3.9 TH-179).
    Returns per-day record count from indicator_daily_profile.
    """
    try:
        from ..core.monitor.coverage_service import CoverageService
        service = CoverageService()
        data = await service.get_derived_trend(indicator=indicator, days=days)
        return StandardResponse(data=data)
    except Exception as e:
        logging.error(f"Derived trend query failed: {str(e)}")
        return ErrorResponse(code=500, detail=f"Derived trend query failed: {str(e)}")


@router.get("/pulse/monitor/user-profile", response_model=Union[StandardResponse, ErrorResponse])
async def get_user_profile(
    user_id: str = Query(..., description="User ID"),
    days: int = Query(14, description="Lookback days for density calculation"),
    authorized: bool = Depends(verify_manage_key),
):
    """
    User data profile (W3.4 TH-181).
    Returns indicator coverage, data density, and analyzability assessment.
    """
    try:
        from ..core.monitor.user_profile_service import UserProfileService
        service = UserProfileService()
        data = await service.get_user_profile(user_id=user_id, days=days)
        return StandardResponse(data=data)
    except Exception as e:
        logging.error(f"User profile query failed: {str(e)}")
        return ErrorResponse(code=500, detail=f"User profile query failed: {str(e)}")


# ===== Insight Engine =====

@router.post("/pulse/insight/run", response_model=Union[StandardResponse, ErrorResponse])
async def run_insight_engine(
    user_id: Optional[str] = Query(None, description="Specific user ID (optional, default: all demo users)"),
    target_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD (optional, default: yesterday)"),
    end_date: Optional[str] = Query(None, description="End date for simulation range (optional, single day if omitted)"),
    skip_llm: bool = Query(False, description="Skip Layer 2 LLM calls (faster for simulation)"),
    authorized: bool = Depends(verify_manage_key),
):
    """
    Run insight engine for a specific user or all demo users.
    If end_date is provided, slides day by day from target_date to end_date.
    """
    try:
        from ..core.insight.engine_task import InsightEnginePullTask
        task = InsightEnginePullTask()
        result = await task.execute(
            user_id=user_id,
            target_date_str=target_date,
            end_date_str=end_date,
            skip_llm=skip_llm,
        )
        return StandardResponse(data={
            "success": result,
            "user_id": user_id,
            "target_date": target_date,
            "end_date": end_date,
            "skip_llm": skip_llm,
        })
    except Exception as e:
        logging.error(f"Insight engine run failed: {str(e)}")
        return ErrorResponse(code=500, detail=f"Insight engine run failed: {str(e)}")


@router.post("/pulse/insight/eval", response_model=Union[StandardResponse, ErrorResponse])
async def eval_insight(
    user_id: str = Query(..., description="User ID"),
    target_date: str = Query(..., description="Target date YYYY-MM-DD"),
    recipe_name: Optional[str] = Query(None, description="Specific recipe (optional, evals all if omitted)"),
    authorized: bool = Depends(verify_manage_key),
):
    """
    Run EvalAgent on insights for a specific user+date, comparing against event.* ground truth.
    """
    try:
        from datetime import date, timedelta
        from ..core.insight.database_service import InsightDatabaseService
        from ..core.insight.eval_agent import EvalAgent

        db = InsightDatabaseService()
        agent = EvalAgent()
        td = date.fromisoformat(target_date)

        # Get insights to evaluate
        from ..core.database import execute_query
        sql = """
            SELECT id, user_id, target_date, recipe_name, recipe_version,
                   severity, observation, hypothesis, touch_message,
                   indicators_detail
            FROM user_behavior_insight
            WHERE user_id = :user_id AND target_date = :target_date
        """
        params = {"user_id": user_id, "target_date": target_date}
        if recipe_name:
            sql += " AND recipe_name = :recipe_name"
            params["recipe_name"] = recipe_name

        insights = await execute_query(sql, params, query_type="select")
        if not insights:
            return ErrorResponse(code=404, detail="No insights found for this user+date")

        # Get events within ±7 days
        events = await db.get_user_events(user_id, td - timedelta(days=7), td + timedelta(days=7))

        # Evaluate each insight
        results = []
        for insight in insights:
            eval_result = await agent.evaluate(insight, events)
            if eval_result:
                # Write score back to DB
                await db.update_benchmark_score(
                    insight["id"],
                    eval_result.get("match_score", 0),
                    eval_result,
                )
                results.append({
                    "recipe": insight["recipe_name"],
                    "eval": eval_result,
                })

        return StandardResponse(data={
            "user_id": user_id,
            "target_date": target_date,
            "insights_evaluated": len(results),
            "events_in_window": len(events),
            "results": results,
        })
    except Exception as e:
        logging.error(f"Insight eval failed: {str(e)}", exc_info=True)
        return ErrorResponse(code=500, detail=f"Insight eval failed: {str(e)}")

