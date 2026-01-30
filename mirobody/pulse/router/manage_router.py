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
