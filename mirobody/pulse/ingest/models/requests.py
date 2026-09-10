"""
Health API request models
"""

from typing import Any

from pydantic import BaseModel, Field


# ==================== FormatData Input Models ====================
# Structured input for format_data(), separating pre-resolved context
# from raw vendor payload so format_data can be a pure transformation.


class FormatDataContext(BaseModel):
    """Pre-resolved context for format_data() — no DB calls needed inside."""
    theta_user_id: str | None = Field(default="", description="Internal platform user ID")
    external_user_id: str | None = Field(None, description="Vendor-side user ID (Garmin userId, Whoop numeric ID, Vital user_id)")
    user_timezone: str = Field(default="UTC", description="Pre-resolved user timezone")
    msg_id: str | None = Field(None, description="Message/request tracking ID")


class FormatDataInput(BaseModel):
    """The one argument of `Provider.format_data`: resolved context + untouched payload."""
    context: FormatDataContext
    payload: dict[str, Any] = Field(..., description="Original vendor data, untouched")


class VitalHealthRecord(BaseModel):
    """Vital health record"""

    source: str = Field(..., description="Data source")
    type: str = Field(..., description="Data type")
    timestamp: int = Field(..., description="Timestamp (milliseconds)")
    unit: str | None = Field(None, description="Unit")
    value: float = Field(..., description="Value")
    timezone: str | None = Field("UTC", description="Timezone info, e.g. America/Los_Angeles")  # Add timezone field




# ==================== StandardPulseData Series Models ====================
# Migrated from pulse/core/models.py to solve circular dependency issues


class StandardPulseMetaInfo(BaseModel):
    """Pulse standard metadata format"""

    userId: str = Field(..., description="User ID")
    requestId: str | None = Field(None, description="Request ID")
    timestamp: str | None = Field(None, description="Request timestamp")
    source: str | None = Field(None, description="Data source")
    timezone: str = Field(default="UTC", description="Timezone")
    taskId: str | None = Field(None, description="Task ID, used to identify data from the same batch")
    # Data-repair window (epoch ms); only meaningful for a repair batch. Both must be
    # present for the reconcile to sweep; otherwise the sweep is skipped.
    windowFrom: int | None = Field(None, description="Repair window start (epoch ms)")
    windowTo: int | None = Field(None, description="Repair window end (epoch ms)")


class StandardPulseRecord(BaseModel):
    """Pulse standard data record format

    The first six fields are the record shape the Vital vendor's API used,
    kept verbatim so its payloads needed no conversion. Vital is no longer a
    provider here — the installed three are Garmin, Oura and WHOOP — and the
    `VitalHealthRecord` model that documented that shape is gone with it.
    The field set stays because rows in `th_series_data` were written against
    it: that is why `value` is required and why `source` still reads
    `vital.garmin` in historical data.
    """

    source: str = Field(..., description="Data source, e.g. vital.garmin")
    type: str = Field(..., description="Data type, e.g. heartrate")
    timestamp: int = Field(..., description="Timestamp (milliseconds)")
    unit: str | None = Field(None, description="Unit")
    value: float | str = Field(..., description="Value")  # Required field, consistent with VitalHealthRecord
    timezone: str | None = Field(default="UTC", description="Timezone info, e.g. America/Los_Angeles")

    # Extended fields for complex data (VitalHealthRecord compatible, will be ignored)
    startTime: int | None = Field(None, description="Start timestamp (milliseconds)")
    endTime: int | None = Field(None, description="End timestamp (milliseconds)")

    # Extended fields for apple health
    source_id: str | None = Field(None, description="Data source ID")
    task_id: str | None = Field(None, description="Task ID")
    
    # Extended field for custom comment (e.g., meal details, food items)
    comment: str | None = Field(None, description="Custom comment to be merged with system-generated comment")


class StandardPulseData(BaseModel):
    """Pulse standard data format

    This is the unified format that all Platform internal Event Providers should return,
    replacing the original Dict[str, Any] to provide type safety and standardization
    """

    metaInfo: StandardPulseMetaInfo = Field(..., description="Meta information")
    healthData: list[StandardPulseRecord] = Field(..., description="Health data record list")

    # Optional batch information
    batchInfo: dict[str, Any] | None = Field(None, description="Batch processing information")
    processingInfo: dict[str, Any] | None = Field(None, description="Processing status information")
