"""Apple Health request models.

The wire vocabulary is Apple's own: a record's ``type`` is a HealthKit
identifier (``HKQuantityTypeIdentifierHeartRate``) and its span is
``startDate``/``endDate``, which is what `export.xml` writes and what
`mirobody.kernel.decoders.apple` decodes. There used to be a second
vocabulary here, a `FlutterHealthTypeEnum` of names like ``HEART_RATE`` with a
74-entry table mapping it onto the same catalogue the decoder already targets.
It described one client that no longer exists, and two tables for one mapping
is how they drift.
"""

from typing import Any
from pydantic import BaseModel, Field, field_validator


class MetaInfo(BaseModel):
    """Request metadata"""

    # userId: str = Field(..., description="User ID")
    timezone: str = Field(default="UTC", description="timezone")
    taskId: str | None = Field(None, description="task id")
    directly_from_watch: bool | None = Field(False, description="Whether the data is directly from watch")
    # Data-repair window (epoch ms). For a repair batch (taskId starts with "repair-"),
    # the mark-and-sweep reconcile deletes stale rows ONLY within [windowFrom, windowTo].
    # If either is missing, the sweep is skipped (the upload is still upserted).
    windowFrom: int | None = Field(None, description="Repair window start (epoch ms)")
    windowTo: int | None = Field(None, description="Repair window end (epoch ms)")


class AppleHealthRecord(BaseModel):
    """One HealthKit sample, in Apple's own field names.

    ``value`` is a number for a quantity type and the ``HKCategoryValue*`` name
    for a category one; ``systolic``/``diastolic`` carry a blood pressure
    correlation. ``unit`` is per record, because Apple writes it per record.
    """

    type: str = Field(..., description="HealthKit identifier, e.g. HKQuantityTypeIdentifierHeartRate")
    startDate: int | str | None = Field(None, description="Sample start: epoch ms or Apple's date string")
    endDate: int | str | None = Field(None, description="Sample end: epoch ms or Apple's date string")
    value: float | str | None = Field(None, description="Quantity value, or the HKCategoryValue* name")
    unit: str | None = Field(None, description="Unit as Apple wrote it, e.g. count/min")
    systolic: float | None = Field(None, description="Blood pressure correlation, systolic")
    diastolic: float | None = Field(None, description="Blood pressure correlation, diastolic")

    uuid: str | None = Field(None, description="Unique record identifier")
    sourceId: str | None = Field(None, description="Data source ID")
    sourceName: str | None = Field(None, description="Data source name, e.g. Apple Watch")
    sourcePlatform: str | None = Field(None, description="Data source platform")
    sourceDeviceId: str | None = Field(None, description="Device ID")
    # No default: a default of "UTC" was always set, so `metaInfo.timezone`
    # was never read and every record without its own zone landed in UTC.
    timezone: str | None = Field(None, description="Timezone for this record (overrides metaInfo)")
    recordingMethod: str | None = Field(None, description="Recording method")
    createdAt: int | None = Field(None, description="Creation timestamp (milliseconds)")

    def sample(self) -> dict[str, Any]:
        """The record as `decoders.apple.decode` reads it."""
        return {
            "startDate": self.startDate,
            "endDate": self.endDate,
            "value": self.value,
            "unit": self.unit,
            "sourceName": self.sourceName,
            "systolic": self.systolic,
            "diastolic": self.diastolic,
        }


class AppleHealthRequest(BaseModel):
    """Apple Health data request"""

    request_id: str | None = Field(None, description="Request ID")
    metaInfo: MetaInfo = Field(..., description="Metadata information")
    healthData: list[AppleHealthRecord] = Field(..., description="Health data records")


# ==================== Statistics Models ====================


class StatisticsMetaInfo(BaseModel):
    """Metadata for statistics request"""

    userId: str | None = Field(None, description="User ID (ignored, extracted from token)")
    timezone: str = Field(default="UTC", description="Default timezone for statistics")


class AppleHealthStatistic(BaseModel):
    """A single Apple Health statistic record (pre-aggregated by client)"""

    type: str = Field(..., description="HealthKit identifier, e.g. HKQuantityTypeIdentifierStepCount")
    dateFrom: int = Field(..., description="Start timestamp in milliseconds")
    dateTo: int = Field(..., description="End timestamp in milliseconds")
    timezone: str | None = Field(None, description="Timezone for this statistic (overrides metaInfo)")
    grouping: str = Field(..., description="Time grouping: hour, day, week, month")
    sum: float | None = Field(None, description="Sum/total value")
    average: float | None = Field(None, description="Average value")
    minimum: float | None = Field(None, description="Minimum value")
    maximum: float | None = Field(None, description="Maximum value")
    mostRecent: float | None = Field(None, description="Most recent value")
    unit: str | None = Field(None, description="Unit type (e.g., COUNT, BEATS_PER_MINUTE)")
    unitSymbol: str | None = Field(None, description="Unit symbol (e.g., count, bpm)")

    @field_validator("grouping")
    @classmethod
    def validate_grouping(cls, v: str) -> str:
        """Validate grouping is one of the supported values"""
        allowed = {"hour", "day", "week", "month"}
        if v not in allowed:
            raise ValueError(f"Invalid grouping: {v!r}, expected one of {allowed}")
        return v


class AppleHealthStatisticsRequest(BaseModel):
    """Apple Health statistics request"""

    metaInfo: StatisticsMetaInfo = Field(..., description="Metadata information")
    statistics: list[AppleHealthStatistic] = Field(..., description="Statistics records")
