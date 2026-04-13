"""
FormatTestRunner — Reusable testing framework for Provider.format_data()

Loads fixture JSON files, instantiates providers without DB dependencies,
applies mocks, calls format_data(), and validates output with three-layer checks:
  1. Rule assertions (success, health_data_count, required_indicators)
  2. Value checks (hand-calculated expected values for key fields)
  3. Snapshot comparison (exact match minus dynamic fields)
"""

import importlib
import json
import logging
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

logger = logging.getLogger(__name__)

# Fields in processingInfo that change every run
DYNAMIC_FIELDS = frozenset({
    "requestId",
    "timestamp",
    "start_time",
    "end_time",
    "processing_duration_ms",
    "success_rate",
})


@dataclass
class TestCase:
    """A single test case loaded from a fixture JSON."""
    test_id: str
    description: str
    provider_class: str
    platform: str
    mock_context: Dict[str, Any]
    input_data: Dict[str, Any]
    expected: Dict[str, Any]
    fixture_path: Path
    patch_targets: List[Any] = field(default_factory=list)
    init_kwargs: Dict[str, Any] = field(default_factory=dict)
    context: Optional[Dict[str, Any]] = None


@dataclass
class TestResult:
    """Result of running a single test case."""
    passed: bool = True
    errors: List[str] = field(default_factory=list)

    @property
    def error_report(self) -> str:
        return "\n".join(self.errors) if self.errors else "OK"


class FormatTestRunner:
    """Loads fixtures, runs format_data(), validates output."""

    def __init__(self, fixtures_dir: Path):
        self._fixtures_dir = fixtures_dir
        self._cases: List[TestCase] = []
        self._load_fixtures()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def cases(self) -> List[TestCase]:
        return self._cases

    @property
    def case_ids(self) -> List[str]:
        return [c.test_id for c in self._cases]

    async def run(self, case: TestCase) -> TestResult:
        """Import provider, apply mocks, call format_data, validate."""
        result = TestResult()

        # 1. Import & instantiate
        cls = self._import_provider_class(case.provider_class)
        instance = self._instantiate_provider(cls, case)

        # 2. Call format_data_v2 (with context) or legacy format_data
        try:
            output = await self._call_format(instance, case)
        except Exception as exc:
            if case.expected.get("success", True):
                result.passed = False
                result.errors.append(f"format_data raised {type(exc).__name__}: {exc}")
            return result

        # 3. Validate
        if case.expected.get("success", True):
            self._validate(output, case.expected, result)
        else:
            # Expected failure but didn't raise — check empty output
            if output.healthData:
                result.passed = False
                result.errors.append(
                    f"Expected failure but got {len(output.healthData)} records"
                )

        return result

    def update_snapshot(self, case: TestCase, output_data: Dict[str, Any]) -> None:
        """Write format_data output into fixture's expected.snapshot."""
        normalized = self._normalize_for_comparison(output_data)

        # Re-read fixture file, update snapshot, write back
        with open(case.fixture_path, "r", encoding="utf-8") as f:
            fixture = json.load(f)

        fixture["expected"]["snapshot"] = normalized

        with open(case.fixture_path, "w", encoding="utf-8") as f:
            json.dump(fixture, f, indent=2, ensure_ascii=False)
            f.write("\n")

    async def run_and_update_snapshot(self, case: TestCase) -> TestResult:
        """Run format_data and overwrite the snapshot in the fixture file."""
        result = TestResult()

        cls = self._import_provider_class(case.provider_class)
        instance = self._instantiate_provider(cls, case)

        try:
            output = await self._call_format(instance, case)
        except Exception as exc:
            result.passed = False
            result.errors.append(f"format_data raised {type(exc).__name__}: {exc}")
            return result

        output_dict = output.model_dump()
        self.update_snapshot(case, output_dict)
        logger.info(f"Updated snapshot for {case.test_id}")
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _call_format(self, instance: Any, case: TestCase) -> Any:
        """Call format_data_v2 (if context provided) or legacy format_data."""
        if case.context is not None:
            from mirobody.pulse.data_upload.models.requests import (
                FormatDataContext,
                FormatDataInput,
            )
            ctx = FormatDataContext(**case.context)
            fmt_input = FormatDataInput(context=ctx, payload=case.input_data)
            return await instance.format_data_v2(fmt_input)
        else:
            input_data = self._prepare_input(case)
            return await instance.format_data(input_data)

    def _load_fixtures(self) -> None:
        """Recursively scan fixtures_dir for *.json files."""
        if not self._fixtures_dir.exists():
            return
        for path in sorted(self._fixtures_dir.rglob("*.json")):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                case = TestCase(
                    test_id=data["test_id"],
                    description=data.get("description", ""),
                    provider_class=data["provider_class"],
                    platform=data.get("platform", ""),
                    mock_context=data.get("mock_context", {}),
                    input_data=data["input"],
                    expected=data.get("expected", {}),
                    fixture_path=path,
                    patch_targets=data.get("patch_targets", []),
                    init_kwargs=data.get("init_kwargs", {}),
                    context=data.get("context"),
                )
                self._cases.append(case)
            except (json.JSONDecodeError, KeyError) as exc:
                logger.warning(f"Skipping invalid fixture {path}: {exc}")

    @staticmethod
    def _import_provider_class(dotted_path: str) -> type:
        """Import 'mirobody.pulse.theta...ThetaGarminProvider' dynamically."""
        module_path, class_name = dotted_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)

    @staticmethod
    def _instantiate_provider(cls: type, case: TestCase) -> Any:
        """Instantiate provider with fixture-declared patches and init args.

        Fully data-driven — no platform-specific branching.  Each fixture
        declares patch_targets (modules to mock) and init_kwargs (constructor
        args, where "__mock__" is replaced with MagicMock()).
        """
        patches = []
        for target in case.patch_targets:
            if isinstance(target, str):
                patches.append(patch(target, MagicMock))
            else:
                patches.append(
                    patch(target["target"], return_value=target.get("return_value"))
                )

        init_kwargs = {
            k: MagicMock() if v == "__mock__" else v
            for k, v in case.init_kwargs.items()
        }

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            instance = cls(**init_kwargs)

        _apply_mocks(instance, case.mock_context)
        return instance

    @staticmethod
    def _prepare_input(case: TestCase) -> Dict[str, Any]:
        """Return input dict, converting Pydantic models where needed.

        Apple Health format_data() expects meta_info as a MetaInfo object
        (attribute access), so we convert the dict from fixtures.
        """
        data = case.input_data
        if case.platform == "apple":
            from mirobody.pulse.apple.models import MetaInfo
            meta = data.get("meta_info")
            if isinstance(meta, dict):
                data = {**data, "meta_info": MetaInfo(**meta)}
        return data

    def _validate(
        self,
        output: Any,
        expected: Dict[str, Any],
        result: TestResult,
    ) -> None:
        """Two-layer validation: rules then snapshot."""
        # --- Layer 1: Rule assertions ---
        health_data = output.healthData

        # health_data_count
        expected_count = expected.get("health_data_count")
        if expected_count is not None and len(health_data) != expected_count:
            result.passed = False
            result.errors.append(
                f"health_data_count: expected {expected_count}, got {len(health_data)}"
            )

        # required_indicators
        required = expected.get("required_indicators", [])
        if required:
            actual_types = {r.type for r in health_data}
            missing = set(required) - actual_types
            if missing:
                result.passed = False
                result.errors.append(
                    f"Missing required indicators: {sorted(missing)}"
                )

        # --- Layer 2: Explicit value checks (hand-calculated assertions) ---
        value_checks = expected.get("value_checks", [])
        if value_checks:
            records_as_dicts = [
                r.model_dump() if hasattr(r, "model_dump") else r
                for r in health_data
            ]
            for check in value_checks:
                idx = check["index"]
                fld = check["field"]
                exp_val = check["expected"]
                if idx >= len(records_as_dicts):
                    result.passed = False
                    result.errors.append(
                        f"value_check: index {idx} out of range "
                        f"({len(records_as_dicts)} records)"
                    )
                    continue
                actual_val = records_as_dicts[idx].get(fld)
                if actual_val != exp_val:
                    result.passed = False
                    result.errors.append(
                        f"value_check: healthData[{idx}].{fld} — "
                        f"expected {exp_val!r}, got {actual_val!r}"
                    )

        # --- Layer 3: Snapshot comparison ---
        snapshot = expected.get("snapshot")
        if snapshot is not None:
            output_dict = output.model_dump()
            normalized_output = self._normalize_for_comparison(output_dict)
            diffs = _deep_diff(snapshot, normalized_output, path="root")
            if diffs:
                result.passed = False
                result.errors.append(
                    f"Snapshot mismatch ({len(diffs)} diff(s)):\n"
                    + "\n".join(f"  {d}" for d in diffs[:20])
                )

    @staticmethod
    def _normalize_for_comparison(data: Dict[str, Any]) -> Dict[str, Any]:
        """Remove dynamic fields that change per-run."""
        # Fields in healthData records that may use time.time() as fallback
        HEALTH_RECORD_DYNAMIC = frozenset({"timestamp"})

        out = {}
        for key, value in data.items():
            if key == "metaInfo":
                meta = {k: v for k, v in value.items() if k not in DYNAMIC_FIELDS}
                out["metaInfo"] = meta
            elif key == "processingInfo":
                if value is not None:
                    pi = {k: v for k, v in value.items() if k not in DYNAMIC_FIELDS}
                    out["processingInfo"] = pi
                else:
                    out["processingInfo"] = None
            elif key == "healthData":
                # Strip timestamp from individual records (some use time.time())
                out["healthData"] = [
                    {k: v for k, v in record.items() if k not in HEALTH_RECORD_DYNAMIC}
                    for record in value
                ]
            else:
                out[key] = value
        return out


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _apply_mocks(instance: Any, mock_context: Dict[str, Any]) -> None:
    """Patch methods on an instance with AsyncMock return values."""
    for method_name, return_value in mock_context.items():
        mock = AsyncMock(return_value=return_value)
        setattr(instance, method_name, mock)


def _deep_diff(
    expected: Any, actual: Any, path: str = ""
) -> List[str]:
    """Recursively compare two data structures, return list of differences."""
    diffs: List[str] = []

    if type(expected) != type(actual):
        diffs.append(f"{path}: type mismatch — expected {type(expected).__name__}, got {type(actual).__name__}")
        return diffs

    if isinstance(expected, dict):
        all_keys = set(expected.keys()) | set(actual.keys())
        for key in sorted(all_keys):
            child_path = f"{path}.{key}"
            if key not in expected:
                diffs.append(f"{child_path}: unexpected key in actual")
            elif key not in actual:
                diffs.append(f"{child_path}: missing key in actual")
            else:
                diffs.extend(_deep_diff(expected[key], actual[key], child_path))
    elif isinstance(expected, list):
        if len(expected) != len(actual):
            diffs.append(f"{path}: list length mismatch — expected {len(expected)}, got {len(actual)}")
        for i, (e, a) in enumerate(zip(expected, actual)):
            diffs.extend(_deep_diff(e, a, f"{path}[{i}]"))
    else:
        if expected != actual:
            exp_str = repr(expected)[:80]
            act_str = repr(actual)[:80]
            diffs.append(f"{path}: {exp_str} != {act_str}")

    return diffs
