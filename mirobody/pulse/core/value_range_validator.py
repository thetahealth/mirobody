"""
Value Range Validator (TH-132 W1.1)

Validates health data values against indicator-specific rules loaded from
the `indicator_valid_rules` database table at startup.

Rules are string expressions like ">0", "<=100", ">=25", "<50", "=10".
All rules for an indicator must be satisfied simultaneously (AND).

Supports multiple rule_sets for extensibility:
- ingestion_filter: W1.1 data ingestion validation (current)
- healthy_range: future healthy reference ranges
- diabetic_range: future condition-specific ranges

Out-of-range values are NOT dropped — they are marked with
task_id='filtered_out_of_range' so W3.2 statistics exclude them
while keeping the data traceable and reversible.
"""

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from ...utils import execute_query


@dataclass
class ValidationResult:
    """Result of a value range validation check."""

    is_valid: bool
    indicator: str
    value: Optional[float] = None
    reason: str = ""


class ValueRangeValidator:
    """Validates health data values against rules loaded from indicator_valid_rules table.

    Usage:
        validator = ValueRangeValidator()
        await validator.load()  # Load rules from DB at startup
        result = validator.validate("heartRates", 300000.1)
        # result.is_valid = False, result.reason = "value 300000.1 violates rule <=350"

    Passthrough cases (always valid):
    - Indicator has no rules loaded
    - Value cannot be converted to float (non-numeric)
    """

    _RULE_PATTERN = re.compile(r'^(>=|<=|>|<|=)\s*(-?[\d.]+)$')

    _OPS: Dict[str, Callable[[float, float], bool]] = {
        '>':  lambda v, t: v > t,
        '>=': lambda v, t: v >= t,
        '<':  lambda v, t: v < t,
        '<=': lambda v, t: v <= t,
        '=':  lambda v, t: v == t,
    }

    def __init__(self, rule_set: str = "ingestion_filter"):
        self._rule_set = rule_set
        # indicator_name → list of (rule_str, op_fn, threshold)
        self._rules: Dict[str, List[Tuple[str, Callable, float]]] = {}
        self._loaded = False

    async def load(self, db_config=None) -> int:
        """Load rules from indicator_valid_rules table.

        Should be called once at startup. After loading, validate() is pure
        in-memory and requires no DB access.

        Returns:
            Number of indicators loaded
        """
        query = """
            SELECT indicator, rules
            FROM indicator_valid_rules
            WHERE rule_set = :rule_set AND enabled = true
        """
        try:
            rows = await execute_query(query, {"rule_set": self._rule_set}, db_config=db_config or "")
        except Exception as e:
            logging.error(f"[ValueRangeValidator] Failed to load rules: {e}. All values will passthrough.")
            self._loaded = True
            return 0

        self._rules.clear()
        loaded = 0

        for row in rows:
            indicator = row["indicator"]
            raw_rules = row["rules"]

            # Parse JSON if needed
            if isinstance(raw_rules, str):
                try:
                    raw_rules = json.loads(raw_rules)
                except json.JSONDecodeError:
                    logging.warning(f"[ValueRangeValidator] Invalid JSON rules for {indicator}: {raw_rules}")
                    continue

            if not isinstance(raw_rules, list):
                logging.warning(f"[ValueRangeValidator] Rules for {indicator} is not a list: {raw_rules}")
                continue

            parsed = []
            for rule_str in raw_rules:
                result = self._parse_rule(rule_str)
                if result is None:
                    logging.warning(f"[ValueRangeValidator] Invalid rule '{rule_str}' for {indicator}")
                    continue
                op_str, threshold = result
                parsed.append((rule_str, self._OPS[op_str], threshold))

            if parsed:
                self._rules[indicator] = parsed
                loaded += 1

        self._loaded = True
        logging.info(f"[ValueRangeValidator] Loaded {loaded} indicators for rule_set '{self._rule_set}'")
        return loaded

    def validate(self, indicator: str, value: Any) -> ValidationResult:
        """Check if value satisfies all rules for the given indicator.

        This is a pure in-memory operation — no DB access. Rules must be
        loaded via load() before calling this method.

        Args:
            indicator: Indicator name (e.g. "heartRates")
            value: The value to validate (will be converted to float)

        Returns:
            ValidationResult with is_valid=True if valid or passthrough
        """
        parsed_rules = self._rules.get(indicator)
        if parsed_rules is None:
            return ValidationResult(is_valid=True, indicator=indicator)

        try:
            num_val = float(value)
        except (ValueError, TypeError):
            return ValidationResult(is_valid=True, indicator=indicator)

        for rule_str, op_fn, threshold in parsed_rules:
            if not op_fn(num_val, threshold):
                return ValidationResult(
                    is_valid=False,
                    indicator=indicator,
                    value=num_val,
                    reason=f"value {num_val} violates rule {rule_str}",
                )

        return ValidationResult(is_valid=True, indicator=indicator, value=num_val)

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    @property
    def indicator_count(self) -> int:
        return len(self._rules)

    def _parse_rule(self, rule: str) -> Optional[Tuple[str, float]]:
        """Parse a rule string like '>0' into ('>', 0.0)."""
        match = self._RULE_PATTERN.match(rule.strip())
        if not match:
            return None
        return match.group(1), float(match.group(2))
