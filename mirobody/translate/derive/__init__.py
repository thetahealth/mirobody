"""Quantities that are computed, not measured.

Sleep efficiency is total sleep over time in bed. Heart-rate range is the day's
max minus its min. Nothing wore a sensor for either: they are two stored
numbers and an arithmetic rule.

It sits in ② Translate beside `aggregate/`, and is not translation proper:
translate answers what a value MEANS, and a daily total is the same quantity
on a different time axis, a LOINC axis change, while a ratio of two different
components is a NEW quantity. It is not ① Collect either: nothing here
collects. It runs on a schedule over what is already stored and writes its
results back beside them, through the same writer as an aggregation pass.

    rules.py  the rule table and the runner
    task.py   the scheduled job

Lazy (PEP 562): importing this must not pull the server stack.
"""

from typing import TYPE_CHECKING

_EXPORTS = {
    "DerivedAggregator": "rules",
    "DerivedRule": "rules",
    "DERIVED_RULES": "rules",
    "DerivedCalculationTask": "task",
    "start_derived_scheduler": "task",
}
__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .rules import DERIVED_RULES, DerivedAggregator, DerivedRule
    from .task import DerivedCalculationTask, start_derived_scheduler


def __getattr__(name: str):
    import importlib

    where = _EXPORTS.get(name)
    if where is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(f".{where}", __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
