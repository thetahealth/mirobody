"""Locale plugins for enriching sibling groups with local drug/vaccine names.

To add a new locale, create a .py file in this directory with a class that
subclasses LocalePlugin and implements from_args(). The plugin will be
auto-discovered at build time.
"""

from __future__ import annotations

import importlib
import logging
import os
import pkgutil
from abc import ABC, abstractmethod
from argparse import Namespace

log = logging.getLogger(__name__)


class LocalePlugin(ABC):
    """Interface for country/region-specific drug and vaccine name enrichment.

    Each locale provides local names keyed by ATC code (drugs) or CVX code
    (vaccines).  The sibling builder calls these methods to enrich group
    names with multilingual labels.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier for this locale (e.g. 'cn', 'jp')."""

    @classmethod
    @abstractmethod
    def from_args(cls, args: Namespace) -> "LocalePlugin | None":
        """Try to construct this locale from CLI args.

        Return an instance if the required data is available, or None to skip.
        """

    @abstractmethod
    def drug_names(self) -> dict[str, set[str]]:
        """Return {ATC code prefix -> set of local drug names}.

        The ATC code can be any length (e.g. 'A10BA' for a subgroup).
        Names are matched to sibling groups by prefix.
        """

    def vaccine_names(self) -> dict[str, str]:
        """Return {CVX code -> local vaccine name}.

        Optional — return empty dict if not applicable.
        """
        return {}


def discover_locales(args: Namespace) -> list[LocalePlugin]:
    """Scan this package for LocalePlugin subclasses and instantiate active ones."""
    locales: list[LocalePlugin] = []
    package_dir = os.path.dirname(__file__)

    for finder, module_name, _ in pkgutil.iter_modules([package_dir]):
        if module_name.startswith("_"):
            continue
        module = importlib.import_module(f".{module_name}", __package__)
        for attr in dir(module):
            obj = getattr(module, attr)
            if (
                isinstance(obj, type)
                and issubclass(obj, LocalePlugin)
                and obj is not LocalePlugin
            ):
                instance = obj.from_args(args)
                if instance is not None:
                    log.info(f"  Locale plugin loaded: {instance.name}")
                    locales.append(instance)

    return locales
