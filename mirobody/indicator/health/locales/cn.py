"""China locale: NHSA drug catalog for Chinese drug names."""

from __future__ import annotations

import json
import logging
import os
import re
from argparse import Namespace
from collections import defaultdict

from . import LocalePlugin

log = logging.getLogger(__name__)


def _nhsa_code_to_atc(nhsa_code: str) -> str:
    """Convert NHSA category code to ATC by stripping X/Z prefix."""
    if nhsa_code and nhsa_code[0] in ("X", "Z"):
        return nhsa_code[1:]
    return nhsa_code


class ChinaLocale(LocalePlugin):
    """Enriches sibling groups with Chinese drug names from NHSA catalog."""

    def __init__(self, nhsa_catalog_path: str):
        self._nhsa_path = nhsa_catalog_path

    @property
    def name(self) -> str:
        return "cn"

    @classmethod
    def from_args(cls, args: Namespace) -> "ChinaLocale | None":
        path = getattr(args, "nhsa_catalog", "")
        if path and os.path.isfile(path):
            return cls(nhsa_catalog_path=path)
        return None

    def drug_names(self) -> dict[str, set[str]]:
        if not self._nhsa_path or not os.path.isfile(self._nhsa_path):
            return {}

        log.info(f"  Loading NHSA catalog: {self._nhsa_path}")
        with open(self._nhsa_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        atc_cn: dict[str, set[str]] = defaultdict(set)
        n_drugs = 0
        for section in ("西药部分", "协议西药", "中成药部分", "协议中成药"):
            if section not in data:
                continue
            for med in data[section].get("medicines", []):
                codes = med.get("all_category_codes", [])
                if not codes:
                    continue
                atc_group = _nhsa_code_to_atc(codes[-1])
                name_cn = re.sub(r"\s+", "", med.get("name", ""))
                if atc_group and name_cn:
                    atc_cn[atc_group].add(name_cn)
                    n_drugs += 1

        log.info(f"  NHSA drugs: {n_drugs:,} entries, {len(atc_cn):,} ATC groups with Chinese names")
        return atc_cn
