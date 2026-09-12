"""Every claim the shipped skills make about the resolver, checked against it.

``skills/`` holds skills for OTHER agents — Claude Code, Codex, OpenClaw — as
opposed to ``mirobody/agent/skills/``, which the shipped agent reads through its
own virtual filesystem. The difference matters here: a skill in this directory
is copied into someone else's harness and runs against nothing but
``pip install mirobody``, so every code, term and command it prints is a promise
made to a stranger with no way to check it.

``dont-guess-my-labs`` is a skill whose entire subject is the resolver refusing
to guess. Its reference tables name 40+ terms and their codes. Those tables are
quoted in two files and derived in none, so nothing short of a gate keeps them
honest — the same reasoning as ``test_readme_numbers.py``, applied to prose that
ships outside the wheel.

Section 4 of ``reference.md`` is the unusual one: it documents terms that
resolve WRONGLY today, so the assertions below pin current wrong answers. When
the resolver is fixed these turn red, which is the point — the fix and the
sentence describing it land in the same commit.

Why this file is inside ``mirobody/`` and not the root ``tests/``: it is
evidence for a public claim, the same category as ``test_engine_coverage.py``
and the README gates. ``/tests/`` is gitignored on purpose (see the note at
that rule), so a gate placed there would never reach a checkout, a reviewer or
CI — and the claims it guards are printed in files a stranger copies into their
own agent. ``scripts/build_backend.py`` prunes every ``test_*`` from the wheel,
so being in the package costs an installer nothing.
"""

from __future__ import annotations

import pathlib
import re

import pytest

from mirobody.engine import resolve, resolve_reading

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_SKILLS = _ROOT / "skills"
_LABS = _SKILLS / "dont-guess-my-labs"


def _text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# Packaging: these files must NOT reach the wheel.
# --------------------------------------------------------------------------


def test_skills_dir_is_outside_the_package() -> None:
    """The README promises `pip install mirobody` is 2 packages / 52 MB.

    `[tool.setuptools.packages.find]` includes `mirobody*` only, so a skill at
    the repo root cannot be swept in. Asserted rather than assumed because the
    natural place for a contributor to put the next skill is
    `mirobody/skills/`, where it WOULD ship.
    """
    assert _SKILLS.is_dir()
    assert not (_ROOT / "mirobody" / "skills").exists()


# --------------------------------------------------------------------------
# Frontmatter: what a harness reads to decide whether to load the skill.
# --------------------------------------------------------------------------


def _frontmatter(skill_md: pathlib.Path) -> dict[str, str]:
    """Parse the top-level scalar keys of a SKILL.md's frontmatter.

    Hand-rolled rather than `yaml.safe_load` because neither pyyaml nor
    ruamel is in `[test]` — importorskip would make this gate vanish on the
    install CONTRIBUTING documents, and a gate nobody runs is an assertion.
    Skill frontmatter is `key: value` with one nested `metadata:` block, so
    top-level scalars are all that is needed and all that is parsed.
    """
    match = re.match(r"^---\n(.*?)\n---\n", _text(skill_md), re.S)
    assert match, f"{skill_md.parent.name}: no YAML frontmatter"

    meta: dict[str, str] = {}
    for line in match.group(1).splitlines():
        if not line or line.startswith((" ", "\t", "#")):
            continue  # a nested block (metadata:) or a comment
        key, sep, value = line.partition(":")
        if sep:
            meta[key.strip()] = value.strip()
    return meta


@pytest.mark.parametrize("skill_dir", sorted(p for p in _SKILLS.iterdir() if p.is_dir()))
def test_skill_frontmatter(skill_dir: pathlib.Path) -> None:
    """A harness matches on `description`; a missing one loads nothing."""
    skill_md = skill_dir / "SKILL.md"
    assert skill_md.is_file(), f"{skill_dir.name} has no SKILL.md"

    meta = _frontmatter(skill_md)

    assert meta["name"] == skill_dir.name, "name must equal the directory name"
    assert meta["license"] == "Apache-2.0"
    # Long enough to say when to fire, short enough to stay in a system prompt.
    assert 80 <= len(meta["description"]) <= 500


# --------------------------------------------------------------------------
# reference.md §1 — the unit decides the code.
# --------------------------------------------------------------------------

# The percentage/count pair is the most common misreading of a CBC, and the
# mass/moles pair the most common misreading of a lipid panel. Both are the
# reason the skill tells the agent to pass the unit.
_READING_CODES = [
    ("中性粒细胞", "62", "%", "26511-6"),
    ("中性粒细胞", "4.2", "10*9/L", "26499-4"),
    ("淋巴细胞", "30", "%", "26478-8"),
    ("淋巴细胞", "1.8", "10*9/L", "26474-7"),
    ("total cholesterol", "5.0", "mmol/L", "14647-2"),
    ("total cholesterol", "193", "mg/dL", "2093-3"),
    ("triglycerides", "1.7", "mmol/L", "14927-8"),
    ("triglycerides", "150", "mg/dL", "2571-8"),
    ("creatinine", "88", "umol/L", "14682-9"),
    ("creatinine", "1.0", "mg/dL", "2160-0"),
]


@pytest.mark.parametrize(("term", "value", "unit", "loinc"), _READING_CODES)
def test_unit_selects_the_code(term: str, value: str, unit: str, loinc: str) -> None:
    assert resolve_reading(term, value, unit).loinc == loinc


def test_same_name_two_codes() -> None:
    """The claim in one line: identical name, different unit, different test."""
    pct = resolve_reading("中性粒细胞", "62", "%").loinc
    count = resolve_reading("中性粒细胞", "4.2", "10*9/L").loinc
    assert pct != count


# --------------------------------------------------------------------------
# reference.md §2 — adjacent names that are not the same test.
# --------------------------------------------------------------------------

_ADJACENT_CODES = [
    ("HDL", "2085-9"),
    ("高密度脂蛋白", "2085-9"),
    ("LDL", "13457-7"),
    ("低密度脂蛋白", "13457-7"),
    ("non-HDL cholesterol", "43396-1"),
    ("Cholesterol/HDL Ratio", "9830-1"),
    ("total bilirubin", "1975-2"),
    ("direct bilirubin", "15152-2"),
    ("indirect bilirubin", "1971-1"),
    ("free T4", "3024-7"),
    ("游离甲状腺素", "3024-7"),
    ("total T4", "3026-2"),
    ("TSH", "3016-3"),
]


@pytest.mark.parametrize(("term", "loinc"), _ADJACENT_CODES)
def test_adjacent_names_resolve_distinctly(term: str, loinc: str) -> None:
    assert resolve(term).loinc == loinc


def test_hdl_ldl_and_non_hdl_are_three_codes() -> None:
    """Six override rows exist because these once collapsed onto each other —
    `HDL` answered with "Cholesterol non HDL", inverting the reading. See
    docs/roadmap.md."""
    codes = {resolve(t).loinc for t in ("HDL", "LDL", "non-HDL cholesterol")}
    assert len(codes) == 3


# --------------------------------------------------------------------------
# reference.md §3 — category headings are not results.
# --------------------------------------------------------------------------

# The refusal these skills are built around. `血脂` is the example the README
# and every piece of outbound writing uses, so it is the one that must not rot.
_ABSTAINS = [
    "血脂",
    "三大常规",
    "甲功",
    "心肌酶",
    "生化全套",
    "凝血功能",
    "免疫球蛋白",
    "微量元素",
    "激素六项",
]


@pytest.mark.parametrize("term", _ABSTAINS)
def test_category_word_abstains(term: str) -> None:
    result = resolve(term)
    assert not result.resolved, f"{term} now resolves to {result.loinc} ({result.canonical})"


# A heading with a real LOINC panel code is correct, not a defect. The skill
# tells the agent to treat `panel` in the canonical name as "this is a heading".
_PANEL_CODES = [
    ("肝功能", "24325-3"),
    ("肾功能", "24362-6"),
    ("血常规", "57021-8"),
    ("血压", "85354-9"),
]


@pytest.mark.parametrize(("term", "loinc"), _PANEL_CODES)
def test_panel_heading_resolves_to_a_panel(term: str, loinc: str) -> None:
    result = resolve(term)
    assert result.loinc == loinc
    assert "panel" in result.canonical.lower(), (
        f"{term} resolves to {result.canonical!r}, which is not a panel — "
        "reference.md §3 lists it as a legitimate panel code"
    )


# --------------------------------------------------------------------------
# reference.md §4 — KNOWN DEFECTS. These pin WRONG answers on purpose.
# --------------------------------------------------------------------------

# Each of these is a category word that lands on a specific code. When one is
# fixed this test fails, and the failure is the reminder that reference.md §4,
# SKILL.md §6 and internal/plans/resolver-category-word-defects.md all name it.
# Delete the row and the prose together.
_KNOWN_BAD = [
    ("维生素", "96450-2"),
    ("尿常规", "19159-3"),
    ("肿瘤标志物", "53959-3"),
    ("电解质", "19096-7"),
]


@pytest.mark.parametrize(("term", "loinc"), _KNOWN_BAD)
def test_known_bad_resolution_still_reproduces(term: str, loinc: str) -> None:
    result = resolve(term)
    assert result.resolved and result.loinc == loinc, (
        f"{term} no longer resolves to {loinc} — if it was FIXED, drop this row "
        "and update reference.md §4, SKILL.md §6 and the defects plan"
    )


def test_known_bad_terms_are_documented() -> None:
    """A defect that is pinned in code but absent from the prose is a trap for
    the next reader."""
    reference = _text(_LABS / "reference.md")
    for term, loinc in _KNOWN_BAD:
        assert term in reference, f"{term} pinned in tests but missing from reference.md"
        assert loinc in reference, f"{loinc} pinned in tests but missing from reference.md"


# --------------------------------------------------------------------------
# reference.md §5 — surface shapes the extractor produces.
# --------------------------------------------------------------------------

_SURFACES = [
    ("Total Cholesterol-TC", "2093-3"),  # Name-ABBREV, hyphen-stripped variant
    ("空腹血糖(GLU)", "1558-6"),  # name with a parenthesised abbreviation
    ("血紅素", "718-7"),  # Traditional Chinese, folded to Simplified
    ("血红蛋白", "718-7"),
    ("ヘモグロビン", "718-7"),
    ("hemoglobin", "718-7"),
]


@pytest.mark.parametrize(("term", "loinc"), _SURFACES)
def test_surface_shapes(term: str, loinc: str) -> None:
    assert resolve(term).loinc == loinc


def test_four_languages_one_code() -> None:
    """The headline claim of the README, the skill README and every piece of
    outbound writing."""
    codes = {resolve(t).loinc for t in ("hemoglobin", "血红蛋白", "血紅素", "ヘモグロビン")}
    assert codes == {"718-7"}


# --------------------------------------------------------------------------
# reference.md §6 — the shipped demo report, end to end.
# --------------------------------------------------------------------------

# The file the README tells a new user to upload. It went from 12 readings /
# 0 resolved to 8/12 (docs/roadmap.md); these are the eight.
_DEMO_RESOLVED = [
    ("Total Cholesterol-TC", "2093-3"),
    ("Blood Glucose", "2339-0"),
    ("Cholesterol/HDL Ratio", "9830-1"),
    ("LDL/HDL Ratio", "11054-4"),
    ("Lipid-Free Fatty Acids", "15066-4"),
    ("Lipid-Phospholipids", "2568-4"),
    ("Non-HDL Cholesterol-Non-HDL", "43396-1"),
    ("Lipid-Low-Density Lipoprotein Calculated", "13457-7"),
]

# The four the bundle has no key for. Closing one means a curated alias and a
# bundle rebuild, not an override row — so a resolution appearing here is news.
_DEMO_REFUSED = [
    "Postprandial Blood Glucose-PBG",
    "Lipid-Oxidized Low-Density Lipoprotein",
    "Lipid-Small Dense Low-Density Lipoprotein Cholesterol",
    "Lipid-Low-Density Lipoprotein Particle Number",
]


@pytest.mark.parametrize(("term", "loinc"), _DEMO_RESOLVED)
def test_demo_report_resolved(term: str, loinc: str) -> None:
    assert resolve(term).loinc == loinc


@pytest.mark.parametrize("term", _DEMO_REFUSED)
def test_demo_report_refused(term: str) -> None:
    """LDL-P is the load-bearing one: the nearest reachable code, 43727-7, is
    `Lipoprotein.beta.subparticle.small` — a different measurement. Abstaining
    beats the near-miss, and that is the sentence the skill is built on."""
    result = resolve(term)
    assert not result.resolved, (
        f"{term} now resolves to {result.loinc} ({result.canonical}) — if a "
        "bundle rebuild added it, move the row to _DEMO_RESOLVED and update "
        "reference.md §6 and docs/roadmap.md"
    )


def test_demo_report_score_is_eight_of_twelve() -> None:
    """The figure reference.md §6 prints. Quoted in prose, derived nowhere."""
    terms = [t for t, _ in _DEMO_RESOLVED] + _DEMO_REFUSED
    assert len(terms) == 12
    assert sum(resolve(t).resolved for t in terms) == 8


def test_demo_report_file_exists() -> None:
    """reference.md §6 links it by path."""
    assert (_ROOT / "demo" / "lab_report_2025-10-15.pdf").is_file()


# --------------------------------------------------------------------------
# The documents themselves.
# --------------------------------------------------------------------------


def test_skill_docs_quote_a_real_refusal() -> None:
    """Both documents lead with `血脂` returning nothing. If the resolver ever
    learns the term, the pitch is wrong before the tables are."""
    assert not resolve("血脂").resolved
    for name in ("SKILL.md", "README.md", "reference.md"):
        assert "血脂" in _text(_LABS / name)


def test_no_loinc_code_is_invented() -> None:
    """Every `LOINC nnnnn-n` and backticked `nnnnn-n` in the skill's prose must
    be a code the resolver actually produces for some term named in the tests.

    Catches the failure mode this whole file exists for: a plausible code typed
    from memory into a table nobody re-ran.
    """
    known = {
        loinc
        for _, loinc in _ADJACENT_CODES + _PANEL_CODES + _KNOWN_BAD + _SURFACES + _DEMO_RESOLVED
    }
    known |= {loinc for *_, loinc in _READING_CODES}
    # Named in reference.md §6 as the near-miss the resolver declines to make
    # for LDL-P. It is quoted precisely because nothing resolves to it here.
    known.add("43727-7")

    for name in ("SKILL.md", "README.md", "reference.md"):
        body = _text(_LABS / name)
        for code in set(re.findall(r"`(\d{1,5}-\d)`|LOINC (\d{1,5}-\d)", body)):
            quoted = code[0] or code[1]
            assert quoted in known, (
                f"{name} quotes LOINC {quoted}, which no test in this file derives"
            )
