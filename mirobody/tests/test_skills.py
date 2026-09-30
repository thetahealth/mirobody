"""Every claim the shipped skills make, checked against what they describe.

``skills/`` holds skills for someone else's agent (Claude Code, Codex, Cursor,
Gemini CLI, OpenClaw). A skill is copied into a stranger's harness and runs
against nothing but ``pip install mirobody`` or a Docker stack, so every code,
command, service name and number its prose prints is a promise made to someone
with no way to check it. The tables are quoted in prose and derived nowhere,
which is the same reason ``test_engine_coverage.py`` exists for the README's
score: nothing short of a gate keeps them honest.

Three kinds of check:

- The resolver answers what the prose says it answers, code for code, including
  the refusals. ``reference.md`` section 4 pins terms that resolve WRONGLY
  today, so a fixed term turns a test red and the sentence describing it is
  updated in the same commit.
- Every ``mirobody`` subcommand, Compose service, port and health path a skill
  names exists in this checkout.
- The frontmatter meets the SKILL.md specification (name equals the directory,
  six allowed fields, body under 500 lines).

Document parsing is not exercised: ``mirobody parse`` calls a model. The rows
it extracts from ``demo/upload/you_annual_checkup_2026-05.pdf`` are pinned
through ``resolve_reading`` instead, which is the half that has no key.
"""

from __future__ import annotations

import ast
import json
import pathlib
import re
import subprocess
import sys

import pytest

import mirobody
from mirobody.engine import resolve, resolve_reading, standardize_reading
from mirobody.translate import resolve_condition, resolve_symptom
from mirobody.units import convert_value, normalize_unit, parse_value_unit, unit_family

# Through the package, never by walking up from this file: the module has moved
# once and a `parents[n]` count is what breaks silently when it moves again.
_PKG = pathlib.Path(mirobody.__file__).resolve().parent
_ROOT = _PKG.parent
_SKILLS = _ROOT / "skills"

if not _SKILLS.is_dir():
    pytest.skip("skills/ ships in the repository, not in the wheel", allow_module_level=True)

_SKILL_NAMES = ("mirobody", "translate-health-data")
_REF = _SKILLS / "translate-health-data" / "reference.md"


def _text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def _frontmatter(skill_md: pathlib.Path) -> dict[str, str]:
    text = _text(skill_md)
    assert text.startswith("---\n"), f"{skill_md}: no frontmatter"
    block = text.split("---\n", 2)[1]
    out: dict[str, str] = {}
    for line in block.splitlines():
        if line and not line.startswith(" ") and ":" in line:
            key, _, value = line.partition(":")
            out[key.strip()] = value.strip()
    return out


def _fences(path: pathlib.Path, lang: str = "bash") -> list[str]:
    return re.findall(rf"```{lang}\n(.*?)```", _text(path), re.S)


# -- shape ---------------------------------------------------------------------


def test_skills_dir_is_outside_the_package() -> None:
    """`pip install mirobody` stays two packages: the skills are not code."""
    assert not (_PKG / "skills").exists()
    assert _SKILLS.parent == _ROOT
    pyproject = _text(_ROOT / "pyproject.toml")
    find_block = re.search(r"\[tool\.setuptools\.packages\.find\](.*?)\n\[", pyproject, re.S).group(1)
    include_line = re.search(r"^include\s*=\s*(.+)$", find_block, re.M).group(1)
    assert "skills" not in include_line


@pytest.mark.parametrize("name", _SKILL_NAMES)
def test_skill_frontmatter(name: str) -> None:
    """agentskills.io: name equals the directory, lowercase-hyphen, at most 64;
    description non-empty and at most 1024; only six fields; body under 500."""
    path = _SKILLS / name / "SKILL.md"
    fm = _frontmatter(path)
    assert fm["name"] == name
    assert re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", name) and len(name) <= 64
    assert "anthropic" not in name and "claude" not in name
    assert 0 < len(fm["description"]) <= 1024
    allowed = {"name", "description", "license", "compatibility", "metadata", "allowed-tools"}
    assert set(fm) <= allowed, set(fm) - allowed
    assert len(_text(path).splitlines()) < 500


def test_marketplace_names_every_skill() -> None:
    manifest = json.loads(_text(_ROOT / ".claude-plugin" / "marketplace.json"))
    assert manifest["name"] and manifest["owner"]["name"]
    listed = {p for plugin in manifest["plugins"] for p in plugin["skills"]}
    assert listed == {f"./skills/{n}" for n in _SKILL_NAMES}
    for rel in listed:
        assert (_ROOT / rel / "SKILL.md").is_file(), rel


def test_every_skill_the_readmes_install_exists() -> None:
    """`npx skills add thetahealth/mirobody --skill X` only works for an X here."""
    docs = [_SKILLS / "README.md", _ROOT / "README.md", _ROOT / "README.zh-CN.md"]
    named = set()
    for doc in docs:
        if doc.is_file():
            named |= set(re.findall(r"--skill ([a-z0-9-]+)", _text(doc)))
    assert named, "no README names a skill to install"
    assert named <= set(_SKILL_NAMES), named - set(_SKILL_NAMES)


# -- commands, services, ports --------------------------------------------------


def _cli_subcommands() -> set[str]:
    tree = ast.parse(_text(_PKG / "cli.py"))
    names = set()
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "add_parser" and node.args
                and isinstance(node.args[0], ast.Constant)):
            names.add(node.args[0].value)
    assert names, "no subparsers found in cli.py"
    return names


def _compose_services() -> set[str]:
    text = _text(_ROOT / "compose.yaml")
    block = re.split(r"^services:\n", text, maxsplit=1, flags=re.M)[1].split("\nvolumes:", 1)[0]
    return set(re.findall(r"^  ([a-z_]+):", block, re.M))


@pytest.mark.parametrize("name", _SKILL_NAMES)
def test_every_mirobody_command_is_a_real_subcommand(name: str) -> None:
    real = _cli_subcommands()
    for fence in _fences(_SKILLS / name / "SKILL.md"):
        for line in fence.splitlines():
            m = re.match(r"\s*(?:uvx |docker compose exec (?:-T )?\w+ )?mirobody ([a-z-]+)", line)
            if m and m.group(1) not in ("--help", "-h"):
                assert m.group(1) in real, f"{name}: `mirobody {m.group(1)}` is not a subcommand"


def test_install_skill_names_only_real_compose_services() -> None:
    real = _compose_services()
    skill = _text(_SKILLS / "mirobody" / "SKILL.md")
    for svc in re.findall(r"docker compose (?:exec(?: -T)?|logs(?: -f)?) ([a-z_]+)", skill):
        assert svc in real, f"service `{svc}` is not in compose.yaml ({sorted(real)})"
    for svc in ("pg", "mirobody", "mirobody_worker", "mirobody_init"):
        assert f"`{svc}`" in skill, f"the verify step should name {svc}"


def test_install_skill_port_and_health_path_match_the_stack() -> None:
    skill = _text(_SKILLS / "mirobody" / "SKILL.md")
    compose = _text(_ROOT / "compose.yaml")
    dockerfile = _text(_ROOT / "Dockerfile")
    assert "MIROBODY_HOST_PORT:-18060" in compose and "18060" in skill
    assert "PG_HOST_PORT:-18062" in compose and "18062" in skill
    assert "/api/health" in dockerfile and "/api/health" in skill
    assert "SEED_DEMO_DATA" in compose and "you@mirobody.ai" in skill


def test_install_skill_lists_the_keys_doctor_knows() -> None:
    """The provider table in the skill must be the one `config.llm.yaml` reads."""
    llm = _text(_ROOT / "config.llm.yaml")
    skill = _text(_SKILLS / "mirobody" / "SKILL.md")
    for var in re.findall(r"`([A-Z]+_API_KEY)`", skill):
        assert var in llm, f"{var} is in the skill but not in config.llm.yaml"


# -- the resolver, code for code ------------------------------------------------

#: reference.md section 1 and the SKILL.md step 4 examples.
_UNIT_PAIRS = [
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

#: reference.md section 2, and the five-term `mirobody resolve` line.
_NAME_ONLY = [
    ("HDL", "2085-9"), ("高密度脂蛋白", "2085-9"),
    ("LDL", "13457-7"), ("低密度脂蛋白", "13457-7"), ("LDL cholesterol", "13457-7"),
    ("non-HDL cholesterol", "43396-1"),
    ("Cholesterol/HDL Ratio", "9830-1"),
    ("total bilirubin", "1975-2"), ("direct bilirubin", "15152-2"), ("indirect bilirubin", "1971-1"),
    ("free T4", "3024-7"), ("游离甲状腺素", "3024-7"), ("total T4", "3026-2"), ("TSH", "3016-3"),
    ("血红蛋白", "718-7"), ("ヘモグロビン", "718-7"), ("血紅素", "718-7"),
    ("空腹血糖(GLU)", "1558-6"), ("空腹血糖", "1558-6"),
    ("total cholesterol", "2093-3"), ("Total Cholesterol", "2093-3"),
    ("肝功能", "24325-3"), ("肾功能", "24362-6"), ("血常规", "57021-8"), ("血压", "85354-9"),
]

#: reference.md section 3: category headings that must abstain, including the
#: four that once resolved wrongly and were fixed in the 1.5 bundle.
_ABSTAIN = ["血脂", "三大常规", "甲功", "心肌酶", "生化全套", "凝血功能", "免疫球蛋白",
            "微量元素", "激素六项", "维生素", "尿常规", "肿瘤标志物", "电解质",
            "Lipid-Low-Density Lipoprotein Particle Number"]

#: reference.md section 4: category words that must refuse instead of selecting
#: one arbitrarily specific LOINC observation. These were the six known wrong
#: answers in the 1.5.3 bundle.
_KNOWN_CATEGORY = ["免疫", "stool", "重金属", "heavy metals", "激素", "enzymes"]

#: reference.md section 6: demo/upload/you_annual_checkup_2026-05.pdf, every
#: printed row with its unit, and what name-only resolution would have given.
_DEMO_ROWS = [
    ("Glycated Hemoglobin-HbA1c", "5.2", "%", "4548-4", "4548-4"),
    ("Fasting Blood Glucose-FBG", "4.9", "mmol/L", "14771-0", "1558-6"),
    ("Total Cholesterol-TC", "4.45", "mmol/L", "14647-2", "2093-3"),
    ("Low-Density Lipoprotein-LDL", "2.48", "mmol/L", "22748-8", "13457-7"),
    ("High-Density Lipoprotein-HDL", "1.50", "mmol/L", "14646-4", "2085-9"),
    ("Triglycerides-TG", "0.95", "mmol/L", "14927-8", "2571-8"),
    ("Systolic Blood Pressure", "116", "mmHg", "8480-6", "8480-6"),
    ("Diastolic Blood Pressure", "75", "mmHg", "8462-4", "8462-4"),
    ("Resting Heart Rate", "57", "bpm", "40443-4", "40443-4"),
]


@pytest.mark.parametrize("term,value,unit,loinc", _UNIT_PAIRS)
def test_unit_selects_the_code(term, value, unit, loinc) -> None:
    assert resolve_reading(term, value, unit).loinc == loinc


@pytest.mark.parametrize("term,loinc", _NAME_ONLY)
def test_name_resolves_as_documented(term, loinc) -> None:
    r = resolve(term)
    assert r.resolved and r.loinc == loinc, f"{term} -> {r.loinc} {r.canonical!r}"


def test_the_documented_canonical_names() -> None:
    """The prose quotes canonical names, not only codes; a renamed axis row
    would leave the code right and the sentence wrong."""
    assert resolve("肝功能").canonical == "Hepatic function 2000 panel - Serum or Plasma"
    assert resolve("血红蛋白").canonical == "Hemoglobin [Mass/volume] in Blood"
    assert "by calculation" in resolve("LDL").canonical
    assert "Mass Ratio" in resolve("Cholesterol/HDL Ratio").canonical
    assert resolve_reading("Glycated Hemoglobin-HbA1c", "5.2", "%").canonical == \
        "Hemoglobin A1c/Hemoglobin.total in Blood"
    assert resolve_reading("Glycated Hemoglobin-HbA1c", "5.2", "%").method == "lexical"


@pytest.mark.parametrize("term", _ABSTAIN)
def test_category_word_abstains(term) -> None:
    r = resolve(term)
    assert not r.resolved, f"{term} answered {r.loinc} {r.canonical!r}"


@pytest.mark.parametrize("term", _KNOWN_CATEGORY)
def test_category_words_refuse(term) -> None:
    r = resolve(term)
    assert not r.resolved and r.method == "refused", (
        f"{term} still answers {r.loinc} {r.canonical!r}; category words must not "
        "select one specific LOINC observation"
    )


def test_category_words_are_documented() -> None:
    ref = _text(_REF)
    for term in _KNOWN_CATEGORY:
        assert f"`{term}`" in ref, f"{term} missing from reference.md"
    assert "deliberately unresolved" in ref


@pytest.mark.parametrize("name,value,unit,with_unit,name_only", _DEMO_ROWS)
def test_demo_checkup_row(name, value, unit, with_unit, name_only) -> None:
    assert resolve_reading(name, value, unit).loinc == with_unit
    assert resolve(name).loinc == name_only


def test_five_of_nine_demo_codes_change_with_the_unit() -> None:
    """Quoted in three files as the reason the unit travels with the name."""
    changed = sum(1 for _, _, _, a, b in _DEMO_ROWS if a != b)
    assert changed == 5
    assert (_ROOT / "demo" / "upload" / "you_annual_checkup_2026-05.pdf").is_file()


def test_four_languages_one_code() -> None:
    codes = {resolve(t).loinc for t in ("hemoglobin", "血红蛋白", "血紅素", "ヘモグロビン")}
    assert codes == {"718-7"}


# -- the other axes ------------------------------------------------------------


def test_icpc3_examples() -> None:
    assert resolve_symptom("头疼").code == "NS01"
    assert resolve_symptom("headache").code == "NS01"
    assert resolve_symptom("发烧").code == "AS03"
    r = resolve_symptom("疼")
    assert r.outcome == "refused" and r.reason == "icpc3:too-broad"
    assert resolve_condition("2型糖尿病").code == "TD72"
    assert resolve_condition("高血压").code == "KD73"
    assert resolve_condition("hypertension").code == "KD73"
    r = resolve_condition("糖尿病")
    assert r.outcome == "needs-input" and r.reason == "icpc3:ambiguous"


def test_unit_examples() -> None:
    assert normalize_unit("mmol/l") == "mmol/L"
    assert normalize_unit("10^9/L") == "10*9/L"
    assert normalize_unit("µmol/L") == "umol/L"
    assert unit_family("mg/dL") == "MCnc"
    q = parse_value_unit("<0.5 ng/mL")
    assert (q.comparator, q.value, q.unit) == ("<", 0.5, "ng/mL")
    assert round(convert_value(100, "mg/dL", "mmol/L", loinc_code="2339-0"), 2) == 5.55
    assert round(convert_value(1.0, "mg/dL", "umol/L", loinc_code="2160-0"), 1) == 88.4
    assert convert_value(62, "%", "10*9/L", loinc_code="26511-6") is None


def test_fhir_observation_shape() -> None:
    obs = standardize_reading("血红蛋白", "13.5", "g/dL")
    assert obs["resourceType"] == "Observation" and obs["status"] == "final"
    assert obs["code"]["text"] == "血红蛋白"
    coding = obs["code"]["coding"][0]
    assert (coding["system"], coding["code"]) == ("http://loinc.org", "718-7")
    q = obs["valueQuantity"]
    assert (q["value"], q["system"], q["code"]) == (13.5, "http://unitsofmeasure.org", "g/dL")
    ext = next(e for e in obs["extension"] if e["url"].endswith("/coding-decision"))
    assert {"url": "method", "valueString": "lexical"} in ext["extension"]


def test_apple_import_output_shape(tmp_path: pathlib.Path) -> None:
    """`mirobody import apple` on a two-record export: the summary line the
    skill quotes, and the JSON-lines keys."""
    xml = tmp_path / "export.xml"
    xml.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n<HealthData locale="en_US">\n'
        ' <ExportDate value="2026-09-30 08:00:00 +0000"/>\n'
        ' <Record type="HKQuantityTypeIdentifierHeartRate" sourceName="Watch" unit="count/min"'
        ' creationDate="2026-09-29 07:01:00 +0000" startDate="2026-09-29 07:00:00 +0000"'
        ' endDate="2026-09-29 07:00:05 +0000" value="62"/>\n'
        ' <Record type="HKQuantityTypeIdentifierStepCount" sourceName="Phone" unit="count"'
        ' creationDate="2026-09-29 09:00:00 +0000" startDate="2026-09-29 08:00:00 +0000"'
        ' endDate="2026-09-29 09:00:00 +0000" value="1834"/>\n</HealthData>\n',
        encoding="utf-8",
    )
    out = tmp_path / "facts.jsonl"
    proc = subprocess.run([sys.executable, "-m", "mirobody", "import", "apple", str(xml), "--out", str(out)],
                          capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr
    assert "heartRates" in proc.stdout and "2 readings" in proc.stdout
    rows = [json.loads(l) for l in out.read_text(encoding="utf-8").splitlines()]
    assert {r["metric_key"] for r in rows} == {"heartRates", "steps"}
    assert {"value_num", "effective_start_ms", "unit", "modality"} <= set(rows[0])


# -- numbers and codes quoted in prose --------------------------------------------


def test_bundle_and_coverage_quoted_are_current() -> None:
    """A bundle rebuild moves BUNDLE_VERSION; every skill that names it goes
    red, which is the point. Coverage is the README's gated figure."""
    readme = _text(_ROOT / "README.md")
    score = re.search(r"\*\*(\d+/\d+)\*\* on the tests an ordinary checkup prints", readme).group(1)
    for name in _SKILL_NAMES:
        for path in (_SKILLS / name).glob("*.md"):
            text = _text(path)
            if "loinc-2." in text:
                assert mirobody.BUNDLE_VERSION in text, f"{path} quotes a stale bundle"
            if "checkup" in text and re.search(r"\d+/\d+", text):
                assert score in text, f"{path} quotes a coverage other than {score}"


def test_no_loinc_code_is_invented() -> None:
    """Every LOINC-shaped token in the two skills is one a test above derives."""
    derived = {c for *_, c in _UNIT_PAIRS} | {c for _, c in _NAME_ONLY}
    derived |= {a for *_, a, _ in _DEMO_ROWS} | {b for *_, b in _DEMO_ROWS}
    derived |= {"718-7", "26511-6", "26499-4", "2339-0", "2160-0", "43727-7", "24325-3", "22748-8"}
    for name in _SKILL_NAMES:
        for path in (_SKILLS / name).glob("*.md"):
            for code in set(re.findall(r"\b(\d{1,5}-\d)\b", _text(path))):
                assert code in derived, f"{path.name} quotes {code}, which no test derives"


def test_nearest_code_to_ldl_particle_number_is_a_different_measurement() -> None:
    """reference.md section 6: the refusal that the skill is for."""
    assert resolve("Lipoprotein.beta.subparticle.small").loinc == "43727-7"
