"""Every number in an answer is traced to the rows it cites (`kernel.citations`).

This is the check behind "every number has a source": the agent's answers,
the benchmark and the training judge all run it. It needs nothing but the
library, so it runs on a minimal install.
"""

import pytest

from mirobody.kernel import citations
from mirobody.kernel.citations import Segment, check, cite_kind, normalize, numbers, parse, strip

ANSWER = (
    "<statement>你的 LDL 从 3 月的 3.8 降到 8 月的 2.9 mmol/L，下降 0.9<cite>[r3][r9]</cite></statement>"
    "<statement>8 月的报告把范围印为 <3.4<cite>[r9]</cite></statement>"
    "<statement>门诊病历写着三个月后复查<cite>[/library/门诊病历.pdf#L12-L14]</cite></statement>"
    "建议把这条趋势带给医生。"
)
SUPPORT = {"r3": [3.8], "r9": [2.9, 3.4]}


def test_parse_reads_statements_their_cites_and_plain_text():
    segments = parse(ANSWER)
    assert [s.statement for s in segments] == [True, True, True, False]
    assert segments[0].cites == ("r3", "r9")
    assert segments[2].cites == ("/library/门诊病历.pdf#L12-L14",)
    assert segments[3] == Segment(text="建议把这条趋势带给医生。")


def test_an_unclosed_statement_ends_at_the_next_one():
    segments = parse("<statement>a 1<cite>[r1]</cite><statement>b 2<cite>[r2]</cite></statement>")
    assert [(s.text, s.cites) for s in segments] == [("a 1", ("r1",)), ("b 2", ("r2",))]


def test_a_loose_cite_cites_the_text_before_it():
    segments = parse("Weight was 57.0 kg <cite>[r1]</cite> last week.")
    assert segments[0].statement and segments[0].cites == ("r1",) and "57.0" in segments[0].text
    assert not segments[1].statement


def test_a_loose_cite_in_a_table_cites_its_own_cell():
    table = "| TC | 4.60 <cite>[r5]</cite> | 4.45 <cite>[r6]</cite> |"
    assert check(table, {"r5": [4.6], "r6": [4.45]}) == []
    assert [s.cites for s in parse(table) if s.statement] == [("r5",), ("r6",)]


def test_a_cite_in_a_source_column_cites_its_row():
    table = "| TC | 4.60 → 4.38 mmol/L | 3.0–5.18 | <cite>[r7][r5]</cite> |"
    assert check(table, {"r7": [4.6, 3.0, 5.18], "r5": [4.38, 3.0, 5.18]}) == []


def test_a_chart_block_is_not_read_as_prose():
    answer = ('```vis-chart\n{"data":[{"time":"2026-05-06","value":4.45}]}\n```\n'
              "<statement>TC fell to 4.45<cite>[r6]</cite></statement>")
    assert check(answer, {"r6": [4.45]}) == []


def test_strip_leaves_what_a_reader_sees():
    assert strip(ANSWER) == (
        "你的 LDL 从 3 月的 3.8 降到 8 月的 2.9 mmol/L，下降 0.9"
        "8 月的报告把范围印为 <3.4门诊病历写着三个月后复查建议把这条趋势带给医生。"
    )


@pytest.mark.parametrize("written,meant", [
    # DeepSeek, 2026-10-30: a statement closed with a misspelt cite.
    ("<statement>均值 74.4 次/分<xcite>[r4]</cite></statement>",
     "<statement>均值 74.4 次/分<cite>[r4]</cite></statement>"),
    ("体重 69.2 kg<Cite >[r1]</ cite>。", "体重 69.2 kg<cite>[r1]</cite>。"),
    # A close with no open is dropped; an open with no close ends at its ids.
    ("体重 69.2 kg</cite>。", "体重 69.2 kg。"),
    ("体重 69.2 kg<cite>[r1] 之后的话", "体重 69.2 kg<cite>[r1]</cite> 之后的话"),
    ("体重 69.2 kg<xcite>[r1]</xcite>。", "体重 69.2 kg<cite>[r1]</cite>。"),
    ("no markup at all", "no markup at all"),
    # Prose that looks like a tag is not markup: a word, a title, no ids.
    ("Caffeine can <excite>raise heart rate</excite>.", "Caffeine can <excite>raise heart rate</excite>."),
    ("According to <cite>The Lancet</cite>, it is studied.", "According to <cite>The Lancet</cite>, it is studied."),
    ("a <restatement> and a <recite> stay", "a <restatement> and a <recite> stay"),
    ("a < b and <cite> is a tag", "a < b and <cite> is a tag"),
    # A chart's JSON is data, not markup.
    ('```vis-chart\n{"title":"<xcite>[r1]</cite> HR"}\n```', '```vis-chart\n{"title":"<xcite>[r1]</cite> HR"}\n```'),
    # DeepSeek, English, 2026-10-30: every cite after its statement's close.
    ("<statement>HbA1c was 5.8%</statement><cite>[r2]</cite>.",
     "<statement>HbA1c was 5.8%<cite>[r2]</cite></statement>."),
    ("<statement>A1C under 5.7%</statement> <cite>[ref:a:1]</cite><cite>[ref:b:2]</cite> x",
     "<statement>A1C under 5.7%<cite>[ref:a:1]</cite><cite>[ref:b:2]</cite></statement> x"),
    # A cite on the next line is not the statement's.
    ("<statement>a 1</statement>\n<cite>[r1]</cite>", "<statement>a 1</statement>\n<cite>[r1]</cite>"),
])
def test_normalize_spells_the_markup_as_the_format_does(written, meant):
    assert normalize(written) == meant


def test_a_misspelt_tag_reads_as_the_tag_it_meant():
    answer = "<statement>均值 74.4 次/分<xcite>[r4]</cite></statement>"
    assert parse(answer) == [Segment("均值 74.4 次/分", ("r4",), True)]
    assert strip(answer) == "均值 74.4 次/分"
    assert check(answer, {"r4": [74.4]}) == []


def test_a_word_that_looks_like_a_tag_does_not_change_what_is_checked():
    prose = "Your heart rate went from 70 to 85 bpm after intake, per your tracker."
    tagged = "Your heart rate went from 70 to 85 bpm<excite> after intake</excite>, per your tracker."
    assert [p.kind for p in check(tagged, {})] == [p.kind for p in check(prose, {})] == ["uncited_number"] * 2


def test_a_cite_after_its_statement_traces_the_statement():
    answer = "<statement>HbA1c was 5.8%, above the report's upper limit of 5.6%</statement><cite>[r2]</cite>."
    assert check(answer, {"r2": [5.8, 5.6]}) == []


def test_an_unclosed_cite_does_not_swallow_the_answer():
    assert strip("体重 69.2 kg<cite>[r1] 之后的话") == "体重 69.2 kg 之后的话"


@pytest.mark.parametrize("cite,kind", [
    ("r3", "row"), ("r10", "row"), ("r0", "unknown"),
    ("ref:medlineplus:2241", "ref"), ("ref:pmid:41234567", "ref"),
    ("/library/a b.pdf#L12-L14", "lines"), ("/library/a.pdf#L3", "lines"),
    ("web_uploads/17eaf4f6.pdf", "unknown"),
])
def test_cite_kinds(cite, kind):
    assert cite_kind(cite) == kind


@pytest.mark.parametrize("text,expected", [
    ("LDL 3.8 mmol/L", [3.8]),
    ("2026-08-07 的空腹血糖 5.4", [5.4]),
    ("2025-11 至 2026-08 之间 4.6", [4.6]),
    ("2025\u201111 至 2026\u201108", []),
    ("### 2. 各项指标\n1. TC 4.6", [4.6]),
    ("[label](https://dailymed.nlm.nih.gov/lookup.cfm?setid=d9adb9e4-c495-9530) 20 mg", [20.0]),
    ("range 1250-1500 mg", [1250.0, 1500.0]),
    ("2026年8月7日 血红蛋白 128 g/L", [128.0]),
    ("8月7日 07:30 心率 72", [72.0]),
    ("WBC 6.5×10^9/L", [6.5]),
    ("WBC 6.5 10^9/L", [6.5]),
    ("HbA1c 6.1%", [6.1]),
    ("in 2026 it was 6,1", [6.1]),
    ("a change of -0.9", [-0.9]),
    ("r3 and r9", []),
    ("57.0 kg on September 1, 2026 at 06:45", [57.0]),
    ("your Aug 7 report and the 22nd March one", []),
    ("you may 5 times", [5.0]),
    ("BMI 21.8 (18.5--23.9)", [21.8, 18.5, 23.9]),
    ("体重65.2kg，比上月少1.7kg", [65.2, 1.7]),
    ("血压106/77，心率62次/分", [106.0, 77.0, 62.0]),
    ("weight 65.2kg, LDL 3.8mmol/L, 120mmHg", [65.2, 3.8, 120.0]),
    ("CYP2C19, HbA1c, rs4149056 and the 3rd reading", []),
])
def test_numbers_are_values_not_dates_or_ids(text, expected):
    assert numbers(text) == expected


def test_a_number_against_chinese_text_is_still_checked():
    [problem] = check("<statement>体重65.2kg<cite>[r1]</cite></statement>，比上月少1.7kg。", {"r1": [65.2]})
    assert problem.kind == "uncited_number" and problem.detail == "1.7"


def test_a_traced_answer_has_no_problems():
    assert check(ANSWER, SUPPORT) == []


@pytest.mark.parametrize("statement", [
    "下降 0.9",                 # a difference of cited values
    "下降了 24%",               # a percentage change, as written rounded
    "两次平均 3.35",            # a mean
    "约为原来的 76%",           # a ratio
])
def test_derived_values_trace_to_their_inputs(statement):
    assert check(f"<statement>{statement}<cite>[r3][r9]</cite></statement>", {"r3": [3.8], "r9": [2.9]}) == []


def test_a_number_outside_a_statement_is_untraced():
    [problem] = check("你的 LDL 是 3.8。", SUPPORT)
    assert problem.kind == "uncited_number" and problem.detail == "3.8"


def test_a_statement_with_numbers_needs_a_cite():
    [problem] = check("<statement>LDL 3.8<cite></cite></statement>", SUPPORT)
    assert problem.kind == "statement_without_cite"


def test_an_unclosed_statement_is_reported():
    problems = check("<statement>LDL 3.8<cite>[r3]</cite> 下降 0.9。", SUPPORT)
    assert [p.kind for p in problems] == ["unclosed_statement"]
    assert check("<statement>LDL 3.8<cite>[r3]</cite></statement>", SUPPORT) == []


def test_a_made_up_row_is_unknown():
    [problem] = check("<statement>LDL 3.8<cite>[r4]</cite></statement>", SUPPORT)
    assert problem.kind == "unknown_cite" and problem.detail == "r4"


def test_a_fabricated_value_is_unsupported_even_when_a_row_is_cited():
    [problem] = check("<statement>LDL 4.2<cite>[r3]</cite></statement>", SUPPORT)
    assert problem.kind == "unsupported_number" and problem.detail == "4.2"


def test_document_and_reference_statements_are_checked_for_existence_only():
    answer = ("<statement>说明书写的是每日一次 20 mg<cite>[ref:openfda:atorvastatin-2]</cite></statement>"
              "<statement>报告写的是 3 个月后复查<cite>[/library/a.pdf#L3]</cite></statement>")
    assert check(answer, {}) == []
    [problem] = check(answer, {}, known={"/library/a.pdf#L3"})
    assert problem.kind == "unknown_cite" and problem.detail == "ref:openfda:atorvastatin-2"


def test_the_module_is_stdlib_only():
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(citations))
    imported = {(node.module or "").split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    imported |= {alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    assert imported <= {"__future__", "re", "collections", "dataclasses", "itertools"}
