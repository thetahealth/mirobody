"""Every number in an answer is traced to the rows it cites (`kernel.citations`).

This is the check behind "every number has a source": the agent's answers,
the benchmark and the training judge all run it. It needs nothing but the
library, so it runs on a minimal install.
"""

import pytest

from mirobody.kernel import citations
from mirobody.kernel.citations import Segment, check, cite_kind, numbers, parse, strip

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


def test_a_chart_block_is_not_read_as_prose():
    answer = ('```vis-chart\n{"data":[{"time":"2026-05-06","value":4.45}]}\n```\n'
              "<statement>TC fell to 4.45<cite>[r6]</cite></statement>")
    assert check(answer, {"r6": [4.45]}) == []


def test_strip_leaves_what_a_reader_sees():
    assert strip(ANSWER) == (
        "你的 LDL 从 3 月的 3.8 降到 8 月的 2.9 mmol/L，下降 0.9"
        "8 月的报告把范围印为 <3.4门诊病历写着三个月后复查建议把这条趋势带给医生。"
    )


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
    ("2026年8月7日 血红蛋白 128 g/L", [128.0]),
    ("8月7日 07:30 心率 72", [72.0]),
    ("WBC 6.5×10^9/L", [6.5]),
    ("WBC 6.5 10^9/L", [6.5]),
    ("HbA1c 6.1%", [6.1]),
    ("in 2026 it was 6,1", [6.1]),
    ("a change of -0.9", [-0.9]),
    ("r3 and r9", []),
])
def test_numbers_are_values_not_dates_or_ids(text, expected):
    assert numbers(text) == expected


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
