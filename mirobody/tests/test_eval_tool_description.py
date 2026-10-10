"""The eval tool the model builds carries the citation contract in its
description, and the registration path works end to end.

Two failure histories, one test module:

1. The contract did not exist at all (pilots measured DeepSeek silently
   computing ~90% of quoted numbers in its reasoning channel; the judge
   rightfully rejects those).
2. `RefCollectingInterpreter._build_tool` once declared the `code` argument
   with a closure-captured annotation under `from __future__ import
   annotations`; langgraph's `_get_all_injected_args` evaluates the string
   against module globals and every chat turn died with NameError.
"""

from mirobody.agent.middleware.eval_refs import (
    EVAL_CITATION_CONTRACT,
    RefCollectingInterpreter,
)


def _description() -> str:
    # __init__ builds the tool once; closing over the middleware keeps the
    # closures alive for langgraph's later introspection.
    mw = RefCollectingInterpreter()
    tool = mw.tools[0]
    annotations = tool.func.__annotations__
    # The closure's `code_doc` must already be an eager Annotated object, not
    # a string the type-hint evaluator cannot resolve.
    assert not isinstance(annotations.get("code"), str)
    return tool.description or ""


def test_eval_description_states_the_citation_contract():
    desc = _description()
    assert EVAL_CITATION_CONTRACT in desc


def test_contract_points_at_log_or_return():
    desc = _description()
    assert "console.log" in desc
    assert "cannot be cited" in desc
    assert "reasoning" in desc
