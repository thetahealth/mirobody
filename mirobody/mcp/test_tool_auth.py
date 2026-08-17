"""Caller identity comes from the JWT, never from the model.

`user_info` is the injection hook: the loader detects it, `func_metadata`'s
`skip_names` hides it from the schema, and `call_tool` fills it from the
verified token. Any *other* parameter meaning "who is calling" — `user_id` and
friends — is not injected and stays in the schema, which means the MODEL
supplies it and an MCP client can request another person's health data by
sending a different value.

The main README instructed tool authors to take `user_id: str` until a
walkthrough by someone integrating over MCP caught it. These tests pin both the
mechanism and the warning that now fires on the wrong shape, because a tool
that is silently unauthenticated looks exactly like one that works.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

import pytest

from .tool import parse_function


def _tool_with_user_info(query: str, user_info: Dict[str, Any]) -> dict:
    """Correct shape.

    Args:
        query: what to look up.
    """
    return {}


def _tool_with_user_id(query: str, user_id: str) -> dict:
    """The shape the README used to describe.

    Args:
        query: what to look up.
    """
    return {}


def test_user_info_is_injected_and_hidden():
    tool, requires_auth, _ = parse_function(_tool_with_user_info)
    assert requires_auth is True, "the server must fill user_info from the JWT"
    assert "user_info" not in tool["inputSchema"]["properties"], (
        "an injected argument must never appear in the schema the model sees"
    )
    assert "query" in tool["inputSchema"]["properties"]


def test_a_user_id_parameter_is_not_authentication():
    """The hole: not injected, and visible to the caller."""
    tool, requires_auth, _ = parse_function(_tool_with_user_id)
    assert requires_auth is False
    assert "user_id" in tool["inputSchema"]["properties"], (
        "this is the danger — it is model-supplied, so any client can pass any id"
    )


def test_the_wrong_shape_warns_loudly(caplog):
    with caplog.at_level(logging.WARNING):
        parse_function(_tool_with_user_id)
    assert any("user_info" in r.getMessage() for r in caplog.records), (
        "loading an unauthenticated tool must warn"
    )


def test_the_right_shape_is_silent(caplog):
    with caplog.at_level(logging.WARNING):
        parse_function(_tool_with_user_info)
    assert not caplog.records, f"unexpected warning: {[r.message for r in caplog.records]}"


@pytest.mark.parametrize("shipped", ["query_health_indicators", "get_genetic_data"])
def test_shipped_tools_that_read_user_data_are_authenticated(shipped: str):
    """The tools that touch a person's records must be on the injected path."""
    from .tool import global_tools, load_tools_from_directories

    load_tools_from_directories(["mirobody/agent/tools"])
    assert shipped in global_tools, f"{shipped} not loaded"
    tool = global_tools[shipped]
    assert tool.get("auth") is True, f"{shipped} does not receive an injected user_info"
    # On a LOADED tool the schema lives under `description` (the MCP tool
    # definition), not at the top level as `parse_function` returns it.
    schema = tool["description"]["inputSchema"]
    assert "user_id" not in schema["properties"], f"{shipped} exposes user_id to the model"
    assert "user_info" not in schema["properties"]
