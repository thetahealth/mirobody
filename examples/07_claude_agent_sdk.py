"""Another agent runtime on this data: Claude Code's harness, over MCP.

    pip install claude-agent-sdk               # Anthropic's SDK; bundles the Claude Code CLI (needs Node 18+)
    MIROBODY_MCP_URL=http://127.0.0.1:18060/mcp/<secret> \\
    ANTHROPIC_API_KEY=sk-ant-... python examples/07_claude_agent_sdk.py

mirobody ships ONE agent (`mirobody/agent/`, on deepagents). Every other agent
runtime is meant to reach the same data through `/mcp` — and this is that path,
exercised: the Claude Agent SDK is handed a mirobody MCP URL, nothing else, and
asked a question only the record can answer. No mirobody import, no glue. The
tool it discovers is `query_health_indicators`, described well enough for a model
that has never seen this project to use it unaided.

The URL is a PERSONAL one (`POST /personal/mcp` with a signed-in token, see
`scripts/e2e_docker.sh` for the login flow): it carries the person's identity
as a secret in the path, so this script sends no header. Keep it out of shell
history on a shared machine.

Model access is the SDK's, not mirobody's: ANTHROPIC_API_KEY, or any
Anthropic-Messages-compatible gateway through ANTHROPIC_BASE_URL — the runs
recorded in `examples/README.md` went through OpenRouter's
(`ANTHROPIC_BASE_URL=https://openrouter.ai/api`, `ANTHROPIC_AUTH_TOKEN=$OPENROUTER_API_KEY`).

Two questions were asked against the demo record. "How has my HbA1c moved?"
came back with both readings and their dates. "How has my LDL moved?" came back
with "LDL has never been recorded in your record — which does not mean it is
normal", after the model had resolved the name, queried, and read the
catalogue. Both are the right answer, and both were reached from the tool
descriptions alone.
"""

from __future__ import annotations

import asyncio
import os

MCP_URL = os.environ.get("MIROBODY_MCP_URL", "")
QUESTION = os.environ.get("QUESTION", "How has my HbA1c moved over time? Cite the dates and values you used.")

# Every example in this directory degrades to its offline half and exits 0 on a
# bare machine — `examples/README.md` promises a reader that a failure here is a
# BUG, not a missing step, and `tests/test_examples.py` holds that promise by
# running all of them with no configuration. These two preconditions used to
# `sys.exit("...")`, which exits 1: on a machine without a running server the
# showroom's own gate went red and said nothing about what to do next.
if not MCP_URL:
    print(__doc__.strip().splitlines()[0])
    print("\nThis one needs a RUNNING mirobody, because it is the only example that")
    print("goes over the wire: it points Anthropic's Claude Agent SDK at your")
    print("personal MCP endpoint and lets the model use your own health tools.")
    print("\n  1. start the stack          ./deploy.sh")
    print("  2. sign in, then             POST /personal/mcp   → a URL with a secret in it")
    print("  3. MIROBODY_MCP_URL=<that URL> python examples/07_claude_agent_sdk.py")
    print("\nNothing was called; no key was read.")
    raise SystemExit(0)

try:
    from claude_agent_sdk import (
        AssistantMessage,
        ClaudeAgentOptions,
        ResultMessage,
        TextBlock,
        ToolResultBlock,
        ToolUseBlock,
        UserMessage,
        query,
    )
except ImportError:
    print(__doc__.strip().splitlines()[0])
    print("\nclaude-agent-sdk is not installed — it is Anthropic's, not a mirobody")
    print("dependency, so this example asks for it rather than shipping it:")
    print("\n    pip install claude-agent-sdk")
    raise SystemExit(0)


async def main() -> int:
    options = ClaudeAgentOptions(
        mcp_servers={"mirobody": {"type": "http", "url": MCP_URL}},
        # Only mirobody's tools: no file system, no shell — this runtime is
        # reading a health record, not editing a repository.
        allowed_tools=["mcp__mirobody__*"],
        disallowed_tools=["Bash", "Read", "Write", "Edit", "Glob", "Grep", "WebSearch", "WebFetch"],
        permission_mode="bypassPermissions",
        max_turns=8,
        system_prompt=(
            "You answer questions about the person's own health record using the "
            "mirobody tools. Start from the tool's own description. Cite dates and "
            "values from the data; do not diagnose."
        ),
    )

    print(f"question: {QUESTION}\n")
    outcome = 1
    async for message in query(prompt=QUESTION, options=options):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, ToolUseBlock):
                    print(f"→ tool  {block.name}  {block.input}")
                elif isinstance(block, TextBlock) and block.text.strip():
                    print(f"\n{block.text.strip()}\n")
        elif isinstance(message, UserMessage) and isinstance(message.content, list):
            for block in message.content:
                if isinstance(block, ToolResultBlock):
                    text = block.content if isinstance(block.content, str) else str(block.content)
                    first = text.strip().splitlines()[0] if text.strip() else ""
                    print(f"← result {len(text)} chars: {first[:100]}")
        elif isinstance(message, ResultMessage):
            cost = f" cost=${message.total_cost_usd:.4f}" if message.total_cost_usd is not None else ""
            print(f"[{message.subtype}] turns={message.num_turns}{cost}")
            outcome = 0 if message.subtype == "success" else 1
    return outcome


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
