"""Pydantic models for MixAgent two-phase data flow.

`OrchestratorManifest` is passed to `create_agent` in Phase 1 wrapped in an
explicit `ToolStrategy(OrchestratorManifest)` (see `mix_agent._create_phase1_agent`).
The explicit ToolStrategy is required: a bare schema would auto-select
`ProviderStrategy` for Claude/Gemini-3 and bind tools WITHOUT `tool_choice`,
letting the model skip data collection. With ToolStrategy the manifest registers
as a structured-output tool and LangChain forces `tool_choice="any"`, so every
Phase 1 turn must produce a tool call (a real tool, or this manifest) — uniformly
across providers. When the model outputs this manifest, LangChain parses it and
sets `state.structured_response` — Phase 1 ends.

Modeled after theta-smart's `HealthV2Manifest` pattern (loops + note).
The manifest carries only lightweight observability and hint metadata;
real data lives in the AIMessage + ToolMessage history that Phase 2 reads
via `mixin._stream_agent` → `collected_messages`.
"""

from pydantic import BaseModel, Field


class OrchestratorManifest(BaseModel):
    """Signal that Phase 1 data collection is complete.

    Phase 2 (responder) consumes `note` as a hint and reads `tool_rounds`
    for trace/observability. The real data is the tool-call history that
    Phase 1 accumulated — not any field on this model.
    """

    tool_rounds: int = Field(
        description=(
            "Number of tool-calling rounds taken in this Phase 1 to gather "
            "enough information to answer. 0 if no tools were called."
        ),
    )
    note: str = Field(
        default="",
        description=(
            "One short sentence (under 15 words) hinting the responder at "
            "what was collected or what tone to take. Optional; leave "
            "empty for routine data-bearing replies, required when no "
            "tools were called so the responder has context for the user "
            "intent."
        ),
    )
