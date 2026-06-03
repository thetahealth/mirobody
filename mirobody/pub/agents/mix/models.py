"""Pydantic models for MixAgent two-phase data flow.

`OrchestratorManifest` is passed to `create_agent` via the `response_format`
parameter in Phase 1. LangChain's `create_agent` auto-converts a Pydantic
response_format into an implicit structured-output tool and sets
`tool_choice="any"`, forcing every Phase 1 model turn to produce a tool
call (real tool, or this manifest). When the model outputs this manifest,
LangChain parses it and sets `state.structured_response` — Phase 1 ends.

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
