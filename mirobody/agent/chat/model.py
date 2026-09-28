"""The chat request body, and the one question worth asking about it."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ChatStreamRequest(BaseModel):
    """The body of `POST /api/chat`.

    `extra="forbid"` is the unknown-field rejection the MCP surface also does:
    a caller's typo (or a natural guess like `model`) answers with the accepted
    names instead of a bare 500.

    Four fields used to be accepted and ignored here (`agent`, `enable_mcp`,
    `group_id`, `reference_task_id`) for clients that had not stopped sending
    them. No client in this repository sends any of them and none was ever
    read, so they are gone: with `extra="forbid"`, a sender now learns that
    rather than having the value silently dropped.
    """

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def _null_is_absent(cls, data: Any) -> Any:
        """An explicit `null` means the field was not supplied.

        JSON clients send one where a value is optional, and the shipped web
        client sends `prompt_name: null` on EVERY message. Without this, each
        of those is a validation error, and the endpoint answers -4 on a
        perfectly ordinary chat request.
        """
        if isinstance(data, dict):
            return {k: v for k, v in data.items() if v is not None}
        return data

    question: str = ""
    query_user_id: str = ""
    provider: str = ""

    session_id: str = ""
    question_id: str = ""

    file_list: list[dict[str, Any]] = Field(default_factory=list)
    prompt_name: str = ""

    user_id: str = ""
    language: str = ""
    timezone: str = ""

    #: What `th_messages.scene` records: which surface the turn came from.
    scene: str = "web"


def has_attachment(file_list: Any) -> bool:
    """True when this turn really carries a file the agent can reach.

    Truthiness of `file_list` is not the same question. `[{}]` and `"x"` are
    both truthy and neither names a file: `/uploads/` would be empty and
    `attachment_reminder` would return None, while the guard had already let
    the turn through and the stand-in question had already promised the model
    an attachment. `file_key` is what the whole upload path keys on, so it is
    what counts here.
    """
    if not isinstance(file_list, (list, tuple)):
        return False
    return any(isinstance(f, dict) and f.get("file_key") for f in file_list)

