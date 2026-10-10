"""The half of a record tool that readings, medications and genetics share:
who is asking, and what happens when the run fails.

Underscore-prefixed so the tool loader never scans this file, and the class is
not named `*Service` so `load_tools_from_module` would not consider it either.
`load_tools_from_class` also skips an inherited method (it compares
`__qualname__` against the class it is loading), and `__tools__` is a third
guard. Three, because what they prevent is an undocumented tool in `tools/list`.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

from mirobody.kernel import tools
from mirobody.kernel.ops import is_driver_exception
from ._authz import caller_of, denied

logger = logging.getLogger(__name__)

#: The conversation a tool call belongs to, while `envelope` runs. Empty for an
#: MCP or REST caller: rows are numbered for citing only inside a chat.
CITATION_SESSION: ContextVar[str] = ContextVar("citation_session", default="")


class RecordTool:
    """A tool that reads one person's record.

    It never raises. The `eval` REPL can call these directly (PTC), and a PTC
    call bypasses the tool middleware, so there is nothing above it to contain
    a fault: every path returns an envelope, including the ones that failed.
    """

    #: The tool's name, for the log line. The subclass sets it.
    TOOL_NAME: str = ""

    #: Injected in tests; production reads the clock.
    _now: Any = None

    async def envelope(self, user_info: Mapping[str, Any], **args: Any) -> tools.Envelope:
        """The call, returning the envelope rather than a rendering: what the
        REST route and the chat tool's `content_and_artifact` need."""
        caller_id = caller_of(user_info)
        if not caller_id:
            return denied("authorization required")
        token = CITATION_SESSION.set(str(user_info.get("session_id") or ""))
        try:
            return await self._run(caller_id, args)
        except Exception as e:
            # Never hand the raw exception to the model: driver messages quote
            # the SQL with its bound parameters, and a model echoes what it is
            # given. The type goes to the log, the class to the envelope.
            tool_name = self.TOOL_NAME  # a local the PHI log lint reads as a name
            logger.error("[%s] error_type=%s", tool_name, type(e).__name__, exc_info=not is_driver_exception(e))
            return tools.fault_envelope(e)
        finally:
            CITATION_SESSION.reset(token)

    async def _run(self, caller_id: str, args: Mapping[str, Any]) -> tools.Envelope:
        raise NotImplementedError

    def _clock(self) -> datetime:
        return self._now() if callable(self._now) else datetime.now(UTC)
