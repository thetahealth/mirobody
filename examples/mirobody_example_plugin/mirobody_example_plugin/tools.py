"""An MCP tool from a plugin. Same shape as `mirobody/agent/tools/*`: a class
whose public methods become tools; `__tools__` names the ones to publish."""

from __future__ import annotations


class ExampleService:
    __tools__ = ("example_echo",)

    async def example_echo(self, text: str) -> dict:
        """Return the text you sent, so you can see a plugin tool round-trip.

        Use it once to confirm the plugin is installed; it has no other use.
        `text` is echoed back unchanged.
        """
        return {"echo": text}
