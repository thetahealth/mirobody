"""
MixAgent - Two-phase model fusion agent.

Phase 1 (Collector): Data collection with tool calls
Phase 2 (Responder): Response generation with collected context

Components:
- MixMixin: Core two-phase streaming capabilities
- GenerateAnswerMiddleware: Signals end of data collection
"""

from .mixin import MixMixin

__all__ = ["MixMixin"]
