"""
MixAgent - Two-phase model fusion agent.

Phase 1 (Orchestrator): Data collection with tool calls — forced via
    response_format=OrchestratorManifest (auto tool_choice="any").
Phase 2 (Responder): Response generation with collected context, plus
    the manifest's optional `note` as a hint.

Components:
- MixMixin: Core two-phase streaming capabilities
- OrchestratorManifest: Phase-1-end sentinel + lightweight metadata
"""

from .mixin import MixMixin
from .models import OrchestratorManifest

__all__ = ["MixMixin", "OrchestratorManifest"]
