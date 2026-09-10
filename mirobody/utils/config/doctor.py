"""`mirobody doctor`, and the boot-time self-check: which model does each surface use?

Until this existed a deployment with zero usable keys started, issued tokens,
accepted uploads and answered "indicators: none" — the only hint was one INFO
line, "no models loaded — the agent may be disabled intentionally"
(#68's reporter had one key, in the wrong table, and saw nothing at all).
The same judgement now runs at boot (a WARNING per surface with nothing, an
ERROR when no surface has anything) and on demand from the CLI, from the
routes in `config.llm.yaml`.

Deliberately NOT exposed on `/api/health`: that route is unauthenticated, and
which vendors a deployment holds keys for is not public information.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .llm import (
    ROUTE_KEYS,
    RouteSpec,
    chat_default,
    chat_entries,
    keys_present,
    no_provider_message,
    read_api_key,
    resolve_route,
    retired_model_keys,
    route_candidates,
)

#: surface → what a person loses without it.
SURFACES: tuple[tuple[str, str], ...] = (
    ("chat", "the agent's answers (MODELS entries whose key is present)"),
    ("vision", "report photos and scanned pages (UTILS_VISION_MODEL)"),
    ("text", "indicator extraction, titles, summaries (UTILS_TEXT_MODEL)"),
    ("embedding", "semantic search over indicator names (UTILS_EMBEDDING_MODEL; without it, the lexical index answers)"),
)


@dataclass(frozen=True)
class SurfaceStatus:
    surface: str
    what: str
    provider: str | None   # what the surface selects right now
    model: str | None
    hint: str              # "" when the surface has a provider
    considered: str = ""   # the candidates, for the table


def _chat_status() -> SurfaceStatus:
    what = SURFACES[0][1]
    entries = chat_entries()
    usable = [n for n, e in entries.items() if not (e or {}).get("api_key") or read_api_key(str((e or {}).get("api_key")))]
    default = chat_default()
    if default:
        others = [n for n in usable if n != default]
        picked = default + (f" (also {', '.join(others)})" if others else "")
        return SurfaceStatus("chat", what, picked, str((entries.get(default) or {}).get("model") or "") or None, "")
    hint = "MODELS is empty — no chat entry is configured at all." if not entries else (
        "none of the MODELS entries has its key: " + ", ".join(f"{n} ({(e or {}).get('api_key')})" for n, e in entries.items())
    )
    return SurfaceStatus("chat", what, None, None, hint)


def _route_status(surface: str, what: str) -> SurfaceStatus:
    considered = ", ".join(c.alias if isinstance(c, RouteSpec) else f"{c}?" for c in route_candidates(surface))
    spec = resolve_route(surface)
    if spec is not None:
        return SurfaceStatus(surface, what, spec.alias, spec.model, "", considered)
    return SurfaceStatus(surface, what, None, None, no_provider_message(surface), considered)


def _embedding_status() -> SurfaceStatus:
    from ..embedding import embedding_model_id, resolve_embedding_provider

    what = SURFACES[3][1]
    provider = resolve_embedding_provider()
    if provider:
        return SurfaceStatus("embedding", what, provider, embedding_model_id(provider) or None, "")
    return SurfaceStatus("embedding", what, None, None, no_provider_message("embedding"))


def provider_report(cfg=None) -> list[SurfaceStatus]:
    """One row per surface, against the current configuration. `cfg` is
    accepted for the callers that pass one; the routes read the global."""
    return [
        _chat_status(),
        _route_status("vision", SURFACES[1][1]),
        _route_status("text", SURFACES[2][1]),
        _embedding_status(),
    ]


def format_report(rows: list[SurfaceStatus]) -> str:
    """The `mirobody doctor` table."""
    lines = ["LLM models by surface (config.llm.yaml)", "-" * 72]
    keys = keys_present()
    lines.append("keys present   : " + (", ".join(keys) if keys else "none — put ONE in .env"))
    lines.append("")
    width = max(len(r.surface) for r in rows)
    for r in rows:
        if r.provider:
            picked = r.provider + (f" / {r.model}" if r.model else "")
            lines.append(f"  {r.surface:<{width}}  OK    {picked}")
        else:
            lines.append(f"  {r.surface:<{width}}  --    {r.what}")
            lines.append(f"  {'':<{width}}        {r.hint}")
    retired = retired_model_keys()
    if retired:
        lines.append("")
        lines.append("retired keys   : " + ", ".join(retired) + " — no longer read; a model belongs to a MODELS entry, a surface to UTILS_*")
    lines.append("-" * 72)
    return "\n".join(lines)


def log_report(rows: list[SurfaceStatus], log: logging.Logger) -> None:
    """The boot-time version: one line per surface without a provider, and an
    ERROR when nothing at all is usable — a zero-key server used to boot in
    silence."""
    missing = [r for r in rows if not r.provider]
    for name in retired_model_keys():
        # Bound to a name `phi_lint` recognises.
        key_id = name
        log.warning("config key %s is no longer read (1.4.1): a model belongs to a MODELS entry, and a surface's choice to UTILS_VISION_MODEL / UTILS_TEXT_MODEL / UTILS_EMBEDDING_MODEL in config.llm.yaml", key_id)
    if len(missing) == len(rows):
        reason = "no LLM API key is set; put ONE in .env (see config.llm.yaml) and restart"
        log.error("no LLM model on any surface — chat, file parsing and indicator extraction will fail on every request: %s", reason)
        return
    for r in missing:
        surface_type = r.surface
        reason = f"{r.what} — {r.hint}"
        log.warning("no LLM model for %s: %s", surface_type, reason)
    ok = ", ".join(f"{r.surface}={r.provider}" for r in rows if r.provider)
    log.info("LLM models by surface: %s (`mirobody doctor` for the table)", ok)


__all__ = ["SURFACES", "ROUTE_KEYS", "SurfaceStatus", "provider_report", "format_report", "log_report"]
