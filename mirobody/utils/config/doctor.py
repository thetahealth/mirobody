"""`mirobody doctor`, and the boot-time self-check: which model does each surface use?

Until this existed a deployment with zero usable keys started, issued tokens,
accepted uploads and answered "indicators: none" (the only hint was one INFO
line, "no models loaded) the agent may be disabled intentionally"
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
    entry_ready,
    keys_present,
    no_provider_message,
    resolve_route,
    retired_model_keys,
    route_candidates,
    unread_entry_keys,
)

#: surface → what a person loses without it.
SURFACES: tuple[tuple[str, str], ...] = (
    ("chat", "the agent's answers (MODELS entries whose key is present)"),
    ("vision", "report photos and scanned pages (UTILS_VISION_MODEL)"),
    ("text", "indicator extraction, titles, summaries (UTILS_TEXT_MODEL)"),
)


#: The status of a surface whose key came from .env, the environment or a
#: config file: nothing has sent it a request yet, so "OK" would be a guess.
UNVERIFIED = "present (unverified)"


@dataclass(frozen=True)
class SurfaceStatus:
    surface: str
    what: str
    provider: str | None   # what the surface selects right now
    model: str | None
    hint: str              # "" when the surface has a provider
    considered: str = ""   # the candidates, for the table
    key: str = ""          # the API key name the provider reads; "" = it reads none
    via: str = ""          # the surface doing this one's work: "ocr" for vision


def _chat_status() -> SurfaceStatus:
    what = SURFACES[0][1]
    entries = chat_entries()
    usable = [n for n, e in entries.items() if entry_ready(e)]
    default = chat_default()
    if default:
        entry = entries.get(default) or {}
        others = [n for n in usable if n != default]
        picked = default + (f" (also {', '.join(others)})" if others else "")
        return SurfaceStatus("chat", what, picked, str(entry.get("model") or "") or None, "",
                             key=str(entry.get("api_key") or "").strip())
    hint = "MODELS is empty — no chat entry is configured at all." if not entries else (
        "none of the MODELS entries has its key: "
        + ", ".join(f"{n} ({(e or {}).get('api_key') or (e or {}).get('base_url')})" for n, e in entries.items())
    )
    return SurfaceStatus("chat", what, None, None, hint)


def _route_status(surface: str, what: str) -> SurfaceStatus:
    considered = ", ".join(c.alias if isinstance(c, RouteSpec) else f"{c}?" for c in route_candidates(surface))
    spec = resolve_route(surface)
    if spec is not None:
        return SurfaceStatus(surface, what, spec.alias, spec.model, "", considered, spec.api_key_env)
    return SurfaceStatus(surface, what, None, None, no_provider_message(surface), considered)


def provider_report() -> list[SurfaceStatus]:
    """One row per surface, against the global configuration.

    With no model that sees but an OCR entry routed (the small local pair:
    MiniCPM5-2B cannot see), report photos and scanned pages go to the OCR
    entry, so vision is covered, not missing: the rule `--probe` applies."""
    vision = _route_status("vision", SURFACES[1][1])
    ocr = resolve_route("ocr")
    if vision.provider is None and ocr is not None:
        vision = SurfaceStatus("vision", vision.what, ocr.alias, ocr.model, "", vision.considered,
                               ocr.api_key_env, via="ocr")
    rows = [_chat_status(), vision, _route_status("text", SURFACES[2][1])]
    # Optional: shown when it reads documents, silent when the vision entry does.
    if ocr is not None:
        rows.append(_route_status("ocr", "report images and pages, text and tables (UTILS_OCR_MODEL)"))
    return rows


def _verified_keys() -> frozenset[str]:
    """The key names the setup page saved: it sends the chosen model a real
    request before it keeps a key (`setup_router._save`). A key from .env, the
    environment or a config file has answered nothing yet."""
    from .settings import allowed_names, set_in_environment

    return allowed_names() - set_in_environment()


def _status(row: SurfaceStatus, verified: frozenset[str]) -> str:
    if not row.provider:
        return "--"
    return "OK" if not row.key or row.key in verified else UNVERIFIED


def format_report(rows: list[SurfaceStatus]) -> str:
    """The `mirobody doctor` table."""
    lines = ["LLM models by surface (config.llm.yaml)", "-" * 72]
    keys = keys_present()
    # A local server needs no key, so "none" is advice only when nothing works.
    advice = "" if any(r.provider for r in rows) else " — choose a model on the setup page, or put ONE key in .env"
    lines.append("keys present   : " + (", ".join(keys) if keys else "none" + advice))
    lines.append("")
    verified = _verified_keys()
    statuses = [_status(r, verified) for r in rows]
    width = max(len(r.surface) for r in rows)
    status_width = max(len(s) for s in statuses)
    for r, status in zip(rows, statuses, strict=True):
        if r.via:
            picked = f"(via {r.via}: {r.provider})"
        elif r.provider:
            picked = r.provider + (f" / {r.model}" if r.model else "")
        else:
            picked = r.what
        lines.append(f"  {r.surface:<{width}}  {status:<{status_width}}  {picked}")
        if not r.provider:
            lines.append(f"  {'':<{width}}  {'':<{status_width}}  {r.hint}")
    if UNVERIFIED in statuses:
        lines.append("")
        lines.append(f"{UNVERIFIED}: the key is set but has not answered a request yet;")
        lines.append("  `mirobody doctor --probe` sends one to each surface")
    retired = retired_model_keys()
    if retired:
        lines.append("")
        lines.append("retired keys   : " + ", ".join(retired) + " — no longer read; a model belongs to a MODELS entry, a surface to UTILS_*")
    unread = unread_entry_keys()
    if unread:
        lines.append("")
        lines.append("unread fields  : nothing in mirobody reads these — check the spelling")
        for alias, fields in unread.items():
            lines.append(f"  {alias}: {', '.join(fields)}")
    lines.append("-" * 72)
    return "\n".join(lines)


def log_report(rows: list[SurfaceStatus], log: logging.Logger) -> None:
    """The boot-time version: one line per surface without a provider, and an
    ERROR when nothing at all is usable: a zero-key server used to boot in
    silence."""
    missing = [r for r in rows if not r.provider]
    for name in retired_model_keys():
        # Bound to a name `phi_lint` recognises.
        key_id = name
        log.warning("config key %s is no longer read (1.4.1): a model belongs to a MODELS entry, and a surface's choice to UTILS_VISION_MODEL / UTILS_TEXT_MODEL in config.llm.yaml", key_id)
    if len(missing) == len(rows):
        # "and restart" sent Docker users to `docker compose restart`, which
        # keeps the container's old environment: the key they had just added
        # was never read, and this line came back unchanged.
        reason = (
            "no LLM API key is set; choose one on the setup page (the server prints its link once "
            "it listens), or put ONE in .env (see config.llm.yaml), then run "
            "`docker compose up -d` (a plain `restart` keeps the old environment), "
            "or start `mirobody serve` again"
        )
        log.error("no LLM model on any surface — chat, file parsing and indicator extraction will fail on every request: %s", reason)  # phi: ok a fixed sentence
        return
    for r in missing:
        surface_type = r.surface
        reason = f"{r.what} — {r.hint}"
        log.warning("no LLM model for %s: %s", surface_type, reason)  # phi: ok the surface's description and its config hint
    for alias, fields in unread_entry_keys().items():
        # `openai-utils` declared `reasoning_effort: none`, nothing read it, and
        # the deployment extracted zero indicators from every report: the
        # field was right and invisible. A declaration no code consumes is
        # worth one line at boot, named.
        # Bound to names `phi_lint` recognises, like the retired-key line above:
        # both are config identifiers out of config.llm.yaml, never a value.
        entry_slug, unread_kind = alias, ", ".join(fields)
        log.warning("MODELS entry %s declares fields nothing reads: %s", entry_slug, unread_kind)
    ok = ", ".join(f"{r.surface}={r.provider}" for r in rows if r.provider)
    log.info("LLM models by surface: %s (`mirobody doctor` for the table)", ok)


__all__ = ["SURFACES", "ROUTE_KEYS", "SurfaceStatus", "provider_report", "format_report", "log_report"]
