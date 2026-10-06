"""The first-run page's API: choose a vendor key, or run every model locally.

`GET /api/setup` says whether a model is still needed and what each choice
takes; it returns no saved value. `POST /api/setup` changes where health data
is sent, so it takes the setup token: `SETUP_TOKEN` from `.env`, which
`deploy.sh` generates, or the one the app prints at startup while no model is
set up. Once one is, the token alone no longer changes it: the request must
also come from a signed-in session, which the JWT middleware has already held
to its second factor. A token copied out of an old log, or out of a log
shipper, is then not enough to point every later question at another server.
A key is saved only after one real request through it succeeds; a local
address only when the server there serves the models the local entries name.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
import secrets
import time
import urllib.request
from typing import Any, Literal

from fastapi import APIRouter, Header, Request
from pydantic import BaseModel

from mirobody.server.envelope import err, ok
from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/setup", tags=["setup"])

#: What the page calls each vendor. The key names and where to get one are
#: `KEYS_URL`; which model a key turns on is the MODELS table.
_LABELS = {
    "OPENROUTER_API_KEY": "OpenRouter",
    "DASHSCOPE_API_KEY": "Alibaba Cloud Model Studio (Qwen)",
    "GOOGLE_API_KEY": "Google Gemini",
    "OPENAI_API_KEY": "OpenAI",
    "ANTHROPIC_API_KEY": "Anthropic",
    "DEEPSEEK_API_KEY": "DeepSeek",
}

_token = os.environ.get("SETUP_TOKEN") or secrets.token_urlsafe(18)
_failures: dict[str, list[float]] = {}
_MAX_FAILURES, _FAILURE_WINDOW_SEC = 10, 600
#: One save at a time: two would each check a key against a state the other
#: is about to change.
_save_lock = asyncio.Lock()


def setup_token() -> str:
    return _token


def _client(request: Request) -> str:
    return request.client.host if request.client else "?"


def _trusted(request: Request, token: str) -> bool:
    """The token matches; a wrong or missing one counts toward the client's
    limit. Compared as bytes: `compare_digest` refuses a non-ASCII str with
    a TypeError, which was a 500."""
    if token and hmac.compare_digest(token.encode(), _token.encode()):
        return True
    now = time.monotonic()
    if len(_failures) > 1000:
        for client in [c for c, times in _failures.items() if not times or now - times[-1] >= _FAILURE_WINDOW_SEC]:
            del _failures[client]
    _failures.setdefault(_client(request), []).append(now)
    return False


def _refused(client: str) -> bool:
    """Ten wrong tokens in ten minutes from one address. Asked only after a
    wrong one: behind a proxy every client shares an address, and the owner's
    right token must still work while a guesser is being refused."""
    now = time.monotonic()
    recent = [t for t in _failures.get(client, []) if now - t < _FAILURE_WINDOW_SEC]
    _failures[client] = recent
    return len(recent) >= _MAX_FAILURES


def _would_leave(name: str, value: str, cleared: frozenset[str]):
    """The lookup (`llm.Lookup`) of the state a save would leave: `name` set
    to `value`, the page's other choices gone, everything else as it is. Asked
    instead of changing `os.environ` for the check, which every other request
    in this process reads: a chat summary sent mid-check went to the vendor
    under a key nobody had accepted yet."""
    from mirobody.utils.config import safe_read_cfg

    def lookup(n: str) -> str:
        if n == name:
            return value
        return "" if n in cleared else (safe_read_cfg(n, "") or "")

    return lookup


def _local_setup() -> dict[str, Any]:
    from mirobody.utils.config.config import global_config

    cfg = global_config()
    value = cfg.get("LOCAL_SETUP") if cfg else None
    return value if isinstance(value, dict) else {}


def _candidates() -> list[str]:
    urls = _local_setup().get("base_urls") or []
    return [str(u) for u in urls if str(u).startswith(("http://", "https://"))]


def _chat_model() -> str:
    """The model the default chat entry runs, which is what a person knows it
    by ("qwen3.8-27b"), rather than the entry's name ("local")."""
    from mirobody.utils.config.llm import chat_default, chat_entries

    name = chat_default()
    return str((chat_entries().get(name) or {}).get("model") or name) if name else ""


def _entry_for_key(name: str) -> tuple[str, dict] | None:
    from mirobody.utils.config.llm import chat_entries

    for entry_name, entry in chat_entries().items():
        if str(entry.get("api_key") or "") == name:
            return entry_name, entry
    return None


def _local_models() -> dict[str, str]:
    from mirobody.utils.config.llm import model_entries

    table = model_entries()
    return {role: str((table.get(entry) or {}).get("model") or "")
            for role, entry in (("agent", "local"), ("utils", "local-utils"), ("ocr", "local-ocr"))}


def _served(base_url: str) -> dict[str, str]:
    """Model id -> status as the server reports it: llama.cpp's router says
    unloaded, loading or loaded; any other server just lists what it serves."""
    root = base_url.rstrip("/").removesuffix("/v1")
    for url, router_listing in ((root + "/models", True), (base_url.rstrip("/") + "/models", False)):
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                import json

                rows = json.load(response).get("data") or []
        except Exception:
            continue
        return {str(m.get("id")): (str((m.get("status") or {}).get("value") or "ready") if router_listing else "ready")
                for m in rows}
    return {}


def _load(base_url: str, model: str) -> None:
    """Ask llama.cpp's router to fetch and load a model now rather than on the
    first question. Another server has no such call, which is fine."""
    import json

    root = base_url.rstrip("/").removesuffix("/v1")
    request = urllib.request.Request(root + "/models/load", json.dumps({"model": model}).encode(),
                                     {"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception as e:
        logger.info("model not preloaded: error_type=%s", type(e).__name__)


@router.get("")
async def setup_state(request: Request, x_setup_token: str = Header(default="")) -> Any:
    """Anyone may learn whether a model is needed and what each choice takes;
    which keys are set, the model in use and the local address take the token."""
    from mirobody.utils.config import settings
    from mirobody.utils.config.llm import KEYS_URL, chat_default

    trusted = bool(x_setup_token) and _trusted(request, x_setup_token)
    fixed = settings.set_in_environment() if trusted else frozenset()
    providers = []
    for name, url in KEYS_URL.items():
        found = _entry_for_key(name)
        providers.append({"key": name, "label": _LABELS.get(name, name), "get_key_url": url,
                          "model": str(found[1].get("model") or "") if found else "",
                          "set": trusted and bool(os.environ.get(name)), "in_env_file": name in fixed})
    local = _local_setup()
    base_url = (os.environ.get("LOCAL_BASE_URL") or "") if trusted else ""
    ocr_base_url = os.environ.get("LOCAL_OCR_BASE_URL") or base_url
    models = _local_models()
    status = {}
    if base_url:
        status = await asyncio.to_thread(_served, base_url)
        if ocr_base_url != base_url:
            status = {**status, **await asyncio.to_thread(_served, ocr_base_url)}
    return ok({
        "needed": chat_default() is None,
        "trusted": trusted,
        "chat_model": _chat_model() if trusted else "",
        "providers": providers,
        "local": {
            "configured": bool(base_url),
            "base_url": base_url,
            "candidates": _candidates(),
            "models": models,
            "status": {role: status.get(model, "missing") for role, model in models.items() if model} if base_url else {},
            "download_gb": local.get("download_gb"),
            "memory_gb": local.get("memory_gb"),
            "preset": local.get("preset") or "",
            "in_env_file": "LOCAL_BASE_URL" in fixed,
        },
    })


class SetupChoice(BaseModel):
    mode: Literal["key", "local"]
    name: str = ""
    value: str = ""
    base_url: str = ""
    ocr_base_url: str = ""


@router.post("")
async def setup_save(choice: SetupChoice, request: Request, x_setup_token: str = Header(default="")) -> Any:
    from mirobody.utils.config.llm import chat_default

    if not _trusted(request, x_setup_token):
        if _refused(_client(request)):
            return err(429, "Too many wrong setup tokens. Wait ten minutes.")
        return err(403, "The setup token is wrong. It is SETUP_TOKEN in the .env next to compose.yaml, "
                        "or the one the app prints at startup while no model is set up.")
    if chat_default() is not None and not getattr(request.state, "user_id", 0):
        return err(401, "Sign in to change the model. Once a model is set up, the setup token alone does not change it.")
    async with _save_lock:
        try:
            return await _save(choice)
        except Exception as e:
            logger.warning("setup not saved: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))
            return err(500, f"The choice could not be saved ({type(e).__name__}); the app's log has the details.")


async def _save(choice: SetupChoice) -> Any:
    from mirobody.agent import probe, registry
    from mirobody.utils.config import settings
    from mirobody.utils.config.llm import KEYS_URL, chat_default

    fixed = settings.set_in_environment()
    if choice.mode == "key":
        name, value = choice.name.strip(), choice.value.strip()
        if name not in KEYS_URL or not value:
            return err(400, "Choose a supported service and paste its key.")
        if name in fixed:
            return err(409, f"{name} is set in .env; change it there.")
        if not _entry_for_key(name):
            return err(400, f"No chat model uses {name}.")
        # Tried in the state the save would leave: the page's earlier choices
        # gone, `.env` as it is. A key there that ranks higher would keep
        # answering, so the page says so instead of saving.
        from mirobody.utils.config.llm import chat_entries, read_api_key

        lookup = _would_leave(name, value, settings.allowed_names() - fixed)
        winner = chat_default(lookup) or ""
        winner_key = str((chat_entries().get(winner) or {}).get("api_key") or "")
        passed, detail = False, ""
        if winner_key == name:
            def resolve(n: str) -> str | None:
                return (read_api_key(n, lookup) if n.endswith("_API_KEY") else lookup(n)) or None

            try:
                passed, detail = await asyncio.wait_for(probe._chat(winner, resolve=resolve), timeout=90)
            except Exception as e:
                passed, detail = False, type(e).__name__
        if not winner:
            return err(400, f"No chat model uses {name}.")
        if winner_key != name:
            return err(409, f"{winner_key or winner} is set in .env and is used before {name}; change it there.")
        if not passed:
            return err(400, f"The key did not work: {detail}")
        await settings.save(dict.fromkeys(settings.allowed_names() - fixed, "") | {name: value})
    else:
        if "LOCAL_BASE_URL" in fixed:
            return err(409, "LOCAL_BASE_URL is set in .env; change it there.")
        # A vendor key is chosen before the local models whenever both exist.
        keys_in_env = sorted(n for n in fixed if n in KEYS_URL)
        if keys_in_env:
            return err(409, f"{', '.join(keys_in_env)} is set in .env, and a key is used before local models. "
                            "Remove it from .env, run docker compose up -d, and choose local again.")
        given = choice.base_url.strip()
        if given and not given.startswith(("http://", "https://")):
            return err(400, "Give the address of the model server, e.g. http://host.docker.internal:8080/v1.")
        models = _local_models()
        wanted = sorted({m for m in models.values() if m})
        base_url, ocr_base_url, answers = "", "", []
        for candidate in [given] if given else _candidates():
            ocr = (choice.ocr_base_url.strip() if given else "") or candidate
            served = await asyncio.to_thread(_served, candidate)
            if ocr != candidate:
                served = {**served, **await asyncio.to_thread(_served, ocr)}
            if served and not set(wanted) - set(served):
                base_url, ocr_base_url = candidate, ocr
                break
            if served:
                answers.append(f"{candidate} serves {', '.join(sorted(served))}")
        if not base_url:
            if answers:
                return err(400, f"No server serves {', '.join(wanted)}: {'; '.join(answers)}.")
            tried = given or ", ".join(_candidates())
            return err(400, f"No model server answered at {tried}. Start it and try again.")
        await settings.save(dict.fromkeys(KEYS_URL, "") | {"LOCAL_BASE_URL": base_url, "LOCAL_OCR_BASE_URL": ocr_base_url})
        for model in {models["agent"], models["ocr"]} - {""}:
            await asyncio.to_thread(_load, ocr_base_url if model == models["ocr"] else base_url, model)

    registry.reload_llm_clients()
    return ok({"needed": chat_default() is None, "chat_model": _chat_model()})
