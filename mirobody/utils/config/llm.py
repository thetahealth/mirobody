"""Model routing read from configuration.

Nothing in this module names a model. `config.llm.yaml` does: `MODELS` is a
table of entries (alias → llm_type / api_key NAME / base_url / model /
capabilities; "providers" in this project are devices), and three keys say
which entry each utility surface uses: `UTILS_VISION_MODEL` (report photos,
scanned pages) and `UTILS_TEXT_MODEL` (indicator extraction from text, titles,
summaries).
A value is an entry name, a list of them (the FIRST whose key is present wins,
that is how one key runs everything), a `provider/model` string, or an inline
spec shaped like an entry. The chat picker is the `MODELS` table itself,
first present key first. Every one of these is a plain environment variable,
so whatever a deployment already uses to hand out configuration can set them
without this package knowing about it.

Why routing is data. Issue #68: a deployment with `DEEPSEEK_API_KEY` alone
uploaded three documents into silence. The key was declared in config.yaml and
absent from four of the five Python tables that decided which provider a
surface used, and whether a provider could read an image was recorded nowhere
it was implied by membership in a list. A first fix moved the five tables
into one Python registry; the owner's verdict on that was that a platform's
users must be able to read AND change every routing decision in config.yaml,
without a release. So the registry became `config.llm.yaml`, and this module
only knows how to read it.

Selection happens once per surface, on first use. A call that then fails is a
failure, reported with the entry and the vendor's message, not a reason to
try the next entry: two reports of one person must not be read by two
different models.

What does live here as code: the key-name aliases vendors document
(`GEMINI_API_KEY` for `GOOGLE_API_KEY`), the endpoint a bare `provider/model`
string implies (the ONE table of URL literals, each the fallback for
`<PREFIX>_BASE_URL`), and the Vertex hostname rules. There is no embedding
surface: the semantic tier it served was deleted in 1.5.0, and the client
factory for it (`LLMConfig`, `Config.get_llm`) had no other caller.

NOT the same thing as `mirobody/utils/llm/`, that package holds the callers
(structured extraction, text, vision dispatch). Both read the same routes.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# The three things that are code, not config.

#: Where a bare `provider/model` route value points, and which key it reads.
#: The one table of endpoint literals outside config.yaml; each URL is the
#: fallback for `<PREFIX>_BASE_URL`. Gemini's is Google's OpenAI-COMPATIBLE
#: endpoint; Anthropic's is its NATIVE base (the SDK appends `/v1/messages`),
#: because `anthropic` is a family the utility surfaces call directly:
#: its compatibility endpoint cannot serve schema-constrained JSON.
KNOWN_ENDPOINTS: dict[str, tuple[str, str]] = {
    "openai":     ("OPENAI_API_KEY",     "https://api.openai.com/v1"),
    "openrouter": ("OPENROUTER_API_KEY", "https://openrouter.ai/api/v1"),
    "dashscope":  ("DASHSCOPE_API_KEY",  "https://dashscope.aliyuncs.com/compatible-mode/v1"),
    "deepseek":   ("DEEPSEEK_API_KEY",   "https://api.deepseek.com/v1"),
    "gemini":     ("GOOGLE_API_KEY",     "https://generativelanguage.googleapis.com/v1beta/openai/"),
    "anthropic":  ("ANTHROPIC_API_KEY",  "https://api.anthropic.com"),
}

#: Where to get each key: for the message a person reads when none is set.
KEYS_URL: dict[str, str] = {
    "OPENROUTER_API_KEY": "https://openrouter.ai/keys",
    "DASHSCOPE_API_KEY":  "https://dashscope.console.aliyun.com/apiKey",
    "GOOGLE_API_KEY":     "https://aistudio.google.com/apikey",
    "OPENAI_API_KEY":     "https://platform.openai.com/api-keys",
    "DEEPSEEK_API_KEY":   "https://platform.deepseek.com/api_keys",
    "ANTHROPIC_API_KEY":  "https://platform.claude.com/settings/keys",
}

#: Env names that mean the same key. Google's own docs and SDK say
#: `GEMINI_API_KEY`; models.dev lists all three. A key pasted under the name
#: its vendor documents must not read as "no key" on any surface.
KEY_ALIASES: dict[str, tuple[str, ...]] = {
    "GOOGLE_API_KEY": ("GEMINI_API_KEY", "GOOGLE_GENERATIVE_AI_API_KEY"),
}

#: The `llm_type` values a utility surface (vision, text, structured) can call.
#: `openai`/`openrouter` go through `utils.llm.clients`, `anthropic` through
#: `utils.llm.backends_anthropic`. A chat-only family (google_genai,
#: google_anthropic_vertex) is not one of these: it reaches models through
#: LangChain, which is the agent's path, not extraction's.
UTILITY_FAMILIES = ("openai", "openrouter", "anthropic")

#: surface → the config key that routes it.
ROUTE_KEYS: dict[str, str] = {
    "vision": "UTILS_VISION_MODEL",
    "text": "UTILS_TEXT_MODEL",
    # Optional: a document-OCR model (GLM-OCR) that takes over reading report
    # images and pages from the vision entry. Unset, the vision entry reads them.
    "ocr": "UTILS_OCR_MODEL",
}

#: Vertex locations served from a MULTI-REGIONAL endpoint, whose hostname is
#: neither the global one nor the `<region>-` one. Not a guess: both official
#: SDKs carry exactly this set: `google.genai._api_client._MULTI_REGIONAL_LOCATIONS`
#: and `anthropic.lib.vertex._client`, which hardcodes
#: `https://aiplatform.us.rep.googleapis.com/v1`.
_VERTEX_MULTI_REGIONS = frozenset({"us", "eu"})


def vertex_location(location: str) -> str:
    """The canonical spelling of a Vertex location: stripped, lowercased, and
    `global` when nothing is named.

    Exists so a caller that needs the location in a URL PATH as well as in the
    hostname normalises once and uses that value twice. `location: " US "`
    otherwise built the right host and the path `/locations/ US /`, a 404 that
    names nothing. (#75)
    """
    return (location or "").strip().lower() or "global"


def vertex_host(location: str) -> str:
    """The `aiplatform` hostname for a Vertex location. THREE shapes, not two.

        global      aiplatform.googleapis.com
        us / eu     aiplatform.<loc>.rep.googleapis.com     <- the one that was missing
        <region>    <loc>-aiplatform.googleapis.com

    The multi-regional pair is where the newest Claude and Gemini releases are
    often published, and a deployment that must keep data in the US cannot use
    `global`, so `location: us` is the only value that satisfies both. That is
    exactly the value the two-shape formula got wrong, and it got it wrong
    silently: `us-aiplatform.googleapis.com` is not a host, so the failure is a
    name that does not resolve rather than an error naming the cause. (#73)
    """
    loc = vertex_location(location)
    if loc == "global":
        return "aiplatform.googleapis.com"
    if loc in _VERTEX_MULTI_REGIONS:
        return f"aiplatform.{loc}.rep.googleapis.com"
    return f"{loc}-aiplatform.googleapis.com"


class NoProviderError(ValueError):
    """No routable entry exists for a surface. A `ValueError` so the callers
    that already catch one keep working; the message is for the person who
    has to fix the deployment, not for a log."""


#-----------------------------------------------------------------------------
# Keys.

def read_api_key(env_name: str) -> str:
    """The value of `env_name`, or of any name that means the same key.

    THE admission function: every surface that decides "is this entry usable"
    goes through here (through `safe_read_cfg`, which tests control), so the
    surfaces cannot disagree about what counts as a key being present.
    """
    from . import safe_read_cfg

    for name in (env_name, *KEY_ALIASES.get(env_name, ())):
        value = (safe_read_cfg(name, "") or "").strip()
        if value:
            return value
    return ""


def is_endpoint_name(value: str) -> bool:
    """Whether an entry's `base_url` is a NAME to look up (`LOCAL_BASE_URL`)
    rather than a URL. The agent's client builder already read it that way."""
    return bool(value) and "://" not in value


def endpoint_value(name: str) -> str:
    """The URL a `base_url` NAME holds (environment first), or ""."""
    from . import safe_read_cfg

    return (safe_read_cfg(name, "") or "").strip()


def entry_ready(entry: dict[str, Any] | None) -> bool:
    """Whether an entry's key and endpoint are both there: the one test the
    chat picker, its default and `mirobody doctor` share. An entry whose
    `base_url` names an unset variable is off: that is how the shipped
    `local` entries stay out of the way until `LOCAL_BASE_URL` is set."""
    entry = entry or {}
    ref = str(entry.get("api_key") or "").strip()
    if ref and not read_api_key(ref):
        return False
    base = str(entry.get("base_url") or "").strip()
    return not is_endpoint_name(base) or bool(endpoint_value(base) or base_url_override(ref))


def base_url_override(api_key_env: str) -> str:
    """`<PREFIX>_BASE_URL` for the key named `api_key_env` (OPENROUTER_API_KEY
    → OPENROUTER_BASE_URL), or "". The one redirect rule, applied to every
    entry that reads the key: chat, vision, text (#52)."""
    from . import safe_read_cfg

    if not api_key_env.endswith("_API_KEY"):
        return ""
    prefix = api_key_env[: -len("_API_KEY")]
    names = [prefix] + [a[: -len("_API_KEY")] for a in KEY_ALIASES.get(api_key_env, ()) if a.endswith("_API_KEY")]
    for name in names:
        if value := (safe_read_cfg(f"{name}_BASE_URL", "") or "").strip():
            return value
    return ""


#-----------------------------------------------------------------------------
# Entries and routes.

@dataclass(frozen=True)
class RouteSpec:
    """One resolved model for one surface: everything a client needs."""

    alias: str                     # MODELS entry name, "provider/model", or "<inline>"
    model: str
    api_key_env: str               # the NAME of the key; "" = the endpoint needs none
    base_url: str                  # `<PREFIX>_BASE_URL` already applied
    llm_type: str = "openai"
    supports_image: bool | None = None   # None = the entry does not say
    supports_pdf: bool | None = None
    response_format: str = "json_schema"   # what this endpoint accepts; see RESPONSE_FORMATS
    reasoning_effort: str | None = None    # sent only when the entry declares it
    extra_body: dict[str, Any] = field(default_factory=dict)
    temperature: float | None = None
    base_url_env: str = ""         # the NAME `base_url` was given as; "" = a literal URL
    ocr_prompts: dict[str, str] = field(default_factory=dict)   # OCR entries: {"text": ..., "tables": ...}
    timeout: float | None = None      # seconds per request; None = the SDK's 600
    max_retries: int | None = None    # None = the SDK's 2

    @property
    def takes_json_object(self) -> bool:
        """Whether `response_format: {"type": "json_object"}` may be sent: the
        vision path's only use of the parameter."""
        return self.response_format in ("json_schema", "json_object")

    @property
    def key(self) -> str:
        return read_api_key(self.api_key_env) if self.api_key_env else ""

    @property
    def routable(self) -> bool:
        if self.base_url_env and not self.base_url:
            return False
        return bool(self.model) and (not self.api_key_env or bool(self.key))

    @property
    def label(self) -> str:
        return f"{self.alias} ({self.model})" if self.alias != self.model else self.model


#: What an entry's `response_format` may say. Measured behaviour, not a
#: capability ladder, one vendor per value (measured 2026-09-10):
#:
#:   json_schema   OpenAI structured outputs; the default
#:   json_object   loose JSON mode only. DeepSeek answers "This response_format
#:                 type is unavailable now" to a schema.
#:   none          the parameter cannot be sent; the schema goes in the prompt.
#:                 Anthropic's compatibility endpoint rejects `json_object`.
RESPONSE_FORMATS = ("json_schema", "json_object", "none")


def _response_format(alias: str, value: Any) -> str:
    if value is None:
        return "json_schema"
    text = str(value).strip().lower()
    if text in RESPONSE_FORMATS:
        return text
    logger.warning(  # phi: ok configuration identifiers: an entry name, the bad value, the accepted spellings
        "MODELS entry %s: response_format %r is not one of %s — treating it as json_schema",
        alias, value, ", ".join(RESPONSE_FORMATS),
    )
    return "json_schema"


def _flag(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes", "on")


def _spec_from_mapping(alias: str, entry: dict[str, Any]) -> RouteSpec | None:
    """An entry (or inline spec) → RouteSpec, or None when it cannot serve an
    OpenAI-compatible utility surface (no model, or a chat-only llm_type)."""
    model = str(entry.get("model") or "").strip()
    llm_type = str(entry.get("llm_type") or "openai").strip().lower().replace("-", "_")
    # A utility surface can call two families: any OpenAI-compatible endpoint,
    # and Anthropic's own API (`backends_anthropic`, for the structured
    # outputs its compatibility endpoint does not serve).
    if not model or llm_type not in UTILITY_FAMILIES:
        return None
    api_key_env = str(entry.get("api_key") or "").strip()
    base_url = str(entry.get("base_url") or "").strip()
    base_url_env = ""
    if is_endpoint_name(base_url):
        base_url_env, base_url = base_url, endpoint_value(base_url)
    elif not base_url and api_key_env:
        for _name, (key, url) in KNOWN_ENDPOINTS.items():
            if key == api_key_env:
                base_url = url
                break
    base_url = base_url_override(api_key_env) or base_url
    temperature = entry.get("temperature")
    return RouteSpec(
        alias=alias, model=model, api_key_env=api_key_env, base_url=base_url, llm_type=llm_type,
        supports_image=_flag(entry.get("supports_image")), supports_pdf=_flag(entry.get("supports_pdf")),
        response_format=_response_format(alias, entry.get("response_format")),
        reasoning_effort=(str(entry["reasoning_effort"]).strip() or None) if entry.get("reasoning_effort") else None,
        extra_body=dict(entry.get("extra_body") or {}),
        temperature=float(temperature) if isinstance(temperature, (int, float)) else None,
        base_url_env=base_url_env,
        ocr_prompts={str(k): str(v) for k, v in (entry.get("ocr_prompts") or {}).items()},
        timeout=float(entry["timeout"]) if isinstance(entry.get("timeout"), (int, float)) else None,
        max_retries=int(entry["max_retries"]) if isinstance(entry.get("max_retries"), int) else None,
    )


def _spec_from_string(value: str, entries: dict[str, dict]) -> RouteSpec | None:
    """An entry name, or `provider/model` against `KNOWN_ENDPOINTS`."""
    value = value.strip()
    if value in entries:
        return _spec_from_mapping(value, entries[value] or {})
    if "/" in value:
        provider, model = value.split("/", 1)
        name = provider.strip().lower()
        known = KNOWN_ENDPOINTS.get(name)
        if known and model.strip():
            key, url = known
            return RouteSpec(
                alias=value, model=model.strip(), api_key_env=key,
                base_url=base_url_override(key) or url,
                # `anthropic/claude-…` means the native family, the same as an
                # entry writing `llm_type: anthropic`: the shorthand must not
                # be the one spelling that lands on the weaker endpoint.
                llm_type="anthropic" if name == "anthropic" else "openai",
            )
    return None


#: Entry keys something actually reads. `RouteSpec` is a whitelist: a key it
#: does not name is dropped on the floor, silently, which is how `openai-utils`
#: came to declare `reasoning_effort: none` (REQUIRED there: without it
#: gpt-5.6-terra keeps reasoning on and then refuses the extraction callers'
#: `temperature: 0`) and have it read by nobody. `unread_entry_keys` turns that
#: into a line at boot instead of zero indicators over a successful upload.
KNOWN_ENTRY_KEYS: frozenset[str] = frozenset({
    # read here, into a RouteSpec
    "llm_type", "api_key", "base_url", "model", "temperature",
    "supports_image", "supports_pdf", "response_format", "reasoning_effort",
    "extra_body", "chat", "ocr_prompts", "timeout", "max_retries",
    # read by the agent's client builder (`agent/models/clients.py`)
    "profile", "thinking_style", "auth_type", "prompt_cache", "response_with_tools",
    "project", "location", "reasoning", "max_tokens", "max_output_tokens",
    "streaming", "stream_usage", "model_kwargs", "output_config", "stream_chunk_timeout",
})


def unread_entry_keys() -> dict[str, list[str]]:
    """`{entry alias: keys nothing reads}`: dead configuration, by name."""
    out: dict[str, list[str]] = {}
    for alias, entry in model_entries().items():
        unknown = sorted(k for k in (entry or {}) if k not in KNOWN_ENTRY_KEYS)
        if unknown:
            out[alias] = unknown
    return out


def model_entries() -> dict[str, dict[str, Any]]:
    """The `MODELS` table as configured ({} with no Config loaded)."""
    from .config import global_config

    cfg = global_config()
    if cfg is None:
        return {}
    return dict((cfg.get_agent_settings() or {}).get("providers") or {})


def route_value(surface: str) -> Any:
    """The raw configured value for a surface: a string (an entry name,
    `provider/model`, or JSON) through `safe_read_cfg` (environment first, and
    the one place tests control), else the structured value (a list, a spec)
    from the config file."""
    from . import safe_read_cfg
    from .config import global_config

    key = ROUTE_KEYS[surface]
    text = (safe_read_cfg(key, "") or "").strip()
    if text:
        value = _maybe_json(text)
        # `get_str` renders a YAML list as its Python repr ("['a', 'b']"),
        # which is not JSON and not a value: the structured read below has it.
        if not (isinstance(value, str) and value.startswith("[")):
            return value
    cfg = global_config()
    if cfg is None:
        return None
    raw = cfg.get(key)
    return _maybe_json(raw) if isinstance(raw, str) else raw


def _maybe_json(text: str) -> Any:
    text = text.strip()
    if text[:1] in "[{":
        try:
            return json.loads(text)
        except ValueError:
            return text
    return text


def route_candidates(surface: str) -> list[RouteSpec | str]:
    """Every candidate the surface's key names, in order. A string in the
    result is a candidate that could not be turned into a spec (an unknown
    entry name), kept so the doctor can name it."""
    raw = route_value(surface)
    if raw is None or raw == "":
        return []
    items = raw if isinstance(raw, list) else [raw]
    entries = model_entries()
    out: list[RouteSpec | str] = []
    for item in items:
        if isinstance(item, dict):
            spec = _spec_from_mapping("<inline>", item)
            out.append(spec or "<inline spec without a model>")
        elif isinstance(item, str) and item.strip():
            out.append(_spec_from_string(item, entries) or item.strip())
    return out


def _fits(surface: str, spec: RouteSpec) -> bool:
    if surface == "ocr":
        return spec.supports_image is not False and bool(spec.ocr_prompts)
    return not (surface == "vision" and spec.supports_image is False)


def resolve_route(surface: str) -> RouteSpec | None:
    """The spec `surface` uses right now: the first candidate whose key is
    present and which the surface accepts (vision skips an entry that says
    `supports_image: false`). None when there is none."""
    for candidate in route_candidates(surface):
        if isinstance(candidate, RouteSpec) and candidate.routable and _fits(surface, candidate):
            return candidate
    return None


def resolve_named(value: str, *, model: str | None = None) -> RouteSpec | None:
    """A caller-named route (an entry name or `provider/model`) with an
    optional model override. For the `provider=` parameters the utility
    functions keep for explicit callers."""
    spec = _spec_from_string(value, model_entries())
    if spec is None:
        return None
    if model:
        spec = RouteSpec(**{**spec.__dict__, "model": model})
    return spec


def no_provider_message(surface: str) -> str:
    """One actionable sentence: which key routes the surface, what it lists,
    which keys those need, and where to get one."""
    key = ROUTE_KEYS[surface]
    candidates = route_candidates(surface)
    if not candidates:
        return (
            f"No {surface} model available: {key} is not set. In config.llm.yaml, set it to a "
            f"MODELS entry (or a list of them), or to provider/model."
        )
    names, keys, urls, skipped = [], [], [], []
    for c in candidates:
        if isinstance(c, str):
            names.append(f"{c} (not a MODELS entry)")
            continue
        names.append(f"{c.alias} ({c.api_key_env or c.base_url_env or 'no key'})")
        if c.api_key_env and not c.key:
            keys.append(c.api_key_env)
        elif c.base_url_env and not c.base_url:
            urls.append(c.base_url_env)
        elif not _fits(surface, c):
            skipped.append(f"{c.alias} declares supports_image: false")
    keys, urls = list(dict.fromkeys(keys)), list(dict.fromkeys(urls))
    where = ", ".join(f"{k} ({KEYS_URL[k]})" if k in KEYS_URL else k for k in keys)
    if urls:
        where += (", or " if where else "") + " / ".join(urls) + " (the URL of your own model server)"
    text = f"No {surface} model available: {key} lists {', '.join(names)}"
    if keys or urls:
        text += f"; none of these keys is set. Put ONE in .env: {where}."
    elif skipped:
        text += f"; {'; '.join(skipped)} — set {key} to an entry whose model reads images."
    else:
        text += "."
    return text


def chat_entries() -> dict[str, dict[str, Any]]:
    """`MODELS` minus the utility-only entries (`chat: false`), in order."""
    return {
        name: entry for name, entry in model_entries().items()
        if _flag((entry or {}).get("chat")) is not False
    }


def chat_default() -> str | None:
    """The chat picker's default: the first `MODELS` entry (config order,
    utility-only entries excluded) that `entry_ready` admits. None when none is."""
    for name, entry in chat_entries().items():
        if entry_ready(entry):
            return name
    return None


def keys_present() -> list[str]:
    """Every key name a `MODELS` entry reads, in order, that is set."""
    out: list[str] = []
    for entry in model_entries().values():
        ref = str((entry or {}).get("api_key") or "").strip()
        if ref and ref not in out and read_api_key(ref):
            out.append(ref)
    return out


#: Keys 1.4.1 dropped, and what replaced them. `<PREFIX>_MODEL` /
#: `_VISION_MODEL` / `_EMBEDDING_MODEL` chose a model per KEY; a model now
#: belongs to a MODELS entry, and a surface's choice to UTILS_*. Named, not
#: silently ignored: a deployment that set them would otherwise get the
#: shipped default and see nothing wrong.
_RETIRED_MODEL_KEY = re.compile(r"^(OPENROUTER|DASHSCOPE|QWEN|GOOGLE|GEMINI|OPENAI|DEEPSEEK)_(VISION_MODEL|EMBEDDING_MODEL|MODEL)$")


def retired_model_keys() -> list[str]:
    """The retired per-key model overrides that are still set (env or file)."""
    from .config import global_config

    names = set(os.environ)
    cfg = global_config()
    if cfg is not None:
        names |= set(cfg._raw)
    return sorted(n for n in names if _RETIRED_MODEL_KEY.match(n))
