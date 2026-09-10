"""Model routing read from configuration, and `LLMConfig`, the `Config` family's member for it.

Nothing in this module names a model. `config.llm.yaml` does: `MODELS` is a
table of entries (alias → llm_type / api_key NAME / base_url / model /
capabilities; "providers" in this project are devices), and three keys say
which entry each utility surface uses — `UTILS_VISION_MODEL` (report photos,
scanned pages), `UTILS_TEXT_MODEL` (indicator extraction from text, titles,
summaries) and `UTILS_EMBEDDING_MODEL`.
A value is an entry name, a list of them (the FIRST whose key is present wins —
that is how one key runs everything), a `provider/model` string, or an inline
spec shaped like an entry. The chat picker is the `MODELS` table itself,
first present key first. These are the keys mirovital's config-server already
uses (`MODEL_PROVIDERS`, `UTILS_*_MODEL`), so a spec written for one reads in
the other.

Why routing is data. Issue #68: a deployment with `DEEPSEEK_API_KEY` alone
uploaded three documents into silence. The key was declared in config.yaml and
absent from four of the five Python tables that decided which provider a
surface used, and whether a provider could read an image was recorded nowhere
— it was implied by membership in a list. A first fix moved the five tables
into one Python registry; the owner's verdict on that was that a platform's
users must be able to read AND change every routing decision in config.yaml,
without a release. So the registry became `config.llm.yaml`, and this module
only knows how to read it.

Selection happens once per surface, on first use. A call that then fails is a
failure, reported with the entry and the vendor's message — not a reason to
try the next entry: two reports of one person must not be read by two
different models.

What does live here as code: the key-name aliases vendors document
(`GEMINI_API_KEY` for `GOOGLE_API_KEY`), the endpoint a bare `provider/model`
string implies (the ONE table of URL literals, each the fallback for
`<PREFIX>_BASE_URL`), and `LLMConfig`, the client the embedding layer uses.

NOT the same thing as `mirobody/utils/llm/` — that package holds the callers
(structured extraction, text, vision dispatch). Both read the same routes.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

import aiohttp

if TYPE_CHECKING:
    from anthropic import Anthropic, AsyncAnthropic
    from google.genai.client import AsyncClient as AsyncGenaiClient, Client as GenaiClient
    from openai import AsyncAzureOpenAI, AsyncOpenAI, AzureOpenAI, OpenAI

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

class LLMProvider(str, Enum):
    OPENAI     = "openai"
    OPENROUTER = "openrouter"
    DASHSCOPE  = "dashscope"
    DEEPSEEK   = "deepseek"
    ANTHROPIC  = "anthropic"
    GEMINI     = "gemini"
    VERTEX_AI  = "vertex_ai"
    AZURE      = "azure"
    BEDROCK    = "bedrock"

#-----------------------------------------------------------------------------
# The three things that are code, not config.

#: Where a bare `provider/model` route value points, and which key it reads.
#: The one table of endpoint literals outside config.yaml; each URL is the
#: fallback for `<PREFIX>_BASE_URL`. Gemini's is Google's OpenAI-COMPATIBLE
#: endpoint; Anthropic's is its NATIVE base (the SDK appends `/v1/messages`),
#: because `anthropic` is a family the utility surfaces call directly —
#: its compatibility endpoint cannot serve schema-constrained JSON.
KNOWN_ENDPOINTS: dict[str, tuple[str, str]] = {
    "openai":     ("OPENAI_API_KEY",     "https://api.openai.com/v1"),
    "openrouter": ("OPENROUTER_API_KEY", "https://openrouter.ai/api/v1"),
    "dashscope":  ("DASHSCOPE_API_KEY",  "https://dashscope.aliyuncs.com/compatible-mode/v1"),
    "deepseek":   ("DEEPSEEK_API_KEY",   "https://api.deepseek.com/v1"),
    "gemini":     ("GOOGLE_API_KEY",     "https://generativelanguage.googleapis.com/v1beta/openai/"),
    "anthropic":  ("ANTHROPIC_API_KEY",  "https://api.anthropic.com"),
}

#: Where to get each key — for the message a person reads when none is set.
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
    "embedding": "UTILS_EMBEDDING_MODEL",
}

#: The two vendors `Config.get_llm` does NOT reach with an OpenAI client.
#: Gemini: the embedding factory needs `output_dimensionality`, so it goes to
#: the REST API with `x-goog-api-key`. Anthropic: its row above is the native
#: base, and `LLMConfig._build_anthropic` builds the native SDK client.
_NOT_OPENAI_CLIENT = ("gemini", "anthropic")

#: provider → (api key config key, default base_url), the shape `Config.get_llm`
#: reads.
_OPENAI_COMPAT: dict[LLMProvider, tuple[str, str]] = {
    LLMProvider(name): (key, url) for name, (key, url) in KNOWN_ENDPOINTS.items() if name not in _NOT_OPENAI_CLIENT
}


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


def base_url_override(api_key_env: str) -> str:
    """`<PREFIX>_BASE_URL` for the key named `api_key_env` (OPENROUTER_API_KEY
    → OPENROUTER_BASE_URL), or "". The one redirect rule, applied to every
    entry that reads the key — chat, vision, text, embeddings (#52)."""
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
    extra_body: dict[str, Any] = field(default_factory=dict)
    temperature: float | None = None
    embedding: str | None = None   # the vector-column family an embedding entry writes

    @property
    def takes_json_object(self) -> bool:
        """Whether `response_format: {"type": "json_object"}` may be sent — the
        vision path's only use of the parameter."""
        return self.response_format in ("json_schema", "json_object")

    @property
    def key(self) -> str:
        return read_api_key(self.api_key_env) if self.api_key_env else ""

    @property
    def routable(self) -> bool:
        return bool(self.model) and (not self.api_key_env or bool(self.key))

    @property
    def label(self) -> str:
        return f"{self.alias} ({self.model})" if self.alias != self.model else self.model


#: What an entry's `response_format` may say, and what each means at the call
#: site. Not a capability ladder — measured behaviour, one vendor per value:
#:
#:   json_schema   OpenAI structured outputs (the default; OpenAI, OpenRouter,
#:                 DashScope, Google's compatibility endpoint)
#:   json_object   only the loose JSON mode. DeepSeek answers "This
#:                 response_format type is unavailable now" to a schema.
#:   none          the parameter cannot be sent at all, and the schema goes into
#:                 the prompt. Anthropic's compatibility endpoint REJECTS
#:                 `json_object` outright ("Input should be 'json_schema'") and
#:                 takes a schema only in OpenAI strict mode — `strict: true`
#:                 plus `additionalProperties: false` on every object, which the
#:                 extraction schemas do not carry (measured 2026-09-10).
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
    embedding = str(entry.get("embedding") or "").strip().lower() or None
    # A utility surface can call two families: any OpenAI-compatible endpoint,
    # and Anthropic's own API (`backends_anthropic`, for the structured
    # outputs its compatibility endpoint does not serve). An embedding entry
    # may name a third (Gemini's REST embedding endpoint); the embedding
    # factory for that family decides how to call it.
    if not model or (llm_type not in UTILITY_FAMILIES and not embedding):
        return None
    api_key_env = str(entry.get("api_key") or "").strip()
    base_url = str(entry.get("base_url") or "").strip()
    if not base_url and api_key_env:
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
        extra_body=dict(entry.get("extra_body") or {}),
        temperature=float(temperature) if isinstance(temperature, (int, float)) else None,
        embedding=embedding,
    )


def _spec_from_string(value: str, entries: dict[str, dict], surface: str = "") -> RouteSpec | None:
    """An entry name, or `provider/model` against `KNOWN_ENDPOINTS`. On the
    embedding surface a vector-column FAMILY name (`qwen`, `openrouter`, …)
    also resolves, to the entry that writes it — the spelling `EMBEDDING_PROVIDER:
    qwen` used, and the one the database columns carry."""
    value = value.strip()
    if surface == "embedding":
        # A family name is also a chat entry's name in the shipped file
        # (`qwen` chats; `qwen-embed` writes the `qwen` columns), so on this
        # surface an entry name counts only when the entry embeds.
        if value in entries and (entries[value] or {}).get("embedding"):
            return _spec_from_mapping(value, entries[value] or {})
        for alias, entry in entries.items():
            if str((entry or {}).get("embedding") or "").strip().lower() == value.lower():
                return _spec_from_mapping(alias, entry or {})
    elif value in entries:
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


def model_entries() -> dict[str, dict[str, Any]]:
    """The `MODELS` table as configured ({} with no Config loaded)."""
    from .config import global_config

    cfg = global_config()
    if cfg is None:
        return {}
    return dict((cfg.get_agent_settings() or {}).get("providers") or {})


def route_value(surface: str) -> Any:
    """The raw configured value for a surface: a string — an entry name,
    `provider/model`, or JSON — through `safe_read_cfg` (environment first, and
    the one place tests control), else the structured value (a list, a spec)
    from the config file."""
    from . import safe_read_cfg
    from .config import global_config

    key = ROUTE_KEYS[surface]
    text = (safe_read_cfg(key, "") or "").strip()
    if text:
        value = _maybe_json(text)
        # `get_str` renders a YAML list as its Python repr ("['a', 'b']"),
        # which is not JSON and not a value — the structured read below has it.
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
            out.append(_spec_from_string(item, entries, surface) or item.strip())
    return out


def _fits(surface: str, spec: RouteSpec) -> bool:
    if surface == "vision" and spec.supports_image is False:
        return False
    if surface == "embedding" and not spec.embedding:
        return False
    if surface != "embedding" and spec.embedding:
        return False
    return True


def resolve_route(surface: str) -> RouteSpec | None:
    """The spec `surface` uses right now: the first candidate whose key is
    present and which the surface accepts (vision skips an entry that says
    `supports_image: false`). None when there is none."""
    for candidate in route_candidates(surface):
        if isinstance(candidate, RouteSpec) and candidate.routable and _fits(surface, candidate):
            return candidate
    return None


def resolve_named(value: str, *, model: str | None = None) -> RouteSpec | None:
    """A caller-named route — an entry name or `provider/model` — with an
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
    names, needed, skipped = [], [], []
    for c in candidates:
        if isinstance(c, str):
            names.append(f"{c} (not a MODELS entry)")
            continue
        names.append(f"{c.alias} ({c.api_key_env or 'no key'})")
        if c.api_key_env and not c.key:
            needed.append(c.api_key_env)
        elif not _fits(surface, c):
            skipped.append(f"{c.alias} declares supports_image: false")
    seen: list[str] = []
    for k in needed:
        if k not in seen:
            seen.append(k)
    where = ", ".join(f"{k} ({KEYS_URL[k]})" if k in KEYS_URL else k for k in seen)
    text = f"No {surface} model available: {key} lists {', '.join(names)}"
    if seen:
        text += f"; none of these keys is set. Put ONE in .env: {where}."
    elif skipped:
        text += f"; {'; '.join(skipped)} — set {key} to an entry whose model reads images."
    else:
        text += "."
    return text


def chat_entries() -> dict[str, dict[str, Any]]:
    """`MODELS` minus the utility-only and embedding entries, in order."""
    return {
        name: entry for name, entry in model_entries().items()
        if _flag((entry or {}).get("chat")) is not False and not (entry or {}).get("embedding")
    }


def chat_default() -> str | None:
    """The chat picker's default: the first `MODELS` entry (config order,
    utility-only entries excluded) whose key is present — or that names no
    key (ambient auth). None when none is."""
    for name, entry in chat_entries().items():
        ref = str((entry or {}).get("api_key") or "").strip()
        if not ref or read_api_key(ref):
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


#-----------------------------------------------------------------------------

class LLMConfig:
    """
    Single LLM provider's configuration. Created by ``Config.get_llm()``.

    Holds credentials and endpoints read from configuration;
    ``get_client()`` / ``get_async_client()`` lazily create and cache
    the native SDK client.
    """

    _GEMINI_BASE = "https://generativelanguage.googleapis.com"

    def __init__(
        self,
        provider    : LLMProvider,
        *,
        api_key     : str = "",
        base_url    : str = "",
        # Azure
        endpoint    : str = "",
        deployment  : str = "gpt-4o",
        api_version : str = "2024-12-01-preview",
        # Gemini
        gemini_api_version: str = "v1beta",
        # GCP
        gcp_project : str = "",
        gcp_location: str = "",
        # AWS
        aws_region  : str = "",
    ):
        self.provider     = provider
        self.api_key      = api_key
        self.base_url     = base_url
        self.endpoint     = endpoint
        self.deployment   = deployment
        self.api_version  = api_version
        self.gcp_project  = gcp_project
        self.gcp_location = gcp_location
        self.aws_region   = aws_region

        # Gemini / Vertex AI: derive base_url if not explicitly set
        self.gemini_api_version = gemini_api_version
        if provider == LLMProvider.GEMINI and not base_url:
            self.base_url = f"{self._GEMINI_BASE}/{gemini_api_version}"
        elif provider == LLMProvider.VERTEX_AI and not base_url:
            self.base_url = (
                f"https://{gcp_location}-aiplatform.googleapis.com/v1"
                f"/projects/{gcp_project}/locations/{gcp_location}"
            )

        self._client: Any = None
        self._async_client: Any = None

    #-------------------------------------------------

    def print(self):
        if self.provider in _OPENAI_COMPAT:
            print(f"llm             : {self.provider.value}  base_url={self.base_url}")
        elif self.provider == LLMProvider.AZURE:
            print(f"llm             : azure  endpoint={self.endpoint}  deployment={self.deployment}")
        elif self.provider == LLMProvider.VERTEX_AI:
            print(f"llm             : vertex_ai  project={self.gcp_project}  location={self.gcp_location}")
        elif self.provider == LLMProvider.BEDROCK:
            print(f"llm             : bedrock  region={self.aws_region}")
        else:
            print(f"llm             : {self.provider.value}")

    #-------------------------------------------------
    # aiohttp session
    #-------------------------------------------------

    def get_aiohttp_session(self, **kwargs) -> aiohttp.ClientSession:
        """Create an aiohttp.ClientSession with base_url and auth headers pre-configured.

        Caller is responsible for closing the session (use ``async with``).
        Extra *kwargs* are forwarded to ``aiohttp.ClientSession()``.
        """
        headers = kwargs.pop("headers", {})
        headers.setdefault("Content-Type", "application/json")

        p = self.provider
        if p in _OPENAI_COMPAT or p == LLMProvider.AZURE:
            headers.setdefault("Authorization", f"Bearer {self.api_key}")
        elif p == LLMProvider.ANTHROPIC:
            headers.setdefault("x-api-key", self.api_key)
            headers.setdefault("anthropic-version", "2023-06-01")
        elif p == LLMProvider.GEMINI:
            headers.setdefault("x-goog-api-key", self.api_key)
        elif p == LLMProvider.VERTEX_AI:
            import google.auth
            import google.auth.transport.requests

            creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            creds.refresh(google.auth.transport.requests.Request())
            headers.setdefault("Authorization", f"Bearer {creds.token}")

        base_url = self.base_url.rstrip("/") + "/"
        return aiohttp.ClientSession(
            base_url=base_url,
            headers=headers,
            **kwargs,
        )

    #-------------------------------------------------
    # Client builders (lazy, cached per instance)
    #-------------------------------------------------

    def get_client(self) -> OpenAI | Anthropic | GenaiClient | AzureOpenAI:
        if self._client is None:
            self._client = self._build(sync=True)
        return self._client

    def get_async_client(self) -> AsyncOpenAI | AsyncAnthropic | AsyncGenaiClient | AsyncAzureOpenAI:
        if self._async_client is None:
            self._async_client = self._build(sync=False)
        return self._async_client

    #-------------------------------------------------

    def _build(self, *, sync: bool) -> Any:
        p = self.provider

        if p in _OPENAI_COMPAT:
            return self._build_openai_compat(sync=sync)
        if p == LLMProvider.ANTHROPIC:
            return self._build_anthropic(sync=sync)
        if p == LLMProvider.GEMINI:
            return self._build_gemini(sync=sync)
        if p == LLMProvider.VERTEX_AI:
            return self._build_vertex_ai(sync=sync)
        if p == LLMProvider.AZURE:
            return self._build_azure(sync=sync)
        if p == LLMProvider.BEDROCK:
            return self._build_bedrock(sync=sync)

        raise ValueError(f"Unsupported provider: {p!r}")

    #-------------------------------------------------

    def _build_openai_compat(self, *, sync: bool) -> OpenAI | AsyncOpenAI:
        from openai import AsyncOpenAI, OpenAI
        cls = OpenAI if sync else AsyncOpenAI
        return cls(api_key=self.api_key, base_url=self.base_url)

    def _build_anthropic(self, *, sync: bool) -> Anthropic | AsyncAnthropic:
        import anthropic
        cls = anthropic.Anthropic if sync else anthropic.AsyncAnthropic
        # `base_url` only when ANTHROPIC_BASE_URL says so: the SDK's own
        # default is the right one, and `None` is how you ask for it.
        return cls(api_key=self.api_key, base_url=self.base_url or None)

    def _build_gemini(self, *, sync: bool) -> GenaiClient | AsyncGenaiClient:
        from google import genai
        client = genai.Client(api_key=self.api_key, vertexai=False)
        return client if sync else client.aio

    def _build_vertex_ai(self, *, sync: bool) -> GenaiClient | AsyncGenaiClient:
        from google import genai
        if not os.environ.get("GOOGLE_CLOUD_PROJECT"):
            raise ValueError(
                "GOOGLE_CLOUD_PROJECT not set. "
                "Configure GCP_PROJECT in YAML and call export_to_env() at startup."
            )
        client = genai.Client()
        return client if sync else client.aio

    def _build_azure(self, *, sync: bool) -> AzureOpenAI | AsyncAzureOpenAI:
        from azure.identity import DefaultAzureCredential, get_bearer_token_provider
        from openai import AsyncAzureOpenAI, AzureOpenAI

        token_provider = get_bearer_token_provider(
            DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
        )
        cls = AzureOpenAI if sync else AsyncAzureOpenAI
        return cls(
            azure_ad_token_provider=token_provider,
            azure_endpoint=self.endpoint,
            azure_deployment=self.deployment,
            api_version=self.api_version,
        )

    def _build_bedrock(self, *, sync: bool) -> Any:
        if sync:
            import boto3
            return boto3.client("bedrock-runtime", region_name=self.aws_region)
        import aioboto3
        return aioboto3.Session().client("bedrock-runtime", region_name=self.aws_region)
