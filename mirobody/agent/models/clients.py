"""LLM client construction: one builder for every provider family.

`build_chat_model(entry)` turns one `MODELS` entry (config.llm.yaml) into a
LangChain chat model; `build_llm_clients(table)`
does it for the whole table and stands in a `_PlaceholderClient` where a key
is missing, so a zero-key deployment boots and the picker can say what is
usable.

Every reference in an entry (`api_key`, `base_url`, `project`, `location`) 
is a NAME resolved through `resolve`: the environment and `safe_read_cfg` by
default; a consumer with a config server passes its own resolver. A literal
URL passes through.

Families, chosen by `llm_type`:

* `openai` / `openrouter`: any OpenAI-compatible endpoint (base_url +
  api_key), built on `ReasoningChatOpenAI` so DashScope/DeepSeek
  `reasoning_content` reaches `additional_kwargs`; `auth_type: azure_wif`
  swaps the key for an Entra bearer-token provider.
* `google_anthropic_vertex`: Claude on Vertex through `init_chat_model`; the
  prompt-cache breakpoint and the extended-thinking budget travel in
  `model_kwargs`, because that class drops unknown top-level kwargs silently.
* `google_genai` / `google_vertexai`: Gemini through `ChatGoogleGenerativeAI`:
  Vertex-hosted (ambient credentials) when the entry names a `project` and no
  key, AI Studio otherwise.
* `anthropic`: the direct API through `init_chat_model`, thinking as its
  native top-level parameter.
* anything else: `init_chat_model(model_provider=llm_type)`, untouched.

`thinking` is one normalised effort level (`normalize_thinking`) translated
into each family's dialect here, so every surface that accepts a thinking hint
means the same thing by "high". Only the families the shipped configuration
uses are declared dependencies; `init_chat_model` names the missing package
for any other.

What an entry DECLARES, this module keeps (#70). An entry's own `thinking`
block, `reasoning` dict or `output_config` is never overwritten by the
translation of an effort level: the level fills in what the entry left
unsaid (`setdefault`, the way `cache_control` was always applied). Before
this, `_vertex_anthropic_kwargs` assigned `thinking` unconditionally, so an
entry that declared the adaptive shape for Claude Opus 4.7+ had it replaced
with `{"type": "enabled", "budget_tokens": N}` (which those models reject
with a 400) and no configuration could avoid it.
"""

from __future__ import annotations

import inspect
import logging
import os
import re
from functools import lru_cache
from collections.abc import Callable
from typing import Any

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.config import safe_read_cfg
from mirobody.utils.config.llm import is_endpoint_name, vertex_host, vertex_location

logger = logging.getLogger(__name__)

#: ``resolve(name) -> value``: how a reference in an entry becomes a value.
Resolver = Callable[[str], "str | None"]

#: Entry keys consumed here, never forwarded to a model constructor.
NON_INIT_CONFIG_KEYS = frozenset({
    "model", "llm_type", "response_with_tools",
    "profile", "supports_pdf", "supports_image",
    "thinking_style", "auth_type", "prompt_cache",
    # read by the utility surfaces (config.llm), never by a chat constructor
    "chat", "response_format", "ocr_prompts",
    # read by config.llm.model_entries, which has already put its value in `model`
    "model_env",
})

OPENAI_COMPATIBLE_TYPES = frozenset({"openai", "openrouter"})
ANTHROPIC_VERTEX_TYPES = frozenset({"google_anthropic_vertex"})
GEMINI_TYPES = frozenset({"google_genai", "google_vertexai"})

# --- thinking: one effort scale, one dialect table -----------------------------------------

THINKING_EFFORTS = ("off", "low", "medium", "high")
#: Budget tokens per effort, shared by every budget-style dialect (qwen
#: thinking_budget, Anthropic budget_tokens, Gemini thinking_budget).
THINKING_BUDGET_TOKENS = {"low": 2048, "medium": 8192, "high": 24576}


def normalize_thinking(value: Any) -> str | None:
    """A caller's thinking hint → an effort level, or ``None`` (provider default).

    Accepts a bool (``True`` → high, ``False`` → off), the OpenAI effort words
    (``minimal`` folds into ``low``), and ``None``/``""``. An unknown string is
    ``None`` rather than an error: thinking is an enhancement, never a reason
    to fail the request.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return "high" if value else "off"
    text = str(value).strip().lower()
    if text == "minimal":
        return "low"
    return text if text in THINKING_EFFORTS else None


def thinking_dialect(entry: dict | None, model_name: str, base_url: str = "") -> str:
    """Which thinking parameter dialect a provider speaks: the entry's
    ``thinking_style`` if set, else a conservative guess from model name and
    base_url. Unknown → ``"none"``, which means "send nothing", never a
    parameter a provider might reject with a 400.

    The qwen ``enable_thinking``/``thinking_budget`` shape is DashScope's, and
    applies to whatever DashScope hosts (qwen, kimi, a hosted deepseek), so it
    is keyed on the base_url; a qwen served by another host does not speak it.

    ``thinking_style: anthropic_budget`` / ``anthropic_adaptive`` both mean the
    Anthropic family here; which of its two shapes is sent is
    `anthropic_thinking_shape`'s question.
    """
    explicit = str((entry or {}).get("thinking_style") or "").strip().lower()
    if explicit.startswith("anthropic"):
        return "anthropic"
    if explicit:
        return explicit
    model = (model_name or "").lower()
    host = (base_url or "").lower()
    if "claude" in model:
        return "anthropic"
    if "gemini" in model:
        return "gemini"
    if "dashscope" in host or "aliyuncs" in host:
        return "qwen"
    if model.startswith("qwen") and not host:
        return "qwen"
    if model.startswith(("gpt-", "o1", "o3", "o4")):
        return "openai"
    return "none"


#: Claude models that still take the fixed thinking budget: the 3.x line and
#: the 4.0 / 4.1 / 4.5 releases (Vertex spells them `claude-sonnet-4-5@date`).
#: From 4.6 on the shape is `{"type": "adaptive"}` + `output_config.effort`;
#: 4.7 and later REJECT `budget_tokens` with a 400 (Anthropic's thinking
#: troubleshooting page, and issue #70's exact error text).
_ANTHROPIC_BUDGET_MODELS = re.compile(r"claude-3|-4-[015](?!\d)")


def anthropic_thinking_shape(entry: dict | None, model_name: str) -> str:
    """``"budget"`` or ``"adaptive"``: the entry's ``thinking_style``
    (``anthropic_budget`` / ``anthropic_adaptive``) if it says, else a guess
    from the model name. The guess is a fallback, not the contract: a
    deployment on an unlisted spelling declares the style."""
    explicit = str((entry or {}).get("thinking_style") or "").strip().lower()
    if explicit == "anthropic_budget":
        return "budget"
    if explicit == "anthropic_adaptive":
        return "adaptive"
    return "budget" if _ANTHROPIC_BUDGET_MODELS.search((model_name or "").lower()) else "adaptive"


def _apply_anthropic_thinking(kwargs: dict, entry: dict, model_name: str, effort: str | None, *, request: dict) -> None:
    """Translate an effort level into the Anthropic request shape, in place.

    ``request`` is the dict the parameters travel in: ``kwargs`` itself for
    `ChatAnthropic` (native parameters) and ``model_kwargs`` for
    `ChatAnthropicVertex` (which forwards that dict and drops unknown
    top-level kwargs). Everything is `setdefault`: an entry's own ``thinking``
    block or ``output_config`` stands, and the effort only fills what the
    entry left unsaid (#70).

    ``off`` sends nothing, on both shapes: omitting ``thinking`` is accepted by
    every Claude model, while ``{"type": "disabled"}`` is rejected by some of
    the newest ones.
    """
    if not effort or effort == "off":
        return
    if anthropic_thinking_shape(entry, model_name) == "budget":
        budget = THINKING_BUDGET_TOKENS[effort]
        request.setdefault("thinking", {"type": "enabled", "budget_tokens": budget})
        kwargs.pop("temperature", None)  # must be unset (or 1) with extended thinking
        if int(kwargs.get("max_tokens") or 0) <= budget:
            kwargs["max_tokens"] = budget + 4096
        return
    # `display: summarized` because this product SHOWS the reasoning channel;
    # the newer models default to `omitted`, which streams empty thinking
    # blocks and reads as a long pause before the answer.
    request.setdefault("thinking", {"type": "adaptive", "display": "summarized"})
    output_config = dict(request.get("output_config") or {})
    output_config.setdefault("effort", effort)
    request["output_config"] = output_config
    # Sampling parameters are rejected alongside thinking on the models that
    # take this shape (Opus 4.7+, Sonnet 5 and 5.5).
    for field in ("temperature", "top_p", "top_k"):
        kwargs.pop(field, None)


def _openai_thinking_kwargs(entry: dict, model_name: str, base_url: str, effort: str | None) -> dict:
    """The ChatOpenAI kwarg fragment for an effort level on an OpenAI-compatible
    endpoint. qwen's dialect rides in ``extra_body`` and is merged into the
    entry's own; OpenAI's is ``reasoning_effort`` (which a reasoning-native
    model cannot switch off: ``off`` leaves the default), or, when the entry
    already carries a ``reasoning`` dict, the effort folded into that dict."""
    if not effort:
        return {}
    dialect = thinking_dialect(entry, model_name, base_url)
    if dialect == "qwen":
        body = dict(entry.get("extra_body") or {})
        if effort == "off":
            body["enable_thinking"] = False
        else:
            body["enable_thinking"] = True
            body["thinking_budget"] = THINKING_BUDGET_TOKENS[effort]
        return {"extra_body": body}
    if dialect == "openai":
        if effort == "off":
            logger.info("thinking=off ignored for an openai-dialect model (model_name=%s)", model_name)
            return {}
        reasoning = entry.get("reasoning")
        if isinstance(reasoning, dict):
            # An entry on the Responses API declares `reasoning: {summary:
            # auto}`; langchain-openai 1.5 then passes a separate
            # `reasoning_effort` through as a bare kwarg, and
            # `Responses.create()` raises TypeError before any request is sent
            # (#70). Either parameter alone works; the effort goes INTO the
            # dict, and a declared effort wins.
            return {"reasoning": {**reasoning, "effort": reasoning.get("effort", effort)}}
        if "reasoning_effort" in entry:
            # The entry decided (#70). It has to be able to: `reasoning_effort:
            # none` is what lets an OpenAI reasoning model call a function tool
            # on /v1/chat/completions at all, and a thinking hint from the UI
            # must not silently undo it.
            return {}
        return {"reasoning_effort": effort}
    thinking_level, thinking_mode = effort, dialect
    logger.info("thinking ignored: thinking_level=%s model_name=%s thinking_mode=%s", thinking_level, model_name, thinking_mode)
    return {}


# --- the OpenAI-compatible class that keeps reasoning_content ------------------------------

_REASONING_CHAT_OPENAI: type | None = None


def reasoning_chat_openai() -> type:
    """``ChatOpenAI`` that surfaces DashScope/DeepSeek ``reasoning_content``.

    langchain-openai drops this non-OpenAI field (it keeps the reasoning
    TOKEN COUNT in usage but not the reasoning TEXT) so a thinking model's
    thoughts never reach ``additional_kwargs``. This subclass captures the
    field from the raw streaming delta and from the non-stream message, where
    `messages.message_reasoning` then finds it. Built lazily so langchain-openai
    is imported only when a client is.
    """
    global _REASONING_CHAT_OPENAI
    if _REASONING_CHAT_OPENAI is None:
        from langchain_openai import ChatOpenAI

        class ReasoningChatOpenAI(ChatOpenAI):
            def _convert_chunk_to_generation_chunk(self, chunk, default_chunk_class, base_generation_info):
                gen = super()._convert_chunk_to_generation_chunk(chunk, default_chunk_class, base_generation_info)
                try:
                    choices = (chunk or {}).get("choices") or []
                    reasoning = (choices[0].get("delta") or {}).get("reasoning_content") if choices else None
                    if gen is not None and reasoning:
                        extra = gen.message.additional_kwargs
                        extra["reasoning_content"] = (extra.get("reasoning_content") or "") + reasoning
                except Exception as exc:  # reasoning capture must never break generation
                    logger.debug("reasoning capture (stream) skipped: %s", type(exc).__name__)
                return gen

            def _create_chat_result(self, response, generation_info=None):
                result = super()._create_chat_result(response, generation_info)
                try:
                    payload = response if isinstance(response, dict) else response.model_dump()
                    choice = (payload.get("choices") or [{}])[0]
                    reasoning = (choice.get("message") or {}).get("reasoning_content")
                    if reasoning and result.generations:
                        result.generations[0].message.additional_kwargs.setdefault("reasoning_content", reasoning)
                except Exception as exc:
                    logger.debug("reasoning capture (non-stream) skipped: %s", type(exc).__name__)
                return result

        _REASONING_CHAT_OPENAI = ReasoningChatOpenAI
    return _REASONING_CHAT_OPENAI


# --- references -> values ----------------------------------------------------------------------

#: What a reference must look like to be repeated back: a variable NAME. An
#: entry that holds the secret itself (`api_key: sk-proj-...`) resolves to
#: nothing as a name, and the "is not set" warning printed the secret at boot
#: and showed it to whoever picked that model.
_VARIABLE_NAME = re.compile(r"[A-Z][A-Z0-9_]*")


def _shown(reference: str) -> str:
    """`reference` when it is a variable name, else a placeholder for it."""
    return reference if _VARIABLE_NAME.fullmatch(reference) else "(a value that is not a variable name)"


class MissingKeyError(RuntimeError):
    """The entry names an ``api_key`` that resolves to nothing. ``key`` is the
    reference as it may be shown (`_shown`), never a literal written there."""

    field = "api_key"

    def __init__(self, alias: str, key: str):
        self.alias, self.key = alias, _shown(key)
        super().__init__(f"model {alias!r}: {self.field} {self.key} is not set")


class MissingEndpointError(MissingKeyError):
    """The entry's ``base_url`` is a NAME (``LOCAL_BASE_URL``) that resolves to nothing."""

    field = "base_url"


def default_resolver(name: str) -> str | None:
    """The reference server's lookup: the environment, then `safe_read_cfg`.

    An `_API_KEY` name goes through `read_api_key`, so the vendor-documented
    aliases count here too. They did not: a deployment holding only
    `GEMINI_API_KEY` (the name Google's own docs use) had every route resolve
    (`read_api_key` knows the alias) while the agent built a `_PlaceholderClient`
    and the chat picker offered nothing, with `mirobody doctor` reporting the
    chat surface healthy. One key, two answers, is #68.
    """
    if name.endswith("_API_KEY"):
        from mirobody.utils.config.llm import read_api_key

        return read_api_key(name) or None
    return os.environ.get(name) or safe_read_cfg(name) or None


def _resolve_ref(value: Any, resolve: Resolver) -> Any:
    """A field that is either a literal (a URL, a region) or a NAME: resolved
    when a value exists under that name, kept literally otherwise."""
    if not value or not isinstance(value, str):
        return value
    if value.startswith(("http://", "https://")):
        return value
    return resolve(value) or value


def _base_url_override(entry: dict, resolve: Resolver) -> str | None:
    """`<PREFIX>_BASE_URL`, derived from the entry's ``api_key`` NAME: an
    entry reading ``OPENROUTER_API_KEY`` follows ``OPENROUTER_BASE_URL``.

    Chat was the one surface that did not. A literal URL passed `_resolve_ref`
    untouched, so "point the whole stack at one self-hosted gateway" meant
    editing the YAML for the agent while vision, extraction and embeddings all
    followed the variable: the rule `config.llm.yaml` documents for every
    entry. The MODEL is not overridden this way: a model belongs to an entry,
    and a gateway that serves different ids gets its own entry.
    """
    ref = entry.get("api_key")
    if not isinstance(ref, str) or not ref.endswith("_API_KEY"):
        return None
    return resolve(ref[: -len("_API_KEY")] + "_BASE_URL") or None


def _resolve_key(alias: str, entry: dict, resolve: Resolver) -> str | None:
    """The API key: ``None`` when the entry names none (the provider's own
    default auth), `MissingKeyError` when it names one that is not set."""
    ref = entry.get("api_key")
    if not ref or not isinstance(ref, str):
        return None
    key = resolve(ref)
    if not key:
        raise MissingKeyError(alias, ref)
    return key


def _llm_type(entry: dict) -> str:
    return str(entry.get("llm_type") or "openai").strip().lower().replace("-", "_")


def is_routable(entry: Any, *, resolve: Resolver | None = None) -> bool:
    """Whether `build_chat_model` could build this entry right now: a model,
    and the credential its family needs: a resolvable key, the federated
    token file for Azure WIF, or a resolvable project for a Vertex model."""
    if not isinstance(entry, dict) or not entry.get("model"):
        return False
    resolve = resolve or default_resolver
    family = _llm_type(entry)
    if family in OPENAI_COMPATIBLE_TYPES:
        auth = str(entry.get("auth_type") or "").strip().lower()
        if auth == "azure_wif":
            return bool(os.environ.get("AZURE_FEDERATED_TOKEN_FILE"))
        if auth == "gcp_adc":
            # Vertex MaaS: the credential is ambient, so what has to resolve is
            # the project the endpoint is built from.
            return bool(_resolve_ref(entry.get("project"), resolve)) or bool(entry.get("base_url"))
        base = entry.get("base_url")
        if isinstance(base, str) and is_endpoint_name(base) and not (resolve(base) or _base_url_override(entry, resolve)):
            return False
        return not entry.get("api_key") or bool(resolve(entry["api_key"]))
    if family in ANTHROPIC_VERTEX_TYPES:
        return bool(_resolve_ref(entry.get("project"), resolve))
    if family in GEMINI_TYPES:
        return bool(_resolve_ref(entry.get("project"), resolve)) or bool(
            entry.get("api_key") and resolve(entry["api_key"])
        )
    return not entry.get("api_key") or bool(resolve(entry["api_key"]))


def _coerce_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes", "on")


def _profile_override(entry: dict) -> dict[str, Any]:
    """The entry's multimodal declaration as a ``ModelProfile`` fragment.

        supports_pdf:   true   # accepts a native PDF block (read_file)
        supports_image: true   # accepts a native image block

    These booleans state what the MODEL accepts, so they set the ``*_inputs``
    fields only. They deliberately do NOT set ``pdf_tool_message`` /
    ``image_tool_message``, which are about what the WIRE carries inside a
    ToolMessage, a different question with a different answer: `claude-sonnet`
    reads PDFs and reaches us over OpenRouter's Chat Completions endpoint,
    which answers `400 tool messages must include a non-empty string
    tool_call_id` for a file block. Writing the model's answer into the wire's
    field is how that 400 survived the fix that was meant to end it.

    Dropping them changes nothing downstream, both halves verified against
    `deepagents.middleware.filesystem._multimodal_block_supported`: a `True`
    was already a no-op there ("Only an explicit `False` rejects a block
    type"), and a `False` veto still lands, because that function falls through
    to `profile.get("pdf_inputs") is not False`, which these booleans do set.

    An advanced ``profile:`` dict of raw ModelProfile fields still wins over
    them. ``{}`` when nothing is declared, so the model's own profile stands.
    """
    override: dict[str, Any] = {}
    if "supports_pdf" in entry:
        override["pdf_inputs"] = _coerce_flag(entry["supports_pdf"])
    if "supports_image" in entry:
        override["image_inputs"] = _coerce_flag(entry["supports_image"])
    raw = entry.get("profile")
    if isinstance(raw, dict):
        override.update(raw)
    return override


def _base_kwargs(entry: dict) -> dict[str, Any]:
    return {k: v for k, v in entry.items() if k not in NON_INIT_CONFIG_KEYS}


# --- one family at a time -----------------------------------------------------------------------

def _openai_kwargs(alias: str, entry: dict, thinking: str | None, resolve: Resolver) -> dict[str, Any]:
    kwargs = _base_kwargs(entry)
    # `stream_options` is rejected by OpenAI-compatible backends on a
    # NON-streaming call (a title summary is one); `stream_usage` does the same
    # job and langchain-openai applies it to streaming requests only, so the
    # final chunk carries usage_metadata for the cost line.
    kwargs.pop("stream_options", None)
    kwargs.setdefault("streaming", True)
    kwargs.setdefault("stream_usage", True)
    base_url = _base_url_override(entry, resolve) or _resolve_ref(entry.get("base_url"), resolve)
    if isinstance(base_url, str) and is_endpoint_name(base_url):
        raise MissingEndpointError(alias, base_url)
    if base_url:
        kwargs["base_url"] = base_url
    else:
        kwargs.pop("base_url", None)
    auth = str(entry.get("auth_type") or "").strip().lower()
    if auth == "azure_wif":
        kwargs.update(_azure_wif_kwargs(alias, base_url))
    elif auth == "gcp_adc":
        # On this family the two keys are inputs to endpoint derivation, not
        # request parameters. Left in kwargs, langchain-openai moves them to
        # `model_kwargs` and posts them in the body of every call. (#75)
        for field in ("project", "location"):
            kwargs.pop(field, None)
        kwargs.update(_vertex_maas_kwargs(alias, entry, base_url, resolve))
    else:
        key = _resolve_key(alias, entry, resolve)
        if key:
            kwargs["api_key"] = key
        elif base_url:
            # No key named, own endpoint: a self-hosted server that needs none.
            # Left to the SDK, the client read OPENAI_API_KEY and sent it there,
            # or refused to build on a machine without one (1.5.4).
            kwargs["api_key"] = "-"
        else:
            kwargs.pop("api_key", None)
    kwargs.update(_openai_thinking_kwargs(entry, str(entry["model"]), str(kwargs.get("base_url") or ""), thinking))
    return kwargs


def _azure_wif_kwargs(alias: str, endpoint: Any) -> dict[str, Any]:
    """Azure OpenAI through Workload Identity Federation: no key, an Entra
    bearer-token provider built from the pod's federated token, on the
    ``{endpoint}/openai/v1/`` endpoint. Needs `azure-identity`, which `[parse]`
    carries and a library consumer on Azure installs."""
    token_file = os.environ.get("AZURE_FEDERATED_TOKEN_FILE")
    if not token_file:
        raise RuntimeError(
            f"model {alias!r}: auth_type azure_wif needs AZURE_FEDERATED_TOKEN_FILE "
            "(the federated token is not mounted on this process)"
        )
    endpoint = str(endpoint or "").rstrip("/")
    if not endpoint:
        raise RuntimeError(f"model {alias!r}: auth_type azure_wif needs a base_url (the Azure OpenAI endpoint)")
    from azure.identity import WorkloadIdentityCredential, get_bearer_token_provider

    credential = WorkloadIdentityCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        token_file_path=token_file,
    )
    return {
        "api_key": get_bearer_token_provider(credential, "https://cognitiveservices.azure.com/.default"),
        "base_url": endpoint if "/openai/v1" in endpoint else f"{endpoint}/openai/v1/",
    }


def _gcp_access_token_provider() -> Callable[[], str]:
    """A callable returning a live GCP access token, for a client that wants a
    bearer string rather than a credentials object.

    `langchain-openai` accepts a callable `api_key` and calls it per request
    (the same seam `_azure_wif_kwargs` uses for Entra), so the hour-long
    lifetime of an ADC token is handled by refreshing here rather than by
    rebuilding the client. One credentials object per `MODELS` entry: refresh
    mutates it in place, and a second object would re-do the metadata-server
    round trip on every call."""
    import google.auth
    import google.auth.transport.requests

    credentials, _project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    request = google.auth.transport.requests.Request()

    def token() -> str:
        # `valid` is False both when the token has expired and before the first
        # fetch, which is exactly when a refresh is wanted.
        if not credentials.valid:
            credentials.refresh(request)
        return str(credentials.token or "")

    return token


def _vertex_maas_kwargs(alias: str, entry: dict, base_url: Any, resolve: Resolver) -> dict[str, Any]:
    """A Model-Garden partner model (xAI Grok, DeepSeek, Qwen, Llama, …) through
    Vertex's OpenAI-compatible endpoint.

    Two families of Vertex model, two protocols, and the split is not ours to
    choose: Claude speaks Anthropic's native Messages API
    (`_vertex_anthropic_kwargs`) and Gemini speaks Google's own
    (`_gemini_kwargs`), while every OTHER Model Garden publisher is served
    only over `/endpoints/openapi/chat/completions`. So a partner model rides
    the ordinary OpenAI-compatible client and differs from it in exactly two
    places: the base URL and where the bearer token comes from.

    The endpoint is DERIVED from `project`/`location` rather than written down,
    because a literal endpoint is the thing a redeploy to another region
    silently gets wrong. `base_url` in the entry still wins, for a private or
    self-deployed endpoint. The hostname has THREE shapes and `vertex_host`
    owns all of them, this used to carry two, and the missing one was the
    multi-regional `us` / `eu` pair a US-resident deployment has to use (#73).
    """
    if base_url:
        endpoint = str(base_url).rstrip("/")
    else:
        project = _resolve_ref(entry.get("project"), resolve)
        if not project:
            raise RuntimeError(f"model {alias!r}: auth_type gcp_adc needs a resolvable project (or a base_url)")
        location = vertex_location(str(_resolve_ref(entry.get("location"), resolve) or ""))
        endpoint = (f"https://{vertex_host(location)}/v1"
                    f"/projects/{project}/locations/{location}/endpoints/openapi")
    return {"base_url": endpoint, "api_key": _gcp_access_token_provider()}


#: Sampling parameters that were parameters of the Anthropic messages API and
#: are not any more. Asked of the INSTALLED SDK rather than hardcoded, the same
#: shape `utils/llm/backends_anthropic._accepted_params` already uses on the
#: extraction side: 1.x removed `temperature`, `top_p` and `top_k` and the
#: signature has no `**kwargs`, so one of them reaching the client is a
#: `TypeError` before a request is built: nothing in a provider log to read.
@lru_cache(maxsize=1)
def _anthropic_rejects() -> frozenset[str]:
    try:
        from anthropic.resources.messages import Messages
        sig = inspect.signature(Messages.create).parameters
    except Exception as e:                      # SDK absent or renamed the call
        logger.debug(  # phi: ok an import error from the SDK, not a document
            f"anthropic signature unavailable ({type(e).__name__}); assuming 1.x: {e}")
        return frozenset({"temperature", "top_p", "top_k"})
    if any(p.kind is p.VAR_KEYWORD for p in sig.values()):
        return frozenset()                      # takes **kwargs: nothing to strip
    return frozenset(f for f in ("temperature", "top_p", "top_k") if f not in sig)


def _drop_rejected_sampling(kwargs: dict, alias: str) -> None:
    """Strip sampling keys this SDK no longer takes, in place.

    `_apply_anthropic_thinking` already popped them, but only on the branch
    where an effort level was given: `thinking=None` or `"off"` returns early,
    and that is the DEFAULT path. So the same entry failed or worked depending
    on whether the caller happened to ask for thinking. Config gives almost
    every entry a `temperature` and copying one into a Claude entry is the
    natural thing to do. (#72)
    """
    for field in sorted(_anthropic_rejects() & kwargs.keys()):
        kwargs.pop(field)
        logger.warning(  # phi: ok a config entry name and a parameter name
            f"model {alias!r}: the installed anthropic SDK does not accept "
            f"{field!r}; dropped. Sampling parameters left that API in 1.x."
        )


def _vertex_anthropic_kwargs(alias: str, entry: dict, thinking: str | None, resolve: Resolver) -> dict[str, Any]:
    """Claude on Vertex. Auth is ambient (the process's Google credentials):
    a named key that resolves is passed, one that does not is dropped rather
    than refused. ``ChatAnthropicVertex`` ignores unknown top-level kwargs, so
    the cache breakpoint and the thinking budget travel in ``model_kwargs``,
    which it forwards to the request."""
    kwargs = _base_kwargs(entry)
    for field in ("project", "location"):
        if field in kwargs:
            kwargs[field] = _resolve_ref(kwargs[field], resolve)
    if not kwargs.get("project"):
        raise RuntimeError(f"model {alias!r}: a Vertex model needs a resolvable project")
    if isinstance(kwargs.get("api_key"), str):
        key = resolve(kwargs["api_key"])
        if key:
            kwargs["api_key"] = key
        else:
            kwargs.pop("api_key")
    model_kwargs = dict(kwargs.get("model_kwargs") or {})
    if _coerce_flag(entry.get("prompt_cache", True)):
        # deepagents' AnthropicPromptCachingMiddleware applies only to
        # ChatAnthropic, which ChatAnthropicVertex is not, so without this
        # breakpoint Claude-on-Vertex runs with zero cache, every tool round.
        model_kwargs.setdefault("cache_control", {"type": "ephemeral", "ttl": "5m"})
    _apply_anthropic_thinking(kwargs, entry, str(entry["model"]), thinking, request=model_kwargs)
    _drop_rejected_sampling(kwargs, alias)
    if model_kwargs:
        kwargs["model_kwargs"] = model_kwargs
    return kwargs


def _gemini_kwargs(alias: str, entry: dict, thinking: str | None, resolve: Resolver) -> dict[str, Any]:
    """Gemini through ``ChatGoogleGenerativeAI``: Vertex-hosted (``vertexai=True``,
    ambient credentials, location ``global`` unless given: regional endpoints
    404 for Gemini) when the entry names a project and no usable key; AI
    Studio with the key otherwise. Uses ``max_output_tokens``, so an entry's
    ``max_tokens`` is renamed."""
    kwargs = _base_kwargs(entry)
    for field in ("api_key", "project", "location", "base_url", "max_tokens"):
        kwargs.pop(field, None)
    if "max_tokens" in entry or "max_output_tokens" in entry:
        kwargs["max_output_tokens"] = entry.get("max_output_tokens") or entry.get("max_tokens")
    key = resolve(entry["api_key"]) if isinstance(entry.get("api_key"), str) else None
    project = _resolve_ref(entry.get("project"), resolve)
    if key:
        kwargs["api_key"] = key
    elif project:
        kwargs.update(vertexai=True, project=project, location=_resolve_ref(entry.get("location"), resolve) or "global")
    elif entry.get("api_key"):
        raise MissingKeyError(alias, str(entry["api_key"]))
    else:
        raise RuntimeError(f"model {alias!r}: a Gemini model needs an api_key (AI Studio) or a project (Vertex)")
    if thinking and thinking != "off":
        kwargs["thinking_budget"] = THINKING_BUDGET_TOKENS[thinking]
        kwargs["include_thoughts"] = True
    elif thinking == "off":
        kwargs["thinking_budget"] = 0
    return kwargs


def _anthropic_kwargs(alias: str, entry: dict, thinking: str | None, resolve: Resolver) -> dict[str, Any]:
    """The direct Anthropic API: ``thinking`` and ``output_config`` are native
    `ChatAnthropic` parameters, so the request dict IS ``kwargs``."""
    kwargs = _generic_kwargs(alias, entry, resolve)
    _apply_anthropic_thinking(kwargs, entry, str(entry["model"]), thinking, request=kwargs)
    _drop_rejected_sampling(kwargs, alias)
    return kwargs


def _generic_kwargs(alias: str, entry: dict, resolve: Resolver) -> dict[str, Any]:
    kwargs = _base_kwargs(entry)
    for field in ("base_url", "project", "location"):
        if field in kwargs:
            kwargs[field] = _resolve_ref(kwargs[field], resolve)
    # The same redirect the OpenAI-compatible family follows: an entry reading
    # ANTHROPIC_API_KEY follows ANTHROPIC_BASE_URL. Only ever set when the
    # variable is, so a family that takes no base_url is untouched unless the
    # deployment asked for one.
    if override := _base_url_override(entry, resolve):
        kwargs["base_url"] = override
    key = _resolve_key(alias, entry, resolve)
    if key:
        kwargs["api_key"] = key
    else:
        kwargs.pop("api_key", None)
    return kwargs


def build_chat_model(
    entry: dict[str, Any],
    *,
    alias: str = "",
    thinking: str | None = None,
    resolve: Resolver | None = None,
):
    """One `MODELS` entry → one LangChain chat model.

    Raises `MissingKeyError` when the entry names a key that is not set,
    `RuntimeError` for an entry its family cannot build (no project for a
    Vertex model, no federated token for Azure WIF), and whatever the
    provider package raises for anything else. `build_llm_clients` decides
    what those mean for a whole table; a consumer building one model at a
    time gets them as they are.
    """
    if not isinstance(entry, dict) or not entry.get("model"):
        raise ValueError(f"model {alias!r}: entry has no 'model'")
    resolve = resolve or default_resolver
    model = model_name = str(entry["model"])
    family = llm_type = _llm_type(entry)
    thinking_level = thinking or "-"

    if family in OPENAI_COMPATIBLE_TYPES:
        client = reasoning_chat_openai()(**_openai_kwargs(alias, entry, thinking, resolve), model=model)
    elif family in ANTHROPIC_VERTEX_TYPES:
        from langchain.chat_models import init_chat_model

        client = init_chat_model(model=model, model_provider=family, **_vertex_anthropic_kwargs(alias, entry, thinking, resolve))
    elif family in GEMINI_TYPES:
        from langchain_google_genai import ChatGoogleGenerativeAI

        client = ChatGoogleGenerativeAI(model=model, **_gemini_kwargs(alias, entry, thinking, resolve))
    elif family == "anthropic":
        from langchain.chat_models import init_chat_model

        client = init_chat_model(model=model, model_provider=family, **_anthropic_kwargs(alias, entry, thinking, resolve))
    else:
        from langchain.chat_models import init_chat_model

        if thinking:
            logger.info("thinking ignored: thinking_level=%s llm_type=%s model_name=%s", thinking_level, llm_type, model_name)
        client = init_chat_model(model=model, model_provider=family, **_generic_kwargs(alias, entry, resolve))

    override = _profile_override(entry)
    if override:
        try:
            client.profile = {**(getattr(client, "profile", None) or {}), **override}
        except Exception as exc:
            logger.warning("model %s: capability override not applied (%s)",  # phi: ok a MODELS entry name
                           alias, type(exc).__name__)
    logger.info("model %s ready: llm_type=%s model_name=%s thinking_level=%s",  # phi: ok a MODELS entry name
                alias, llm_type, model_name, thinking_level)
    return client


# --- the table --------------------------------------------------------------------------------

class _PlaceholderClient:
    """Stand-in for a `MODELS` entry whose key or address is missing.

    Holds the model name (so `getattr(client, "model_name")` works for
    diagnostics) and the sentence `unavailable_reason` returns; any other
    attribute raises `AttributeError` with that sentence, so nothing can call
    it as a model by mistake.
    """

    def __init__(self, model_name: str, missing: str, hint: str = ""):
        hint = hint or "Get an API key from the provider and set it in .env or the environment."
        object.__setattr__(self, "_reason", f"{missing} is not set. {hint}")
        object.__setattr__(self, "model_name", model_name)
        object.__setattr__(self, "model", model_name)

    def __getattribute__(self, name):
        if name in ("model_name", "model", "_reason"):
            return object.__getattribute__(self, name)
        raise AttributeError(object.__getattribute__(self, "_reason"))


def unavailable_reason(client: Any) -> str:
    """Why `client` cannot answer (the missing key or address of a placeholder
    `build_llm_clients` stood in), or "" for a real model."""
    return client._reason if isinstance(client, _PlaceholderClient) else ""


def build_llm_clients(
    llm_client_config: dict[str, Any],
    owner: str = "agent",
    *,
    resolve: Resolver | None = None,
) -> dict[str, Any]:
    """One chat model per `MODELS` entry.

    An entry whose key is not set becomes a `_PlaceholderClient` rather than
    an error, so a zero-key deployment still boots and the model picker can
    say which entries are usable (`registry.available_models`). An entry its
    family cannot build is logged by name and skipped.
    """
    class_name = owner
    if not llm_client_config:
        logger.warning("[%s] no MODELS entries configured", class_name)
        return {}
    clients: dict[str, Any] = {}
    failed: list[tuple[str, str]] = []
    placeholder_count = 0
    # `model` is an entry's name, the word `DEFAULT_MODEL` and `/api/models` use.
    for model, entry in llm_client_config.items():
        if not isinstance(entry, dict) or not entry.get("model"):
            failed.append((model, "entry is not a dict with a 'model'"))
            continue
        try:
            clients[model] = build_chat_model(entry, alias=model, resolve=resolve)
        except MissingKeyError as exc:
            logger.warning("[%s] model %s: %s %s not set — placeholder", class_name, model, exc.field, exc.key)
            hint = "Set it in .env to the URL of the model server (…/v1)." if isinstance(exc, MissingEndpointError) else ""
            clients[model] = _PlaceholderClient(str(entry["model"]), exc.key, hint)
            placeholder_count += 1
        except Exception as exc:
            logger.error("[%s] model %s failed: error_type=%s", class_name, model, type(exc).__name__,
                         exc_info=not is_driver_exception(exc))
            failed.append((model, type(exc).__name__))
    for model, error_type in failed:
        logger.warning("[%s] model %s skipped: error_type=%s", class_name, model, error_type)
    logger.info(
        "[%s] models loaded: loaded_count=%d total_count=%d placeholder_count=%d",
        class_name, len(clients), len(llm_client_config), placeholder_count,
    )
    if not clients:
        logger.warning("[%s] no models loaded — the agent may be disabled intentionally", class_name)
    return clients
