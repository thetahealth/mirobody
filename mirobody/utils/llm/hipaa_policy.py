"""Routing LLM traffic through BAA-covered providers, when you need that.

Mirobody works with any OpenAI-compatible provider, and the default priority
list starts at plain `api.openai.com`. That is deliberate — an engine should
not dictate your vendor.

If you are handling PHI and need a HIPAA-eligible path, this module is how you
get one without touching call sites: sign a BAA with Azure OpenAI and/or Google
Cloud, put the values in your `config.{env}.yaml`, and call `export_to_env()`
once at startup. It bridges the config into the env vars the SDKs read
natively, after which the existing clients route to those endpoints:

    AZURE_OPENAI_ENDPOINT                          Azure OpenAI v1 endpoint
    GOOGLE_GENAI_USE_VERTEXAI=true                 force the google-genai
                                                   SDK onto the Vertex backend
    GOOGLE_CLOUD_PROJECT / GOOGLE_CLOUD_LOCATION   Vertex project + region

    get_azure_deployment()                         model name -> Azure
                                                   deployment name

`export_to_env()` is not called for you: doing so unconditionally would break
every install that has only an `OPENAI_API_KEY`, and choosing a provider is the
operator's decision. Call it from your own startup path.

Two error messages elsewhere (`llm/clients.py`, `config/llm.py`) already tell
you to do exactly that when Vertex configuration is missing.

The module docstring used to open "All LLM traffic routes through compliant
providers", stated as fact. It describes what happens once you have configured
this, not what a default install does — worth being precise about, since it is
the kind of line someone handling PHI would reasonably rely on.
"""

import logging
import os
import re
from typing import Optional

from mirobody.utils.config import global_config, safe_read_cfg

logger = logging.getLogger(__name__)


def _normalize_deployment_name(model: str) -> str:
    """Normalize a model name into a valid Azure deployment name.

    Rules: lowercase, underscores/spaces → hyphens, strip invalid chars,
    collapse repeated hyphens, trim leading/trailing hyphens.

    Examples:
        GPT-4O          → gpt-4o
        GPT_4o_mini     → gpt-4o-mini
        gpt-4.1         → gpt-4.1
        TEXT_EMBEDDING_3_SMALL → text-embedding-3-small
        gpt 4o          → gpt-4o
    """
    name = model.strip().lower()
    name = name.replace("_", "-").replace(" ", "-")
    # keep only alphanumerics, hyphens, dots
    name = re.sub(r"[^a-z0-9.\-]", "", name)
    # collapse repeated hyphens
    name = re.sub(r"-{2,}", "-", name)
    return name.strip("-.")


def _get_azure_yaml_cfg() -> dict:
    """Read the AZURE_OPENAI block from YAML config.

    Example:

        AZURE_OPENAI:
          endpoint: https://my-resource.openai.azure.com/
          api_version: 2025-03-01-preview
          deployments:
            gpt-4o: my-gpt4o-deployment
            gpt-4.1: my-gpt41-deployment
            text-embedding-3-small:
              deployment: embed-small-prod
              endpoint: https://embed.openai.azure.com/
    """
    cfg = global_config()
    return (cfg.get_dict("AZURE_OPENAI") or {}) if cfg else {}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def export_to_env() -> None:
    """Bridge config center values into standard SDK env vars.

    Call once at startup. Only sets vars that are not already present
    in the environment (explicit env vars take precedence).

    Azure OpenAI v1 endpoint reads:
      - AZURE_OPENAI_ENDPOINT

    google-genai SDK reads:
      - GOOGLE_GENAI_USE_VERTEXAI  (forces Vertex AI backend)
      - GOOGLE_CLOUD_PROJECT
      - GOOGLE_CLOUD_LOCATION
    """
    azure_cfg = _get_azure_yaml_cfg()
    gcp_project = safe_read_cfg("GCP_PROJECT") or ""

    mappings = {
        "AZURE_OPENAI_ENDPOINT": azure_cfg.get("endpoint", ""),
        "GOOGLE_CLOUD_PROJECT": gcp_project,
        "GOOGLE_CLOUD_LOCATION": safe_read_cfg("GCP_LOCATION") or "us-east5",
    }
    # Enable Vertex AI backend in google-genai SDK when GCP project is configured
    if gcp_project:
        mappings["GOOGLE_GENAI_USE_VERTEXAI"] = "true"

    for key, value in mappings.items():
        if value and not os.environ.get(key):
            os.environ[key] = value
            logger.info(f"export_to_env: {key}={value[:30]}{'...' if len(value) > 30 else ''}")


def get_azure_deployment(model_hint: Optional[str] = None) -> str:
    """Resolve Azure deployment name for a model.

    1. YAML explicit overrides take priority (for custom deployment names).
    2. Otherwise, normalize the model name into a valid deployment name.
       Convention: Azure deployments are created using the model's
       normalized name (e.g. "gpt-4o", "gpt-4.1", "text-embedding-3-small").
    """
    # YAML explicit overrides (optional, for non-standard deployment names)
    yaml_overrides: dict[str, str] = {}
    for name, value in _get_azure_yaml_cfg().get("deployments", {}).items():
        if isinstance(value, str):
            yaml_overrides[_normalize_deployment_name(name)] = value
        elif isinstance(value, dict) and "deployment" in value:
            yaml_overrides[_normalize_deployment_name(name)] = value["deployment"]

    if not model_hint:
        return "gpt-4o"

    normalized = _normalize_deployment_name(model_hint)
    if normalized in yaml_overrides:
        return yaml_overrides[normalized]
    return normalized