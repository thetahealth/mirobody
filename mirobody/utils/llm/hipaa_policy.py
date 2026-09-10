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

from mirobody.utils.config import global_config, safe_read_cfg

logger = logging.getLogger(__name__)


def _get_azure_yaml_cfg() -> dict:
    """Read the AZURE_OPENAI block from YAML config.

    Example:

        AZURE_OPENAI:
          endpoint: https://my-resource.openai.azure.com/
          api_version: 2025-03-01-preview
    """
    cfg = global_config()
    return (cfg.get_dict("AZURE_OPENAI") or {}) if cfg else {}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def export_to_env() -> None:
    """Bridge configured values into standard SDK env vars.

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
