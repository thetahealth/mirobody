import base64
import dotenv
import io
import json
import logging
import os
import re

from ruamel.yaml import YAML
from typing import Any

from ... import __version__
from typing import TYPE_CHECKING

from .encrypt import FernetEncrypter
from .log import LogConfig
from .http import HttpConfig
from .llm import LLMConfig, LLMProvider, _OPENAI_COMPAT

if TYPE_CHECKING:  # heavy drivers — imported lazily inside the accessors below
    from .postgresql import PostgreSQLConfig
    from .redis import RedisConfig

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

_global_config = None

#: The value shipped in place of every secret that a real deployment must
#: replace. Named in one place because two behaviors key on it: the auto-
#: encryption pass skips it (encrypting the placeholder would hide that it was
#: never changed), and `PRODUCTION: true` refuses to start while any key still
#: carries it (see `server/bootstrap.py`).
PLACEHOLDER_SENTINEL = "REPLACE_THIS_VALUE_IN_PRODUCTION"

#-----------------------------------------------------------------------------
# Keys 1.4.0 and 1.4.1 renamed, and the one place that knows every spelling.
#
# When the agent stopped being "the DeepAgent" its config keys lost the `_DEEP`
# suffix (1.4.0); when the model table stopped being called `PROVIDERS` — in
# this project a provider is a device — it became `MODELS` (1.4.1). The upgrade
# failure that motivates this table was SILENT: an overlay written for 1.3.x
# still said `PROVIDERS_DEEP`, the new key was simply absent from it, and the
# agent booted with zero models and an empty `/api/models` — nothing raised,
# nothing logged, and the deployment looked healthy. Owner's call
# (2026-09-07): map the old spelling onto the new one.
#
# It has to happen at LOAD time, not read time. The shipped `config.llm.yaml`
# declares `MODELS`, `PROMPTS`, `ALLOWED_TOOLS` and `DISALLOWED_TOOLS` itself,
# so a "fall back when the new key is missing" alias would never fire — the
# shipped default shadows the user's overlay, which is the whole bug.
# Renaming as each file merges means ordinary layering decides: a later file's
# old spelling overrides an earlier file's new one, exactly as it did in 1.3.x.
_RENAMED_KEYS = {
    "PROVIDERS_DEEP": "MODELS",
    "PROMPTS_DEEP": "PROMPTS",
    "ALLOWED_TOOLS_DEEP": "ALLOWED_TOOLS",
    "DISALLOWED_TOOLS_DEEP": "DISALLOWED_TOOLS",
    "DEFAULT_PROVIDER_DEEP": "DEFAULT_MODEL",
    # 1.4.1: in this project a "provider" is a device or data source
    # (PROVIDER_DIRS, mirobody/pulse/providers); the model table is MODELS.
    "PROVIDERS": "MODELS",
    "DEFAULT_PROVIDER": "DEFAULT_MODEL",
    "EMBEDDING_PROVIDER": "UTILS_EMBEDDING_MODEL",
}

#: new spelling -> its old spellings, for the environment-variable half.
_RENAMED_FROM: dict[str, tuple[str, ...]] = {}
for _old, _new in _RENAMED_KEYS.items():
    _RENAMED_FROM[_new] = (*_RENAMED_FROM.get(_new, ()), _old)

#: Keys 1.4.0 REMOVED, with what replaced them. Deliberately not aliased:
#: `SSE_HEARTBEAT_SECONDS` is not `HEARTBEAT_INTERVAL` under a new name (the
#: old pair multiplied to a first ping at 40 s; the new one fires on silence),
#: and the two directory keys have no successor. Silently ignoring them is what
#: makes an upgrade look fine while behaving differently, so they are named.
_REMOVED_KEYS = {
    "PRIVATE_AGENT_DIRS": "removed; `AGENT_DIRS` is the one agent search path",
    "MCP_RESOURCE_DIRS": "removed with the MCP `resources` capability",
    "HEARTBEAT_INTERVAL": "replaced by `SSE_HEARTBEAT_SECONDS` (seconds of silence, default 8)",
    "HEARTBEAT_COUNTER_THRESHOLD": "replaced by `SSE_HEARTBEAT_SECONDS`",
}

#: Keys already warned about, so an overlay layered over three files says it
#: once. Module-level because `Config` is instantiated more than once in a
#: process (tests, and the worker's own reload).
_warned_keys: set[str] = set()


def _warn_renamed(old_key: str, new_key: str) -> None:
    if old_key in _warned_keys:
        return
    _warned_keys.add(old_key)
    logger.warning(
        "config key %s was renamed to %s; the old spelling is being "
        "read as the new one. Rename it in your overlay — this alias is a "
        "migration courtesy, not the contract.", old_key, new_key,
    )


def _warn_removed(key: str, reason: str) -> None:
    """`reason` is passed in rather than looked up here: `phi_lint` flags every
    subscript inside a `logger.*` call, and a table lookup at the call site is
    not a log statement."""
    if key in _warned_keys:
        return
    _warned_keys.add(key)
    logger.warning("config key %s is %s; it is being ignored.", key, reason)


def _legacy_env(upper_key: str) -> str | None:
    """The 1.3.x environment variable for `upper_key`, if that is where the
    value is. `None` when the key was never renamed or the old one is unset,
    so a deployment on the current spelling pays one dict lookup and warns
    never."""
    for old in _RENAMED_FROM.get(upper_key, ()):
        s = os.environ.get(old)
        if s is not None:
            _warn_renamed(old, upper_key)
            return s
    return None

#-----------------------------------------------------------------------------

class Config:

    yaml = YAML()

    #-------------------------------------------------

    def __init__(
        self,
        yaml_filenames: str | list[str | io.StringIO] | None = None,
        encrypter: FernetEncrypter | None = None
    ):
        if isinstance(yaml_filenames, str | io.StringIO):
            self._yaml_filenames = [yaml_filenames]
        elif isinstance(yaml_filenames, list):
            self._yaml_filenames = yaml_filenames
        else:
            self._yaml_filenames = []

        #-------------------------------------------------

        self._raw = {}

        self._postgresqls = {}
        self._redises = {}
        self._llms: dict[LLMProvider, LLMConfig] = {}

        self._agent_options = {}

        #-------------------------------------------------

        self._encrypter = encrypter
        if not self._encrypter:
            self._encrypter = FernetEncrypter(self.get_fernet_key("CONFIG_ENCRYPTION_KEY"))

        #-------------------------------------------------

        # Load YAML files. A file's INCLUDE list loads right after it and
        # before the next file, so a later overlay still wins over everything
        # an earlier file pulled in.
        for yaml_filename in self._yaml_filenames:
            self._load_with_includes(yaml_filename)

        self.refresh()

        global _global_config
        _global_config = self

    #-----------------------------------------------------

    def refresh(self, data: dict | None = None):
        if data is None:
            data = {}
        if data:
            self._raw.update(data)

        # Clear cached configuration objects to ensure they use updated _raw values
        self._postgresqls = {}
        self._redises = {}
        self._llms = {}

        self.log = LogConfig(
            name        = self.get_str("LOG_NAME"),
            dir         = self.get_str("LOG_DIR"),
            level       = logging.getLevelNamesMapping().get(self.get_str("LOG_LEVEL").strip().upper(), logging.INFO),
            secret_key  = self.get_fernet_key("LOG_ENCRYPTION_KEY")
        )

        self.http = HttpConfig(
            name        = self.get_str("HTTP_SERVER_NAME"),
            version     = self.get_str("HTTP_SERVER_VERSION") or __version__,
            host        = self.get_str("HTTP_HOST"),
            port        = self.get_int("HTTP_PORT"),
            uri_prefix  = self.get_str("HTTP_URI_PREFIX"),
            htdoc       = self.get_str("HTTP_ROOT"),
            headers     = self.get_dict("HTTP_HEADERS", {})
        )

        self.jwt_key            = self.get_str("JWT_KEY")
        self.jwt_private_key    = self.get_str("JWT_PRIVATE_KEY")

        self.mcp_tool_dirs      = self.get_dirs("MCP_TOOL_DIRS", [])
        self.agent_dirs         = self.get_dirs("AGENT_DIRS", [])
        self.task_dirs          = self.get_dirs("TASK_DIRS", [])

        self.mcp_server_url = self.get_str("MCP_PUBLIC_URL")

        self.api_keys = self.get_api_keys()


    def _load_with_includes(self, file: str | io.StringIO, depth: int = 0) -> None:
        from .yaml_files import include_paths

        includes = self.load_yaml(file)
        if depth >= 3:
            if includes:
                logger.warning("INCLUDE nesting deeper than 3 in %s is ignored", file)
            return
        for path in include_paths(file if isinstance(file, str) else None, includes):
            if not os.path.exists(path):
                logger.warning("INCLUDE names %s, which does not exist; skipped", path)  # phi: ok a filename from our own INCLUDE list
                continue
            self._load_with_includes(path, depth + 1)

    def load_yaml(self, file: str | io.StringIO) -> list:
        """Merge one YAML document into the configuration. Returns the file's
        `INCLUDE` list (empty when it has none) for the caller to load next."""
        if not file:
            return []

        stream = None

        if isinstance(file, str):
            # Filename.
            try:
                with open(file, encoding="utf-8") as f:
                    s = f.read()
                    stream = io.StringIO(s)

            except Exception as e:
                logger.warning(f"Failed to load YAML file '{file}': {str(e)}")
                return []

        elif isinstance(file, io.StringIO):
            # File content from StringIO (e.g., remote config)
            stream = file

        if stream is None:
            return []

        #-------------------------------------------------

        Config.yaml = YAML()

        modified = False

        data = Config.yaml.load(stream)
        if not isinstance(data, dict):
            return []

        includes: list = []
        for key, value in data.items():
            if not isinstance(key, str):
                continue

            upper_key = key.upper()

            if upper_key == "INCLUDE":
                # A loading instruction, not a setting: consumed here, never
                # stored, so an overlay's own INCLUDE adds files rather than
                # "overriding" a value nothing reads.
                includes = list(value) if isinstance(value, list) else []
                continue

            if upper_key in _REMOVED_KEYS:
                _warn_removed(upper_key, _REMOVED_KEYS[upper_key])
                continue

            renamed = _RENAMED_KEYS.get(upper_key)
            if renamed:
                if any(isinstance(k, str) and k.upper() == renamed for k in data):
                    # This file spells it both ways. The current name wins,
                    # rather than whichever `data` happened to yield last.
                    continue
                _warn_renamed(upper_key, renamed)
                upper_key = renamed

            if self._encrypter and isinstance(value, str) and len(value) > 0:
                # Check non-empty strings.

                if self._encrypter.is_encrypted(value):
                    # Decrypt it.
                    self._raw[upper_key] = self._encrypter.decrypt(value)
                    continue

                if re.search(r"_KEY|_PASSWORD|_PASS|_PWD|_SECRET|_SK|_TOKEN", upper_key) and \
                    not upper_key.endswith("_URL") and \
                    value != PLACEHOLDER_SENTINEL:

                    # Encrypt it.
                    #
                    # An empty result means the encrypter is a no-op (an
                    # unusable key — see `get_fernet_key`). Writing that back
                    # would REPLACE the user's real secret with "" in the
                    # config file. `self._raw` keeps the plaintext, so the
                    # process keeps working and the loss only surfaces on the
                    # next restart, with nothing to point at. Leave the file
                    # alone and say so.
                    encrypted = self._encrypter.encrypt(value)
                    if not encrypted:
                        logger.error(
                            "refusing to write config: %s could not be "
                            "encrypted (encryption key unusable). The value on "
                            "disk is left untouched and remains in plaintext.",
                            upper_key,
                        )
                    else:
                        data[key] = encrypted
                        if not modified:
                            modified = (data[key] != value)

            self._raw[upper_key] = value

        #-------------------------------------------------

        if isinstance(file, str) and modified:
            try:
                with open(file, "w+", encoding="utf-8") as f:
                    if f.writable():
                        Config.yaml.dump(data, f)

            except Exception as e:
                logger.warning(f"Failed to update YAML file '{file}': {str(e)}")

        return includes

    #-----------------------------------------------------

    def get(self, key: str, default: Any = None) -> Any:
        stripped_key = key.strip()
        if not stripped_key:
            return default

        # Check environment variables beforehand.
        s = os.environ.get(stripped_key)
        if s is not None:
            return s

        # Check key in upper case again.
        upper_key = stripped_key.upper()
        s = os.environ.get(upper_key)
        if s is not None:
            return s

        # The pre-1.4.0 spelling, if the deployment sets it in the environment
        # rather than in an overlay. Before `self._raw`, because environment
        # beats file — and the shipped `config.yaml` declares four of these, so
        # checking after would mean the default always won.
        s = _legacy_env(upper_key)
        if s is not None:
            return s

        # Then check the configuration variables.
        return self._raw.get(upper_key, default)


    def get_str(self, key: str, default: str = "") -> str:
        stripped_key = key.strip()
        if not stripped_key:
            return default

        # Check environment variables beforehand.
        s = os.environ.get(stripped_key)
        if s is not None:
            return s

        # Check key in upper case again.
        upper_key = stripped_key.upper()
        s = os.environ.get(upper_key)
        if s is not None:
            return s

        # The pre-1.4.0 spelling — see `get`.
        s = _legacy_env(upper_key)
        if s is not None:
            return s

        # Then check the configuration variables.
        s = self._raw.get(upper_key, default)
        return s if isinstance(s, str) else str(s)


    def get_int(self, key: str, default: int = 0) -> int:
        obj = self.get(key)

        if isinstance(obj, int):
            return obj

        try:
            n = int(obj)
            return n
        except Exception:
            return default


    def get_bool(self, key: str, default: bool = False) -> bool:
        obj = self.get(key)

        if isinstance(obj, bool):
            return obj

        if isinstance(obj, str):
            # Same truthy set as `demo.enabled()`: environment variables
            # arrive as strings, and "1"/"yes"/"on" must not silently read
            # as False (REDIS_SSL=1 used to).
            return obj.strip().upper() in ("TRUE", "1", "YES", "ON")

        if isinstance(obj, int):
            return obj != 0

        return default


    def placeholder_keys(self) -> list[str]:
        """Config keys whose value is still the shipped placeholder sentinel.

        The demo runs fine on placeholders; a deployment that declared
        `PRODUCTION: true` must not — `server/bootstrap.py` refuses to start
        while this list is non-empty.
        """
        return sorted(
            key for key, value in self._raw.items()
            if value == PLACEHOLDER_SENTINEL
        )


    def get_dict(self, key: str, default: dict | None = None) -> dict:
        obj = self.get(key)

        if isinstance(obj, dict):
            return obj

        if isinstance(obj, str | bytes | bytearray):
            try:
                l = json.loads(obj)
                if isinstance(l, dict):
                    return l
            except Exception:
                return default

        return default


    def get_list(self, key: str, default: list | None = None) -> list:
        obj = self.get(key)

        if isinstance(obj, list):
            return obj

        if isinstance(obj, str | bytes | bytearray):
            try:
                l = json.loads(obj)
                if isinstance(l, list):
                    return l
            except Exception:
                return default

        return default


    def get_dirs(self, key: str, default: list | None = None) -> list[str]:
        l = self.get_list(key, default)

        dirs = []
        for s in l:
            if not s or not isinstance(s, str):
                continue

            s = s.strip()
            if not s:
                continue

            s = s.replace("/", os.sep)
            dirs.append(s)

        return dirs if dirs else default


    def get_fernet_key(self, key: str) -> str:
        """Derive a Fernet key from the configured passphrase.

        Fernet needs exactly 32 raw BYTES, urlsafe-base64 encoded. This used to
        truncate to 32 CHARACTERS and then pad to 32 BYTES, which are the same
        operation only for ASCII. A passphrase with any CJK, accented or emoji
        character produced 33-96 bytes, `ljust(32)` padded nothing, `Fernet()`
        rejected the result, and the encrypter silently became a no-op — see
        `FernetEncrypter.__init__`, and `_load_data` for what a no-op encrypter
        then did to the config file.

        Slicing the ENCODED bytes fixes it and changes nothing for an ASCII
        passphrase, so no existing deployment has to re-encrypt.

        The all-zeros fallback when the passphrase is empty is kept — changing
        the derivation would strand every config already encrypted under it —
        but it is no longer silent. It is a publicly known key; anything
        "encrypted" with it is plaintext with extra steps.
        """
        s = self.get_str(key).strip()

        if not s:
            logger.error(
                "%s is not set. Config secrets will be 'encrypted' with a "
                "fixed, publicly-known key and are effectively plaintext. Set "
                "it to a random 32-character value.", key,
            )

        try:
            # Slice AFTER encoding: `s[:32].encode()` can exceed 32 bytes.
            raw = s.encode("utf-8")[:32].ljust(32, b"0")
            return base64.urlsafe_b64encode(raw).decode()

        except Exception as e:
            # Deliberately not logging `s`: it is the encryption passphrase,
            # and the old version put it in the log line verbatim.
            logger.error("could not derive a Fernet key from %s: %s", key, e)
            return ""


    #-----------------------------------------------------

    def get_jwt_options(self) -> dict[str, str]:
        return {
            "jwt_key"           : self.get_str("JWT_KEY"),
            "jwt_iss"           : self.get_str("JWT_ISS"),
            "jwt_aud"           : self.get_str("JWT_AUD"),
            "jwt_client_id"     : self.get_str("JWT_CLIENT_ID"),
            "jwt_scope"         : self.get_str("JWT_SCOPE"),
            "jwt_expires_in"    : self.get_int("JWT_EXPIRES_IN")
        }

    #-----------------------------------------------------

    def get_api_keys(self) -> dict[str, str]:
        results = {}

        for key, value in self._raw.items():
            if key.endswith("_API_KEY") and value and isinstance(value, str):
                results[key] = value
                os.environ.setdefault(key, value)

        for key in [
            "AZURE_TENANT_ID",
            "AZURE_CLIENT_ID",
            "AZURE_FEDERATED_TOKEN_FILE",

            "GOOGLE_CLOUD_PROJECT",
            "GOOGLE_CLOUD_LOCATION",
            "GOOGLE_GENAI_USE_VERTEXAI",
        ]:
            value = self.get_str(key)
            if value:
                results[key] = value
                os.environ.setdefault(key, value)

        return results

    #-----------------------------------------------------

    def get_mcp_options(self) -> dict[str, str | list[str]]:
        return {
            "tool_dirs"         : self.mcp_tool_dirs,
        }


    def get_agent_options(self) -> dict[str, list[str] | dict[str, str]]:
        return {
            "agent_dirs"        : self.agent_dirs,
            "api_keys"          : self.api_keys
        }


    def get_agent_settings(self) -> dict[str, Any]:
        """The agent's runtime settings from the four plain keys, cached.

        `MODELS`, `PROMPTS`, `ALLOWED_TOOLS`, `DISALLOWED_TOOLS` — one agent,
        one set of keys (they used to carry the agent's name as a suffix). The
        two halves that do real work — resolving `PROMPTS` path references into
        template text, and normalising the three accepted shapes of `MODELS`
        — live in `agent_options.py`, testable without a Config or a
        filesystem. What is left here is the caching.
        """
        if self._agent_options:
            return self._agent_options

        from .agent_options import load_prompt_templates, parse_providers

        options = {
            "allowed_tools"     : self.get_list("ALLOWED_TOOLS", []),
            "disallowed_tools"  : self.get_list("DISALLOWED_TOOLS", []),
            "prompt_templates"  : load_prompt_templates(self),
            "providers"         : parse_providers(self),
        }

        self._agent_options = options
        return options

    #-----------------------------------------------------

    def get_email_options(self) -> dict[str, str | int | dict[str, str]]:
        return {
            "email_from"        : self.get_str("EMAIL_FROM"),
            "email_from_name"   : self.get_str("EMAIL_FROM_NAME"),
            "email_template"    : self.get_str("EMAIL_TEMPLATE"),
            "email_password"    : self.get_str("EMAIL_SMTP_PASS"),
            "email_predefined"  : self.get_dict("EMAIL_PREDEFINE_CODES", {}),
            "email_smtp_host"   : self.get_str("EMAIL_SMTP_HOST"),
            "email_smtp_port"   : self.get_int("EMAIL_SMTP_PORT", 0),
            "email_smtp_user"   : self.get_str("EMAIL_SMTP_USER"),
        }


    def get_apple_options(self) -> dict[str, str]:
        return {
            "apple_client_id"   : self.get_str("APPLE_CLIENT_ID"),
            "apple_team_id"     : self.get_str("APPLE_TEAM_ID"),
            "apple_key_id"      : self.get_str("APPLE_KEY_ID"),
            "apple_private_key" : self.get_str("APPLE_PRIVATE_KEY"),
            "apple_auth_client_id" : self.get_str("APPLE_CLIENT_ID_APP")
        }


    def get_google_options(self) -> dict[str, str]:
        return {
            "google_client_id"      : self.get_str("GOOGLE_CLIENT_ID")
        }


    def get_webauthn_options(self) -> dict:
        return {
            "webauthn_rp_id"            : self.get_str("WEBAUTHN_RP_ID"),
            "webauthn_rp_name"          : self.get_str("WEBAUTHN_RP_NAME"),
            "webauthn_origin"           : self.get_str("WEBAUTHN_ORIGIN"),
            "webauthn_mfa_ticket_ttl"   : self.get_int("WEBAUTHN_MFA_TICKET_TTL", 300),
        }

    def get_postgresql(self, key: str="") -> "PostgreSQLConfig":
        # Imported here, not at module scope. `mirobody.utils.config` is on the
        # import path of the whole ENGINE — `mirobody.engine`, `indicator`,
        # `pulse` — so a module-level `from .postgresql import …` made psycopg +
        # SQLAlchemy a hard requirement of `resolve()`, which touches no
        # database at all. Only a caller that actually wants a DB handle pays.
        from .postgresql import PostgreSQLConfig

        upper_key = key.strip().upper()
        if upper_key in self._postgresqls:
            return self._postgresqls[upper_key]

        #-------------------------------------------------

        suffix = upper_key
        if suffix:
            suffix = "_" + suffix

        pg_config = PostgreSQLConfig(
            host        = self.get_str(f"PG_HOST{suffix}"),
            port        = self.get_int(f"PG_PORT{suffix}"),
            user        = self.get_str(f"PG_USER{suffix}"),
            password    = self.get_str(f"PG_PASSWORD{suffix}"),
            database    = self.get_str(f"PG_DBNAME{suffix}"),
            schema      = self.get_str(f"PG_SCHEMA{suffix}"),
            minconn     = self.get_int(f"PG_MIN_CONNECTION{suffix}"),
            maxconn     = self.get_int(f"PG_MAX_CONNECTION{suffix}"),
            timeout     = self.get_int(f"PG_TIMEOUT{suffix}"),
            encrypt_key = self.get_str("PG_ENCRYPTION_KEY"),
        )

        self._postgresqls[upper_key] = pg_config
        return pg_config

    #-----------------------------------------------------

    def get_redis(self, key: str="") -> "RedisConfig":
        from .redis import RedisConfig          # lazy — see get_postgresql

        upper_key = key.strip().upper()
        if upper_key in self._redises:
            return self._redises[upper_key]

        #-------------------------------------------------

        suffix = upper_key
        if suffix:
            suffix = "_" + suffix

        redis_config = RedisConfig(
            host                = self.get_str(f"REDIS_HOST{suffix}"),
            port                = self.get_int(f"REDIS_PORT{suffix}"),
            password            = self.get_str(f"REDIS_PASSWORD{suffix}"),
            database            = self.get_int(f"REDIS_DB{suffix}"),
            minconn             = self.get_int(f"REDIS_MIN_CONNECTION{suffix}"),
            maxconn             = self.get_int(f"REDIS_MAX_CONNECTION{suffix}"),
            timeout             = self.get_int(f"REDIS_TIMEOUT{suffix}"),
            ssl                 = self.get_bool(f"REDIS_SSL{suffix}"),
            ssl_check_hostname  = self.get_bool(f"REDIS_SSL_CHECK_HOSTNAME{suffix}"),
            ssl_cert_reqs       = self.get_str(f"REDIS_SSL_CERT_REQS{suffix}")
        )

        self._redises[upper_key] = redis_config
        return redis_config

    #-----------------------------------------------------

    def get_llm(self, provider: LLMProvider) -> LLMConfig:
        if provider in self._llms:
            return self._llms[provider]

        if provider in _OPENAI_COMPAT:
            api_key_env, default_base_url = _OPENAI_COMPAT[provider]
            # `<PROVIDER>_BASE_URL` (OPENROUTER_BASE_URL, DASHSCOPE_BASE_URL, …)
            # redirects the provider to a self-hosted OpenAI-compatible
            # endpoint — the mechanism behind the README's "serve the same
            # embedding model yourself and point the provider's base_url at
            # it". Config.get reads the environment first, so an env var or a
            # config key both work.
            llm_config = LLMConfig(
                provider = provider,
                api_key  = self.get_str(api_key_env),
                base_url = self.get_str(api_key_env.replace("_API_KEY", "_BASE_URL"))
                           or default_base_url,
            )
        elif provider == LLMProvider.ANTHROPIC:
            llm_config = LLMConfig(
                provider = provider,
                api_key  = self.get_str("ANTHROPIC_API_KEY"),
                base_url = self.get_str("ANTHROPIC_BASE_URL"),
            )
        elif provider == LLMProvider.GEMINI:
            llm_config = LLMConfig(
                provider           = provider,
                api_key            = self.get_str("GOOGLE_API_KEY"),
                gemini_api_version = self.get_str("GEMINI_API_VERSION") or "v1beta",
            )
        elif provider == LLMProvider.VERTEX_AI:
            llm_config = LLMConfig(
                provider     = provider,
                gcp_project  = self.get_str("GCP_PROJECT"),
                gcp_location = self.get_str("GCP_LOCATION") or "us-east5",
            )
        elif provider == LLMProvider.AZURE:
            azure_cfg = self.get_dict("AZURE_OPENAI") or {}
            llm_config = LLMConfig(
                provider    = provider,
                endpoint    = azure_cfg.get("endpoint", ""),
                api_version = azure_cfg.get("api_version", "2024-12-01-preview"),
            )
        elif provider == LLMProvider.BEDROCK:
            llm_config = LLMConfig(
                provider   = provider,
                aws_region = self.get_str("AWS_REGION") or "us-east-1",
            )
        else:
            raise ValueError(f"Unsupported LLM provider: {provider!r}")

        self._llms[provider] = llm_config
        return llm_config

    #-----------------------------------------------------

    def print(self):
        print(f"Configuration loaded from {self._yaml_filenames}:")
        print("----------------------------------------------------------")
        print(f"env             : {os.environ.get("ENV", "").strip().lower()}")
        print(f"debug           : {self.log.level <= logging.DEBUG}")

        self.log.print()
        self.http.print()

        self.get_redis().print()
        self.get_postgresql().print()

        if self.jwt_key:
            print(f"jwt             : {Config.to_masked_str(self.jwt_key)}")
        if self.mcp_server_url:
            print(f"mcp             : {self.mcp_server_url}")
        if self.mcp_tool_dirs:
            print(f"tools           : {self.mcp_tool_dirs}")
        if self.agent_dirs:
            print(f"agents          : {self.agent_dirs}")
        if self.task_dirs:
            print(f"tasks           : {self.task_dirs}")

        if self.api_keys:
            print()
            for key in self.api_keys:
                print(f"{key.lower():<32}: {Config.to_masked_str(self.api_keys[key].lower())}")

        print("----------------------------------------------------------")


    def print_predefined_codes(self):
        codes = self.get_dict("EMAIL_PREDEFINE_CODES", {})
        if codes:
            BOLD = "\033[1m"
            GREEN = "\033[32m"
            RESET = "\033[0m"

            mcp_public_url = self.get_str("MCP_PUBLIC_URL")

            # Use MCP_PUBLIC_URL if set, otherwise construct URL from HTTP config
            default_url = f"http://localhost:{self.http.port}" if self.http.port != 80 else "http://localhost"
            print(f"\nNow you can open {BOLD}{GREEN}{mcp_public_url if mcp_public_url else default_url}{RESET} in browser, and then\n  login with the following:")
            print("------------------------------------------------")
            print(f"{"EMAIL":<25} | VERIFICATION CODE")
            print("------------------------------------------------")

            i = 0
            n = len(codes)
            printed_dots = False
            for email, verification_code in codes.items():
                if i >= 10 and i < n-1:
                    if not printed_dots:
                        print(f"{"...":<25} | ...")
                        printed_dots = True
                else:
                    print(f"{BOLD}{GREEN}{email:<25}{RESET} | {BOLD}{GREEN}{verification_code}{RESET}")
                i += 1
            print("------------------------------------------------\n")

    #-------------------------------------------------------------------------

    @staticmethod
    def to_masked_str(s: str) -> str:
        n = len(s)
        if n <= 0:
            return ""
        if n < 6:
            return "************"

        return f"{s[:3]}******{s[n-3:]}"

    #-------------------------------------------------------------------------

    @staticmethod
    def load_dotenv(filenames: str | list[str] | None = None):
        if isinstance(filenames, str):
            l = [filenames]
        elif isinstance(filenames, list):
            l = filenames
        else:
            return

        for filename in l:
            filename = filename.strip()
            if not filename:
                continue

            for key, value in dotenv.dotenv_values(filename).items():
                if value:
                    value = value.strip()
                if not value:
                    continue

                key = key.strip()
                if not key:
                    continue

                os.environ.setdefault(key.upper(), value)

    #-------------------------------------------------------------------------
    @staticmethod
    async def init(
        yaml_filenames  : str | list[str] | None = None,
        dotenv_filenames: str | list[str] = None,
        log_extra       : dict | None = None
    ):
        if log_extra is None:
            log_extra = {}
        if dotenv_filenames is None:
            dotenv_filenames = [".env"]
        log_extra = dict(log_extra) if log_extra else {}

        # `.env` first, because ENV feeds the log fields below and the formatter
        # has to be built with them already in place.
        Config.load_dotenv(dotenv_filenames)

        env = os.environ.get("ENV", "").strip().lower()
        if env and log_extra:
            log_extra["env"] = env

        from ..log import init_log_console
        init_log_console(extra=log_extra)

        #-----------------------------------------------------

        class URLFilter(logging.Filter):
            def __init__(self, blocked_urls):
                super().__init__()
                self.blocked_urls = blocked_urls

            def filter(self, record: logging.LogRecord) -> bool:
                # The URL is typically found in record.args at index 2 for uvicorn access logs
                if record.args and len(record.args) >= 3:
                    requested_url = record.args[2]
                    if requested_url in self.blocked_urls:
                        return False  # Do not log this record
                return True  # Log all other records

        logging.getLogger("uvicorn.access").addFilter(
            URLFilter(["/api/health"])
        )

        #-----------------------------------------------------

        # `env` and `load_dotenv` used to run HERE, ~25 lines after
        # `init_log_console` had already been handed `log_extra`. The `env`
        # field still reached the log records, but only because JsonFormatter
        # stores the dict it is given by reference rather than copying it —
        # so adding a defensive `dict(extra)` to the formatter, an obviously
        # safe-looking change, would have silently dropped `env` from every log
        # line in production. Ordering, not aliasing, now makes it work.

        #-----------------------------------------------------
        # Which files to look for: each requested `x.yaml` also brings its
        # `x.key.yaml` secret sibling and their `{env}` variants. That
        # expansion is pure and lives in `yaml_files.py`, where it is tested;
        # existence is checked below because one entry (the remote config) has
        # no path.

        from .yaml_files import expand_yaml_filenames

        yaml_file_list = expand_yaml_filenames(yaml_filenames, env)

        #-------------------------------------------------

        final_yaml_file_list = []

        default_yaml = "config.yaml"
        if os.path.exists(default_yaml) and default_yaml not in yaml_file_list:
            final_yaml_file_list.append(default_yaml)
            logger.info("Default config has been loaded.")


        for yaml_filename in yaml_file_list:
            if os.path.exists(yaml_filename):
                final_yaml_file_list.append(yaml_filename)

        config = Config(yaml_filenames=final_yaml_file_list)

        #-----------------------------------------------------

        from ..log import init_log
        init_log(
            name        = config.log.name,
            dir         = config.log.dir,
            level       = config.log.level,
            extra       = log_extra,
            secret_key  = config.log.secret_key
        )

        return config

#-----------------------------------------------------------------------------

def global_config() -> Config | None:
    """The process-wide Config, or None if `Config.init()` has not run.

    Took `*args, **kargs` and discarded them. That is not harmless: callers
    reasonably read `global_config(path)` as "load this config file", and one
    did — `pulse/setup.py` threaded a `config_file_path` parameter down from its
    public signature into this call, where it evaporated. Accepting arguments
    you ignore turns a wrong call into a silent no-op instead of a TypeError.
    """
    return _global_config

#-----------------------------------------------------------------------------

def safe_read_cfg(key: str, default: str = "") -> str:
    """A config value as a string, or `default`.

    With no Config loaded — a library caller, `mirobody parse`, a test — the
    ENVIRONMENT still answers, as it does first when a Config is loaded
    (`Config.get_str`). Before this the no-Config case returned the default
    outright, so `OPENROUTER_API_KEY=... mirobody parse x.pdf` needed a
    second lookup path in every caller that wanted to work without config.yaml
    (`_default_provider`, `resolve_embedding_provider` each grew their own
    `os.environ.get(...) or safe_read_cfg(...)`). One rule, here.
    """
    if not _global_config:
        value = os.environ.get(key.strip()) or os.environ.get(key.strip().upper())
        return value.strip() if value is not None else default

    return _global_config.get_str(key, default).strip()


def get_default_timezone() -> str:
    tz = safe_read_cfg("DEFAULT_TIMEZONE") or safe_read_cfg("_DEFAULT_TIMEZONE")
    return tz if tz else "America/Los_Angeles"

#-----------------------------------------------------------------------------