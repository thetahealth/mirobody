import aiohttp, base64, dotenv, importlib, importlib.resources, importlib.metadata, inspect, io, json, logging, os, re

from ruamel.yaml import YAML
from typing import Any

from .encrypt import AbstractEncrypter, FernetEncrypter
from .log import LogConfig
from .http import HttpConfig
from .postgresql import PostgreSQLConfig
from .redis import RedisConfig
from .storage.abstract import AbstractStorage

#-----------------------------------------------------------------------------

_global_config = None

#-----------------------------------------------------------------------------

class Config:

    yaml = YAML()

    #-------------------------------------------------

    def __init__(
        self,
        yaml_filenames: str | list[str | io.StringIO] | None = None,
        encrypter: AbstractEncrypter | None = None
    ):
        if isinstance(yaml_filenames, str|io.StringIO):
            self._yaml_filenames = [yaml_filenames]
        elif isinstance(yaml_filenames, list):
            self._yaml_filenames = yaml_filenames
        else:
            self._yaml_filenames = []

        #-------------------------------------------------

        self._raw = {}

        self._postgresqls = {}
        self._redises = {}

        self._agent_options = {}

        #-------------------------------------------------

        self._encrypter = encrypter
        if not self._encrypter:
            self._encrypter = FernetEncrypter(self.get_fernet_key("CONFIG_ENCRYPTION_KEY"))

        #-------------------------------------------------

        # Load YAML files.
        for yaml_filename in self._yaml_filenames:
            self.load_yaml(yaml_filename)

        self.refresh()

        global _global_config
        _global_config = self

    #-----------------------------------------------------

    def refresh(self, data: dict = {}):
        if data:
            self._raw.update(data)

        # Clear cached configuration objects to ensure they use updated _raw values
        self._postgresqls = {}
        self._redises = {}

        self.log = LogConfig(
            name        = self.get_str("LOG_NAME"),
            dir         = self.get_str("LOG_DIR"),
            level       = logging.getLevelNamesMapping().get(self.get_str("LOG_LEVEL").strip().upper(), logging.INFO),
            secret_key  = self.get_fernet_key("LOG_ENCRYPTION_KEY")
        )

        self.http = HttpConfig(
            name        = self.get_str("HTTP_SERVER_NAME"),
            version     = self.get_str("HTTP_SERVER_VERSION"),
            host        = self.get_str("HTTP_HOST"),
            port        = self.get_int("HTTP_PORT"),
            uri_prefix  = self.get_str("HTTP_URI_PREFIX"),
            htdoc       = self.get_str("HTTP_ROOT"),
            headers     = self.get_dict("HTTP_HEADERS", {})
        )

        self.jwt_key            = self.get_str("JWT_KEY")
        self.jwt_private_key    = self.get_str("JWT_PRIVATE_KEY")

        self.mcp_tool_dirs      = self.get_dirs("MCP_TOOL_DIRS", [])
        self.mcp_resource_dirs  = self.get_dirs("MCP_RESOURCE_DIRS", [])
        self.agent_dirs         = self.get_dirs("AGENT_DIRS", [])

        self.private_mcp_tool_dirs      = self.get_dirs("PRIVATE_MCP_TOOL_DIRS", [])
        self.private_mcp_resource_dirs  = self.get_dirs("PRIVATE_MCP_RESOURCE_DIRS", [])
        self.private_agent_dirs         = self.get_dirs("PRIVATE_AGENT_DIRS", [])

        self.web_server_url = self.get_str("MCP_FRONTEND_URL")
        self.mcp_server_url = self.get_str("MCP_PUBLIC_URL")
        self.data_server_url= self.get_str("DATA_PUBLIC_URL")

        self.api_keys = self.get_api_keys()


    def load_yaml(self, file: str | io.StringIO):
        if not file:
            return

        stream = None

        if isinstance(file, str):
            # Filename.
            try:
                with open(file, "r", encoding="utf-8") as f:
                    s = f.read()
                    stream = io.StringIO(s)

            except Exception as e:
                logging.warning(f"Failed to load YAML file '{file}': {str(e)}")
                return

        elif isinstance(file, io.StringIO):
            # File content from StringIO (e.g., remote config)
            stream = file

        if stream is None:
            return

        #-------------------------------------------------

        Config.yaml = YAML()

        modified = False

        data = Config.yaml.load(stream)
        if not isinstance(data, dict):
            return

        for key, value in data.items():
            if not isinstance(key, str):
                continue

            upper_key = key.upper()

            if self._encrypter and isinstance(value, str) and len(value) > 0:
                # Check non-empty strings.

                if self._encrypter.is_encrypted(value):
                    # Decrypt it.
                    self._raw[upper_key] = self._encrypter.decrypt(value)
                    continue

                if re.search(r"_KEY|_PASSWORD|_PASS|_PWD|_SECRET|_SK|_TOKEN", upper_key) and \
                    not upper_key.endswith("_URL") and \
                    value != "REPLACE_THIS_VALUE_IN_PRODUCTION":

                    # Encrypt it.
                    data[key] = self._encrypter.encrypt(value)
                    if not modified:
                        modified = (data[key] != value)

            self._raw[upper_key] = value

        #-------------------------------------------------

        if isinstance(file, str) and modified:
            try:
                with open(file, "w+t", encoding="utf-8") as f:
                    if f.writable():
                        Config.yaml.dump(data, f)

            except Exception as e:
                logging.warning(f"Failed to update YAML file '{file}': {str(e)}")

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
        except:
            return default


    def get_bool(self, key: str, default: bool = False) -> bool:
        obj = self.get(key)

        if isinstance(obj, bool):
            return obj

        if isinstance(obj, str):
            return obj.strip().upper() == "TRUE"

        if isinstance(obj, int):
            return obj != 0

        return default


    def get_dict(self, key: str, default: dict | None = None) -> dict:
        obj = self.get(key)

        if isinstance(obj, dict):
            return obj

        if isinstance(obj, str | bytes | bytearray):
            try:
                l = json.loads(obj)
                if isinstance(l, dict):
                    return l
            except:
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
            except:
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
        s = self.get_str(key)
        s = s.strip()
        if len(s) > 32:
            s = s[:32]

        try:
            result = base64.urlsafe_b64encode(
                s.encode().ljust(32, b"0")
            ).decode()
            return result

        except Exception as e:
            logging.error(str(e), extra={"key": s})
            return ""


    #-----------------------------------------------------

    def get_api_keys(self) -> dict[str, str]:
        results = {}

        for key in [
            "OPENROUTER_API_KEY",
            "OPENAI_API_KEY",
            "GOOGLE_API_KEY",
            "ANTHROPIC_API_KEY",
            "MIROTHINKER_API_KEY",
            "E2B_API_KEY",
            "FAL_KEY",
            "JINA_API_KEY"
        ]:
            value = self.get_str(key)
            if value and isinstance(value, str):
                results[key] = value
                os.environ.setdefault(key, value)

        return results

    #-----------------------------------------------------

    def get_mcp_options(self) -> dict[str, str | list[str]]:
        return {
            "tool_dirs"         : self.mcp_tool_dirs,
            "resource_dirs"     : self.mcp_resource_dirs,

            "private_tool_dirs"     : self.private_mcp_tool_dirs,
            "private_resource_dirs" : self.private_mcp_resource_dirs,

            "web_server_url"    : self.web_server_url,
            "mcp_server_url"    : self.mcp_server_url,
            "data_server_url"   : self.data_server_url
        }


    def get_agent_options(self) -> dict[str, list[str] | dict[str, str]]:
        return {
            "agent_dirs"        : self.agent_dirs,
            "private_agent_dirs": self.private_agent_dirs,
            "api_keys"          : self.api_keys
        }


    def get_options_for_agent(self, agent_name: str) -> dict[str, Any]:
        if self._agent_options:
            result = self._agent_options.get(agent_name)
            if result:
                return result

        #-------------------------------------------------

        options = {
            "allowed_tools"     : [],
            "disallowed_tools"  : [],
            "prompt_templates"  : {},
            "providers"         : {}
        }

        suffix = agent_name.strip()
        if not suffix:
            self._agent_options[agent_name] = options
            return options

        suffix = suffix.upper()

        options["allowed_tools"]    = self.get_list(f"ALLOWED_TOOLS_{suffix}", [])
        options["disallowed_tools"] = self.get_list(f"DISALLOWED_TOOLS_{suffix}", [])

        #-------------------------------------------------

        options_prompts = {}

        try:
            dist = importlib.resources.files("mirobody")
        except:
            dist = None

        prompts = self.get(f"PROMPTS_{suffix}")
        if prompts:
            if isinstance(prompts, str):
                try:
                    obj = json.loads(prompts)
                    if isinstance(obj, list):
                        prompts = obj
                except Exception as e:
                    logging.error(str(e), exc_info=True)

                if isinstance(prompts, str):
                    prompts = [prompts]

            if isinstance(prompts, list):
                i = 0
                for s in prompts:
                    if not isinstance(s, str):
                        continue

                    k = ""
                    v = s

                    if os.path.isfile(s):
                        try:
                            with open(s, "r") as f:
                                v = f.read()
                            k = os.path.basename(s).removesuffix(".jinja").strip()
                        except Exception as e:
                            logging.warning(str(e), exc_info=True)
                            v = s

                    elif dist and dist.is_dir():
                        try:
                            v = dist.joinpath(s).read_text(encoding="utf-8")
                            k = os.path.basename(s).removesuffix(".jinja").strip()
                        except Exception as e:
                            logging.warning(str(e), exc_info=True)
                            v = s

                    if not k:
                        i = i + 1
                        k = f"Prompt_{i}"

                    options_prompts[k] = v

        options["prompt_templates"] = options_prompts

        #-------------------------------------------------

        options_providers = {}

        providers = self.get(f"PROVIDERS_{suffix}")
        if providers:
            if isinstance(providers, str):
                try:
                    obj = json.loads(providers)
                    if isinstance(obj, dict | list):
                        providers = obj
                except Exception as e:
                    logging.error(str(e), exc_info=True)

            #---------------------------------------------

            if isinstance(providers, dict):
                options_providers = providers

            elif isinstance(providers, list):
                for item in providers:
                    if not isinstance(item, dict) or "provider" not in item:
                        continue

                    provider_name = item["provider"]
                    if not isinstance(provider_name, str):
                        continue
                    provider_name = provider_name.strip()
                    if not provider_name:
                        continue

                    del item["provider"]
                    options_providers[provider_name] = item

        options["providers"] = options_providers

        #-------------------------------------------------

        self._agent_options[agent_name] = options
        return options

    #-----------------------------------------------------

    def get_email_options(self) -> dict[str, str | int | dict[str, str]]:
        return {
            "email_from"        : self.get_str("EMAIL_FROM"),
            "email_from_name"   : self.get_str("EMAIL_FROM_NAME"),
            "email_template"    : self.get_str("EMAIL_TEMPLATE"),
            "email_password"    : self.get_str("EMAIL_SMTP_PASS"),
            "email_predefined"  : self.get_dict("EMAIL_PREDEFINE_CODES", {})
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


    def get_qr_options(self) -> dict[str, str]:
        return {
            "qr_login_url"  : self.get_str("QR_LOGIN_URL")
        }

    def get_firebase_options(self) -> dict[str, str]:
        return {
            "firebase_project_id"       : self.get_str("FIREBASE_PROJECT_ID"),
            "firebase_api_key"          : self.get_str("FIREBASE_API_KEY"),
            "firebase_auth_domain"      : self.get_str("FIREBASE_AUTH_DOMAIN"),
            "firebase_storage_bucket"   : self.get_str("FIREBASE_STORAGE_BUCKET"),
            "firebase_messaging_sender_id": self.get_str("FIREBASE_MESSAGING_SENDER_ID"),
            "firebase_app_id"           : self.get_str("FIREBASE_APP_ID"),
            "firebase_measurement_id"   : self.get_str("FIREBASE_MEASUREMENT_ID")
        }

    #-----------------------------------------------------

    def get_postgresql(self, key: str="") -> PostgreSQLConfig:
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
            encrypt_key = self.get_str(f"PG_ENCRYPTION_KEY"),
        )

        self._postgresqls[upper_key] = pg_config
        return pg_config

    #-----------------------------------------------------

    def get_redis(self, key: str="") -> RedisConfig:
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

    def get_storage(self, storage_name: str="") -> AbstractStorage:
        storage_name = storage_name.strip().upper()
        if storage_name:
            storage_name = f"_{storage_name}"

        cluster = self.get_str("CLUSTER").strip().lower()
        if not cluster:
            cluster = "aws"

        try:
            module = importlib.import_module(f".storage.{cluster}", __package__)

            classes = inspect.getmembers(module, predicate=inspect.isclass)
            for class_name, klass in classes:
                if inspect.isabstract(klass) or \
                    inspect.isbuiltin(klass) or \
                    class_name == "Any" or \
                    not class_name.endswith("Storage") or \
                    not klass.__module__.endswith(cluster):

                    continue

                return klass(config=self)

        except Exception as e:
            logging.error(str(e), exc_info=True)

        return None

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
        if self.web_server_url:
            print(f"web             : {self.web_server_url}")
        if self.mcp_server_url:
            print(f"mcp             : {self.mcp_server_url}")
        if self.data_server_url:
            print(f"data            : {self.data_server_url}")
        if self.mcp_tool_dirs:
            print(f"tools           : {self.mcp_tool_dirs}")
        if self.private_mcp_tool_dirs:
            print(f"private tools   : {self.private_mcp_tool_dirs}")
        if self.mcp_resource_dirs:
            print(f"resources       : {self.mcp_resource_dirs}")
        if self.private_mcp_resource_dirs:
            print(f"private resources: {self.private_mcp_resource_dirs}")
        if self.agent_dirs:
            print(f"agents          : {self.agent_dirs}")
        if self.private_agent_dirs:
            print(f"private agents  : {self.private_agent_dirs}")

        if self.api_keys:
            print()
            for key in self.api_keys:
                print(f"{key.lower():<18}: {Config.to_masked_str(self.api_keys[key].lower())}")

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
    async def load_remote_config(server: str, token: str, env: str) -> tuple[str | None, str | None]:
        if not server:
            return None, "Empty 'server'."

        if not token:
            return None, "Empty 'token'."

        if not env:
            return None, "Empty 'env'."

        #-----------------------------------------------------

        try:
            async with aiohttp.ClientSession() as session:
                url = f"{server}/api/v1/config/environments/{env}/configs/resolved?is_yaml=true"
                headers = {
                    "X-Config-Token": token
                }

                async with session.get(url=url, headers=headers) as resp:
                    resp_text = await resp.text()
                    if not resp_text:
                        return None, "Empty HTTP response body."

                    if not resp.ok:
                        return None, f"{resp.status}: {resp_text}"

                    return resp_text, None

        except Exception as e:
            return None, str(e)

    #-------------------------------------------------------------------------
    @staticmethod
    async def init(
        yaml_filenames  : str | list[str] | None = None,
        dotenv_filenames: str | list[str] = [".env"],
        log_extra       : dict = {}
    ):
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

        Config.load_dotenv(dotenv_filenames)

        env = os.environ.get("ENV", "").strip().lower()
        if env and log_extra:
            log_extra["env"] = env

        #-----------------------------------------------------
        # Load user yaml files.

        yaml_file_list = []

        if isinstance(yaml_filenames, str):
            yaml_file_list.append(yaml_filenames)

        elif isinstance(yaml_filenames, list):
            yaml_file_list.extend(yaml_filenames)

        #-----------------------------------------------------
        # Fill .key.yaml files.

        temp_yaml_file_list = []

        for yaml_filename in yaml_file_list:
            if not isinstance(yaml_filename, str):
                continue

            yaml_filename = yaml_filename.strip()
            if not yaml_filename:
                continue

            if yaml_filename in temp_yaml_file_list:
                continue

            temp_yaml_file_list.append(yaml_filename)

            #---------------------------------------------

            if re.match(".*\\.key\\.yaml$", yaml_filename, re.IGNORECASE):
                continue

            elif not re.match(".*\\.yaml$", yaml_filename, re.IGNORECASE):
                continue

            n = len(yaml_filename)
            temp_yaml_file_list.append(f"{yaml_filename[:n-5]}.key.yaml")

        yaml_file_list = temp_yaml_file_list

        #-----------------------------------------------------
        # Fill .{env}.yaml and .{env}.key.yaml files.

        if env:
            if not yaml_file_list:
                yaml_file_list = [f"config.{env}.yaml", f"config.{env}.key.yaml"]

            else:
                temp_yaml_file_list = []

                for yaml_filename in yaml_file_list:
                    if not isinstance(yaml_filename, str):
                        continue

                    yaml_filename = yaml_filename.strip()
                    if not yaml_filename:
                        continue

                    if yaml_filename in temp_yaml_file_list:
                        continue

                    temp_yaml_file_list.append(yaml_filename)

                    #---------------------------------------------

                    env_yaml_filename = ""
                    n = len(yaml_filename)

                    if re.match(".*\\.key\\.yaml$", yaml_filename, re.IGNORECASE):
                        env_yaml_filename = f"{yaml_filename[:n-9]}.{env}.key.yaml"

                    elif re.match(".*\\.yaml$", yaml_filename, re.IGNORECASE):
                        env_yaml_filename = f"{yaml_filename[:n-5]}.{env}.yaml"

                    else:
                        continue

                    if env_yaml_filename not in temp_yaml_file_list:
                        temp_yaml_file_list.append(env_yaml_filename)

                yaml_file_list = temp_yaml_file_list

        #-------------------------------------------------

        final_yaml_file_list = []

        default_yaml = "config.yaml"
        if os.path.exists(default_yaml) and default_yaml not in yaml_file_list:
            final_yaml_file_list.append(default_yaml)
            logging.info("Default config has been loaded.")

        remote_yaml, err = await Config.load_remote_config(
            server  = os.environ.get("CONFIG_SERVER", ""),
            token   = os.environ.get("CONFIG_TOKEN", ""),
            env     = env
        )
        if not err:
            final_yaml_file_list.append(io.StringIO(remote_yaml))
            logging.info("Remote config has been loaded.")

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

        # TODO:
        #   It is not a good idea to initiate such a global database instance.
        from ..db import init_db
        init_db(config)

        return config

#-----------------------------------------------------------------------------

def global_config(*args, **kargs) -> Config | None:
    global _global_config
    if not _global_config:
        return None

    return _global_config

#-----------------------------------------------------------------------------

def safe_read_cfg(key: str, default: str = "") -> str:
    global _global_config
    if not _global_config:
        return ""

    return _global_config.get_str(key, default)

#-----------------------------------------------------------------------------