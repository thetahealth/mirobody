import base64
import hmac
import secrets
from collections.abc import Callable
import datetime
import hashlib
import json
import logging
import os

from ..kernel.ops import PHIPolicy
from .config import FernetEncrypter
from .req_ctx import get_req_ctx

#-----------------------------------------------------------------------------

_fernet_encryptor = None

#-----------------------------------------------------------------------------

def secret_fingerprint(secret: str | None) -> str:
    """A stable, non-reversible handle for a credential, safe to log.

    Bearer tokens, ID tokens and OAuth client secrets were being logged whole,
    and in one case returned to the caller in an HTTP 401 body. Truncation
    (`token[:50]`, used elsewhere in this repo) is not a fix for a JWT: the
    first 50 characters are the header plus the start of the *payload*, which
    is base64 of the claims — email, subject, issuer. It hides the signature,
    which is the one part that is not sensitive on its own.

    A digest prefix keeps what logging a token is actually for — correlating
    "this same token failed here and here" across lines — while carrying none
    of the claims.
    """
    if not secret or not isinstance(secret, str):
        return "<none>"
    return "sha256:" + hashlib.sha256(secret.encode("utf-8")).hexdigest()[:12]

#-----------------------------------------------------------------------------

#: How the pseudonym salt is found. A user id is a small integer, and any
#: UNKEYED hash of it is reversed by enumerating 1..1e7 in milliseconds — so the
#: salt must be a real secret. The reference server reads `LOG_PSEUDONYM_SALT`
#: from the environment; a deployment with other secrets already in the process
#: installs a reader with `use_pseudonym_salt` rather than adding a config key.
_pseudonym_salt_reader: Callable[[], str | None] | None = None
_pseudonym_fallback = secrets.token_hex(16)
_pseudonym_warned = False


def use_pseudonym_salt(reader: Callable[[], str | None] | None) -> None:
    """Install how `user_tag` finds its salt (``None`` restores the default)."""
    global _pseudonym_salt_reader
    _pseudonym_salt_reader = reader


def _pseudonym_salt() -> str:
    global _pseudonym_warned
    value = (_pseudonym_salt_reader() if _pseudonym_salt_reader else None) or os.environ.get("LOG_PSEUDONYM_SALT") or ""
    if value:
        return value
    if not _pseudonym_warned:
        _pseudonym_warned = True
        logging.getLogger(__name__).warning("pseudonym: no salt configured; using a per-process one")
    # Still aligns within one process (enough for one investigation) and dies with
    # it — never the fixed-salt kind that is enumerable.
    return _pseudonym_fallback


def user_tag(user_id: int | str | None) -> str:
    """``45`` → ``u#3f2a9c14``: a keyed pseudonym for LOG LINES only.

    It aligns the same person across lines while a reader of the logs cannot
    recover who. One-way by design: never use it to query or authorise —
    operators look someone up by computing the tag from a known id and grepping,
    not by decrypting. Empty input → ``u#-``; never raises.
    """
    if user_id is None or user_id == "":
        return "u#-"
    digest = hmac.new(_pseudonym_salt().encode(), str(user_id).encode(), hashlib.sha256).hexdigest()
    return f"u#{digest[:8]}"


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        
        if isinstance(o, bytes):
            try:
                s = str(o)
                return s
            except Exception:
                return base64.urlsafe_b64encode(o).decode()
        
        elif isinstance(o, list):
            return [self.default(item) for item in o]
        
        elif isinstance(o, tuple):
            return tuple(self.default(item) for item in o)
        
        elif isinstance(o, dict):
            return {key: self.default(value) for key, value in o.items()}
        
        return super().default(o)

#-----------------------------------------------------------------------------

class JsonFormatter(logging.Formatter):
    def __init__(self, extra: dict | None = None):
        super().__init__()

        self._extra = extra

        self._predefined_fields = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "taskName",
            # Additional field.
            "sql",
            "encrypted_info",
            "exception"  # Added to predefined fields
        }

    #-----------------------------------------------------

    def format(self, record: logging.LogRecord):
        # Common fields.
        json_record = {
            "time"  : self.formatTime(record, self.datefmt),
            "level" : getattr(record, "levelname", "INFO"),
            "msg"   : record.getMessage()
        }

        #-------------------------------------------------
        # Exception information.

        if record.exc_info:
            json_record["exception"] = self.formatException(record.exc_info)
        
        if record.stack_info:
            json_record["stack_info"] = record.stack_info

        #-------------------------------------------------
        # Check for function name in different places.

        function = None
        if hasattr(record, "function_name"):
            # Custom function name from our logger
            function = record.function_name
        elif hasattr(record, "funcName"):
            function = record.funcName

        # In case it is the module.
        if function and function != "<module>":
            json_record["function"] = function

        #-------------------------------------------------
        # Filename and line number.

        if hasattr(record, "pathname") and hasattr(record, "lineno"):
            filename = record.pathname.removeprefix(os.getcwd()).removeprefix(os.sep)
            json_record["file"] = f"{filename}:{record.lineno}"
        
        # Module name.
        if hasattr(record, "module") and record.module:
            json_record["module"] = record.module

        #-------------------------------------------------
        # Field for encrypted info.

        encrypted_info = getattr(record, "encrypted_info", "")
        if encrypted_info:
            plain_encrypted_info = json.dumps(
                encrypted_info,
                ensure_ascii=False,
                separators=(',', ':'),
                cls=JsonEncoder
            )
            
            if _fernet_encryptor:
                try:
                    encrypted_encrypted_info = _fernet_encryptor.encrypt(plain_encrypted_info)
                except Exception as e:
                    logging.warning(str(e))
                    encrypted_encrypted_info = "<unencrypted: encryption failed>"
            else:
                # Fail CLOSED. This used to fall back to the plaintext, so with
                # no LOG_ENCRYPT_KEY configured every "encrypted_info" payload
                # was written in the clear under a field name that promises the
                # opposite — the worst of both, since a reader trusts the name.
                encrypted_encrypted_info = (
                    "<unencrypted: LOG_ENCRYPT_KEY not configured>"
                )

            json_record["encrypted_info"] = encrypted_encrypted_info \
                if len(encrypted_encrypted_info) <= 200 \
                else f"{encrypted_encrypted_info[:100]}**********{encrypted_encrypted_info[-100:]}"


        #-------------------------------------------------
        # Other fields.

        # Fill extra fields.
        for k in record.__dict__:
            if k not in self._predefined_fields:
                json_record[k] = record.__dict__[k]

        if self._extra:
            json_record.update(self._extra)

        if "trace_id" not in json_record:
            trace_id = get_req_ctx("trace_id")
            if trace_id:
                json_record["trace_id"] = trace_id
        
        if "url" not in json_record:
            url = get_req_ctx("path")
            if url:
                json_record["url"] = url

        if "method" not in json_record:
            method = get_req_ctx("method")
            if method:
                json_record["method"] = method

        # To JSON string.
        return json.dumps(json_record, ensure_ascii=False, separators=(",", ":"), cls=JsonEncoder)

#-----------------------------------------------------------------------------

class TqdmLoggingHandler(logging.Handler):
    def __init__(self):
        super().__init__()

    def emit(self, record):
        from tqdm import tqdm
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()

        except Exception:
            self.handleError(record)

#-----------------------------------------------------------------------------

# Third-party libraries that output verbose DEBUG logs (e.g., full request bodies)
# Set these to WARNING to reduce noise while keeping your own DEBUG logs visible
VERBOSE_LOGGERS = [
    "google.genai",           # Google GenAI SDK - logs full request/response bodies
    "google.genai._interactions",
    "httpx",                  # HTTP client - logs request details
    "httpcore",               # HTTP core - logs connection details
    "urllib3",                # URL lib - logs request details
    "openai",                 # OpenAI SDK
    "anthropic",              # Anthropic SDK
    "langchain",              # LangChain - can be verbose
    "langchain_core",
]


def _silence_verbose_loggers(app_level: int):
    """
    Set higher log level for verbose third-party libraries.

    When app log level is DEBUG, these libraries output extremely verbose logs
    (full HTTP request bodies, etc.). This function sets them to WARNING
    to reduce noise while keeping your application's DEBUG logs visible.
    """
    if app_level <= logging.DEBUG:
        for logger_name in VERBOSE_LOGGERS:
            logging.getLogger(logger_name).setLevel(logging.WARNING)


def init_log_console(level: int = logging.INFO, extra: dict | None = None, secret_key: str = ""):
    if extra is None:
        extra = {}
    if secret_key:
        global _fernet_encryptor
        _fernet_encryptor = FernetEncrypter(secret_key)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter(extra))

    # logging.basicConfig(level=level, handlers=[stream_handler])

    logging.root.handlers = [stream_handler]
    logging.root.setLevel(level=level)
    PHIPolicy().install(logging.root)

    # Silence verbose third-party library logs (especially in DEBUG mode)
    _silence_verbose_loggers(level)

#-----------------------------------------------------------------------------

def init_log_file(name: str, dir: str, level: int = logging.INFO, extra: dict | None = None, secret_key: str = ""):
    if extra is None:
        extra = {}
    if secret_key:
        global _fernet_encryptor
        _fernet_encryptor = FernetEncrypter(secret_key)

    if dir:
        os.makedirs(dir, exist_ok=True)

    formatter = JsonFormatter(extra)

    now = datetime.datetime.now()
    file_handler = logging.FileHandler(
        os.path.join(dir, f"{now.strftime('%Y-%m-%d')}_{name}_{now.strftime('%H%M%S_%f')}.log"),
        mode="w+"
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    # logging.basicConfig(level=level, handlers=[file_handler, stream_handler])

    logging.root.handlers = [file_handler, stream_handler]
    logging.root.setLevel(level=level)
    PHIPolicy().install(logging.root)

    # Silence verbose third-party library logs (especially in DEBUG mode)
    _silence_verbose_loggers(level)

#-----------------------------------------------------------------------------

def init_log_tqdm(level: int = logging.INFO):
    tqdm_handler = TqdmLoggingHandler()
    tqdm_handler.setFormatter(JsonFormatter())

    logging.root.handlers = [tqdm_handler]
    logging.root.setLevel(level=level)

#-----------------------------------------------------------------------------

def init_log(name: str = "", dir: str = "", level: int = logging.INFO, extra: dict | None = None, secret_key: str = ""):
    if extra is None:
        extra = {}
    if name:
        init_log_file(name, dir, level, extra, secret_key)
    else:
        init_log_console(level, extra, secret_key)

#-----------------------------------------------------------------------------
