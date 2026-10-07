import datetime
import hashlib
import json
import logging
import os

from mirobody.kernel.ops import PHIPolicy
from .req_ctx import get_req_ctx

#-----------------------------------------------------------------------------

def secret_fingerprint(secret: str | None) -> str:
    """A stable, non-reversible handle for a credential, safe to log.

    Bearer tokens, ID tokens and OAuth client secrets were being logged whole,
    and in one case returned to the caller in an HTTP 401 body. Truncation
    (`token[:50]`, used elsewhere in this repo) is not a fix for a JWT: the
    first 50 characters are the header plus the start of the *payload*, which
    is base64 of the claims: email, subject, issuer. It hides the signature,
    which is the one part that is not sensitive on its own.

    A digest prefix keeps what logging a token is actually for (correlating
    "this same token failed here and here" across lines) while carrying none
    of the claims.
    """
    if not secret or not isinstance(secret, str):
        return "<none>"
    return "sha256:" + hashlib.sha256(secret.encode("utf-8")).hexdigest()[:12]

#-----------------------------------------------------------------------------

class JsonEncoder(json.JSONEncoder):
    """Any value a record carries becomes text. Only `datetime` was handled, so
    a `date`, `Decimal` or `UUID` extra raised inside `format()` and the line
    was lost; `json` never hands this a list, tuple or dict."""

    def default(self, o: object) -> str:
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        return str(o)

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
            "_phi_seen",  # PHIFilter's mark that it has run on this record
            # Additional field.
            "sql",
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
        # Function name, unless the call was at module level.

        if record.funcName and record.funcName != "<module>":
            json_record["function"] = record.funcName

        #-------------------------------------------------
        # Filename and line number.

        if hasattr(record, "pathname") and hasattr(record, "lineno"):
            filename = record.pathname.removeprefix(os.getcwd()).removeprefix(os.sep)
            json_record["file"] = f"{filename}:{record.lineno}"
        
        # Module name.
        if hasattr(record, "module") and record.module:
            json_record["module"] = record.module

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


def _install_root(handlers: list[logging.Handler], level: int, extra: dict) -> None:
    """Make `handlers` the root logger's, each behind the PHI filter.

    `Config.init` installs twice (console, then the configured file), and the
    filter used to reach the first set of handlers only: the second install
    found it on the root logger and stopped. A record from a module logger
    passes the root's handlers, never the root logger's own filters, so the
    server and the worker logged unfiltered."""
    formatter = JsonFormatter(extra)
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.root.handlers = handlers
    logging.root.setLevel(level)
    PHIPolicy().install(logging.root)
    _silence_verbose_loggers(level)


def init_log_console(level: int = logging.INFO, extra: dict | None = None):
    if extra is None:
        extra = {}
    _install_root([logging.StreamHandler()], level, extra)

#-----------------------------------------------------------------------------

def init_log_file(name: str, dir: str, level: int = logging.INFO, extra: dict | None = None):
    if extra is None:
        extra = {}
    if dir:
        os.makedirs(dir, exist_ok=True)

    now = datetime.datetime.now()
    # Appending: uvicorn's `dictConfig` closes every existing handler, and a
    # closed FileHandler reopens on its next record in this mode. "w+"
    # truncated the file there, and the boot log was gone.
    file_handler = logging.FileHandler(
        os.path.join(dir, f"{now.strftime('%Y-%m-%d')}_{name}_{now.strftime('%H%M%S_%f')}.log"),
        mode="a", encoding="utf-8",
    )
    _install_root([file_handler, logging.StreamHandler()], level, extra)

#-----------------------------------------------------------------------------

def init_log(name: str = "", dir: str = "", level: int = logging.INFO, extra: dict | None = None):
    if extra is None:
        extra = {}
    if name:
        init_log_file(name, dir, level, extra)
    else:
        init_log_console(level, extra)

#-----------------------------------------------------------------------------
