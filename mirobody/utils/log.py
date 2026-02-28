import base64, datetime, json, logging, os

from .config import FernetEncrypter
from .req_ctx import get_req_ctx

#-----------------------------------------------------------------------------

_fernet_encryptor = None

#-----------------------------------------------------------------------------

class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        
        if isinstance(o, bytes):
            try:
                s = str(o)
                return s
            except:
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
            function = getattr(record, "function_name")
        elif hasattr(record, "funcName"):
            function = getattr(record, "funcName")

        # In case it is the module.
        if function and function != "<module>":
            json_record["function"] = function

        #-------------------------------------------------
        # Filename and line number.

        if hasattr(record, "pathname") and hasattr(record, "lineno"):
            filename = getattr(record, "pathname").removeprefix(os.getcwd()).removeprefix(os.sep)
            json_record["file"] = f"{filename}:{getattr(record, 'lineno')}"
        
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
            
            global _fernet_encryptor
            if _fernet_encryptor:
                try:
                    encrypted_encrypted_info = _fernet_encryptor.encrypt(plain_encrypted_info)
                except Exception as e:
                    logging.warning(str(e))
                    encrypted_encrypted_info = plain_encrypted_info
            else:
                encrypted_encrypted_info = plain_encrypted_info

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


def init_log_console(level: int = logging.INFO, extra: dict = {}, secret_key: str = ""):
    if secret_key:
        global _fernet_encryptor
        _fernet_encryptor = FernetEncrypter(secret_key)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter(extra))

    # logging.basicConfig(level=level, handlers=[stream_handler])

    logging.root.handlers = [stream_handler]
    logging.root.setLevel(level=level)

    # Silence verbose third-party library logs (especially in DEBUG mode)
    _silence_verbose_loggers(level)

#-----------------------------------------------------------------------------

def init_log_file(name: str, dir: str, level: int = logging.INFO, extra: dict = {}, secret_key: str = ""):
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

    # Silence verbose third-party library logs (especially in DEBUG mode)
    _silence_verbose_loggers(level)

#-----------------------------------------------------------------------------

def init_log_tqdm(level: int = logging.INFO):
    tqdm_handler = TqdmLoggingHandler()
    tqdm_handler.setFormatter(JsonFormatter())

    logging.root.handlers = [tqdm_handler]
    logging.root.setLevel(level=level)

#-----------------------------------------------------------------------------

def init_log(name: str = "", dir: str = "", level: int = logging.INFO, extra: dict = {}, secret_key: str = ""):
    if name:
        init_log_file(name, dir, level, extra, secret_key)
    else:
        init_log_console(level, extra, secret_key)

#-----------------------------------------------------------------------------
