import base64, datetime, json, logging, os

from deprecated import deprecated

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

def init_log_console(level: int = logging.INFO, extra: dict = {}, secret_key: str = ""):
    if secret_key:
        global _fernet_encryptor
        _fernet_encryptor = FernetEncrypter(secret_key)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter(extra))

    # logging.basicConfig(level=level, handlers=[stream_handler])

    logging.root.handlers = [stream_handler]
    logging.root.setLevel(level=level)

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

#-----------------------------------------------------------------------------

def init_log(name: str = "", dir: str = "", level: int = logging.INFO, extra: dict = {}, secret_key: str = ""):
    if name:
        init_log_file(name, dir, level, extra, secret_key)
    else:
        init_log_console(level, extra, secret_key)

#-----------------------------------------------------------------------------
