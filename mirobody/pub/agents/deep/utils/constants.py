"""
Public constants for DeepAgent module.

Centralizes shared constants used across multiple files to eliminate duplication.
"""

# File type mapping: extension -> type name
# Used by backend.py, global_files_middleware.py, file_handler.py
FILE_TYPE_MAP: dict[str, str] = {
    '.pdf': 'PDF',
    '.docx': 'DOCX',
    '.doc': 'DOC',
    '.png': 'IMAGE',
    '.jpg': 'IMAGE',
    '.jpeg': 'IMAGE',
    '.gif': 'IMAGE',
    '.webp': 'IMAGE',
    '.bmp': 'IMAGE',
    '.txt': 'TEXT',
    '.md': 'TEXT',
    '.csv': 'CSV',
    '.xlsx': 'EXCEL',
    '.xls': 'EXCEL',
    '.html': 'HTML',
    '.htm': 'HTML',
    '.json': 'JSON',
    '.xml': 'XML',
}

# Binary file types that require parsing (PDF, images, documents)
BINARY_FILE_TYPES: set[str] = {'PDF', 'DOCX', 'DOC', 'IMAGE', 'EXCEL'}

# Text file types that can be read directly
TEXT_FILE_TYPES: set[str] = {'TEXT', 'CSV', 'HTML', 'JSON', 'XML'}

# Parser: Supported file type mapping (from parser.py)
# Maps file extensions to category for parsing strategy
SUPPORTED_FILE_TYPES: dict[str, str] = {
    # Document types
    "pdf": "document",
    "docx": "document",
    "doc": "document",
    "csv": "document",
    "xlsx": "excel",
    "xls": "excel",
    "pptx": "presentation",
    "ppt": "presentation",
    # Image types
    "jpg": "image",
    "jpeg": "image",
    "png": "image",
    "gif": "image",
    "webp": "image",
    "bmp": "image",
    # Text/Code types
    "py": "text",
    "js": "text",
    "ts": "text",
    "jsx": "text",
    "tsx": "text",
    "java": "text",
    "c": "text",
    "cpp": "text",
    "md": "text",
    "txt": "text",
    "json": "text",
    "xml": "text",
    "yaml": "text",
    "yml": "text",
}

# Derived sets for quick extension lookups
IMAGE_EXTENSIONS: set[str] = {
    ext for ext, type_ in SUPPORTED_FILE_TYPES.items() if type_ == "image"
}
TEXT_CODE_EXTENSIONS: set[str] = {
    ext for ext, type_ in SUPPORTED_FILE_TYPES.items() if type_ == "text"
}

# Parsing prompts for DeepAgent
FULL_TEXT_PROMPT = """Please extract and return ALL the original text content from this file.
Return the complete text exactly as it appears in the document, preserving formatting where possible.
Do not summarize or modify the content - return the full original text."""

FULL_IMAGE_PROMPT = """Please extract and return ALL text content visible in this image.
Return the complete text exactly as it appears, preserving the order and structure where possible.
If there is no text or minimal text in the image, provide a detailed description of the visual content including main subjects, scene, colors, actions, and notable details."""

# Model pricing configuration (USD per million tokens)
# Used by StreamConverter for cost calculation
#
# Pricing rules (computed in create_cost_statistics):
# - cache_read = input * 0.1 (all models, 90% discount)
# - cache_creation = input * 1.25 (Claude only, 25% premium)
#
MODEL_PRICING: dict[str, dict[str, float]] = {
    # Claude series (has cache_creation cost)
    # https://openrouter.ai/anthropic/claude-sonnet-4.5/providers ! 125% create cache 
    "claude-opus-4.5": {"input": 5.00, "output": 25.00},
    "claude-opus-4.6": {"input": 5.00, "output": 25.00},
    "claude-sonnet-4.5": {"input": 3.00, "output": 15.00},
    "claude-sonnet-4.6": {"input": 3.00, "output": 15.00},
    "claude-haiku-4.5": {"input": 1.00, "output": 5.00},

    # OpenAI GPT-5 series (automatic caching, no cache_creation cost)
    # https://platform.openai.com/docs/pricing
    "gpt-5.2": {"input": 1.75, "output": 14.00},
    "gpt-5.1": {"input": 1.25, "output": 10.00},
    "gpt-5": {"input": 1.25, "output": 10.00},
    "gpt-5-mini": {"input": 0.25, "output": 2.00},
    "gpt-5-nano": {"input": 0.05, "output": 0.40},

    # DeepSeek - https://www.deepseek.com/pricing
    "deepseek-v3.2": {"input": 0.28, "output": 0.42},

    # Google Gemini - https://cloud.google.com/vertex-ai/generative-ai/pricing
    "gemini-3-flash": {"input": 0.50, "output": 3.00},
    "gemini-3-pro": {"input": 2.00, "output": 12.00},

    # Moonshot Kimi - https://platform.moonshot.ai/pricing
    "kimi-k2": {"input": 0.60, "output": 2.50},
    "kimi-k2.5": {"input": 0.60, "output": 3.00},

    # Alibaba Qwen - https://openrouter.ai/qwen/qwen-plus
    "qwen-plus": {"input": 0.40, "output": 1.20},
    "qwen-max": {"input": 1.60, "output": 6.40},
}

# Stream processing node names
FINAL_OUTPUT_NODES: set[str] = {"tools", "model"}

# API provider information for error messages
API_PROVIDERS: dict[str, dict[str, str]] = {
    "GOOGLE_API_KEY": {
        "name": "Google AI",
        "url": "https://aistudio.google.com/app/apikey"
    },
    "OPENAI_API_KEY": {
        "name": "OpenAI",
        "url": "https://platform.openai.com/api-keys"
    },
    "OPENROUTER_API_KEY": {
        "name": "OpenRouter",
        "url": "https://openrouter.ai/keys"
    },
    "ANTHROPIC_API_KEY": {
        "name": "Anthropic",
        "url": "https://console.anthropic.com/settings/keys"
    },
}

# Common API keys to check during initialization
COMMON_API_KEYS: list[str] = [
    "GOOGLE_API_KEY",
    "OPENAI_API_KEY",
    "OPENROUTER_API_KEY",
    "ANTHROPIC_API_KEY",
]

# Default configuration values
DEFAULT_CACHE_TTL: int = 300  # Cache TTL in seconds (5 minutes)
DEFAULT_CACHE_MAXSIZE: int = 100  # Maximum cache entries
DEFAULT_RECURSION_LIMIT: int = 50
DEFAULT_PROVIDER_DEEP: str = "gemini-3-flash"

# Legacy DEFAULT_CONFIG for backwards compatibility
DEFAULT_CONFIG = {
    "FILE_CACHE_TTL": DEFAULT_CACHE_TTL,
    "FILE_CACHE_MAXSIZE": DEFAULT_CACHE_MAXSIZE,
    "RECURSION_LIMIT": DEFAULT_RECURSION_LIMIT,
    "DEFAULT_PROVIDER_DEEP": DEFAULT_PROVIDER_DEEP,
}
