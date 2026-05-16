"""
Public constants for DeepAgent module.

Centralizes shared constants used across multiple files to eliminate duplication.
"""

# File type mapping: extension -> type name
# Used by backend.py, files_utils.py, file_handler.py
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
# Source of truth: openrouter.ai (refreshed 2026-05-09).
# For models openrouter doesn't carry (e.g. gemini-3.1-flash, doubao-seed-2-0-lite-260215),
# the closest analog or provider-direct rate is used — see per-key comments.
#
# Lookup is substring + longest-key match (see message_converter.create_cost_statistics).
# When adding new entries, ensure the key is unique enough that the substring matcher
# can't accidentally collide with another entry's longer name.
#
MODEL_PRICING: dict[str, dict[str, float]] = {
    # Claude series (has cache_creation cost)
    # https://openrouter.ai/anthropic
    "claude-opus-4.5": {"input": 5.00, "output": 25.00},
    "claude-opus-4.6": {"input": 5.00, "output": 25.00},
    "claude-sonnet-4.5": {"input": 3.00, "output": 15.00},
    "claude-sonnet-4.6": {"input": 3.00, "output": 15.00},
    "claude-haiku-4.5": {"input": 1.00, "output": 5.00},

    # OpenAI GPT-5 series (automatic caching, no cache_creation cost)
    # https://openrouter.ai/openai
    "gpt-5.4": {"input": 2.50, "output": 15.00},
    "gpt-5.2": {"input": 1.75, "output": 14.00},
    "gpt-5.1": {"input": 1.25, "output": 10.00},
    "gpt-5-mini": {"input": 0.25, "output": 2.00},
    "gpt-5-nano": {"input": 0.05, "output": 0.40},

    # DeepSeek - https://openrouter.ai/deepseek
    "deepseek-v3.2": {"input": 0.252, "output": 0.378},
    "deepseek-v4-flash": {"input": 0.14, "output": 0.28},
    "deepseek-v4-pro": {"input": 0.435, "output": 0.87},

    # Google Gemini - https://openrouter.ai/google
    # gemini-3.1-flash not yet on openrouter; using gemini-3-flash rate as best estimate.
    "gemini-3-flash": {"input": 0.50, "output": 3.00},
    "gemini-3.1-flash": {"input": 0.50, "output": 3.00},
    "gemini-3.1-pro": {"input": 2.00, "output": 12.00},
    "gemini-3.1-flash-lite": {"input": 0.25, "output": 1.50},

    # Moonshot Kimi - https://openrouter.ai/moonshotai
    "kimi-k2.5": {"input": 0.44, "output": 2.00},
    "kimi-k2.6": {"input": 0.75, "output": 3.50},

    # Alibaba Qwen - https://openrouter.ai/qwen
    "qwen-plus": {"input": 0.26, "output": 0.78},
    "qwen-max": {"input": 1.04, "output": 4.16},
    "qwen3.5-plus": {"input": 0.40, "output": 2.40},
    "qwen3.5-flash": {"input": 0.065, "output": 0.26},
    "qwen3.6-plus": {"input": 0.325, "output": 1.95},

    # MiniMax - https://openrouter.ai/minimax
    # dashscope id "MiniMax/MiniMax-M2.7" lowercases to "minimax/minimax-m2.7" — matched via "minimax-m2.7".
    "minimax-m2.5": {"input": 0.15, "output": 1.15},
    "minimax-m2.7": {"input": 0.299, "output": 1.20},

    # ByteDance Doubao/Seed
    # openrouter does not carry the exact volcengine model id "doubao-seed-2-0-lite-260215";
    # falling back to openrouter's analog "Seed-2.0-Lite" rate. Replace with volcengine console
    # rate if direct billing diverges materially.
    "doubao-seed-2-0-lite-260215": {"input": 0.25, "output": 2.00},
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
