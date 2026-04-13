"""Universal Prompt Caching Middleware.

Supports prompt caching for multiple providers including:
- Anthropic Claude (direct API or via OpenRouter)
- Google Gemini (via OpenRouter or native)
- OpenAI (automatic caching)
- DeepSeek (automatic caching via OpenRouter)

This middleware detects model support based on model name and client type,
applying the appropriate caching strategy for each provider.

References:
- Anthropic: https://docs.anthropic.com/en/docs/build-with-claude/prompt-caching
- OpenRouter: https://openrouter.ai/docs/guides/best-practices/prompt-caching
- OpenAI: https://platform.openai.com/docs/guides/prompt-caching
- Gemini: https://ai.google.dev/gemini-api/docs/caching
"""

import copy
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from langchain.agents.middleware.types import (
    AgentMiddleware,
    ModelCallResult,
    ModelRequest,
    ModelResponse,
)

logger = logging.getLogger(__name__)


# Caching strategies for different providers
class CacheStrategy:
    """Enum-like class for caching strategies."""

    NONE = "none"  # No caching or automatic caching (no action needed)
    MODEL_SETTINGS = "model_settings"  # Native Anthropic SDK style
    MESSAGE_CONTENT = "message_content"  # OpenRouter style (in message blocks)


# Models that require manual cache_control configuration
MANUAL_CACHE_MODELS = {
    # Anthropic Claude models - require manual cache_control
    "claude": {"min_tokens": 1024, "supports_ttl": True},
    # Google Gemini models - require manual cache_control via OpenRouter
    "gemini": {"min_tokens": 1024, "supports_ttl": False},
}

# Models with automatic caching (no manual configuration needed)
AUTO_CACHE_MODELS = {"deepseek", "gpt", "openai", "o1", "o3"}


class UniversalPromptCachingMiddleware(AgentMiddleware):
    """Universal Prompt Caching Middleware.

    Optimizes API usage by enabling caching for supported models.
    Automatically detects the appropriate caching strategy based on
    the model and client type.

    Caching Strategies:
    - Native Anthropic SDK: Uses model_settings with cache_control
    - OpenRouter + Claude/Gemini: Adds cache_control to message content blocks
    - OpenAI/DeepSeek: Automatic caching, no configuration needed

    Learn more about prompt caching:
    - Anthropic: https://docs.anthropic.com/en/docs/build-with-claude/prompt-caching
    - OpenRouter: https://openrouter.ai/docs/guides/best-practices/prompt-caching
    - OpenAI: https://platform.openai.com/docs/guides/prompt-caching
    """

    def __init__(
        self,
        type: Literal["ephemeral"] = "ephemeral",  # noqa: A002
        ttl: Literal["5m", "1h"] | None = "5m",
        min_messages_to_cache: int = 0,
        unsupported_model_behavior: Literal["ignore", "warn", "raise"] = "ignore",
    ) -> None:
        """Initialize the middleware with cache control settings.

        Args:
            type: The type of cache to use, only `'ephemeral'` is supported.
            ttl: The time to live for the cache. Only `'5m'` and `'1h'` are
                supported. Only applicable to Anthropic models.
            min_messages_to_cache: The minimum number of messages until the
                cache is used.
            unsupported_model_behavior: The behavior to take when an
                unsupported model is used.

                `'ignore'` will ignore the unsupported model and continue without
                caching (default).

                `'warn'` will warn the user and continue without caching.

                `'raise'` will raise an error and stop the agent.
        """
        self.type = type
        self.ttl = ttl
        self.min_messages_to_cache = min_messages_to_cache
        self.unsupported_model_behavior = unsupported_model_behavior

    def _get_model_name(self, model: Any) -> str:
        """Extract model name from various client types."""
        for attr in ("model_name", "model", "name"):
            if hasattr(model, attr):
                value = getattr(model, attr)
                if isinstance(value, str):
                    return value.lower()
        return ""

    def _get_client_type(self, model: Any) -> str:
        """Detect the client type (anthropic, openai, google-genai, etc.)."""
        model_class = type(model).__name__.lower()
        module_name = (
            type(model).__module__.lower()
            if hasattr(type(model), "__module__")
            else ""
        )

        # Check for Google Vertex AI FIRST (including Anthropic models via Vertex)
        # Vertex AI does NOT support cache_control parameter
        if "vertexai" in module_name or "model_garden" in module_name:
            return "google-vertexai"

        # Check for native Anthropic client (direct API only)
        if "anthropic" in model_class or "anthropic" in module_name:
            if "openai" not in model_class and "openai" not in module_name:
                return "anthropic"

        # Check for Google GenAI client
        if "google" in module_name or "genai" in module_name:
            return "google-genai"

        # Check for OpenAI-compatible client (includes OpenRouter)
        if "openai" in model_class or "openai" in module_name:
            return "openai"

        return "unknown"

    def _is_openrouter(self, model: Any) -> bool:
        """Check if the model is using OpenRouter."""
        # Check for base_url attribute pointing to OpenRouter
        for attr in ("base_url", "openai_api_base", "api_base"):
            if hasattr(model, attr):
                value = getattr(model, attr)
                if value and isinstance(value, str) and "openrouter" in value.lower():
                    return True

        # Check client's base_url if available
        if hasattr(model, "client"):
            client = model.client
            if hasattr(client, "base_url"):
                base_url = str(client.base_url) if client.base_url else ""
                if "openrouter" in base_url.lower():
                    return True

        return False

    def _get_model_config(self, model_name: str) -> dict | None:
        """Get caching config for a model that requires manual configuration."""
        for pattern, config in MANUAL_CACHE_MODELS.items():
            if pattern in model_name:
                return config
        return None

    def _has_auto_cache(self, model_name: str) -> bool:
        """Check if model has automatic caching (no manual config needed)."""
        return any(pattern in model_name for pattern in AUTO_CACHE_MODELS)

    def _determine_cache_strategy(
        self, model: Any, model_name: str
    ) -> tuple[str, dict | None]:
        """Determine the appropriate caching strategy.

        Returns:
            Tuple of (strategy, model_config)
        """
        client_type = self._get_client_type(model)
        model_config = self._get_model_config(model_name)
        is_openrouter = self._is_openrouter(model)

        # Models with automatic caching - no action needed
        if self._has_auto_cache(model_name):
            logger.debug(
                f"Model '{model_name}' has automatic caching, no manual config needed"
            )
            return CacheStrategy.NONE, None

        # No manual config available for this model
        if not model_config:
            return CacheStrategy.NONE, None

        # Native Anthropic SDK - use model_settings
        if client_type == "anthropic":
            return CacheStrategy.MODEL_SETTINGS, model_config

        # OpenRouter with Claude/Gemini - use message content blocks
        if client_type == "openai" and is_openrouter:
            return CacheStrategy.MESSAGE_CONTENT, model_config

        # Google Vertex AI (including Claude via Vertex) - no cache_control support
        if client_type == "google-vertexai":
            logger.debug(
                f"Model '{model_name}' using Google Vertex AI, "
                "cache_control not supported"
            )
            return CacheStrategy.NONE, None

        # Native Google GenAI - relies on implicit caching
        # Explicit caching requires separate CachedContent API which is
        # beyond the scope of this middleware
        if client_type == "google-genai":
            logger.debug(
                f"Model '{model_name}' using Google GenAI SDK, "
                "relying on implicit caching"
            )
            return CacheStrategy.NONE, None

        # Native OpenAI - automatic caching
        if client_type == "openai" and not is_openrouter:
            logger.debug(
                f"Model '{model_name}' using native OpenAI SDK, automatic caching"
            )
            return CacheStrategy.NONE, None

        return CacheStrategy.NONE, None

    def _should_apply_caching(
        self, request: ModelRequest
    ) -> tuple[str, dict | None]:
        """Check if and how caching should be applied to the request.

        Args:
            request: The model request to check.

        Returns:
            Tuple of (strategy, model_config)

        Raises:
            ValueError: If model is unsupported and behavior is set to `'raise'`.
        """
        model_name = self._get_model_name(request.model)
        strategy, config = self._determine_cache_strategy(request.model, model_name)

        if strategy == CacheStrategy.NONE and not self._has_auto_cache(model_name):
            model_config = self._get_model_config(model_name)
            if not model_config:
                msg = (
                    f"UniversalPromptCachingMiddleware: Model '{model_name}' does not "
                    f"match any known cacheable patterns"
                )
                if self.unsupported_model_behavior == "raise":
                    raise ValueError(msg)
                if self.unsupported_model_behavior == "warn":
                    logger.warning(msg)

        if strategy == CacheStrategy.NONE:
            return strategy, None

        # Check minimum messages requirement
        messages_count = (
            len(request.messages) + 1 if request.system_message else len(request.messages)
        )
        if messages_count < self.min_messages_to_cache:
            return CacheStrategy.NONE, None

        return strategy, config

    def _build_cache_control(self, model_config: dict) -> dict:
        """Build cache control dict based on model capabilities."""
        cache_control = {"type": self.type}

        # Only add TTL for models that support it (Anthropic)
        if model_config.get("supports_ttl") and self.ttl:
            cache_control["ttl"] = self.ttl

        return cache_control

    def _apply_model_settings_cache(
        self, request: ModelRequest, model_config: dict
    ) -> ModelRequest:
        """Apply caching via model_settings (for native Anthropic SDK)."""
        cache_control = self._build_cache_control(model_config)
        model_settings = request.model_settings
        new_model_settings = {
            **model_settings,
            "cache_control": cache_control,
        }

        model_name = self._get_model_name(request.model)
        logger.debug(f"Applying model_settings cache for model: {model_name}")

        return request.override(model_settings=new_model_settings)

    def _convert_to_content_blocks(self, content: Any) -> list[dict]:
        """Convert message content to content blocks format."""
        if isinstance(content, str):
            return [{"type": "text", "text": content}]
        if isinstance(content, list):
            blocks = []
            for item in content:
                if isinstance(item, str):
                    blocks.append({"type": "text", "text": item})
                elif isinstance(item, dict):
                    blocks.append(item)
            return blocks
        return [{"type": "text", "text": str(content)}]

    def _apply_message_content_cache(
        self, request: ModelRequest, model_config: dict
    ) -> ModelRequest:
        """Apply caching via message content blocks (for OpenRouter).

        Adds cache_control to the last content block of the system message
        or the first user message if no system message exists.
        """
        cache_control = self._build_cache_control(model_config)
        model_name = self._get_model_name(request.model)

        # Deep copy to avoid modifying original
        new_messages = copy.deepcopy(request.messages)
        system_message = copy.deepcopy(request.system_message) if request.system_message else None

        cache_applied = False

        # Try to apply cache to system message first
        if system_message:
            if hasattr(system_message, "content"):
                content = system_message.content
                content_blocks = self._convert_to_content_blocks(content)

                if content_blocks:
                    # Add cache_control to the last block
                    content_blocks[-1]["cache_control"] = cache_control
                    system_message.content = content_blocks
                    cache_applied = True
                    logger.debug(
                        f"Applied message_content cache to system message for: {model_name}"
                    )

        # If no system message, apply to first user message
        if not cache_applied and new_messages:
            for msg in new_messages:
                if hasattr(msg, "type") and msg.type == "human":
                    if hasattr(msg, "content"):
                        content = msg.content
                        content_blocks = self._convert_to_content_blocks(content)

                        if content_blocks:
                            content_blocks[-1]["cache_control"] = cache_control
                            msg.content = content_blocks
                            cache_applied = True
                            logger.debug(
                                f"Applied message_content cache to first user message for: {model_name}"
                            )
                            break

        if not cache_applied:
            logger.warning(
                f"Could not apply message_content cache for: {model_name} - "
                "no suitable message found"
            )
            return request

        # Build override kwargs
        override_kwargs = {"messages": new_messages}
        if system_message:
            override_kwargs["system_message"] = system_message

        return request.override(**override_kwargs)

    def _apply_caching(
        self, request: ModelRequest, strategy: str, model_config: dict
    ) -> ModelRequest:
        """Apply the appropriate caching strategy to the request."""
        if strategy == CacheStrategy.MODEL_SETTINGS:
            return self._apply_model_settings_cache(request, model_config)
        elif strategy == CacheStrategy.MESSAGE_CONTENT:
            return self._apply_message_content_cache(request, model_config)
        return request

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelCallResult:
        """Modify the model request to add cache control blocks.

        Args:
            request: The model request to potentially modify.
            handler: The handler to execute the model request.

        Returns:
            The model response from the handler.
        """
        strategy, model_config = self._should_apply_caching(request)
        if strategy == CacheStrategy.NONE:
            return handler(request)

        modified_request = self._apply_caching(request, strategy, model_config)
        return handler(modified_request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelCallResult:
        """Modify the model request to add cache control blocks (async version).

        Args:
            request: The model request to potentially modify.
            handler: The async handler to execute the model request.

        Returns:
            The model response from the handler.
        """
        strategy, model_config = self._should_apply_caching(request)
        if strategy == CacheStrategy.NONE:
            return await handler(request)

        modified_request = self._apply_caching(request, strategy, model_config)
        return await handler(modified_request)
