#!/usr/bin/env python3
"""
Embedding Service - Supports multiple providers: OpenAI, Local models
"""

import asyncio
import logging
import os
import ssl
from abc import ABC, abstractmethod
from typing import List, Optional

import aiohttp

from mirobody.utils.config import safe_read_cfg


class EmbeddingProvider(ABC):
    """Abstract base class for embedding providers"""

    @abstractmethod
    async def get_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding for a single text"""
        pass

    @abstractmethod
    async def get_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Get embeddings for multiple texts"""
        pass


class OpenAIEmbeddingProvider(EmbeddingProvider):
    """OpenAI Embedding Provider"""

    def __init__(
        self,
        api_key: str,
        model: str = "text-embedding-3-small",
        base_url: str = "https://api.openai.com/v1",
        dimensions: int = None,
    ):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.dimensions = dimensions
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def _build_payload(self, input_data):
        """Build API payload with optional dimensions"""
        payload = {"model": self.model, "input": input_data}
        if self.dimensions:
            payload["dimensions"] = self.dimensions
        return payload

    async def get_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding for a single text"""
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            async with aiohttp.ClientSession() as session:
                payload = self._build_payload(text)
                async with session.post(
                    f"{self.base_url}/embeddings", headers=self.headers, json=payload, ssl=ssl_context
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data["data"][0]["embedding"]
                    else:
                        logging.error(f"OpenAI API error: {response.status}")
                        return None
        except Exception as e:
            logging.error(str(e), stack_info=True)
            return None

    async def get_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Get embeddings for multiple texts"""
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            async with aiohttp.ClientSession() as session:
                payload = self._build_payload(texts)
                async with session.post(
                    f"{self.base_url}/embeddings", headers=self.headers, json=payload, ssl=ssl_context
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return [item["embedding"] for item in data["data"]]
                    else:
                        logging.error(f"OpenAI API error: {response.status}")
                        return [None] * len(texts)
        except Exception as e:
            logging.error(str(e), stack_info=True)
            return [None] * len(texts)


class LocalEmbeddingProvider(EmbeddingProvider):
    """Local Embedding Provider using sentence-transformers"""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model_name = model_name
        self.model = None

    @staticmethod
    def _install_sentence_transformers():
        """Attempt to auto-install sentence-transformers"""
        import subprocess
        import sys
        
        logging.info("📦 sentence-transformers not found, attempting to install...")
        
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "sentence-transformers", "-q"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            logging.info("✅ sentence-transformers installed successfully")
            return True
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ Failed to install sentence-transformers: {e}", stack_info=True)
            return False

    async def _load_model(self):
        """Load local model, auto-install sentence-transformers if needed"""
        if self.model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self.model = SentenceTransformer(self.model_name)
            except ImportError:
                # Attempt to auto-install
                if self._install_sentence_transformers():
                    try:
                        from sentence_transformers import SentenceTransformer
                        self.model = SentenceTransformer(self.model_name)
                    except ImportError:
                        logging.error("❌ sentence-transformers still not available after install", stack_info=True)
                        raise
                else:
                    logging.error("❌ Cannot load local embedding model. Please install manually: pip install sentence-transformers", stack_info=True)
                    raise ImportError("sentence-transformers is required for local embeddings")

    async def get_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding for a single text"""
        try:
            await self._load_model()
            embedding = self.model.encode([text])[0]
            return embedding.tolist()
        except Exception as e:
            logging.error(str(e), stack_info=True)
            return None

    async def get_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Get embeddings for multiple texts"""
        try:
            await self._load_model()
            embeddings = self.model.encode(texts)
            return [emb.tolist() for emb in embeddings]
        except Exception as e:
            logging.error(str(e), stack_info=True)
            return [None] * len(texts)


class GeminiEmbeddingProvider(EmbeddingProvider):
    """Google Gemini Embedding Provider (via REST API)"""

    BATCH_LIMIT = 100  # Gemini batchEmbedContents max requests per call

    def __init__(
        self,
        api_key: str = None,
        model: str = "gemini-embedding-001",
        dimensions: int = 1024,
    ):
        self.api_key = api_key or safe_read_cfg("GOOGLE_API_KEY", "")
        self.model = model
        self.dimensions = dimensions
        self.base_url = "https://generativelanguage.googleapis.com/v1beta"

        if not self.api_key:
            logging.error("GOOGLE_API_KEY not configured for Gemini embedding")

    async def get_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding for a single text"""
        results = await self.get_embeddings_batch([text])
        return results[0] if results else None

    async def get_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Get embeddings for multiple texts, auto-chunking at BATCH_LIMIT"""
        all_results = []
        for i in range(0, len(texts), self.BATCH_LIMIT):
            chunk = texts[i : i + self.BATCH_LIMIT]
            chunk_results = await self._batch_embed(chunk)
            all_results.extend(chunk_results)
        return all_results

    async def _batch_embed(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Call Gemini batchEmbedContents API for a single chunk"""
        try:
            requests = [
                {
                    "model": f"models/{self.model}",
                    "content": {"parts": [{"text": text}]},
                    "output_dimensionality": self.dimensions,
                }
                for text in texts
            ]
            payload = {"requests": requests}
            url = f"{self.base_url}/models/{self.model}:batchEmbedContents"
            headers = {
                "Content-Type": "application/json",
                "x-goog-api-key": self.api_key,
            }

            ssl_ctx = ssl.create_default_context()
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload, ssl=ssl_ctx) as response:
                    if response.status == 200:
                        data = await response.json()
                        return [item["values"] for item in data["embeddings"]]
                    else:
                        error_text = await response.text()
                        logging.error(f"Gemini Embedding API error: {response.status}, {error_text}")
                        return [None] * len(texts)
        except Exception as e:
            logging.error(f"Gemini Embedding API error: {str(e)}", stack_info=True)
            return [None] * len(texts)


class DashScopeEmbeddingProvider(EmbeddingProvider):
    """DashScope (Qwen) Embedding Provider"""

    def __init__(
        self,
        api_key: str = None,
        model: str = "text-embedding-v4",
        base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1",
        dimensions: int = 1024,
    ):
        self.api_key = api_key or os.environ.get("DASHSCOPE_API_KEY")
        self.model = model
        self.base_url = base_url
        self.dimensions = dimensions
        self.client = None

        if not self.api_key:
            logging.error("DASHSCOPE_API_KEY not found in environment variables")

    def _get_client(self):
        """Get DashScope client (OpenAI-compatible)"""
        if self.client is None:
            try:
                from openai import OpenAI
                self.client = OpenAI(api_key=self.api_key, base_url=self.base_url)
            except ImportError:
                logging.error("openai package required: pip install openai", stack_info=True)
                raise
        return self.client

    async def get_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding for a single text"""
        try:
            loop = asyncio.get_event_loop()

            def _sync_get_embedding():
                client = self._get_client()
                response = client.embeddings.create(
                    model=self.model,
                    input=text,
                    dimensions=self.dimensions
                )
                return response.data[0].embedding

            return await loop.run_in_executor(None, _sync_get_embedding)
        except Exception as e:
            logging.error(f"DashScope API error: {str(e)}", stack_info=True)
            return None

    async def get_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Get embeddings for multiple texts (auto-chunks into batches of 10 per DashScope limit)"""
        if not texts:
            return []

        max_batch = 10
        results: List[Optional[List[float]]] = []

        for i in range(0, len(texts), max_batch):
            chunk = texts[i : i + max_batch]
            try:
                loop = asyncio.get_event_loop()

                def _sync_get(c=chunk):
                    client = self._get_client()
                    response = client.embeddings.create(
                        model=self.model,
                        input=c,
                        dimensions=self.dimensions,
                    )
                    return [item.embedding for item in response.data]

                chunk_results = await loop.run_in_executor(None, _sync_get)
                results.extend(chunk_results)
            except Exception as e:
                logging.error(f"DashScope API error: {str(e)}", stack_info=True)
                results.extend([None] * len(chunk))

        return results


class EmbeddingService:
    """Embedding Service Manager"""

    def __init__(self, provider: EmbeddingProvider):
        self.provider = provider

    @property
    def model_name(self) -> str:
        """Return the model name used by this service (e.g., 'dashscope/text-embedding-v4')"""
        if hasattr(self.provider, 'model'):
            provider_name = type(self.provider).__name__.replace('EmbeddingProvider', '').lower()
            return f"{provider_name}/{self.provider.model}"
        return "unknown"

    async def get_text_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding for text"""
        if not text or not text.strip():
            return None
        return await self.provider.get_embedding(text.strip())

    async def get_texts_embeddings(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Get embeddings for multiple texts"""
        if not texts:
            return []

        valid_texts = []
        valid_indices = []

        for i, text in enumerate(texts):
            if text and text.strip():
                valid_texts.append(text.strip())
                valid_indices.append(i)

        if not valid_texts:
            return [None] * len(texts)

        valid_embeddings = await self.provider.get_embeddings_batch(valid_texts)

        results = [None] * len(texts)
        for i, embedding in enumerate(valid_embeddings):
            if i < len(valid_indices):
                results[valid_indices[i]] = embedding

        return results


def create_embedding_service(provider_type: str, **kwargs) -> EmbeddingService:
    """Factory function to create embedding service"""
    if provider_type.lower() == "openai":
        provider = OpenAIEmbeddingProvider(
            api_key=kwargs.get("api_key"),
            model=kwargs.get("model", "text-embedding-3-small"),
            base_url=kwargs.get("base_url", "https://api.openai.com/v1"),
            dimensions=kwargs.get("dimensions"),
        )
    elif provider_type.lower() == "openrouter":
        # OpenRouter uses OpenAI-compatible API
        provider = OpenAIEmbeddingProvider(
            api_key=kwargs.get("api_key"),
            model=kwargs.get("model", "openai/text-embedding-3-small"),
            base_url=kwargs.get("base_url", "https://openrouter.ai/api/v1"),
            dimensions=kwargs.get("dimensions"),
        )
    elif provider_type.lower() == "gemini":
        provider = GeminiEmbeddingProvider(
            api_key=kwargs.get("api_key"),
            model=kwargs.get("model", "gemini-embedding-001"),
            dimensions=kwargs.get("dimensions", 1024),
        )
    elif provider_type.lower() == "dashscope":
        provider = DashScopeEmbeddingProvider(
            api_key=kwargs.get("api_key"),
            model=kwargs.get("model", "text-embedding-v4"),
            base_url=kwargs.get("base_url", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
            dimensions=kwargs.get("dimensions", 1024),
        )
    elif provider_type.lower() == "local":
        provider = LocalEmbeddingProvider(model_name=kwargs.get("model_name", "all-MiniLM-L6-v2"))
    else:
        raise ValueError(f"Unsupported embedding provider: {provider_type}")

    return EmbeddingService(provider)


# Embedding provider priority configuration
# Note: OpenRouter is excluded because its embedding API requires specific privacy settings
# that may not be compatible with all accounts (Zero data retention policy issue).
# When no cloud provider is available, the system will fallback to local embedding.
EMBEDDING_PROVIDER_PRIORITY = [
    {
        "name": "openai",
        "api_key_env": "OPENAI_API_KEY",
        "model": "text-embedding-3-small",
        "base_url": "https://api.openai.com/v1",
        "description": "OpenAI Embeddings",
    },
    {
        "name": "dashscope",
        "api_key_env": "DASHSCOPE_API_KEY",
        "model": "text-embedding-v4",
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "description": "DashScope/Qwen Embeddings (China)",
        "dimensions": 1024,
    },
]


def get_available_embedding_provider() -> dict:
    """
    Get the first available embedding provider (based on configured API keys).
    
    Priority: openai > dashscope > local (fallback)
    
    Returns:
        Provider config dict, or None if no provider is available.
    """
    for provider in EMBEDDING_PROVIDER_PRIORITY:
        api_key = safe_read_cfg(provider["api_key_env"])
        if api_key:
            logging.info(f"🔍 Embedding provider selected: {provider['name']} ({provider['description']})")
            return {**provider, "api_key": api_key}
    return None


def get_embedding_provider_status() -> dict:
    """Get configuration status of all embedding providers."""
    return {
        provider["name"]: bool(safe_read_cfg(provider["api_key_env"]))
        for provider in EMBEDDING_PROVIDER_PRIORITY
    }


async def get_default_embedding_service() -> EmbeddingService:
    """
    Get default embedding service based on available API keys.
    
    Priority: openai > local (fallback)
    
    Auto-selects the first available provider based on configured API keys.
    """
    # Auto-select available provider
    provider_config = get_available_embedding_provider()
    
    if provider_config:
        provider_name = provider_config["name"]
        logging.info(f"✅ Using {provider_name} embedding service (auto-selected)")
        
        # Use factory function to create embedding service
        create_kwargs = {
            "api_key": provider_config["api_key"],
            "model": provider_config["model"],
            "base_url": provider_config["base_url"],
        }
        
        # Add dimensions for dashscope
        if provider_name == "dashscope" and "dimensions" in provider_config:
            create_kwargs["dimensions"] = provider_config["dimensions"]
        
        return create_embedding_service(provider_name, **create_kwargs)
    
    # No cloud service available, try local model
    status = get_embedding_provider_status()
    logging.warning(f"⚠️ No cloud embedding provider available, trying local model. Status: {status}")
    
    try:
        logging.info("🔄 Using local embedding model (sentence-transformers)")
        return create_embedding_service("local", model_name="all-MiniLM-L6-v2")
    except Exception as local_error:
        logging.error(f"❌ Local model failed: {str(local_error)}. Please configure OPENAI_API_KEY or VOLCENGINE_API_KEY", stack_info=True)
        raise ValueError(
            f"No embedding provider available. Please configure one of:\n"
            f"  - OPENAI_API_KEY (for OpenAI embeddings)\n"
            f"  - DASHSCOPE_API_KEY (for DashScope/Qwen embeddings)\n"
            f"  - Or install sentence-transformers for local embeddings\n"
            f"Current status: {status}"
        )
