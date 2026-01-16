#!/usr/bin/env python3
"""
Embedding Service - Supports multiple providers: OpenAI, Doubao, Local models
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
    ):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    async def get_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding for a single text"""
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            async with aiohttp.ClientSession() as session:
                payload = {"model": self.model, "input": text}
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
                payload = {"model": self.model, "input": texts}
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


class DoubaoEmbeddingProvider(EmbeddingProvider):
    """Doubao (Volcengine) Embedding Provider"""

    def __init__(
        self,
        api_key: str = None,
        model: str = "doubao-embedding-text-240715",
        base_url: str = "https://ark.cn-beijing.volces.com/api/v3",
    ):
        self.api_key = api_key or os.environ.get("ARK_API_KEY")
        self.model = model
        self.base_url = base_url
        self.client = None

        if not self.api_key:
            logging.error("ARK_API_KEY not found in environment variables")

    def _get_client(self):
        """Get Doubao async client"""
        if self.client is None:
            try:
                from volcenginesdkarkruntime import AsyncArk
                self.client = AsyncArk(api_key=self.api_key, base_url=self.base_url)
            except ImportError:
                logging.error("volcenginesdkarkruntime required: pip install volcenginesdkarkruntime", stack_info=True)
                raise
        return self.client

    async def get_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding for a single text"""
        try:
            client = self._get_client()
            response = await client.embeddings.create(
                model=self.model, input=text, encoding_format="float"
            )
            return response.data[0].embedding
        except Exception as e:
            logging.error(f"Doubao API error: {str(e)}", stack_info=True)
            return None

    async def get_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Get embeddings for multiple texts"""
        try:
            client = self._get_client()
            response = await client.embeddings.create(
                model=self.model, input=texts, encoding_format="float"
            )
            return [item.embedding for item in response.data]
        except Exception as e:
            logging.error(f"Doubao API error: {str(e)}", stack_info=True)
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
        """Get embeddings for multiple texts"""
        try:
            loop = asyncio.get_event_loop()

            def _sync_get_embeddings_batch():
                client = self._get_client()
                response = client.embeddings.create(
                    model=self.model,
                    input=texts,
                    dimensions=self.dimensions
                )
                return [item.embedding for item in response.data]

            return await loop.run_in_executor(None, _sync_get_embeddings_batch)
        except Exception as e:
            logging.error(f"DashScope API error: {str(e)}", stack_info=True)
            return [None] * len(texts)


class EmbeddingService:
    """Embedding Service Manager"""

    def __init__(self, provider: EmbeddingProvider):
        self.provider = provider

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
        )
    elif provider_type.lower() == "openrouter":
        # OpenRouter uses OpenAI-compatible API
        provider = OpenAIEmbeddingProvider(
            api_key=kwargs.get("api_key"),
            model=kwargs.get("model", "openai/text-embedding-3-small"),
            base_url=kwargs.get("base_url", "https://openrouter.ai/api/v1"),
        )
    elif provider_type.lower() == "doubao":
        provider = DoubaoEmbeddingProvider(
            api_key=kwargs.get("api_key"),
            model=kwargs.get("model", "doubao-embedding-text-240715"),
            base_url=kwargs.get("base_url", "https://ark.cn-beijing.volces.com/api/v3"),
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
        "name": "doubao",
        "api_key_env": "VOLCENGINE_API_KEY",
        "model": "doubao-embedding-text-240715",
        "base_url": "https://ark.cn-beijing.volces.com/api/v3",
        "description": "Doubao/Volcengine Embeddings",
    },
    {
        "name": "dashscope",
        "api_key_env": "DASHSCOPE_API_KEY",
        "model": "text-embedding-v4",
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "description": "DashScope/Qwen Embeddings",
        "dimensions": 1024,
    },
]


def get_available_embedding_provider() -> dict:
    """
    Get the first available embedding provider (based on configured API keys).
    
    Priority: openai > doubao > dashscope > local (fallback)
    
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
    
    Priority: openai > doubao > local (fallback)
    
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
            f"  - VOLCENGINE_API_KEY (for Doubao embeddings)\n"
            f"  - DASHSCOPE_API_KEY (for DashScope/Qwen embeddings)\n"
            f"  - Or install sentence-transformers for local embeddings\n"
            f"Current status: {status}"
        )
