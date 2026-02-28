import aiohttp, json

from collections import defaultdict
from datetime import datetime, timezone
from enum import StrEnum

#-----------------------------------------------------------------------------

class MemoryType(StrEnum):
    PROFILE     = "profile"         # Stores stable attributes that define a user’s identity.
    EPISODIC    = "episodic_memory" # Captures the narrative flow of a session rather than raw logs.
    EVENT_LOG   = "event_log"       # Stores discrete facts without narrative context.
    FORESIGHT   = "foresight"       # Stores future-oriented signals.

class RetrieveMethod(StrEnum):
    KEYWORD     = "keyword"         # Keyword retrieval (BM25, default).
    VECTOR      = "vector"          # Vector semantic retrieval.
    HYBRID      = "hybrid"          # Hybrid retrieval (keyword + vector).
    RRF         = "rrf"             # RRF fusion retrieval (keyword + vector + RRF ranking fusion).
    AGENTIC     = "agentic"         # LLM-guided multi-round intelligent retrieval.

#-----------------------------------------------------------------------------

class AbstractMemoryClient:
    def __init__(self):
        pass

    async def add(self, user_id: str, content: str) -> tuple[str | None, str | None]:
        """Add memory content for a user. Returns: (request_id, error_message)"""
        ...

    async def get_request_status(self, request_id: str) -> tuple[str | None, str | None]:
        """Get the status of a memory request. Returns: (status, error_message)"""
        ...

    async def get(
        self,
        user_id     : str,
        memory_type : MemoryType | None = None,
        page        : int | None = None,
        page_size   : int | None = None,
        start_time  : str | None = None,
        end_time    : str | None = None
    ) -> tuple[list | None, str | None]:
        """Retrieve memories for a user with optional filters. Returns: (memories_list, error_message)"""
        ...

    async def search(
        self,
        user_id     : str,
        query       : str,
        memory_types: list[MemoryType] | None = None,
        start_time  : str | None = None,
        end_time    : str | None = None,
        top_k       : int | None = None,
        radius      : float | None = None
    ) -> tuple[list | None, str | None]:
        """Search memories using a query string. Returns: (results_list, error_message)"""
        ...

    async def delete(self, memory_id: str) -> str | None:
        """Delete a memory by ID. Returns: error_message (None if successful)"""
        ...

    async def get_profile(self, user_id: str) -> tuple[dict | None, str | None]:
        """Get user's profile. Returns: (user_profile, error_message)"""
        ...

#-----------------------------------------------------------------------------

class DummyMemoryClient(AbstractMemoryClient):
    async def add(self, user_id, content):                          return "", None
    async def get_request_status(self, request_id):                 return "unknown", None
    async def get(self, user_id, **kw):                             return [], None
    async def search(self, user_id, query, **kw):                   return [], None
    async def delete(self, memory_id):                              return None
    async def get_profile(self, user_id):                           return {}, None

#-----------------------------------------------------------------------------

class EverMemOSClient(AbstractMemoryClient):
    def __init__(self, api_key: str):
        super().__init__()

        self._api_key: str | None = None
        self._session: aiohttp.ClientSession | None = None

        if api_key and isinstance(api_key, str):
            self._api_key = api_key.strip()

        if self._api_key:
            self._remote_host = "https://api.evermind.ai"

            self._timeout = aiohttp.ClientTimeout(total=10)

            self._headers = {
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json"
            }

    #-------------------------------------------------------------------------

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout, headers=self._headers)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    #-------------------------------------------------------------------------

    async def add(self, user_id: str, content: str) -> tuple[str | None, str | None]:
        if not self._api_key:
            return None, "Invalid EverMemOS api key."
        if not user_id or not isinstance(user_id, str):
            return None, "Invalid user ID."
        if not content or not isinstance(content, str):
            return None, "Invalid content."

        user_id = user_id.strip()
        if not user_id:
            return None, "Empty user ID."
        content = content.strip()
        if not content:
            return None, "Empty content."

        #-------------------------------------------------

        url = f"{self._remote_host}/api/v0/memories"

        now = datetime.now(timezone.utc)

        payload = {
            "message_id"    : f"{user_id}_{int(now.timestamp()*1e9):x}",
            "create_time"   : now.isoformat(timespec="seconds"),
            "sender"        : user_id,
            "content"       : content,
            "flush"         : True,
        }

        #-------------------------------------------------

        try:
            session = self._get_session()
            async with session.post(url, json=payload) as response:
                text = await response.text()
                if not response.ok:
                    return None, f"HTTP {response.status}: {text}"

                try:
                    data = json.loads(text)
                except (json.JSONDecodeError, ValueError):
                    return None, text

                if data.get("status") in ("ok", "queued"):
                    return data.get("request_id", ""), None
                else:
                    return None, data.get("message", text)

        except Exception as e:
            return None, str(e)

    #-------------------------------------------------------------------------

    async def get_request_status(self, request_id: str) -> tuple[str | None, str | None]:
        if not self._api_key:
            return None, "Invalid EverMemOS api key."
        if not request_id or not isinstance(request_id, str):
            return None, "Invalid request ID."

        request_id = request_id.strip()
        if not request_id:
            return None, "Empty request ID."

        #-------------------------------------------------

        url = f"{self._remote_host}/api/v0/status/request?request_id={request_id}"

        #-------------------------------------------------

        try:
            session = self._get_session()
            async with session.get(url) as response:
                text = await response.text()
                if not response.ok:
                    return None, f"HTTP {response.status}: {text}"

                try:
                    data = json.loads(text)
                except (json.JSONDecodeError, ValueError):
                    return None, text

                if data.get("success") and data.get("found"):
                    return data.get("data", {}).get("status", ""), None
                else:
                    return None, data.get("message", text)

        except Exception as e:
            return None, str(e)
            
    #-------------------------------------------------------------------------

    async def get(
        self,
        user_id     : str,
        memory_type : MemoryType | None = None,
        page        : int | None = None,
        page_size   : int | None = None,
        start_time  : str | None = None,
        end_time    : str | None = None,
    ) -> tuple[list | None, str | None]:
        if not self._api_key:
            return None, "Invalid EverMemOS api key."
        if not user_id or not isinstance(user_id, str):
            return None, "Invalid user ID."

        user_id = user_id.strip()
        if not user_id:
            return None, "Empty user ID."

        #-------------------------------------------------

        url = f"{self._remote_host}/api/v0/memories"

        payload = {
            "user_id": user_id
        }

        if page is not None and page > 1:
            payload["page"] = page

        if page_size is not None and 1 <= page_size <= 100:
            payload["page_size"] = page_size

        if start_time and isinstance(start_time, str):
            start_time = start_time.strip()
            if start_time:
                payload["start_time"] = start_time

        if end_time and isinstance(end_time, str):
            end_time = end_time.strip()
            if end_time:
                payload["end_time"] = end_time

        if memory_type:
            payload["memory_type"] = memory_type

        #-------------------------------------------------

        try:
            session = self._get_session()
            async with session.get(url, json=payload) as response:
                text = await response.text()
                if not response.ok:
                    return None, f"HTTP {response.status}: {text}"

                try:
                    data = json.loads(text)
                except (json.JSONDecodeError, ValueError):
                    return None, text

                if data.get("status") == "ok":
                    return data.get("result", {}).get("memories", []), None
                else:
                    return None, data.get("message", text)

        except Exception as e:
            return None, str(e)

    #-------------------------------------------------------------------------

    async def search(
        self,
        user_id     : str,
        query       : str,
        memory_types: list[MemoryType] | None = None,
        start_time  : str | None = None,
        end_time    : str | None = None,
        top_k       : int | None = None,
        radius      : float | None = None
    ) -> tuple[list | None, str | None]:
        if not self._api_key:
            return None, "Invalid EverMemOS api key."
        if not user_id or not isinstance(user_id, str):
            return None, "Invalid user ID."
        if not query or not isinstance(query, str):
            return None, "Invalid query."

        user_id = user_id.strip()
        if not user_id:
            return None, "Empty user ID."
        query = query.strip()
        if not query:
            return None, "Empty query."

        #-------------------------------------------------

        url = f"{self._remote_host}/api/v0/memories/search"

        payload = {
            "user_id"           : user_id,
            "query"             : query,
            "retrieve_method"   : RetrieveMethod.RRF
        }

        if memory_types:
            filtered = [k for k in memory_types if k in (MemoryType.PROFILE, MemoryType.EPISODIC)]
            if filtered:
                payload["memory_types"] = filtered

        if start_time and isinstance(start_time, str):
            start_time = start_time.strip()
            if start_time:
                payload["start_time"] = start_time

        if end_time and isinstance(end_time, str):
            end_time = end_time.strip()
            if end_time:
                payload["end_time"] = end_time

        if radius is not None and 0 < radius <= 1:
            payload["radius"] = radius

        if top_k is not None and 1 <= top_k <= 100:
            payload["top_k"] = top_k

        #-------------------------------------------------

        try:
            session = self._get_session()
            async with session.get(url, json=payload) as response:
                text = await response.text()
                if not response.ok:
                    return None, f"HTTP {response.status}: {text}"

                try:
                    data = json.loads(text)
                except (json.JSONDecodeError, ValueError):
                    return None, text

                if data.get("status") == "ok":
                    result = data.get("result", {})
                    profiles = result.get("profiles", [])
                    memories = result.get("memories", [])

                    profiles.extend(memories)
                    return profiles, None
                else:
                    return None, data.get("message", text)

        except Exception as e:
            return None, str(e)

    #-------------------------------------------------------------------------

    async def delete(self, memory_id: str) -> str | None:
        if not self._api_key:
            return "Invalid EverMemOS api key."
        if not memory_id or not isinstance(memory_id, str):
            return "Invalid memory ID."

        memory_id = memory_id.strip()
        if not memory_id:
            return "Empty memory ID."

        #-------------------------------------------------

        url = f"{self._remote_host}/api/v0/memories"

        payload = {
            "memory_id": memory_id
        }

        #-------------------------------------------------

        try:
            session = self._get_session()
            async with session.delete(url, json=payload) as response:
                text = await response.text()
                if not response.ok:
                    return f"HTTP {response.status}: {text}"

                try:
                    data = json.loads(text)
                except (json.JSONDecodeError, ValueError):
                    return text

                if data.get("status") == "ok":
                    return None
                else:
                    return data.get("message", text)

        except Exception as e:
            return str(e)

    #-------------------------------------------------------------------------

    async def get_profile(self, user_id: str) -> tuple[dict | None, str | None]:
        if not self._api_key:
            return None, "Invalid EverMemOS api key."
        if not user_id or not isinstance(user_id, str):
            return None, "Invalid user ID."

        user_id = user_id.strip()
        if not user_id:
            return None, "Empty user ID."

        #-------------------------------------------------

        url = f"{self._remote_host}/api/v0/memories"

        payload = {
            "user_id"       : user_id,
            "memory_type"   : MemoryType.PROFILE
        }

        #-------------------------------------------------

        try:
            session = self._get_session()
            async with session.get(url, json=payload) as response:
                text = await response.text()
                if not response.ok:
                    return None, f"HTTP {response.status}: {text}"

                try:
                    data = json.loads(text)
                except (json.JSONDecodeError, ValueError):
                    return None, text

                if data.get("status") != "ok":
                    return None, data.get("message", text)

                memories = data.get("result", {}).get("memories", [])

                def _collect(items, key_field):
                    """Collect unique descriptions from a list of dicts, grouped by key_field."""
                    result = defaultdict(set)
                    for item in items:
                        if isinstance(item, dict) and isinstance(item.get(key_field), str) and isinstance(item.get("description"), str):
                            result[item[key_field]].add(item["description"])
                    return result

                explicit_info = defaultdict(set)
                implicit_traits = defaultdict(set)

                for memory in memories:
                    if not isinstance(memory, dict):
                        continue
                    for profile in memory.get("profiles", []):
                        if not isinstance(profile, dict):
                            continue
                        pd = profile.get("profile_data")
                        if not isinstance(pd, dict):
                            continue
                        for k, v in _collect(pd.get("explicit_info", []), "category").items():
                            explicit_info[k] |= v
                        for k, v in _collect(pd.get("implicit_traits", []), "trait").items():
                            implicit_traits[k] |= v

                return {
                    "explicit_info": {k: list(v) for k, v in explicit_info.items()},
                    "implicit_traits": {k: list(v) for k, v in implicit_traits.items()},
                }, None

        except Exception as e:
            return None, str(e)

#-----------------------------------------------------------------------------
