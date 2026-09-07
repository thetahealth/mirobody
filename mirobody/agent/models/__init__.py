"""The chat model seam: building one, reading its messages, counting its tokens.

    clients.py   one `PROVIDERS` entry → a LangChain chat model, for every
                 provider family (OpenAI-compatible with reasoning capture,
                 Azure WIF, Claude on Vertex with the cache breakpoint and the
                 thinking budget, Gemini on Vertex or AI Studio)
    messages.py  a message's text and its reasoning, across provider shapes
    usage.py     one accumulator over LangChain's `usage_metadata`, and the
                 `costStatistics` chunk it becomes

This is the half of the agent layer a consumer running its own agent imports
most: none of it touches this repository's database, HTTP surface or storage.
Not to be confused with `mirobody/utils/llm/`, which is the ENGINE's one-shot
LLM path — vision extraction and structured output on the provider SDKs, with
no LangChain in it.
"""
