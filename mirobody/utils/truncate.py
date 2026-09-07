import logging

logger = logging.getLogger(__name__)

# tiktoken downloads its o200k_base BPE file from an OpenAI CDN
# (openaipublic.blob.core.windows.net) on first use and caches it under
# TIKTOKEN_CACHE_DIR. That CDN is unreachable from some networks — mainland
# China among them, i.e. exactly the deployments the DashScope gateway exists
# for — and this module sits on the chat request path. A module-scope
# get_encoding() therefore made the FIRST QUESTION crash on a perfectly
# configured offline-from-OpenAI host, with an error naming neither tiktoken
# nor the download. Load lazily, try once, and fall back to a character-count
# estimate: token counts here only pack text into chunk budgets, where a
# conservative estimate is as good as an exact one.
_ENCODING = None
_ENCODING_UNAVAILABLE = False


def _num_tokens(text: str) -> int:
    if not text or text.isspace():
        return 0

    global _ENCODING, _ENCODING_UNAVAILABLE
    if _ENCODING is None and not _ENCODING_UNAVAILABLE:
        try:
            import tiktoken
            _ENCODING = tiktoken.get_encoding("o200k_base")
        except Exception as e:
            _ENCODING_UNAVAILABLE = True
            logger.warning(
                "tiktoken could not load its BPE file (%s: %s); token counts "
                "fall back to a character estimate. For exact counts on a host "
                "that cannot reach openaipublic.blob.core.windows.net, set "
                "TIKTOKEN_CACHE_DIR to a directory pre-seeded with "
                "o200k_base.tiktoken.",
                type(e).__name__, e,
            )

    if _ENCODING is not None:
        return len(_ENCODING.encode(text))

    # Estimate must err HIGH: an undercount packs a chunk past its budget.
    # ~4 chars/token holds for English; CJK runs ~1–2 chars/token, so text
    # containing CJK uses the denser divisor.
    divisor = 2 if any(ord(ch) > 0x2E80 for ch in text) else 4
    return max(1, len(text) // divisor)


def split_by_tokens(
    records: list[dict],
    template: str,
    max_tokens: int,
    header: str = "",
    footer: str = "",
) -> list[str]:
    """Pack formatted records into chunks that fit within *max_tokens*.

    Each record is formatted via ``template.format(**record)`` into a snippet,
    then snippets are greedily grouped so that each chunk's token count stays
    under *max_tokens*. Each returned chunk is ``header + snippets + footer``
    (with header/footer token cost pre-deducted from the budget).
    """
    if header:
        max_tokens -= _num_tokens(header) + 2
        header += "\n\n"
    if footer:
        max_tokens -= _num_tokens(footer) + 2
        footer = "\n\n" + footer

    splits: list[list[str]] = []
    current_tokens = 0
    current_snippets: list[str] = []
    current_idx = 0

    while current_idx < len(records):
        snippet = template.format(**records[current_idx])
        r_tokens = _num_tokens(snippet) + 1
        if current_tokens + r_tokens > max_tokens:
            if current_snippets:
                splits.append(current_snippets)
                current_snippets = []
                current_tokens = 0
            else:
                current_idx += 1
            continue
        current_snippets.append(snippet)
        current_tokens += r_tokens
        current_idx += 1

    if current_snippets:
        splits.append(current_snippets)

    return [header + "\n".join(s) + footer for s in splits]
