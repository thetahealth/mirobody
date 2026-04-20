import tiktoken

_ENCODING = tiktoken.get_encoding("o200k_base")


def _num_tokens(text: str) -> int:
    if not text or text.isspace():
        return 0
    return len(_ENCODING.encode(text))


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
        else:
            current_snippets.append(snippet)
            current_tokens += r_tokens
            current_idx += 1

    if current_snippets:
        splits.append(current_snippets)

    return [header + "\n".join(s) + footer for s in splits]
