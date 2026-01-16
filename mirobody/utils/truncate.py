# !usr/bin/env python
# -*- coding:utf-8 _*-
import tiktoken

MODEL_MAX_TOKEN = {
    "gpt4": ["cl100k_base", 8192],
    "gpt-4o": ["cl100k_base", 128000],
    "gpt-4-32k": ["cl100k_base", 32768],
    "gpt-3.5-turbo": ["cl100k_base", 4096],
    "gpt-3.5-turbo-16k": ["cl100k_base", 16384],
    "text-embedding-ada-002": ["cl100k_base", 4096],
    "text-davinci-003": ["p50k_base", 4096],
    "davinci": ["r50k_base", 2048],
    "text-embedding-3-small": ["cl100k_base", 8000],
}


def truncate_text(text, max_token, model="gpt-3.5-turbo", direct="fd"):
    """
    Truncate long text, extract text with max length corresponding to tokenlen
    param text: str, text content to truncate
    param direct: str, truncation direction,
        "fd": forward truncation
        "bd": backward truncation
    param tokenlen: int, max tokens length to extract
    return truncate_text: str, truncated text
    """
    encoding_name = MODEL_MAX_TOKEN[model][0]
    tokenizer = tiktoken.get_encoding(encoding_name)
    if not text or text.isspace():
        return "", 0
    tokens = tokenizer.encode(text, disallowed_special=())
    if len(tokens) < max_token:
        return text, len(tokens)

    gate = True
    truncate_text = ""
    if direct == "fd":
        truncate_tokens = tokens[:max_token]
        end = max_token
        while gate and end > 0:
            try:
                truncate_text = tokenizer.decode(truncate_tokens)
                gate = False
            except Exception:
                end -= 1
                truncate_tokens = truncate_tokens[:end]
        last_punctuation = max(
            truncate_text.rfind("."),
            truncate_text.rfind("?"),
            truncate_text.rfind("!"),
            truncate_text.rfind("。"),
            truncate_text.rfind("？"),
            truncate_text.rfind("！"),
            truncate_text.rfind("\n"),
        )
        if last_punctuation != -1:
            truncate_text = truncate_text[: last_punctuation + 1]
    else:
        truncate_tokens = tokens[-max_token:]
        start = 0
        while gate and start < max_token:
            try:
                truncate_text = tokenizer.decode(truncate_tokens)
                gate = False
            except Exception:
                start += 1
                truncate_tokens = truncate_tokens[start:]
        indexes = [
            truncate_text.find("."),
            truncate_text.find("?"),
            truncate_text.find("!"),
            truncate_text.find("。"),
            truncate_text.find("？"),
            truncate_text.find("！"),
            truncate_text.find("\n"),
        ]
        filtered_indexes = [index for index in indexes if index != -1]
        if filtered_indexes:
            first_punctuation = min(filtered_indexes)
            truncate_text = truncate_text[first_punctuation + 1 :]
    return truncate_text, len(tokenizer.encode(truncate_text, disallowed_special=()))


def num_tokens_from_string(text: str, model="gpt-3.5-turbo") -> int:
    """
    calculate the number of tokens of given text
    Args:
        text (str): prompt text
        model (str): model name

    Returns:
        int: # of tokens by given text
    """
    if len(text) == 0 or text.isspace():
        return 0
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        # print("Warning: model not found. Using cl100k_base encoding.")
        encoding = tiktoken.get_encoding("cl100k_base")
    return len(encoding.encode(text))


def _split(results, prompt_func, max_tokens, header="", footer=""):
    """
    For object list, convert each row to text, then split by max_token
    Add header and footer to each segment (if provided)
    Returns a list of segments, each segment is header + {multi-line record text} + footer
    """

    MODEL = "gpt-4o"

    if header:
        max_tokens -= num_tokens_from_string(header, model=MODEL) + 2
        header += "\n\n"
    if footer:
        max_tokens -= num_tokens_from_string(footer, model=MODEL) + 2
        footer = "\n\n" + footer

    splits = []
    current_tokens = 0
    current_snippets = []
    current_idx = 0

    while current_idx < len(results):
        r = results[current_idx]

        prompt = prompt_func(r)
        snippet = prompt.format(**r)
        r_tokens = num_tokens_from_string(snippet, model=MODEL) + 1
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
