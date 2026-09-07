"""Prompt shaping and result merging — the pure half of vision extraction.

No provider, no network, no filesystem: strings in, strings out. Which is why
these are the functions the tests can pin exhaustively, and why they are the
first thing to reach for when a multi-page extraction comes back wrong.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# =============================================================================

def clean_json_response(response: str) -> str:
    """Remove markdown code block markers from LLM response."""
    if not response:
        return response
    response = response.strip()
    if response.startswith('```json'):
        response = response[7:]
    elif response.startswith('```'):
        response = response[3:]
    if response.endswith('```'):
        response = response[:-3]
    return response.strip()


def _build_prompt_with_schema(prompt: str, response_schema: Any | None = None) -> str:
    """Embed response_schema into prompt for providers without native schema support."""
    if not response_schema:
        return prompt + "\n\nPlease return the result in JSON format."

    try:
        if hasattr(response_schema, 'to_dict'):
            schema_dict = response_schema.to_dict()
        elif hasattr(response_schema, '__dict__'):
            schema_dict = response_schema.__dict__
        elif isinstance(response_schema, dict):
            schema_dict = response_schema
        else:
            schema_dict = str(response_schema)

        schema_str = json.dumps(schema_dict, indent=2, ensure_ascii=False)
        return f"""{prompt}

Please return the result in JSON format that strictly follows this schema:
```json
{schema_str}
```"""
    except Exception as e:
        logger.warning(f"Failed to serialize response_schema: {e}")
        return prompt + "\n\nPlease return the result in JSON format."


def _merge_json_results(json_strings: list[str]) -> str:
    """Merge per-page JSON results into one combined result.

    Handles BOTH top-level shapes, because both are real. This used to be
    `merged = {}` plus `if not isinstance(data, dict): continue`, which meant a
    page answering with a top-level ARRAY was dropped on the floor — and the
    engine's own extraction prompt (`engine._EXTRACT_PROMPT`) asks for exactly
    that. The visible symptom was `mirobody parse report.pdf` raising
    "extraction returned dict, expected a JSON array" on every multi-page PDF;
    the invisible one was worse, since every reading the model had already read
    off the page was discarded before anyone could notice. Single-page files
    never hit it (`_merge_page_results` returns their content unmerged), which
    is why a JPG worked and a 9-page scan did not.

    A dict page whose only list-valued key holds the rows — `{"readings": [...]}`,
    the other shape json_mode commonly produces — is unwrapped rather than
    dropped, so a model that answers inconsistently across pages still costs
    nothing.
    """
    merged_list: list[Any] = []
    merged_dict: dict[str, Any] = {}
    dropped = 0

    for json_str in json_strings:
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON: {e}, content: {json_str[:100]}...")
            continue

        if isinstance(data, list):
            merged_list.extend(data)
        elif isinstance(data, dict):
            list_keys = [k for k, v in data.items() if isinstance(v, list)]
            if merged_list and len(list_keys) == 1:
                # Array-shaped document, this page wrapped it in a key.
                merged_list.extend(data[list_keys[0]])
                continue
            for key, value in data.items():
                if key not in merged_dict:
                    merged_dict[key] = value
                elif isinstance(value, list) and isinstance(merged_dict[key], list):
                    merged_dict[key].extend(value)
                elif isinstance(value, dict) and isinstance(merged_dict[key], dict):
                    for k, v in value.items():
                        if k not in merged_dict[key] or not merged_dict[key][k]:
                            merged_dict[key][k] = v
                elif not merged_dict[key] and value:
                    merged_dict[key] = value
        else:
            dropped += 1

    if dropped:
        logger.warning(f"{dropped} page result(s) were neither array nor object; dropped")

    if merged_list and merged_dict:
        # Genuinely mixed shapes across pages of one document. Keep both rather
        # than silently choosing: the array is the document, the object's keys
        # ride along beside it.
        logger.warning(
            "pages returned mixed JSON shapes; merging array under 'items' "
            f"alongside {len(merged_dict)} object key(s)"
        )
        return json.dumps({**merged_dict, "items": merged_list}, ensure_ascii=False)
    if merged_list:
        return json.dumps(merged_list, ensure_ascii=False)
    return json.dumps(merged_dict, ensure_ascii=False)


def _merge_page_results(all_results: list[dict[str, Any]], json_mode: bool) -> str:
    """Merge page-by-page results into final output."""
    if not all_results:
        logger.warning("No valid analysis results obtained")
        return ""

    valid_contents = []
    for result in all_results:
        if 'content' in result and result['content']:
            valid_contents.append({'page': result.get('page', 0), 'content': result['content']})
        elif 'error' in result:
            logger.warning(f"Page {result.get('page', 0)} extraction failed: {result['error']}")

    if not valid_contents:
        logger.warning("No valid content extracted from any page")
        return ""

    # Single page: return directly
    if len(valid_contents) == 1:
        content = valid_contents[0]['content']
        return clean_json_response(content) if json_mode else content

    # Multiple pages: merge based on mode
    if json_mode:
        cleaned_contents = [clean_json_response(vc['content']) for vc in valid_contents]
        combined = _merge_json_results(cleaned_contents)
        logger.info(f"Merged {len(valid_contents)} pages JSON results")
        return combined
    text_parts = [f"[Page {vc['page']}]\n{vc['content']}" for vc in valid_contents]
    combined = "\n\n".join(text_parts)
    logger.info(f"Combined {len(valid_contents)} pages, total {len(combined)} characters")
    return combined


