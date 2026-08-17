"""Prompt shaping and result merging — the pure half of vision extraction.

No provider, no network, no filesystem: strings in, strings out. Which is why
these are the functions the tests can pin exhaustively, and why they are the
first thing to reach for when a multi-page extraction comes back wrong.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

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


def _build_prompt_with_schema(prompt: str, response_schema: Optional[Any] = None) -> str:
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
        logging.warning(f"Failed to serialize response_schema: {e}")
        return prompt + "\n\nPlease return the result in JSON format."


def _merge_json_results(json_strings: List[str]) -> str:
    """Merge multiple JSON results into one combined result."""
    merged = {}
    for json_str in json_strings:
        try:
            data = json.loads(json_str)
            if not isinstance(data, dict):
                continue
            for key, value in data.items():
                if key not in merged:
                    merged[key] = value
                elif isinstance(value, list) and isinstance(merged[key], list):
                    merged[key].extend(value)
                elif isinstance(value, dict) and isinstance(merged[key], dict):
                    for k, v in value.items():
                        if k not in merged[key] or not merged[key][k]:
                            merged[key][k] = v
                elif not merged[key] and value:
                    merged[key] = value
        except json.JSONDecodeError as e:
            logging.warning(f"Failed to parse JSON: {e}, content: {json_str[:100]}...")
    return json.dumps(merged, ensure_ascii=False)


def _merge_page_results(all_results: List[Dict[str, Any]], json_mode: bool) -> str:
    """Merge page-by-page results into final output."""
    if not all_results:
        logging.warning("No valid analysis results obtained")
        return ""

    valid_contents = []
    for result in all_results:
        if 'content' in result and result['content']:
            valid_contents.append({'page': result.get('page', 0), 'content': result['content']})
        elif 'error' in result:
            logging.warning(f"Page {result.get('page', 0)} extraction failed: {result['error']}")

    if not valid_contents:
        logging.warning("No valid content extracted from any page")
        return ""

    # Single page: return directly
    if len(valid_contents) == 1:
        content = valid_contents[0]['content']
        return clean_json_response(content) if json_mode else content

    # Multiple pages: merge based on mode
    if json_mode:
        cleaned_contents = [clean_json_response(vc['content']) for vc in valid_contents]
        combined = _merge_json_results(cleaned_contents)
        logging.info(f"Merged {len(valid_contents)} pages JSON results")
        return combined
    else:
        text_parts = [f"[Page {vc['page']}]\n{vc['content']}" for vc in valid_contents]
        combined = "\n\n".join(text_parts)
        logging.info(f"Combined {len(valid_contents)} pages, total {len(combined)} characters")
        return combined


