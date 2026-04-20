"""
LLM-powered product data extraction using OpenAI structured outputs (GPT-4o-mini).

Activated only when:
  1. The --llm CLI flag is passed.
  2. OPENAI_API_KEY environment variable is set.

Used as a fallback when CSS selectors return empty values.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# JSON schema for car parts product extraction
PARTS_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "description": "Full product name / title",
        },
        "brand": {
            "type": "string",
            "description": "Brand or manufacturer name",
        },
        "part_number": {
            "type": "string",
            "description": "OEM or aftermarket part number / SKU",
        },
        "price": {
            "type": "number",
            "description": "Numeric price in EGP (Egyptian Pounds)",
        },
        "compatible_vehicles": {
            "type": "array",
            "items": {"type": "string"},
            "description": "List of compatible vehicle makes, models, and years",
        },
        "category": {
            "type": "string",
            "description": "Product category (e.g. Brakes, Filters, Engine Parts)",
        },
        "oem_or_aftermarket": {
            "type": "string",
            "enum": ["oem", "aftermarket", "unknown"],
            "description": "Whether the part is original equipment or aftermarket",
        },
        "in_stock": {
            "type": "boolean",
            "description": "Whether the product is currently in stock",
        },
        "description": {
            "type": "string",
            "description": "Short product description",
        },
    },
    "required": ["name"],
    "additionalProperties": False,
}

_SYSTEM_PROMPT = (
    "You are an expert at extracting structured car parts product data from HTML or "
    "markdown text. Extract all available fields. If a field is missing, omit it. "
    "Return only valid JSON matching the provided schema. "
    "Prices should be numeric values only (no currency symbols)."
)


async def extract_with_llm(
    html: str,
    schema: Dict[str, Any] = PARTS_SCHEMA,
    api_key: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Use GPT-4o-mini to extract product data from raw HTML or markdown.

    Returns a dict matching *schema*, or None on failure / missing API key.
    Input is truncated to 8 000 chars to stay within context limits.
    """
    key = api_key or os.environ.get("OPENAI_API_KEY", "")
    if not key:
        logger.debug("LLM extraction skipped – OPENAI_API_KEY not set")
        return None

    try:
        from openai import AsyncOpenAI
    except ImportError:
        logger.warning("openai package not installed – LLM extraction disabled. Run: pip install openai")
        return None

    truncated = html[:8_000] if len(html) > 8_000 else html

    try:
        client = AsyncOpenAI(api_key=key)
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        f"Extract product information from the following content. "
                        f"Return JSON matching this schema: {json.dumps(schema, ensure_ascii=False)}\n\n"
                        f"Content:\n{truncated}"
                    ),
                },
            ],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=1024,
        )
        raw = response.choices[0].message.content
        result = json.loads(raw)
        logger.info("LLM extraction succeeded – found fields: %s", list(result.keys()))
        return result
    except Exception as exc:
        logger.warning("LLM extraction failed: %s", exc)
        return None
