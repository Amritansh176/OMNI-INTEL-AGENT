"""
Shared Ollama LLM Client
Single source of truth for all LLM interactions across the system.
Uses ollama.chat() directly with Pydantic validation for speed.
"""
import ollama
import json
import os
from config import Config
from pydantic import BaseModel
from typing import Type, TypeVar
from groq import Groq

T = TypeVar("T", bound=BaseModel)


def _build_schema_description(schema: dict, defs: dict = None) -> str:
    """
    Recursively build a human-readable JSON schema description
    that handles nested models, arrays, and $ref pointers.
    """
    if defs is None:
        defs = schema.get("$defs", {})
    
    lines = []
    properties = schema.get("properties", {})
    
    for name, prop in properties.items():
        desc = prop.get("description", "")
        
        # Handle $ref (nested model reference)
        if "$ref" in prop:
            ref_name = prop["$ref"].split("/")[-1]
            ref_schema = defs.get(ref_name, {})
            nested = _build_schema_description(ref_schema, defs)
            lines.append(f'  "{name}": (object) {desc} — {nested}')
        
        # Handle array with items
        elif prop.get("type") == "array" and "items" in prop:
            items = prop["items"]
            if "$ref" in items:
                ref_name = items["$ref"].split("/")[-1]
                ref_schema = defs.get(ref_name, {})
                nested = _build_schema_description(ref_schema, defs)
                lines.append(f'  "{name}": (array of objects) {desc} — each item: {nested}')
            else:
                item_type = items.get("type", "any")
                lines.append(f'  "{name}": (array of {item_type}) {desc}')
        
        # Handle enum / Literal
        elif "enum" in prop:
            options = ", ".join([f'"{v}"' for v in prop["enum"]])
            lines.append(f'  "{name}": (one of [{options}]) {desc}')
        
        # Handle anyOf (e.g., Optional, Literal)
        elif "anyOf" in prop:
            any_types = []
            for opt in prop["anyOf"]:
                if "enum" in opt:
                    any_types.extend([f'"{v}"' for v in opt["enum"]])
                elif "type" in opt:
                    any_types.append(opt["type"])
            if any_types:
                lines.append(f'  "{name}": (one of [{", ".join(any_types)}]) {desc}')
            else:
                lines.append(f'  "{name}": {desc}')
        
        else:
            ftype = prop.get("type", "any")
            lines.append(f'  "{name}": ({ftype}) {desc}')
    
    return "{" + ", ".join(lines) + "}"


def query_llm(prompt: str, response_model: Type[T], max_retries: int = 2) -> T:
    """
    Send a prompt to the LLM (Groq or Ollama) and return a validated Pydantic object.
    """
    # Build a system message that tells the LLM what JSON schema to produce
    schema = response_model.model_json_schema()
    schema_desc = _build_schema_description(schema)
    
    schema_hint = f"You MUST respond with ONLY valid JSON matching this exact schema (use lowercase keys):\n{schema_desc}"

    groq_client = None
    if Config.USE_GROQ and Config.GROQ_API_KEY:
        groq_client = Groq(api_key=Config.GROQ_API_KEY)

    last_error = None
    for attempt in range(max_retries + 1):
        try:
            if groq_client:
                completion = groq_client.chat.completions.create(
                    model=Config.GROQ_MODEL,
                    messages=[
                        {"role": "system", "content": schema_hint},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    response_format={"type": "json_object"}
                )
                raw_json = completion.choices[0].message.content
            else:
                response = ollama.chat(
                    model=Config.OLLAMA_MODEL,
                    messages=[
                        {'role': 'system', 'content': schema_hint},
                        {'role': 'user', 'content': prompt}
                    ],
                    format='json'
                )
                raw_json = response['message']['content']
            
            # Parse and validate through Pydantic
            parsed = json.loads(raw_json)
            
            # Normalize keys to lowercase (handles LLM capitalization quirks)
            if isinstance(parsed, dict):
                parsed = {k.lower(): v for k, v in parsed.items()}
            
            return response_model.model_validate(parsed)
            
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                continue
    
    raise last_error
