"""
Shared Ollama LLM Client
Single source of truth for all LLM interactions across the system.
Uses ollama.chat() directly with Pydantic validation for speed.
"""
import ollama
import json
from config import Config
from pydantic import BaseModel
from typing import Type, TypeVar

T = TypeVar("T", bound=BaseModel)


def query_llm(prompt: str, response_model: Type[T], max_retries: int = 2) -> T:
    """
    Send a prompt to the local Ollama model and return a validated Pydantic object.
    Uses ollama.chat(format='json') directly — no instructor/openai overhead.
    
    Args:
        prompt: The user prompt to send.
        response_model: A Pydantic BaseModel class to validate the JSON output.
        max_retries: Number of retries on validation failure.
    
    Returns:
        An instance of response_model populated from the LLM's JSON output.
    """
    # Build a system message that tells the LLM what JSON schema to produce
    schema = response_model.model_json_schema()
    field_descriptions = []
    for name, prop in schema.get("properties", {}).items():
        desc = prop.get("description", "")
        ftype = prop.get("type", "any")
        field_descriptions.append(f'  "{name}": ({ftype}) {desc}')
    
    schema_hint = "You MUST respond with ONLY valid JSON matching this exact schema (use lowercase keys):\n{\n" + ",\n".join(field_descriptions) + "\n}"

    last_error = None
    for attempt in range(max_retries + 1):
        try:
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
