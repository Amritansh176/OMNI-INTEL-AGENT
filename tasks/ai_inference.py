"""
Phase 3: Agentic Multi-Step Extraction
The AI doesn't just extract data — it decides its own next action.
It can request subpage crawls, entity searches, or declare data sufficient.
"""
from celery_worker import app
from state_manager import state_manager
from config import Config
import ollama
import json


@app.task(bind=True, name="tasks.ai_inference.extract_structured_data", rate_limit='10/m')
def extract_structured_data(self, job_id, pipeline, target, raw_data, depth=0, 
                            missing_fields=None, query_strategy=None, parent_job_id=None):
    """
    Agentic AI Extractor: Extracts structured intelligence AND decides the next action.
    Returns both extracted leads and a next_action recommendation.
    """
    state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                                {"step": "ai_extraction", "depth": depth, "strategy": query_strategy})

    # Build the agentic prompt
    context_parts = []
    if missing_fields:
        context_parts.append(f"PRIORITY: You are specifically looking for these missing fields: {', '.join(missing_fields)}")
    if query_strategy:
        context_parts.append(f"This data was obtained via strategy: {query_strategy}")
    
    extra_context = "\n".join(context_parts)

    # Get text content from raw_data
    if isinstance(raw_data, dict):
        text_content = raw_data.get("text", json.dumps(raw_data))
        source_url = raw_data.get("url", target)
        interesting_links = raw_data.get("interesting_links", [])
    else:
        text_content = str(raw_data)
        source_url = target
        interesting_links = []

    # Format available links for the AI to consider
    links_context = ""
    if interesting_links:
        links_preview = interesting_links[:10]  # Show up to 10 links
        links_context = f"\n\nAvailable links found on this page:\n" + "\n".join([f"- {l}" for l in links_preview])

    prompt = f"""You are an expert intelligence extraction AI performing multi-step reasoning.

{extra_context}

Analyze the following web data and:
1. Extract ALL identifiable leads (people, companies, organizations)
2. Assess the data completeness
3. Recommend the BEST next action

Return ONLY a valid JSON object matching this EXACT schema:
{{
    "leads": [
        {{
            "name": "person or entity name",
            "organization": "company or org name",
            "designation": "job title or role",
            "contact": "email, phone, or contact info",
            "status": "any relevant status or description"
        }}
    ],
    "confidence": 0.0 to 1.0,
    "next_action": {{
        "type": "crawl_subpage|search_entity|mutate_query|sufficient",
        "target": "URL or entity name for next action",
        "reason": "why this action will help"
    }}
}}

Action types:
- "crawl_subpage": Crawl a specific URL from the available links (use when you see a promising subpage like /about, /contact, /team)
- "search_entity": Perform a new search for a specific entity you found (use when you found a name but need more details)
- "mutate_query": Try a different search approach (use when current data is very low quality)
- "sufficient": Data extraction is complete enough to proceed (use when all key fields are reasonably filled)
{links_context}

Source URL: {source_url}

Raw Data:
{text_content[:4000]}"""

    try:
        response = ollama.chat(model=Config.OLLAMA_MODEL, messages=[
            {'role': 'user', 'content': prompt}
        ])

        output_text = response['message']['content']

        # Parse the JSON response
        try:
            start = output_text.find('{')
            end = output_text.rfind('}') + 1
            if start != -1 and end > start:
                result = json.loads(output_text[start:end])
            else:
                result = {"leads": [], "confidence": 0.0, "next_action": {"type": "mutate_query", "target": target, "reason": "No JSON in AI response"}}
        except json.JSONDecodeError:
            result = {"leads": [], "confidence": 0.0, "next_action": {"type": "mutate_query", "target": target, "reason": "Invalid JSON from AI"}}

        leads = result.get("leads", [])
        confidence = float(result.get("confidence", 0.0))
        next_action = result.get("next_action", {"type": "sufficient"})
        action_type = next_action.get("type", "sufficient")
        action_target = next_action.get("target", target)
        action_reason = next_action.get("reason", "")

        # Validate leads — check for truly empty/null fields
        has_any_real_data = False
        for lead in leads:
            filled = 0
            for field in ["name", "organization", "designation", "contact", "status"]:
                val = lead.get(field, "")
                if val and str(val).lower() not in ["", "n/a", "unknown", "null", "none"]:
                    filled += 1
            if filled >= 2:  # At least 2 fields filled = real data
                has_any_real_data = True
                lead["source_url"] = source_url  # Track where this data came from

        # Decision logic based on AI's recommendation and our validation
        if depth >= Config.MAX_AI_LOOP_DEPTH:
            # Hit max depth — send whatever we have to quality scoring
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target,
                                        {"step": "max_depth_reached", "depth": depth})
            _send_to_quality_scorer(job_id, pipeline, target, result, query_strategy, parent_job_id)
            return f"Job {job_id}: Max depth {depth} reached. Sending to quality scorer."

        if action_type == "sufficient" or (has_any_real_data and confidence >= 0.6):
            # AI says data is good enough — send to quality scoring
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target,
                                        {"step": "extraction_sufficient", "confidence": confidence, "leads": len(leads)})
            _send_to_quality_scorer(job_id, pipeline, target, result, query_strategy, parent_job_id)
            return f"Job {job_id}: Extraction sufficient (confidence: {confidence}). Sending to quality scorer."

        elif action_type == "crawl_subpage" and action_target:
            # AI wants to crawl a specific subpage
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target,
                                        {"step": "agentic_crawl_subpage", "subpage": action_target, "reason": action_reason, "depth": depth})
            
            # Determine missing fields from current leads
            current_missing = _get_missing_fields(leads)
            
            app.send_task("tasks.crawl.execute_crawl", args=[
                job_id, pipeline, action_target, current_missing
            ], kwargs={
                "depth": depth + 1,
                "original_target": target,
                "query_strategy": "subpage_crawl",
                "parent_job_id": parent_job_id or job_id
            })
            return f"Job {job_id}: AI requests subpage crawl: {action_target} ({action_reason})"

        elif action_type == "search_entity" and action_target:
            # AI wants to search for a specific entity it found
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target,
                                        {"step": "agentic_entity_search", "entity": action_target, "reason": action_reason, "depth": depth})
            
            current_missing = _get_missing_fields(leads)
            
            app.send_task("tasks.ai_query_generator.generate_queries", args=[
                job_id, pipeline, action_target, current_missing
            ], kwargs={
                "depth": depth + 1,
                "original_target": target
            })
            return f"Job {job_id}: AI requests entity search: {action_target} ({action_reason})"

        elif action_type == "mutate_query":
            # AI thinks current data is garbage — try different queries
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target,
                                        {"step": "agentic_query_mutation", "reason": action_reason, "depth": depth})
            
            app.send_task("tasks.ai_query_generator.generate_queries", args=[
                job_id, pipeline, action_target or target, missing_fields or ["name", "organization", "contact"]
            ], kwargs={
                "depth": depth + 1,
                "original_target": target
            })
            return f"Job {job_id}: AI requests query mutation ({action_reason})"

        else:
            # Unknown action or no data — send whatever we have
            _send_to_quality_scorer(job_id, pipeline, target, result, query_strategy, parent_job_id)
            return f"Job {job_id}: Fallback — sending to quality scorer."

    except Exception as e:
        if self.request.retries < Config.MAX_RETRIES:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                                        {"step": f"ai_extraction_retry_{self.request.retries + 1}"})
            raise self.retry(exc=e, countdown=2 ** self.request.retries)
        else:
            state_manager.set_job_state(job_id, pipeline, "FAILED", target, 
                                        {"step": "ai_extraction", "error": "Max retries exceeded", "details": str(e)})
            return f"Job {job_id} failed during AI extraction after max retries."


def _get_missing_fields(leads):
    """Identify which fields are missing across all leads."""
    if not leads:
        return ["name", "organization", "designation", "contact", "status"]
    
    missing = set()
    for lead in leads:
        for field in ["name", "organization", "designation", "contact", "status"]:
            val = lead.get(field, "")
            if not val or str(val).lower() in ["", "n/a", "unknown", "null", "none"]:
                missing.add(field)
    return list(missing)


def _send_to_quality_scorer(job_id, pipeline, target, result, query_strategy=None, parent_job_id=None):
    """Route extracted data to the quality scoring stage."""
    app.send_task("tasks.quality_scorer.score_and_gate", args=[
        job_id, pipeline, target, result
    ], kwargs={
        "query_strategy": query_strategy,
        "parent_job_id": parent_job_id
    })
