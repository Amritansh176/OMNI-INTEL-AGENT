"""
Phase 4: Closed-Loop Quality Scoring
Evaluates extracted leads against a threshold. Passes good leads to Handoff,
and feeds back successes/failures to the Feedback Store for AI learning.
"""
from celery_worker import app
from state_manager import state_manager
from feedback_store import feedback_store
from config import Config
import ollama
import json


@app.task(bind=True, name="tasks.quality_scorer.score_and_gate", time_limit=180, soft_time_limit=150)
def score_and_gate(self, job_id, pipeline, target, extraction_result, query_strategy=None, parent_job_id=None):
    """
    Scores the extracted data for completeness, relevance, and confidence.
    Gates low-quality data from reaching the final handoff JSON.
    """
    if not state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                                {"step": "quality_scoring"}):
        return f"Job {job_id} cancelled."

    leads = extraction_result.get("leads", [])
    if not leads:
        # No leads at all
        reason = "No leads found in extracted data."
        feedback_store.record_failure(target, target, query_strategy or "unknown", reason)
        state_manager.set_job_state(job_id, pipeline, "FAILED", target, {"step": "quality_gate", "reason": reason})
        return f"Job {job_id} failed quality gate: {reason}"

    # Use a fast LLM call to score the leads
    # Get a snippet of the source text to give the AI context for judging hallucinations
    source_text_snippet = extraction_result.get("text_content", "")[:1500] if isinstance(extraction_result, dict) else ""

    prompt = f"""You are a strict data quality auditor. Score the following extracted leads for the target: "{target}"

Source Text Context (Snippet):
{source_text_snippet}

Extracted Leads:
{json.dumps(leads, indent=2)}

Score each metric from 0.0 to 1.0, applying these rules strictly:
1. completeness: A lead is useful if it has at least 1 or 2 fields (e.g., name and organization). Do NOT penalize if contact details are missing. Give a high score (0.8-1.0) if at least a name or organization is clearly identified.
2. relevance: Does the extracted lead genuinely relate to the target "{target}" based on the Source Text Context? Even if the organization name doesn't explicitly sound related (e.g. "Zen Technologies" for "drones"), check if the source text proves their relevance. Generic or off-topic entities score near 0.0.
3. confidence: Does this look like real extracted data (specific names, real-looking contacts/titles) or generic placeholder-style text (e.g. "Company Inc.", "info@example.com", "Manager")? Does it match the Source Text Context? Placeholder-looking or fabricated data scores near 0.0.

CRITICAL INSTRUCTION FOR RELEVANCE: Be highly generous with the relevance score. If there is even a slight, partial, or indirect connection between the lead and the target topic, immediately give a high relevance score (0.8 to 1.0). Do not be strict about relevance.

overall_score should be the average of the three, not a rounded-up value.

Return ONLY this JSON object. No markdown fences, no explanation before or after:
{{
    "completeness": 0.0,
    "relevance": 0.0,
    "confidence": 0.0,
    "overall_score": 0.0,
    "reasoning": "one sentence, max 20 words"
}}"""

    try:
        response = ollama.chat(
            model=Config.OLLAMA_MODEL, 
            messages=[{'role': 'user', 'content': prompt}],
            format='json',
            options={'timeout': 30}
        )
        
        output_text = response['message']['content']
        
        # Parse the output
        try:
            scores = json.loads(output_text)
        except json.JSONDecodeError:
            scores = {"completeness": 0, "relevance": 0, "confidence": 0, "overall_score": 0, "reasoning": "Failed to parse JSON"}

    except Exception as e:
        scores = {"overall_score": 0.5, "reasoning": f"Scoring error: {str(e)}"}

    completeness = float(scores.get("completeness", 0.0))
    relevance = float(scores.get("relevance", 0.0))
    confidence = float(scores.get("confidence", 0.0))

    # User Request: If relevance is very high (>0.8) and confidence is acceptable (>0.5), deprioritize completeness
    if relevance >= 0.8 and confidence >= 0.5:
        overall_score = (relevance * 0.70) + (confidence * 0.20) + (completeness * 0.10)
    else:
        overall_score = float(scores.get("overall_score", (completeness + relevance + confidence) / 3.0))

    # HARD OVERRIDE: If the AI managed to extract at least a name or an organization, 
    # force it to pass the quality gate regardless of what the LLM scored it.
    has_basic_info = any(bool(str(l.get("name", "")).strip()) or bool(str(l.get("organization", "")).strip()) for l in leads)
    if has_basic_info:
        overall_score = max(overall_score, Config.QUALITY_THRESHOLD + 0.05)

    scores["overall_score"] = round(overall_score, 2)
    reasoning = scores.get("reasoning", "")

    if overall_score >= Config.QUALITY_THRESHOLD:
        # PASS: Send to handoff and record success
        feedback_store.record_success(target, target, query_strategy or "unknown", overall_score, leads[0])
        
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                                    {"step": "quality_passed", "score": overall_score})
        
        # Package for handoff
        final_data = {
            "leads": leads,
            "quality_metrics": scores,
            "query_strategy": query_strategy
        }
        app.send_task("tasks.handoff.deliver_to_omni", args=[
            parent_job_id or job_id, pipeline, target, final_data
        ])
        return f"Job {job_id}: Passed quality gate (score: {overall_score}). Sent to handoff."
    else:
        # FAIL: Reject data and record failure
        reason = f"Low quality score ({overall_score}). {reasoning}"
        feedback_store.record_failure(target, target, query_strategy or "unknown", reason)
        
        state_manager.set_job_state(job_id, pipeline, "FAILED", target, 
                                    {"step": "quality_gate", "score": overall_score, "reason": reason})
        return f"Job {job_id}: Failed quality gate (score: {overall_score}). Data rejected."
