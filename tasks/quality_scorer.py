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


@app.task(bind=True, name="tasks.quality_scorer.score_and_gate")
def score_and_gate(self, job_id, pipeline, target, extraction_result, query_strategy=None, parent_job_id=None):
    """
    Scores the extracted data for completeness, relevance, and confidence.
    Gates low-quality data from reaching the final handoff JSON.
    """
    state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                                {"step": "quality_scoring"})

    leads = extraction_result.get("leads", [])
    if not leads:
        # No leads at all
        reason = "No leads found in extracted data."
        feedback_store.record_failure(target, target, query_strategy or "unknown", reason)
        state_manager.set_job_state(job_id, pipeline, "FAILED", target, {"step": "quality_gate", "reason": reason})
        return f"Job {job_id} failed quality gate: {reason}"

    # Use a fast LLM call to score the leads
    prompt = f"""Evaluate these extracted intelligence leads for the target: "{target}"

Extracted Leads:
{json.dumps(leads, indent=2)}

Score the data on three metrics (0.0 to 1.0):
1. completeness: Are the fields (name, organization, contact) actually filled with real data?
2. relevance: Do these leads match the target "{target}"?
3. confidence: Are you confident this is not a hallucination or boilerplate?

Return ONLY a valid JSON object:
{{
    "completeness": 0.0,
    "relevance": 0.0,
    "confidence": 0.0,
    "overall_score": 0.0,
    "reasoning": "brief explanation"
}}"""

    try:
        response = ollama.chat(model=Config.OLLAMA_MODEL, messages=[
            {'role': 'user', 'content': prompt}
        ])

        output_text = response['message']['content']
        start = output_text.find('{')
        end = output_text.rfind('}') + 1
        if start != -1 and end > start:
            scores = json.loads(output_text[start:end])
        else:
            # Fallback to a simple heuristic score if JSON parsing fails
            scores = {"overall_score": 0.5, "reasoning": "Failed to parse AI score"}

    except Exception as e:
        scores = {"overall_score": 0.5, "reasoning": f"Scoring error: {str(e)}"}

    overall_score = float(scores.get("overall_score", 0.0))
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
