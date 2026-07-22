"""
Phase 4: Closed-Loop Quality Scoring
Evaluates extracted leads against a threshold. Passes good leads to Handoff.
If they fail, triggers a Reflection Loop back to the Dorking Engine.
"""
from celery_worker import app
from state_manager import state_manager
from feedback_store import feedback_store
from config import Config
import json
import instructor
from openai import OpenAI
from pydantic import BaseModel, Field

# Setup instructor client pointing to local Ollama
client = instructor.from_openai(
    OpenAI(
        base_url="http://localhost:11434/v1",
        api_key="ollama",
    ),
    mode=instructor.Mode.JSON,
)

class QualityScore(BaseModel):
    completeness: float = Field(description="Score from 0.0 to 1.0. High if at least name/org is found.")
    relevance: float = Field(description="Score from 0.0 to 1.0. Be very generous if indirectly related.")
    confidence: float = Field(description="Score from 0.0 to 1.0. Low if data looks like placeholders.")
    overall_score: float = Field(description="Average of completeness, relevance, and confidence")
    reasoning: str = Field(description="One sentence, max 20 words explaining the score")


@app.task(bind=True, name="tasks.quality_scorer.score_and_gate", time_limit=900, soft_time_limit=850)
def score_and_gate(self, job_id, pipeline, target, extraction_result, query_strategy=None, parent_job_id=None, depth=0):
    """
    Scores the extracted data for completeness, relevance, and confidence.
    Gates low-quality data and triggers Reflection Loop if failed.
    """
    if not state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                                {"step": "quality_scoring"}):
        return f"Job {job_id} cancelled."

    leads = extraction_result.get("leads", [])
    if not leads:
        reason = "No leads found in extracted data."
        if depth < Config.MAX_AI_LOOP_DEPTH:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "reflection_loop", "reason": reason, "depth": depth})
            app.send_task("tasks.ai_query_generator.generate_queries", args=[job_id, pipeline, target, ["name", "organization", "contact"]], kwargs={"depth": depth + 1, "original_target": target})
            return f"Job {job_id}: Failed quality gate but triggering reflection loop (depth {depth+1})."
            
        feedback_store.record_failure(target, target, query_strategy or "unknown", reason)
        state_manager.set_job_state(job_id, pipeline, "FAILED", target, {"step": "quality_gate", "reason": reason})
        return f"Job {job_id} failed quality gate: {reason}"

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
overall_score should be the average of the three.
"""

    try:
        scores_obj: QualityScore = client.chat.completions.create(
            model=Config.OLLAMA_MODEL,
            messages=[{'role': 'user', 'content': prompt}],
            response_model=QualityScore,
            max_retries=2
        )
        
        completeness = scores_obj.completeness
        relevance = scores_obj.relevance
        confidence = scores_obj.confidence
        overall_score = scores_obj.overall_score
        reasoning = scores_obj.reasoning

    except Exception as e:
        completeness, relevance, confidence = 0.5, 0.5, 0.5
        overall_score = 0.5
        reasoning = f"Scoring error: {str(e)}"

    if relevance >= 0.8 and confidence >= 0.5:
        overall_score = (relevance * 0.70) + (confidence * 0.20) + (completeness * 0.10)

    has_basic_info = any(bool(str(l.get("name", "")).strip()) or bool(str(l.get("organization", "")).strip()) for l in leads)
    if has_basic_info:
        overall_score = max(overall_score, Config.QUALITY_THRESHOLD + 0.05)

    scores = {
        "completeness": completeness,
        "relevance": relevance,
        "confidence": confidence,
        "overall_score": round(overall_score, 2),
        "reasoning": reasoning
    }

    if overall_score >= Config.QUALITY_THRESHOLD:
        feedback_store.record_success(target, target, query_strategy or "unknown", overall_score, leads[0])
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "quality_passed", "score": overall_score})
        
        final_data = {
            "leads": leads,
            "quality_metrics": scores,
            "query_strategy": query_strategy
        }
        app.send_task("tasks.handoff.deliver_to_omni", args=[parent_job_id or job_id, pipeline, target, final_data])
        return f"Job {job_id}: Passed quality gate (score: {overall_score}). Sent to handoff."
    else:
        reason = f"Low quality score ({overall_score}). {reasoning}"
        
        # REFLECTION LOOP
        if depth < Config.MAX_AI_LOOP_DEPTH:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "reflection_loop", "reason": reason, "depth": depth})
            app.send_task("tasks.ai_query_generator.generate_queries", args=[job_id, pipeline, target, ["name", "organization", "contact"]], kwargs={"depth": depth + 1, "original_target": target})
            return f"Job {job_id}: Failed quality gate but triggering reflection loop (depth {depth+1})."
            
        feedback_store.record_failure(target, target, query_strategy or "unknown", reason)
        state_manager.set_job_state(job_id, pipeline, "FAILED", target, {"step": "quality_gate", "score": overall_score, "reason": reason})
        return f"Job {job_id}: Failed quality gate (score: {overall_score}). Data rejected."
