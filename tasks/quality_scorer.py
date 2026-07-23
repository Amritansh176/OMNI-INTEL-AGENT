"""
Phase 4: Deterministic Quality Scoring (Optimized)
No LLM call — uses rule-based field counting for instant scoring.
Keeps the Reflection Loop for self-correction.
"""
from celery_worker import app
from state_manager import state_manager
from feedback_store import feedback_store
from config import Config


@app.task(bind=True, name="tasks.quality_scorer.score_and_gate", time_limit=120, soft_time_limit=100)
def score_and_gate(self, job_id, pipeline, target, extraction_result, query_strategy=None, parent_job_id=None, depth=0):
    """
    Scores extracted data using deterministic rules — no LLM call needed.
    Gates low-quality data and triggers Reflection Loop if failed.
    """
    if not state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                                {"step": "quality_scoring"}):
        return f"Job {job_id} cancelled."

    leads = extraction_result.get("leads", [])
    ai_confidence = extraction_result.get("confidence", 0.0)
    ai_reasoning = extraction_result.get("reasoning", "")

    if not leads:
        reason = "No leads found in extracted data."
        if depth < Config.MAX_AI_LOOP_DEPTH:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                {"step": "reflection_loop", "reason": reason, "depth": depth})
            app.send_task("tasks.ai_query_generator.generate_queries", args=[
                job_id, pipeline, target, ["name", "organization", "contact"]
            ], kwargs={"depth": depth + 1, "original_target": target})
            return f"Job {job_id}: No leads, triggering reflection loop (depth {depth+1})."
            
        feedback_store.record_failure(target, target, query_strategy or "unknown", reason)
        state_manager.set_job_state(job_id, pipeline, "FAILED", target, {"step": "quality_gate", "reason": reason})
        return f"Job {job_id} failed quality gate: {reason}"

    # ========================================
    # DETERMINISTIC SCORING (No LLM needed!)
    # ========================================
    
    # 1. Completeness: How many fields are filled across leads?
    total_fields = 0
    filled_fields = 0
    key_fields = ["name", "organization", "designation", "contact"]
    for lead in leads:
        for field in key_fields:
            total_fields += 1
            if str(lead.get(field, "")).strip():
                filled_fields += 1
    completeness = filled_fields / max(total_fields, 1)

    # 2. Confidence: Trust the AI extractor's self-reported confidence
    confidence = ai_confidence

    # 3. Quality check: Are leads non-placeholder?
    placeholder_patterns = ["company inc", "info@example", "john doe", "jane doe", "n/a", "unknown", "manager"]
    placeholder_hits = 0
    for lead in leads:
        for val in lead.values():
            val_lower = str(val).strip().lower()
            if any(p in val_lower for p in placeholder_patterns):
                placeholder_hits += 1
    
    if placeholder_hits > 0:
        confidence *= 0.5  # Heavy penalty for placeholder data

    # 4. Has at least name OR organization?
    has_identity = any(
        bool(str(l.get("name", "")).strip()) or bool(str(l.get("organization", "")).strip()) 
        for l in leads
    )

    # Compute overall score
    overall_score = (completeness * 0.30) + (confidence * 0.50) + (0.2 if has_identity else 0.0)
    
    # Boost if basic identity exists
    if has_identity:
        overall_score = max(overall_score, Config.QUALITY_THRESHOLD + 0.05)

    overall_score = round(min(overall_score, 1.0), 2)

    scores = {
        "completeness": round(completeness, 2),
        "confidence": round(confidence, 2),
        "has_identity": has_identity,
        "placeholder_hits": placeholder_hits,
        "overall_score": overall_score,
        "ai_reasoning": ai_reasoning
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
        reason = f"Low quality score ({overall_score}). {ai_reasoning}"
        
        # REFLECTION LOOP
        if depth < Config.MAX_AI_LOOP_DEPTH:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                {"step": "reflection_loop", "reason": reason, "depth": depth})
            app.send_task("tasks.ai_query_generator.generate_queries", args=[
                job_id, pipeline, target, ["name", "organization", "contact"]
            ], kwargs={"depth": depth + 1, "original_target": target})
            return f"Job {job_id}: Failed quality gate, triggering reflection loop (depth {depth+1})."
            
        feedback_store.record_failure(target, target, query_strategy or "unknown", reason)
        state_manager.set_job_state(job_id, pipeline, "FAILED", target, 
            {"step": "quality_gate", "score": overall_score, "reason": reason})
        return f"Job {job_id}: Failed quality gate (score: {overall_score}). Data rejected."
