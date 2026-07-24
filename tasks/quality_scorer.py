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
    # STRICT QUALITY SCORING
    # ========================================
    
    # 0. Source URL Validation — reject leads from known-junk domains
    junk_domains = [
        "wikipedia.org/wiki/Main_Page", "rapidtables.com", "calculator.net",
        "timeanddate.com", "random.org", "example.com", "localhost",
        "w3schools.com", "stackoverflow.com/questions"
    ]
    clean_leads = []
    for lead in leads:
        source_url = str(lead.get("source_url", "")).lower()
        is_junk = any(jd in source_url for jd in junk_domains)
        if not is_junk:
            clean_leads.append(lead)
    
    junk_filtered = len(leads) - len(clean_leads)
    leads = clean_leads
    
    if not leads:
        reason = f"All {junk_filtered} leads rejected: sourced from junk/irrelevant domains."
        if depth < Config.MAX_AI_LOOP_DEPTH:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                {"step": "reflection_loop", "reason": reason, "depth": depth})
            app.send_task("tasks.ai_query_generator.generate_queries", args=[
                job_id, pipeline, target, ["name", "organization", "contact"]
            ], kwargs={"depth": depth + 1, "original_target": target})
            return f"Job {job_id}: Junk sources, triggering reflection loop (depth {depth+1})."
        feedback_store.record_failure(target, target, query_strategy or "unknown", reason)
        state_manager.set_job_state(job_id, pipeline, "FAILED", target, {"step": "quality_gate", "reason": reason})
        return f"Job {job_id} failed quality gate: {reason}"

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

    # 2. Rich leads check: At least 1 lead must have ≥ MIN_LEAD_FIELDS filled (name + org/designation/contact)
    rich_lead_count = 0
    for lead in leads:
        filled = sum(1 for f in key_fields if str(lead.get(f, "")).strip())
        if filled >= Config.MIN_LEAD_FIELDS:
            rich_lead_count += 1
    
    has_rich_leads = rich_lead_count > 0

    # 3. Confidence: Trust the AI extractor's self-reported confidence
    confidence = ai_confidence

    # 4. Quality check: Are leads non-placeholder?
    placeholder_patterns = ["company inc", "info@example", "john doe", "jane doe", "n/a", "unknown", "manager", "not available", "test"]
    placeholder_hits = 0
    for lead in leads:
        for val in lead.values():
            val_lower = str(val).strip().lower()
            if any(p in val_lower for p in placeholder_patterns):
                placeholder_hits += 1
    
    if placeholder_hits > 0:
        confidence *= 0.5  # Heavy penalty for placeholder data

    # 5. Has at least name OR organization?
    has_identity = any(
        bool(str(l.get("name", "")).strip()) or bool(str(l.get("organization", "")).strip()) 
        for l in leads
    )

    # Compute overall score — NO AUTO-BOOST, strict scoring
    overall_score = (completeness * 0.35) + (confidence * 0.40) + (0.15 if has_identity else 0.0) + (0.10 if has_rich_leads else 0.0)
    
    # Penalty if no rich leads (name-only leads are not enough)
    if not has_rich_leads:
        overall_score *= 0.6

    overall_score = round(min(overall_score, 1.0), 2)

    scores = {
        "completeness": round(completeness, 2),
        "confidence": round(confidence, 2),
        "has_identity": has_identity,
        "has_rich_leads": has_rich_leads,
        "rich_lead_count": rich_lead_count,
        "placeholder_hits": placeholder_hits,
        "junk_filtered": junk_filtered,
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
        reason = f"Low quality score ({overall_score}). completeness={completeness:.2f}, rich_leads={rich_lead_count}. {ai_reasoning}"
        
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

