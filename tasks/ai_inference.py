"""
Phase 3: Unified Agentic Extraction (Optimized)
Single LLM call that evaluates AND extracts in one shot.
Uses ollama directly with Pydantic validation — no instructor overhead.
"""
from celery_worker import app
from state_manager import state_manager
from config import Config
from ollama_client import query_llm
import json
from pydantic import BaseModel, Field, model_validator
from typing import List, Optional, Any


class Lead(BaseModel):
    name: str = Field(default="", description="Full name of the person")
    organization: str = Field(default="", description="Company or organization name")
    designation: str = Field(default="", description="Job title or role")
    contact: str = Field(default="", description="Email, phone, or LinkedIn URL")
    status: str = Field(default="")

class IntelligenceReport(BaseModel):
    """Unified evaluation + extraction in a single response."""
    has_useful_data: bool = Field(description="True if the text contains any real leads")
    next_action: str = Field(default="extract_data", description="extract_data | crawl_subpage | mutate_query")
    action_target: str = Field(default="", description="URL for crawl_subpage, otherwise empty")
    action_reason: str = Field(default="", description="Brief reason for chosen action")
    leads: List[Lead] = Field(default_factory=list, description="Extracted leads, empty if none")
    confidence: float = Field(default=0.0, description="0.0-1.0 certainty score")
    reasoning: str = Field(default="", description="Brief justification for confidence")

    @model_validator(mode='before')
    @classmethod
    def normalize(cls, data: Any):
        if isinstance(data, dict):
            return {k.lower(): v for k, v in data.items()}
        return data


def is_low_value_content(text_content):
    if len(text_content.strip()) < 200:
        return True
    boilerplate = ["page not found", "enable javascript", "cookie policy", "captcha", "are you a human"]
    return any(b in text_content.lower() for b in boilerplate)


def deduplicate_leads(leads, target=""):
    """Remove duplicate leads and reject leads that just repeat the target name."""
    seen = set()
    unique = []
    target_lower = target.strip().lower()
    
    for lead in leads:
        name = lead.get("name", "").strip().lower()
        org = lead.get("organization", "").strip().lower()
        
        # Skip leads with no identity at all
        if not name and not org:
            continue
        
        # Skip leads where name is just the target query repeated
        if name and target_lower and name == target_lower:
            # Only skip if no OTHER useful fields were extracted
            has_other_data = any(
                str(lead.get(f, "")).strip() 
                for f in ["designation", "contact"]
            )
            if not has_other_data:
                continue
        
        key = (name, org)
        if key not in seen:
            seen.add(key)
            unique.append(lead)
    return unique


@app.task(bind=True, name="tasks.ai_inference.extract_structured_data", rate_limit='10/m', time_limit=600, soft_time_limit=570)
def extract_structured_data(self, job_id, pipeline, target, raw_data, depth=0, 
                            missing_fields=None, query_strategy=None, parent_job_id=None):
    """
    Unified AI Extractor: Single LLM call that evaluates AND extracts.
    """
    if isinstance(raw_data, dict):
        text_content = raw_data.get("text", json.dumps(raw_data))
        source_url = raw_data.get("url", target)
        interesting_links = raw_data.get("interesting_links", [])
    else:
        text_content = str(raw_data)
        source_url = target
        interesting_links = []

    if is_low_value_content(text_content):
        _send_to_quality_scorer(job_id, pipeline, target, 
            {"leads": [], "confidence": 0.0, "text_content": text_content[:500]}, 
            query_strategy, parent_job_id, depth)
        return f"Job {job_id}: Low value content detected."

    if not state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                                {"step": "ai_extraction", "depth": depth, "strategy": query_strategy}):
        return f"Job {job_id} cancelled."

    # Build context
    links_preview = ""
    if interesting_links:
        links_preview = "\nLinks on page:\n" + "\n".join([f"- {l}" for l in interesting_links[:8]])

    priority_hint = ""
    if missing_fields:
        priority_hint = f"\nPRIORITY fields to find: {', '.join(missing_fields)}"

    pipeline_rules = ""
    if pipeline == "personal_audit":
        pipeline_rules = f"""
4. CRITICAL RULE FOR PERSONAL AUDIT: You are looking for EXACTLY the person holding the role or name specified in the TARGET: "{target}".
   - Do NOT extract random people from the page.
   - If the target specifies a designation (e.g., "Secretary", "CEO"), you MUST extract ONLY the person who holds that specific designation. Ignore deputies, joint secretaries, or other associated people.
"""
    else:
        pipeline_rules = f"""
4. RULE FOR LEAD SCOUTING: "{target}" itself is a topic/industry. Find people MENTIONED IN the text who are ASSOCIATED with "{target}".
   - Extract relevant leadership, founders, or key contacts.
"""

    prompt = f"""You are an expert OSINT data extractor for an INDIA-FOCUSED intelligence system. Extract ACTIONABLE intelligence about REAL INDIAN PEOPLE from crawled web content.

TARGET: "{target}"
Source URL: {source_url}
{priority_hint}
{links_preview}

=== PAGE CONTENT ===
{text_content[:6000]}
=== END CONTENT ===

STEP 1 — RELEVANCE CHECK (do this FIRST):
- Is this page actually about or related to "{target}" in an INDIAN context?
- If the page is a GENERIC page (Wikipedia main page, search engine homepage, random tool/utility, 404 page, cookie notice), set has_useful_data=false IMMEDIATELY.
- If the source URL domain has NO relation to the target topic, be extra skeptical.
- Prioritize INDIAN results.

STEP 2 — EXTRACTION RULES (only if page IS relevant):
1. Extract REAL, NAMED INDIAN PEOPLE — full names like "Rajiv Kumar", "Ashok Gajapathi Raju", not generic roles.
2. EVERY lead MUST have at minimum: a real person NAME + at least ONE of (organization, designation, contact).
   - Name-only leads with everything else empty are WORTHLESS. Do NOT include them.
3. For each person, actively look for:
   - Organization/Company they work at
   - Designation/Title
   - Contact info (email, phone, LinkedIn URL){pipeline_rules}
5. Leave fields genuinely EMPTY ("") if not found — never write "N/A", "unknown", "not available".
6. Do NOT fabricate/hallucinate data. Only extract what is explicitly mentioned in the text.

STEP 3 — CONFIDENCE SCORING:
- 0.8-1.0: Found multiple Indian people with names + orgs + designations + some contacts
- 0.5-0.7: Found people with names + orgs/designations but no contacts
- 0.2-0.4: Found only names, very little other data
- 0.0-0.1: Page is irrelevant or no useful people found

STEP 4 — NEXT ACTION (if has_useful_data is false):
- "crawl_subpage": If you see a promising link (like /about-us, /team, /leadership, /contact) in the links list — set action_target to that URL
- "mutate_query": If the page is completely wrong and a different search query would help
- "extract_data": Default when you found useful leads"""

    try:
        report: IntelligenceReport = query_llm(prompt, IntelligenceReport, max_retries=2)

        # If max depth reached, force extraction
        if depth >= Config.MAX_AI_LOOP_DEPTH:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "max_depth_reached", "depth": depth})
            if not report.has_useful_data:
                _send_to_quality_scorer(job_id, pipeline, target, 
                    {"leads": [], "confidence": 0.0, "text_content": text_content[:1500]}, 
                    query_strategy, parent_job_id, depth)
                return f"Job {job_id}: Max depth, no data."

        # Handle agentic actions when no useful data found
        if not report.has_useful_data:
            if report.next_action == "crawl_subpage" and report.action_target:
                state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                    {"step": "agentic_crawl_subpage", "subpage": report.action_target, "depth": depth})
                app.send_task("tasks.crawl.execute_crawl", args=[
                    job_id, pipeline, report.action_target, missing_fields
                ], kwargs={"depth": depth + 1, "original_target": target, "query_strategy": "subpage_crawl", "parent_job_id": parent_job_id or job_id})
                return f"Job {job_id}: AI requests subpage crawl: {report.action_target}"

            elif report.next_action == "mutate_query":
                state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                    {"step": "agentic_query_mutation", "reason": report.action_reason, "depth": depth})
                app.send_task("tasks.ai_query_generator.generate_queries", args=[
                    job_id, pipeline, target, missing_fields or ["name", "organization", "contact"]
                ], kwargs={"depth": depth + 1, "original_target": target})
                return f"Job {job_id}: AI requests query mutation"

            else:
                _send_to_quality_scorer(job_id, pipeline, target, 
                    {"leads": [], "confidence": 0.0, "text_content": text_content[:1500]}, 
                    query_strategy, parent_job_id, depth)
                return f"Job {job_id}: No useful data, sent to scorer."

        # Process extracted leads
        leads = []
        for lead_obj in report.leads:
            lead_dict = lead_obj.model_dump()
            # Count how many meaningful fields are filled
            filled = sum(1 for k, v in lead_dict.items() if k != "status" and str(v).strip())
            # Enforce minimum fields: must have at least MIN_LEAD_FIELDS filled
            if filled >= Config.MIN_LEAD_FIELDS:
                lead_dict["source_url"] = source_url
                leads.append(lead_dict)

        # Deduplicate and reject target-name-as-lead
        leads = deduplicate_leads(leads, target=target)

        # Boost confidence based on filled fields
        confidence = report.confidence
        if leads:
            best_fill = max(sum(1 for v in l.values() if str(v).strip()) for l in leads)
            baseline_conf = min(0.9, best_fill * 0.15)
            confidence = max(confidence, baseline_conf)
        else:
            # AI said has_useful_data=true but we filtered out all leads
            confidence = min(confidence, 0.2)

        result = {
            "leads": leads,
            "confidence": confidence,
            "reasoning": report.reasoning,
            "text_content": text_content[:1500],
        }

        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
            {"step": "extraction_complete", "confidence": confidence, "leads": len(leads)})
        _send_to_quality_scorer(job_id, pipeline, target, result, query_strategy, parent_job_id, depth)
        return f"Job {job_id}: Extraction complete (confidence: {confidence})."

    except Exception as e:
        if self.request.retries < Config.MAX_RETRIES:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                {"step": f"ai_extraction_retry_{self.request.retries + 1}"})
            raise self.retry(exc=e, countdown=2 ** self.request.retries)
        else:
            state_manager.set_job_state(job_id, pipeline, "FAILED", target, 
                {"step": "ai_extraction", "error": "Max retries exceeded", "details": str(e)})
            return f"Job {job_id} failed during AI extraction after max retries."

def _send_to_quality_scorer(job_id, pipeline, target, result, query_strategy=None, parent_job_id=None, depth=0):
    app.send_task("tasks.quality_scorer.score_and_gate", args=[
        job_id, pipeline, target, result
    ], kwargs={
        "query_strategy": query_strategy,
        "parent_job_id": parent_job_id,
        "depth": depth
    })
