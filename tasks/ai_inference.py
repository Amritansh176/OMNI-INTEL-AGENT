"""
Phase 3: Agentic Multi-Step Extraction (Smart Refactor)
The AI uses a Multi-Agent cascade (Scout -> Extractor) backed by Instructor 
to guarantee perfectly structured Pydantic data and robust decision making.
"""
from celery_worker import app
from state_manager import state_manager
from config import Config
import json
import instructor
from openai import OpenAI
from pydantic import BaseModel, Field, model_validator
from typing import List, Any

# Setup instructor client pointing to local Ollama's OpenAI compatible API
client = instructor.from_openai(
    OpenAI(
        base_url="http://localhost:11434/v1",
        api_key="ollama",
    ),
    mode=instructor.Mode.JSON,
)

class EvaluatorDecision(BaseModel):
    has_useful_data: bool = Field(description="Does this text contain any real names, companies, or contact information relevant to the target?")
    reasoning: str = Field(description="Step-by-step reasoning for your decision")
    next_action_type: str = Field(description="crawl_subpage | search_entity | mutate_query | extract_data")
    action_target: str = Field(description="If crawl_subpage, provide the URL from Available Links. If search_entity, provide the specific name/company. Otherwise empty.")
    action_reason: str = Field(description="Why are we choosing this next action?")

class Lead(BaseModel):
    name: str = Field(default="", description="Full name of the person")
    organization: str = Field(default="", description="Name of the company or organization")
    designation: str = Field(default="", description="Job title or role")
    contact: str = Field(default="", description="Emails, phone numbers, or LinkedIn URLs")
    status: str = Field(default="")

class ExtractionResult(BaseModel):
    leads: List[Lead] = Field(default_factory=list, description="List of extracted leads. Empty list if none found.")

    @model_validator(mode='before')
    @classmethod
    def lowercase_keys(cls, data: Any):
        if isinstance(data, dict):
            # To handle LLM hallucinating 'Leads' instead of 'leads'
            return {k.lower(): v for k, v in data.items()}
        return data

    confidence_reasoning: str = Field(description="Step-by-step thought process justifying the confidence score based on data completeness and relevance")
    confidence: float = Field(description="Score between 0.0 and 1.0 representing certainty")


def is_low_value_content(text_content):
    if len(text_content.strip()) < 200:
        return True
    boilerplate = ["page not found", "enable javascript", "cookie policy", "captcha", "are you a human"]
    return any(b in text_content.lower() for b in boilerplate)


@app.task(bind=True, name="tasks.ai_inference.extract_structured_data", rate_limit='10/m', time_limit=600, soft_time_limit=570)
def extract_structured_data(self, job_id, pipeline, target, raw_data, depth=0, 
                            missing_fields=None, query_strategy=None, parent_job_id=None):
    """
    Agentic AI Extractor (Multi-Agent Cascade): 
    Agent A (Scout) decides if data is present and plans next action.
    Agent B (Extractor) extracts structured data if Scout allows it.
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
        _send_to_quality_scorer(job_id, pipeline, target, {"leads": [], "confidence": 0.0, "next_action": {"type": "mutate_query", "target": target, "reason": "Low value content detected"}}, query_strategy, parent_job_id, depth)
        return f"Job {job_id}: Low value content detected."

    if not state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, 
                                {"step": "ai_extraction", "depth": depth, "strategy": query_strategy}):
        return f"Job {job_id} cancelled."

    context_parts = []
    if missing_fields:
        context_parts.append(f"PRIORITY: You are specifically looking for these missing fields: {', '.join(missing_fields)}")
    if query_strategy:
        context_parts.append(f"This data was obtained via strategy: {query_strategy}")
    
    extra_context = "\n".join(context_parts)
    links_context = ""
    if interesting_links:
        links_preview = interesting_links[:10]
        links_context = f"\n\nAvailable links found on this page:\n" + "\n".join([f"- {l}" for l in links_preview])

    # ==========================================
    # AGENT A: The Scout (Evaluator)
    # ==========================================
    eval_prompt = f"""You are a Scout AI. Read the source text and determine if it contains ANY identifiable leads (person, company, organization, contact info) related to the target '{target}'.

{extra_context}
{links_context}

Source URL: {source_url}
Raw Data:
{text_content[:4000]}

Decide the next action:
- 'extract_data': If there is ANY useful data in the text, you MUST choose this action.
- 'crawl_subpage': Pick a URL from Available links (e.g. /about, /team, /contact) if the current page is useless but a subpage looks promising.
- 'search_entity': If you found a name but need more context, request a search.
- 'mutate_query': If the text is completely irrelevant boilerplate, request a new search strategy.
"""
    try:
        decision: EvaluatorDecision = client.chat.completions.create(
            model=Config.OLLAMA_MODEL,
            messages=[{'role': 'user', 'content': eval_prompt}],
            response_model=EvaluatorDecision,
            max_retries=2
        )
        
        action_type = decision.next_action_type
        action_target = decision.action_target
        action_reason = decision.reasoning
        
        if depth >= Config.MAX_AI_LOOP_DEPTH:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "max_depth_reached", "depth": depth})
            action_type = "extract_data" if decision.has_useful_data else "sufficient"

        if action_type != "extract_data" and not decision.has_useful_data:
            result = {"leads": [], "confidence": 0.0, "next_action": {"type": action_type, "target": action_target, "reason": action_reason}}
            
            if action_type == "crawl_subpage" and action_target:
                state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "agentic_crawl_subpage", "subpage": action_target, "reason": action_reason, "depth": depth})
                app.send_task("tasks.crawl.execute_crawl", args=[job_id, pipeline, action_target, missing_fields], kwargs={"depth": depth + 1, "original_target": target, "query_strategy": "subpage_crawl", "parent_job_id": parent_job_id or job_id})
                return f"Job {job_id}: AI requests subpage crawl: {action_target}"

            elif action_type == "search_entity" and action_target:
                state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "agentic_entity_search", "entity": action_target, "reason": action_reason, "depth": depth})
                app.send_task("tasks.ai_query_generator.generate_queries", args=[job_id, pipeline, action_target, missing_fields], kwargs={"depth": depth + 1, "original_target": target})
                return f"Job {job_id}: AI requests entity search: {action_target}"

            elif action_type == "mutate_query":
                state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "agentic_query_mutation", "reason": action_reason, "depth": depth})
                app.send_task("tasks.ai_query_generator.generate_queries", args=[job_id, pipeline, target, missing_fields or ["name", "organization", "contact"]], kwargs={"depth": depth + 1, "original_target": target})
                return f"Job {job_id}: AI requests query mutation"

            else:
                _send_to_quality_scorer(job_id, pipeline, target, result, query_strategy, parent_job_id, depth)
                return f"Job {job_id}: Sending empty result to quality scorer."

        # ==========================================
        # AGENT B: The Extractor
        # ==========================================
        extract_prompt = f"""You are a precise Data Extractor AI.
Extract all relevant leads for the target '{target}' from the text below.
Do not guess or fabricate information. If a field is missing, leave it empty.

Raw Data:
{text_content[:4000]}
"""
        extraction: ExtractionResult = client.chat.completions.create(
            model=Config.OLLAMA_MODEL,
            messages=[{'role': 'user', 'content': extract_prompt}],
            response_model=ExtractionResult,
            max_retries=2
        )

        leads = []
        for lead_obj in extraction.leads:
            lead_dict = lead_obj.model_dump()
            if any(str(v).strip() for v in lead_dict.values()):
                lead_dict["source_url"] = source_url
                leads.append(lead_dict)
                
        confidence = extraction.confidence
        
        if leads:
            baseline_conf = min(0.9, max(sum(1 for v in l.values() if str(v).strip()) for l in leads) * 0.25)
            confidence = max(confidence, baseline_conf)

        result = {
            "leads": leads,
            "confidence": confidence,
            "confidence_reasoning": extraction.confidence_reasoning,
            "next_action": {"type": "sufficient", "target": target, "reason": "Data extracted"}
        }

        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "extraction_complete", "confidence": confidence, "leads": len(leads)})
        _send_to_quality_scorer(job_id, pipeline, target, result, query_strategy, parent_job_id, depth)
        return f"Job {job_id}: Extraction complete (confidence: {confidence})."

    except Exception as e:
        if self.request.retries < Config.MAX_RETRIES:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": f"ai_extraction_retry_{self.request.retries + 1}"})
            raise self.retry(exc=e, countdown=2 ** self.request.retries)
        else:
            state_manager.set_job_state(job_id, pipeline, "FAILED", target, {"step": "ai_extraction", "error": "Max retries exceeded", "details": str(e)})
            return f"Job {job_id} failed during AI extraction after max retries."

def _send_to_quality_scorer(job_id, pipeline, target, result, query_strategy=None, parent_job_id=None, depth=0):
    app.send_task("tasks.quality_scorer.score_and_gate", args=[
        job_id, pipeline, target, result
    ], kwargs={
        "query_strategy": query_strategy,
        "parent_job_id": parent_job_id,
        "depth": depth
    })
