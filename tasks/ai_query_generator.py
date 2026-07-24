"""
Phase 1: AI-Driven Query Generation (Optimized)
Uses Pydantic-validated output and shorter prompts for speed.
"""
from celery_worker import app
from state_manager import state_manager
from feedback_store import feedback_store
from config import Config
from ollama_client import query_llm
from pydantic import BaseModel, Field, model_validator
from typing import List, Any


class SearchQuery(BaseModel):
    query: str = Field(description="The search query string")
    strategy: str = Field(description="google_dork | linkedin | direct_url | news")

class QueryPlan(BaseModel):
    queries: List[SearchQuery] = Field(default_factory=list, description="List of search queries")

    @model_validator(mode='before')
    @classmethod
    def normalize(cls, data: Any):
        if isinstance(data, dict):
            data = {k.lower(): v for k, v in data.items()}
            # Handle LLM returning a flat list instead of {"queries": [...]}
            if "queries" not in data and isinstance(data, dict):
                # Check if keys look like list items
                for key in list(data.keys()):
                    if isinstance(data[key], list) and len(data[key]) > 0:
                        data["queries"] = data[key]
                        break
        elif isinstance(data, list):
            return {"queries": data}
        return data


@app.task(bind=True, name="tasks.ai_query_generator.generate_queries", time_limit=600, soft_time_limit=570)
def generate_queries(self, job_id, pipeline, target, keywords=None, depth=0, original_target=None):
    """
    Uses LLM to generate diverse search queries. Feeds past patterns for learning.
    """
    actual_target = original_target or target
    if not state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "generating_queries", "depth": depth}):
        return f"Job {job_id} cancelled."

    # Build context from feedback store
    past_successes = feedback_store.get_successful_patterns(limit=3)
    past_failures = feedback_store.get_failed_patterns(limit=2)

    context_parts = []
    if past_successes:
        examples = [f'- "{p["query"]}" (score: {p["score"]})' for p in past_successes]
        context_parts.append("Past successes:\n" + "\n".join(examples))
    if past_failures:
        bad = [f'- "{p["query"]}" failed: {p["reason"][:50]}' for p in past_failures]
        context_parts.append("Avoid these:\n" + "\n".join(bad))

    keyword_hint = f"\nFocus on: {', '.join(keywords)}" if keywords else ""
    context = "\n".join(context_parts)

    # Prevent exponential fan-out on mutations (1 -> 5 -> 25 -> 125)
    query_count = Config.AI_QUERY_COUNT if depth == 0 and pipeline == "lead_scout" else 1

    prompt = f"""You are an elite OSINT analyst for INDIA-FOCUSED intelligence. Generate search queries to find REAL INDIAN PEOPLE (names, designations, contacts) related to: "{target}"
{keyword_hint}
{context}

GENERATE EXACTLY {query_count} DIVERSE QUERIES using these strategies:

STRATEGY: "google_dork"
- Use advanced operators: site:, intitle:, inurl:, filetype:, "exact phrase"
- INDIA-SPECIFIC PATTERNS:
  * site:linkedin.com/in/ "{target}" India (finds Indian people on LinkedIn)
  * site:zaubacorp.com "{target}" (Indian company directors from MCA registry)
  * site:tofler.in "{target}" directors (Indian company leadership)
  * intitle:"team" OR intitle:"leadership" OR intitle:"about us" "{target}" India
  * "{target}" India "CEO" OR "founder" OR "director" OR "managing director" email OR contact
  * filetype:pdf "{target}" India annual report board of directors
  * site:mca.gov.in "{target}" (Ministry of Corporate Affairs — Indian company filings)
- NEVER generate generic queries without "India" or operators

STRATEGY: "linkedin"
- Target LinkedIn specifically:
  * site:linkedin.com/in/ "{target}" India current position
  * site:linkedin.com/company/ "{target}" India employees

STRATEGY: "direct_url"
- Target known INDIAN data-rich sites:
  * site:zaubacorp.com "{target}" (Indian company directors)
  * site:tofler.in "{target}" directors
  * site:theorg.com "{target}" India (org charts)
  * site:crunchbase.com "{target}" India founder CEO

STRATEGY: "news"
- Target INDIAN news sources for recent mentions:
  * site:economictimes.com "{target}" appointed OR named OR joins
  * site:livemint.com "{target}" CEO OR MD OR director
  * site:moneycontrol.com "{target}" management team
  * site:business-standard.com "{target}" leadership

ANTI-PATTERNS (NEVER generate these):
- Generic one-word queries without operators
- Queries that would match Wikipedia main page
- Queries targeting tool/utility websites (calculator, converter, etc.)
- Queries without the target name in quotes
- Queries without India context

Each query MUST include "{target}" in quotes and target INDIAN results."""


    try:
        plan: QueryPlan = query_llm(prompt, QueryPlan, max_retries=2)
        queries = [q.model_dump() for q in plan.queries]

        if not queries:
            queries = [{"query": f"{actual_target} {' '.join(keywords or [])}", "strategy": "basic"}]

        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target,
                                    {"step": "queries_generated", "count": len(queries), "depth": depth})

        # Dispatch each query to the crawler
        for i, q in enumerate(queries):
            query_str = q.get("query", target)
            strategy = q.get("strategy", "unknown")
            
            app.send_task("tasks.crawl.execute_crawl", args=[
                f"{job_id}_q{i}",
                pipeline,
                query_str,
                keywords,
            ], kwargs={
                "depth": depth,
                "original_target": actual_target,
                "query_strategy": strategy,
                "parent_job_id": job_id
            })

        return f"Job {job_id}: Generated {len(queries)} AI queries and dispatched."

    except Exception as e:
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target,
                                    {"step": "query_gen_fallback", "error": str(e)})
        app.send_task("tasks.crawl.execute_crawl", args=[
            job_id, pipeline, target, keywords or []
        ], kwargs={"depth": depth, "original_target": actual_target})
        return f"Job {job_id}: Query generation failed, falling back to basic crawl."
