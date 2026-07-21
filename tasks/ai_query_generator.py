"""
Phase 1: AI-Driven Dynamic Query Generation
Instead of hardcoded dorking strings, LLaMA generates diverse, adaptive search queries.
"""
from celery_worker import app
from state_manager import state_manager
from feedback_store import feedback_store
from config import Config
import ollama
import json


@app.task(bind=True, name="tasks.ai_query_generator.generate_queries")
def generate_queries(self, job_id, pipeline, target, keywords=None, depth=0, original_target=None):
    """
    Uses LLaMA to dynamically generate diverse search queries for a target.
    Feeds successful past patterns into the prompt so the AI learns over time.
    """
    actual_target = original_target or target
    state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target, 
                                {"step": "ai_query_generation", "depth": depth})

    # Build context from feedback store
    past_successes = feedback_store.get_successful_patterns(limit=5)
    past_failures = feedback_store.get_failed_patterns(limit=3)
    top_sites = feedback_store.get_top_sites(limit=5)

    success_context = ""
    if past_successes:
        examples = [f'- Query: "{p["query"]}" (strategy: {p["strategy"]}, score: {p["score"]})' for p in past_successes[:3]]
        success_context = f"\n\nThese query patterns worked well in the past:\n" + "\n".join(examples)

    failure_context = ""
    if past_failures:
        bad_examples = [f'- Query: "{p["query"]}" failed because: {p["reason"]}' for p in past_failures[:3]]
        failure_context = f"\n\nAvoid these patterns that failed before:\n" + "\n".join(bad_examples)

    site_context = ""
    if top_sites:
        sites = [f"- {site} (reliability score: {score})" for site, score in top_sites[:3]]
        site_context = f"\n\nThese websites have historically provided reliable data:\n" + "\n".join(sites)

    keyword_hint = ""
    if keywords:
        keyword_hint = f"\nSpecifically focus on finding: {', '.join(keywords)}"

    prompt = f"""You are an expert OSINT query strategist. Generate exactly {Config.AI_QUERY_COUNT} diverse search queries to find intelligence about: "{target}"
{keyword_hint}
{success_context}
{failure_context}
{site_context}

Each query should use a DIFFERENT strategy. Return ONLY a valid JSON array:
[
    {{"query": "the actual search string", "strategy": "google_dork|linkedin|direct_url|news|academic|social_media"}},
    ...
]

Strategies to use:
- google_dork: Use Google dork syntax like site:, inurl:, intitle:, filetype:
- linkedin: Target LinkedIn profiles/companies
- direct_url: Guess likely URLs (e.g., company websites, about pages)
- news: Search news articles and press releases
- social_media: Target Twitter/X, Facebook company pages

Be creative and specific. Do NOT use generic queries."""

    try:
        response = ollama.chat(
            model=Config.OLLAMA_MODEL, 
            messages=[{'role': 'user', 'content': prompt}],
            format='json'
        )

        output_text = response['message']['content']
        try:
            result = json.loads(output_text)
            if isinstance(result, list):
                queries = result
            elif isinstance(result, dict) and "queries" in result:
                queries = result["queries"]
            else:
                queries = []
        except json.JSONDecodeError:
            queries = []

        if not queries:
            queries = [{"query": f"{actual_target} {' '.join(keywords or [])}", "strategy": "basic"}]

        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target,
                                    {"step": "queries_generated", "count": len(queries), "depth": depth})

        # Dispatch each query to the crawler in parallel
        for i, q in enumerate(queries):
            query_str = q.get("query", target)
            strategy = q.get("strategy", "unknown")
            
            # Send to crawler with the generated query and strategy metadata
            app.send_task("tasks.crawl.execute_crawl", args=[
                f"{job_id}_q{i}",  # Sub-job ID for each query
                pipeline,
                query_str,
                keywords,
            ], kwargs={
                "depth": depth,
                "original_target": actual_target,
                "query_strategy": strategy,
                "parent_job_id": job_id
            })

        return f"Job {job_id}: Generated {len(queries)} AI queries and dispatched to crawler."

    except Exception as e:
        # On failure, fall back to basic query dispatch
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target,
                                    {"step": "query_gen_fallback", "error": str(e)})
        app.send_task("tasks.crawl.execute_crawl", args=[
            job_id, pipeline, target, keywords or []
        ], kwargs={"depth": depth, "original_target": actual_target})
        return f"Job {job_id}: Query generation failed, falling back to basic crawl."
