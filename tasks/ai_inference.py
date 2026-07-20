from celery_worker import app
from state_manager import state_manager
from config import Config
import ollama
import json

@app.task(bind=True, name="tasks.ai_inference.extract_structured_data", rate_limit='10/m')
def extract_structured_data(self, job_id, pipeline, target, raw_data, depth=0, missing_fields=None):
    """
    Uses local Ollama model to extract structured fields from raw crawl data.
    Validates output and loops back to crawl deeper if required fields are missing.
    """
    state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "ai_inference", "depth": depth})
    
    if missing_fields:
        prompt_focus = f"Pay special attention to extracting these missing fields: {', '.join(missing_fields)}"
    else:
        prompt_focus = "Extract key entities (names, organizations, designations, contacts, status) from the following raw web data."

    prompt = f"""
    You are an intelligence extraction AI. {prompt_focus}
    Return ONLY a valid JSON object matching this schema:
    {{
        "leads": [
            {{"name": "...", "organization": "...", "designation": "...", "contact": "...", "status": "..."}}
        ]
    }}
    
    Raw Data:
    {json.dumps(raw_data)[:4000]} # Limit to avoid context window explosion
    """
    
    try:
        response = ollama.chat(model=Config.OLLAMA_MODEL, messages=[
            {
                'role': 'user',
                'content': prompt
            }
        ])
        
        output_text = response['message']['content']
        
        try:
            start = output_text.find('{')
            end = output_text.rfind('}') + 1
            if start != -1 and end != -1:
                structured_data = json.loads(output_text[start:end])
            else:
                structured_data = {"error": "No JSON found", "raw_output": output_text}
        except json.JSONDecodeError:
            structured_data = {"error": "Invalid JSON", "raw_output": output_text}
            
        # Validation Logic
        leads = structured_data.get("leads", [])
        current_missing = []
        if leads and isinstance(leads, list) and len(leads) > 0:
            lead = leads[0] # check first lead
            for field in ["name", "organization", "designation", "contact", "status"]:
                val = lead.get(field, "")
                if not val or val == "" or val.lower() == "n/a" or val.lower() == "unknown":
                    current_missing.append(field)
        else:
            current_missing = ["name", "organization", "designation", "contact", "status"]

        if len(current_missing) > 0 and depth < 10: # Cap at 10 depth
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "validation_failed_retrying", "missing": current_missing, "depth": depth})
            
            # Figure out next URL to crawl or trigger dorking
            next_url = None
            if isinstance(raw_data, dict):
                links = raw_data.get("interesting_links", [])
                # Prioritize links matching missing fields
                prioritized = [l for l in links if any(m.lower() in l.lower() for m in current_missing)]
                if prioritized:
                    next_url = prioritized[0]
                elif links:
                    next_url = links[0] # just take the first interesting one

            if next_url and next_url.startswith("http"):
                # Crawl the next link
                app.send_task("tasks.crawl.execute_crawl", args=[job_id, pipeline, next_url, current_missing, depth + 1, target])
                return f"Job {job_id} missing {current_missing}. Looping to deeper link: {next_url}"
            else:
                # Fallback to dorking
                app.send_task("tasks.crawl.execute_crawl", args=[job_id, pipeline, target, current_missing, depth + 1, target])
                return f"Job {job_id} missing {current_missing}. Looping via dorking."

        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "ai_inference_completed"})
        
        # Send to handoff queue
        app.send_task("tasks.handoff.deliver_to_omni", args=[job_id, pipeline, target, structured_data])
        return f"Job {job_id} AI extraction complete."

    except Exception as e:
        if self.request.retries < Config.MAX_RETRIES:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": f"ai_inference_retry_{self.request.retries+1}"})
            raise self.retry(exc=e, countdown=2 ** self.request.retries)
        else:
            state_manager.set_job_state(job_id, pipeline, "FAILED", target, {"step": "ai_inference", "error": "Max retries exceeded", "details": str(e)})
            return f"Job {job_id} failed during AI inference after max retries."
