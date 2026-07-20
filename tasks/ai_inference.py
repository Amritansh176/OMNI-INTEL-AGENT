from celery_worker import app
from state_manager import state_manager
from config import Config
import ollama
import json

@app.task(bind=True, name="tasks.ai_inference.extract_structured_data", rate_limit='10/m')
def extract_structured_data(self, job_id, pipeline, target, raw_data):
    """
    Uses local Ollama model to extract structured fields from raw crawl data.
    """
    state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "ai_inference"})
    
    prompt = f"""
    You are an intelligence extraction AI. Extract key entities (names, organizations, designations, contacts, status) from the following raw web data.
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
        # We use a smaller context just as an example.
        response = ollama.chat(model=Config.OLLAMA_MODEL, messages=[
            {
                'role': 'user',
                'content': prompt
            }
        ])
        
        output_text = response['message']['content']
        
        # Try to parse JSON from output (rough parsing)
        # In a production app, we would use more robust json extraction.
        try:
            # Find first '{' and last '}'
            start = output_text.find('{')
            end = output_text.rfind('}') + 1
            if start != -1 and end != -1:
                structured_data = json.loads(output_text[start:end])
            else:
                structured_data = {"error": "No JSON found", "raw_output": output_text}
        except json.JSONDecodeError:
            structured_data = {"error": "Invalid JSON", "raw_output": output_text}
            
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "ai_inference_completed"})
        
        # Send to handoff queue
        app.send_task("tasks.handoff.deliver_to_omni", args=[job_id, pipeline, target, structured_data])
        return f"Job {job_id} AI extraction complete."

    except Exception as e:
        if self.request.retries < Config.MAX_RETRIES:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": f"ai_inference_retry_{self.request.retries+1}"})
            # Exponential backoff retry
            raise self.retry(exc=e, countdown=2 ** self.request.retries)
        else:
            state_manager.set_job_state(job_id, pipeline, "FAILED", target, {"step": "ai_inference", "error": "Max retries exceeded", "details": str(e)})
            return f"Job {job_id} failed during AI inference after max retries."
