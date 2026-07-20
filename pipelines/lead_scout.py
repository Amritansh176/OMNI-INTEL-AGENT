from celery_worker import app
from config import Config
import time
import uuid

# Mock dynamic data source for keywords (in real life, this might fetch from an API or file)
def get_keywords_batch():
    return [
        {"target": "https://example.com", "keywords": ["ceo", "director", "founder"]},
        {"target": "AI startup founder", "keywords": ["founder", "ai"]}
    ]

@app.task(bind=True, name="pipelines.lead_scout.run_loop")
def run_loop(self):
    """
    Infinite loop for Lead Scout pipeline.
    Fetches keywords/targets and enqueues them to the crawl_queue.
    """
    batch = get_keywords_batch()
    
    for item in batch:
        job_id = f"ls_{uuid.uuid4().hex[:8]}"
        app.send_task("tasks.crawl.execute_crawl", args=[
            job_id, 
            "lead_scout", 
            item["target"], 
            item["keywords"]
        ])
    
    # Wait a small gap before re-enqueueing to avoid pinning CPU
    time.sleep(Config.LOOP_IDLE_GAP_SEC)
    
    # Self-requeue
    app.send_task("pipelines.lead_scout.run_loop")
    
    return f"Enqueued {len(batch)} jobs in lead_scout_loop"
