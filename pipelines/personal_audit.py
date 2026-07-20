from celery_worker import app
from config import Config
import time
import uuid

# Mock dynamic data source for existing records
def get_existing_records_batch():
    return [
        {"target": "John Doe Google", "keywords": ["John Doe", "Google", "status"]},
        {"target": "https://anotherexample.com/about", "keywords": ["Jane Smith", "director"]}
    ]

@app.task(bind=True, name="pipelines.personal_audit.run_loop")
def run_loop(self):
    """
    Infinite loop for Personal Audit pipeline.
    Fetches existing records and enqueues them to the crawl_queue.
    """
    batch = get_existing_records_batch()
    
    for item in batch:
        job_id = f"pa_{uuid.uuid4().hex[:8]}"
        app.send_task("tasks.crawl.execute_crawl", args=[
            job_id, 
            "personal_audit", 
            item["target"], 
            item["keywords"]
        ])
    
    # Wait a small gap before re-enqueueing to avoid pinning CPU
    time.sleep(Config.LOOP_IDLE_GAP_SEC)
    
    # Self-requeue
    app.send_task("pipelines.personal_audit.run_loop")
    
    return f"Enqueued {len(batch)} jobs in personal_audit_loop"
