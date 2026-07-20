from celery_worker import app
from config import Config
from state_manager import state_manager
import time
import uuid

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
    # Backpressure check
    crawl_len = state_manager.redis_client.llen("crawl_queue")
    ai_len = state_manager.redis_client.llen("ai_inference_queue")
    
    if crawl_len > Config.MAX_QUEUE_SIZE or ai_len > Config.MAX_QUEUE_SIZE:
        time.sleep(Config.LOOP_IDLE_GAP_SEC * 2)
        app.send_task("pipelines.lead_scout.run_loop")
        return f"Backpressure active (Crawl: {crawl_len}, AI: {ai_len}). Slept and requeued."

    batch = get_keywords_batch()
    
    for item in batch:
        job_id = f"ls_{uuid.uuid4().hex[:8]}"
        app.send_task("tasks.crawl.execute_crawl", args=[
            job_id, 
            "lead_scout", 
            item["target"], 
            item["keywords"]
        ])
    
    time.sleep(Config.LOOP_IDLE_GAP_SEC)
    app.send_task("pipelines.lead_scout.run_loop")
    
    return f"Enqueued {len(batch)} jobs in lead_scout_loop"
