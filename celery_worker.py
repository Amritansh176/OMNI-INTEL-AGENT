from celery import Celery
from config import Config

# Initialize Celery app
app = Celery(
    "omni_intel_agent",
    broker=Config.REDIS_URL,
    backend=Config.REDIS_URL,
    include=[
        "tasks.crawl",
        "tasks.ai_inference",
        "tasks.ai_query_generator",
        "tasks.semantic_filter",
        "tasks.quality_scorer",
        "tasks.handoff",
        "tasks.finetune",
        "pipelines.lead_scout",
        "pipelines.personal_audit"
    ]
)

# Optional configuration
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True, # Ensure tasks are ack'd after execution to prevent drops
    worker_prefetch_multiplier=1, # Recommended for long running tasks
    worker_concurrency=2, # Ollama serves 1 inference at a time; 2 workers = 1 LLM + 1 fast task
)

# Removed task_routes so all tasks default to the 'celery' queue. 
# This ensures a single local worker picks up all tasks without needing complex -Q flags.
