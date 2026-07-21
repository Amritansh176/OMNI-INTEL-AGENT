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
    worker_prefetch_multiplier=1 # Recommended for long running tasks
)

# Route tasks to specific queues
app.conf.task_routes = {
    "pipelines.lead_scout.*": {"queue": "lead_scout_loop"},
    "pipelines.personal_audit.*": {"queue": "personal_audit_loop"},
    "tasks.crawl.*": {"queue": "crawl_queue"},
    "tasks.ai_inference.*": {"queue": "ai_inference_queue"},
    "tasks.ai_query_generator.*": {"queue": "ai_inference_queue"},
    "tasks.semantic_filter.*": {"queue": "ai_inference_queue"},
    "tasks.quality_scorer.*": {"queue": "ai_inference_queue"},
    "tasks.handoff.*": {"queue": "handoff_queue"},
    "tasks.finetune.*": {"queue": "finetune_loop"},
}
