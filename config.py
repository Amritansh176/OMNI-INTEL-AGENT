import os

class Config:
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")
    OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    
    # State tracking Redis DB
    STATE_REDIS_URL = os.getenv("STATE_REDIS_URL", "redis://localhost:6379/1")

    # Output directory for handoff to OMNI
    HANDOFF_DIR = os.path.join(os.path.dirname(__file__), "data", "handoff")
    DLQ_DIR = os.path.join(os.path.dirname(__file__), "data", "dlq")

    # Concurrency and timing
    LOOP_IDLE_GAP_SEC = 5
    MAX_RETRIES = 3
    
    # Backpressure Limits
    MAX_QUEUE_SIZE = 100
    
    # Crawling config
    MAX_CRAWL_DEPTH = 1

os.makedirs(Config.HANDOFF_DIR, exist_ok=True)
os.makedirs(Config.DLQ_DIR, exist_ok=True)
