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

    # --- AI Pipeline Settings ---
    # Quality Scorer: minimum score (0-1) for a lead to pass to handoff
    QUALITY_THRESHOLD = float(os.getenv("QUALITY_THRESHOLD", "0.7"))
    # Maximum depth for the agentic extraction loop
    MAX_AI_LOOP_DEPTH = int(os.getenv("MAX_AI_LOOP_DEPTH", "10"))
    # Semantic Filter: number of top text chunks to keep per page
    SEMANTIC_FILTER_TOP_K = int(os.getenv("SEMANTIC_FILTER_TOP_K", "3"))
    # Number of diverse queries the AI Query Generator produces per job
    AI_QUERY_COUNT = int(os.getenv("AI_QUERY_COUNT", "5"))

os.makedirs(Config.HANDOFF_DIR, exist_ok=True)
os.makedirs(Config.DLQ_DIR, exist_ok=True)

