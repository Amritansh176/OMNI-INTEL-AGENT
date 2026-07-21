"""
Feedback Store: Redis-backed memory that records what worked and what didn't.
The AI Query Generator reads this to learn from past successes and avoid repeating failures.
"""
import redis
import json
from datetime import datetime
from config import Config


class FeedbackStore:
    def __init__(self):
        self.redis_client = redis.Redis.from_url(Config.STATE_REDIS_URL, decode_responses=True)
        self.SUCCESS_KEY = "feedback:successful_patterns"
        self.FAILURE_KEY = "feedback:failed_patterns"
        self.SITE_SCORES_KEY = "feedback:site_scores"

    def record_success(self, target, query_used, strategy, score, lead_data):
        """Record a query pattern that yielded a high-quality lead."""
        entry = {
            "target": target,
            "query": query_used,
            "strategy": strategy,
            "score": score,
            "lead_preview": {k: v for k, v in lead_data.items() if k in ["name", "organization"]},
            "timestamp": datetime.utcnow().isoformat()
        }
        # Store as a list, capped at 200 most recent successes
        self.redis_client.lpush(self.SUCCESS_KEY, json.dumps(entry))
        self.redis_client.ltrim(self.SUCCESS_KEY, 0, 199)

        # Track which sites are reliable
        site = lead_data.get("source_url", target)
        self.redis_client.zincrby(self.SITE_SCORES_KEY, score, site)

    def record_failure(self, target, query_used, strategy, reason):
        """Record a query pattern that failed or yielded garbage."""
        entry = {
            "target": target,
            "query": query_used,
            "strategy": strategy,
            "reason": reason,
            "timestamp": datetime.utcnow().isoformat()
        }
        self.redis_client.lpush(self.FAILURE_KEY, json.dumps(entry))
        self.redis_client.ltrim(self.FAILURE_KEY, 0, 99)

    def get_successful_patterns(self, limit=10):
        """Get recent successful query patterns for prompt context."""
        raw = self.redis_client.lrange(self.SUCCESS_KEY, 0, limit - 1)
        return [json.loads(r) for r in raw]

    def get_failed_patterns(self, limit=5):
        """Get recent failures so the AI avoids repeating them."""
        raw = self.redis_client.lrange(self.FAILURE_KEY, 0, limit - 1)
        return [json.loads(r) for r in raw]

    def get_top_sites(self, limit=10):
        """Get the highest-scoring sites that consistently provide good data."""
        return self.redis_client.zrevrange(self.SITE_SCORES_KEY, 0, limit - 1, withscores=True)


# Singleton
feedback_store = FeedbackStore()
