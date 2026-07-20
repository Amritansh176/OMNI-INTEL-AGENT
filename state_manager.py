import redis
import json
from datetime import datetime
from config import Config

class StateManager:
    def __init__(self):
        self.redis_client = redis.Redis.from_url(Config.STATE_REDIS_URL, decode_responses=True)

    def set_job_state(self, job_id, pipeline, state, target, metadata=None):
        """
        Set the state of a specific job.
        """
        data = {
            "pipeline": pipeline,
            "target": target,
            "state": state,
            "updated_at": datetime.utcnow().isoformat(),
            "metadata": metadata or {}
        }
        # Keep job data in a hash
        self.redis_client.hset("jobs", job_id, json.dumps(data))
        
        # Also push to a pub/sub channel for live dashboard updates
        message = {"job_id": job_id, **data}
        self.redis_client.publish("job_updates", json.dumps(message))

    def get_all_jobs(self):
        """
        Retrieve all tracked jobs.
        """
        jobs = self.redis_client.hgetall("jobs")
        return {k: json.loads(v) for k, v in jobs.items()}

    def get_job(self, job_id):
        job = self.redis_client.hget("jobs", job_id)
        if job:
            return json.loads(job)
        return None

# Singleton instance for easy access
state_manager = StateManager()
