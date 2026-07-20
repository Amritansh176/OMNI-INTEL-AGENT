from celery_worker import app
from state_manager import state_manager
from config import Config
import json
import os
import time

@app.task(bind=True, name="tasks.handoff.deliver_to_omni")
def deliver_to_omni(self, job_id, pipeline, target, structured_data):
    """
    Hands off the final structured data to OMNI by saving it as a JSON file.
    """
    state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "handoff"})
    
    # We will use the job_id and timestamp to create a unique filename
    filename = f"{pipeline}_{job_id}_{int(time.time())}.json"
    filepath = os.path.join(Config.HANDOFF_DIR, filename)
    
    output = {
        "job_id": job_id,
        "pipeline": pipeline,
        "target": target,
        "data": structured_data
    }
    
    try:
        with open(filepath, 'w') as f:
            json.dump(output, f, indent=4)
        
        state_manager.set_job_state(job_id, pipeline, "COMPLETED", target, {"step": "handoff_completed", "file": filepath})
        return f"Job {job_id} successfully handed off. File: {filepath}"
    except Exception as e:
        state_manager.set_job_state(job_id, pipeline, "FAILED", target, {"step": "handoff", "error": str(e)})
        return f"Job {job_id} failed during handoff: {str(e)}"
