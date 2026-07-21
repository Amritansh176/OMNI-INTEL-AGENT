from fastapi import FastAPI, WebSocket, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse
from state_manager import state_manager
from config import Config
from celery_worker import app as celery_app
import redis
import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import uuid
import json
import ollama

app = FastAPI(title="OMNI Intel Agent Dashboard")

html = """
<!DOCTYPE html>
<html>
    <head>
        <title>OMNI Intel Agent Dashboard</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f4f4f9; padding: 20px; color: #333; }
            h1 { color: #222; margin: 0; }
            .header-flex { display: flex; justify-content: space-between; align-items: center; }
            .btn { padding: 10px 15px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
            .btn:hover { background: #218838; }
            .btn-danger { background: #dc3545; }
            .btn-danger:hover { background: #c82333; }
            table { width: 100%; border-collapse: collapse; margin-top: 20px; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
            th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
            th { background-color: #007bff; color: white; }
            tr:hover { background-color: #f1f1f1; }
            .status-IN_PROGRESS { color: orange; font-weight: bold; }
            .status-COMPLETED { color: green; font-weight: bold; }
            .status-FAILED { color: red; font-weight: bold; }
            .status-SKIPPED { color: gray; font-weight: bold; }
            .status-QUEUED { color: #6f42c1; font-weight: bold; }
            .status-CANCELLED { color: #dc3545; font-weight: bold; }
            
            .cancel-btn {
                background-color: #dc3545;
                color: white;
                border: none;
                padding: 4px 8px;
                border-radius: 4px;
                cursor: pointer;
                font-size: 12px;
                margin-top: 5px;
            }
            .cancel-btn:hover {
                background-color: #c82333;
            }
            
            /* Modal styles */
            .modal { display: none; position: fixed; z-index: 1; left: 0; top: 0; width: 100%; height: 100%; overflow: auto; background-color: rgba(0,0,0,0.4); }
            .modal-content { background-color: #fefefe; margin: 10% auto; padding: 20px; border: 1px solid #888; width: 500px; border-radius: 8px; }
            .close { color: #aaa; float: right; font-size: 28px; font-weight: bold; cursor: pointer; }
            .close:hover, .close:focus { color: black; text-decoration: none; cursor: pointer; }
            .tabs { display: flex; margin-bottom: 20px; border-bottom: 1px solid #ddd; }
            .tab { padding: 10px 20px; cursor: pointer; border-bottom: 2px solid transparent; }
            .tab.active { border-bottom: 2px solid #007bff; color: #007bff; font-weight: bold; }
            .tab-content { display: none; }
            .tab-content.active { display: block; }
            .form-group { margin-bottom: 15px; }
            .form-group label { display: block; margin-bottom: 5px; font-weight: bold;}
            .form-group input, .form-group textarea { width: 100%; padding: 8px; box-sizing: border-box; border: 1px solid #ccc; border-radius: 4px; }
            .form-group small { display: block; margin-top: 5px; color: #666; }
            .spinner { display: inline-block; width: 12px; height: 12px; border: 2px solid rgba(0,0,0,0.1); border-radius: 50%; border-top-color: #007bff; animation: spin 1s ease-in-out infinite; vertical-align: middle; }
            @keyframes spin { to { transform: rotate(360deg); } }
            .step-badge { display: inline-block; padding: 4px 8px; background-color: #e9ecef; border-radius: 12px; font-size: 11px; color: #495057; margin-top: 5px; font-weight: normal; letter-spacing: 0.5px; }
        </style>
    </head>
    <body>
        <div class="header-flex">
            <div>
                <h1>OMNI Intel Agent Real-Time Dashboard</h1>
                <p style="margin-top: 5px;">Live pipeline state from Redis.</p>
            </div>
            <button class="btn" onclick="openModal()">Start Job</button>
        </div>
        
        <div id="startModal" class="modal">
            <div class="modal-content">
                <span class="close" onclick="closeModal()">&times;</span>
                <h2>Start New Job</h2>
                <div class="tabs">
                    <div class="tab active" onclick="switchTab('prompt')">Type Prompt</div>
                    <div class="tab" onclick="switchTab('csv')">Upload CSV</div>
                </div>
                
                <div id="tab-prompt" class="tab-content active">
                    <form id="promptForm" onsubmit="submitPrompt(event)">
                        <div class="form-group">
                            <label>Describe the scraping task:</label>
                            <textarea id="promptInput" rows="4" placeholder="e.g. Find the CEO and founders of openai.com" required></textarea>
                        </div>
                        <button type="submit" class="btn" style="width: 100%;">Submit Prompt</button>
                    </form>
                </div>
                
                <div id="tab-csv" class="tab-content">
                    <form id="csvForm" onsubmit="submitCsv(event)">
                        <div class="form-group">
                            <label>Upload CSV File:</label>
                            <input type="file" id="csvFile" accept=".csv" required>
                            <small>AI will automatically infer targets and keywords from the content.</small>
                        </div>
                        <button type="submit" class="btn" style="width: 100%;">Upload & Start</button>
                    </form>
                </div>
                
                <div id="loadingContainer" style="display:none; margin-top:15px; text-align: center;">
                    <div style="color:#007bff; font-weight: bold; margin-bottom: 10px;">
                        Processing with AI... <span id="timerSpan">0s</span>
                    </div>
                    <button class="btn btn-danger" onclick="cancelProcessing()">Cancel</button>
                </div>
            </div>
        </div>

        <table>
            <thead>
                <tr>
                    <th>Job ID</th>
                    <th>Pipeline</th>
                    <th>Target</th>
                    <th>State</th>
                    <th>Metadata</th>
                    <th>Updated At</th>
                </tr>
            </thead>
            <tbody id="jobs-body">
            </tbody>
        </table>
        
        <script>
            var ws = new WebSocket("ws://" + location.host + "/ws");
            
            // Modal Logic
            function openModal() { document.getElementById('startModal').style.display = 'block'; }
            function closeModal() { document.getElementById('startModal').style.display = 'none'; }
            function switchTab(tab) {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(tc => tc.classList.remove('active'));
                document.querySelector(`.tab[onclick="switchTab('${tab}')"]`).classList.add('active');
                document.getElementById('tab-' + tab).classList.add('active');
            }
            window.onclick = function(event) {
                if (event.target == document.getElementById('startModal')) closeModal();
            }

            let currentController = null;
            let timerInterval = null;
            let timerSeconds = 0;

            function startTimer() {
                timerSeconds = 0;
                document.getElementById('timerSpan').innerText = "0s";
                document.getElementById('loadingContainer').style.display = 'block';
                document.querySelectorAll('form button[type="submit"]').forEach(b => b.disabled = true);
                timerInterval = setInterval(() => {
                    timerSeconds++;
                    document.getElementById('timerSpan').innerText = timerSeconds + "s";
                }, 1000);
            }

            function stopTimer() {
                if (timerInterval) clearInterval(timerInterval);
                document.getElementById('loadingContainer').style.display = 'none';
                document.querySelectorAll('form button[type="submit"]').forEach(b => b.disabled = false);
            }

            function cancelProcessing() {
                if (currentController) {
                    currentController.abort();
                    currentController = null;
                }
                stopTimer();
            }

            function cancelJob(job_id) {
                if (confirm('Are you sure you want to cancel this job?')) {
                    fetch('/api/cancel_job/' + job_id, { method: 'POST' })
                        .then(response => response.json())
                        .then(data => {
                            if (data.status !== 'success') {
                                alert('Failed to cancel job');
                            }
                        })
                        .catch(err => alert('Error cancelling job'));
                }
            }

            async function submitPrompt(e) {
                e.preventDefault();
                const prompt = document.getElementById('promptInput').value;
                
                currentController = new AbortController();
                startTimer();
                
                const formData = new FormData();
                formData.append('prompt', prompt);
                try {
                    await fetch('/api/start_job/prompt', { 
                        method: 'POST', 
                        body: formData,
                        signal: currentController.signal 
                    });
                    document.getElementById('promptInput').value = '';
                    closeModal();
                } catch (err) {
                    if (err.name === 'AbortError') {
                        console.log("Request cancelled by user");
                    } else {
                        alert("Error submitting job.");
                    }
                }
                stopTimer();
            }

            async function submitCsv(e) {
                e.preventDefault();
                const file = document.getElementById('csvFile').files[0];
                
                currentController = new AbortController();
                startTimer();
                
                const formData = new FormData();
                formData.append('file', file);
                try {
                    await fetch('/api/start_job/csv', { 
                        method: 'POST', 
                        body: formData,
                        signal: currentController.signal
                    });
                    document.getElementById('csvFile').value = '';
                    closeModal();
                } catch (err) {
                    if (err.name === 'AbortError') {
                        console.log("Request cancelled by user");
                    } else {
                        alert("Error uploading CSV.");
                    }
                }
                stopTimer();
            }

            // Function to update or add a row
            function upsertRow(job_id, data) {
                var tbody = document.getElementById('jobs-body');
                var existingRow = document.getElementById('row-' + job_id);
                
                let stepBadge = "";
                if (data.metadata && data.metadata.step) {
                    let stepText = data.metadata.step.replace(/_/g, ' ').toUpperCase();
                    let spinner = (data.state === "IN_PROGRESS" || data.state === "QUEUED") ? '<div class="spinner" style="margin-left: 6px;"></div>' : '';
                    stepBadge = `<br><div class="step-badge">${stepText}${spinner}</div>`;
                }

                let cancelBtnHtml = '';
                if (data.state === 'QUEUED' || data.state === 'IN_PROGRESS') {
                    cancelBtnHtml = `<br><button class="cancel-btn" onclick="cancelJob('${job_id}')">Cancel</button>`;
                }

                var rowHtml = `
                    <td>${job_id}</td>
                    <td>${data.pipeline}</td>
                    <td>${data.target}</td>
                    <td class="status-${data.state}">${data.state}${stepBadge}${cancelBtnHtml}</td>
                    <td><pre style="margin:0; font-size: 12px; max-height: 200px; overflow: auto;">${JSON.stringify(data.metadata, null, 2)}</pre></td>
                    <td>${data.updated_at}</td>
                `;
                
                if (existingRow) {
                    existingRow.innerHTML = rowHtml;
                } else {
                    var newRow = document.createElement('tr');
                    newRow.id = 'row-' + job_id;
                    newRow.innerHTML = rowHtml;
                    // Prepend new row
                    tbody.insertBefore(newRow, tbody.firstChild);
                }
            }

            ws.onmessage = function(event) {
                var message = JSON.parse(event.data);
                if (message.type === 'init') {
                    for (const [job_id, data] of Object.entries(message.data)) {
                        upsertRow(job_id, data);
                    }
                } else if (message.type === 'update') {
                    upsertRow(message.job_id, message.data);
                }
            };
        </script>
    </body>
</html>
"""

@app.get("/")
async def get():
    return HTMLResponse(html)

def extract_targets_from_text(text: str):
    prompt = f"""You are a task-understanding assistant for an intelligence-gathering system. 
Read the input below and figure out what the user actually wants.

There are exactly two possible pipeline types:
1. "lead_scout" — The user wants to FIND NEW entities (companies, startups, people) related to a topic or industry. Example: "counter-drone companies in India", "propeller manufacturers".
2. "personal_audit" — The user wants to INVESTIGATE an ALREADY KNOWN specific person or specific company. Example: "background check on Rohan Mehta", "audit ABC Pvt Ltd".

CRITICAL: DO NOT copy the examples. You MUST extract the target ONLY from the user's actual Input text.

Return ONLY this JSON object. No markdown fences, no explanation before or after:
{{
    "thought_process": "Analyze the query here: Does this seem like a request to find new leads (lead_scout) or investigate a specific known entity (personal_audit)?",
    "pipeline": "lead_scout|personal_audit",
    "jobs": [
        {{"target": "extract the search topic or entity name from the Input text", "keywords": ["attribute1", "attribute2"]}}
    ]
}}

Input text:
{text[:4000]}"""

    try:
        response = ollama.chat(
            model=Config.OLLAMA_MODEL, 
            messages=[{'role': 'user', 'content': prompt}],
            format='json',
            options={'timeout': 30}
        )
        output = response['message']['content']
        result = json.loads(output)
        return result.get("pipeline", "lead_scout"), result.get("jobs", [])
    except Exception as e:
        print("Error parsing with AI:", e)
        return "lead_scout", []

from fastapi import BackgroundTasks

def process_input_background(parent_job_id: str, text: str, source_type: str):
    state_manager.set_job_state(parent_job_id, "manual_run", "IN_PROGRESS", source_type, {"step": "analyzing_prompt_with_AI"})
    
    # Run the heavy LLM extraction
    pipeline, jobs = extract_targets_from_text(text)
    
    if not jobs:
        state_manager.set_job_state(parent_job_id, pipeline, "FAILED", source_type, {"step": "analyzing_prompt_with_AI", "error": "No valid targets extracted from input."})
        return

    # Dispatch actual Celery tasks for each found target
    for i, job in enumerate(jobs):
        job_id = f"manual_{uuid.uuid4().hex[:8]}"
        target = job.get("target")
        keywords = job.get("keywords", [])
        if target:
            state_manager.set_job_state(job_id, pipeline, "QUEUED", target, {"step": "queued", "keywords": keywords})
            
            if pipeline == "lead_scout":
                # topic-based search -> dorking engine finds multiple companies
                celery_app.send_task("tasks.ai_query_generator.generate_queries", args=[
                    job_id, pipeline, target, keywords
                ])
            else:  # personal_audit
                # specific entity -> direct crawl
                celery_app.send_task("tasks.crawl.execute_crawl", args=[
                    job_id, pipeline, target, keywords
                ])
            
    # Mark parent wrapper job as complete
    state_manager.set_job_state(parent_job_id, pipeline, "COMPLETED", source_type, {"step": "prompt_parsed", "targets_found": len(jobs), "pipeline_chosen": pipeline})


@app.post("/api/start_job/prompt")
async def start_job_prompt(background_tasks: BackgroundTasks, prompt: str = Form(...)):
    parent_job_id = f"prompt_{uuid.uuid4().hex[:8]}"
    # Register immediately so it shows on dashboard
    state_manager.set_job_state(parent_job_id, "manual_run", "QUEUED", prompt[:50] + "...", {"step": "queued"})
    
    # Let FastAPI manage the thread lifecycle to prevent zombie processes during shutdown
    background_tasks.add_task(process_input_background, parent_job_id, prompt, "User Prompt")
    
    return {"status": "started", "job_id": parent_job_id}

@app.post("/api/start_job/csv")
async def start_job_csv(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    contents = await file.read()
    text = contents.decode('utf-8', errors='ignore')
    
    parent_job_id = f"csv_{uuid.uuid4().hex[:8]}"
    state_manager.set_job_state(parent_job_id, "manual_run", "QUEUED", file.filename, {"step": "queued"})
    
    # Let FastAPI manage the thread lifecycle to prevent zombie processes during shutdown
    background_tasks.add_task(process_input_background, parent_job_id, text, f"CSV: {file.filename}")
    
    return {"status": "started", "job_id": parent_job_id}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    
    # Send initial state
    all_jobs = state_manager.get_all_jobs()
    await websocket.send_json({"type": "init", "data": all_jobs})
    
    # Subscribe to redis for live updates
    redis_client = redis.Redis.from_url(Config.STATE_REDIS_URL, decode_responses=True)
    pubsub = redis_client.pubsub()
    pubsub.subscribe('job_updates')
    
    try:
        while True:
            message = pubsub.get_message(ignore_subscribe_messages=True)
            if message and message['type'] == 'message':
                data = message['data']
                parsed_data = json.loads(data)
                job_id = parsed_data.pop("job_id")
                await websocket.send_json({"type": "update", "job_id": job_id, "data": parsed_data})
            await asyncio.sleep(0.1)
    except Exception as e:
        print("WebSocket disconnected:", e)
    finally:
        pubsub.close()

@app.post("/api/cancel_job/{job_id}")
async def cancel_job(job_id: str):
    job = state_manager.get_job(job_id)
    pipeline = job.get("pipeline", "unknown") if job else "unknown"
    target = job.get("target", "unknown") if job else "unknown"
    
    # Setting state to CANCELLED will prevent any further Celery task from executing for this job
    state_manager.set_job_state(job_id, pipeline, "CANCELLED", target, {"step": "cancelled_by_user"})
    return {"status": "success"}
