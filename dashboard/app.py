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
                var rowHtml = `
                    <td>${job_id}</td>
                    <td>${data.pipeline}</td>
                    <td>${data.target}</td>
                    <td class="status-${data.state}">${data.state}</td>
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
    prompt = f"""
    You are an AI assistant helping to trigger web scraping jobs. 
    Analyze the following input text (which could be a CSV or a natural language prompt) and extract the target URLs (or company names/entities) and the relevant keywords to search for.
    Return ONLY a valid JSON object matching this schema:
    {{
        "jobs": [
            {{"target": "url_or_entity", "keywords": ["keyword1", "keyword2"]}}
        ]
    }}
    Input text:
    {text[:4000]}
    """
    try:
        response = ollama.chat(model=Config.OLLAMA_MODEL, messages=[{'role': 'user', 'content': prompt}])
        output = response['message']['content']
        start = output.find('{')
        end = output.rfind('}') + 1
        if start != -1 and end != -1:
            return json.loads(output[start:end]).get("jobs", [])
    except Exception as e:
        print("Error parsing with AI:", e)
    return []

def enqueue_jobs(jobs):
    for job in jobs:
        job_id = f"manual_{uuid.uuid4().hex[:8]}"
        target = job.get("target")
        keywords = job.get("keywords", [])
        if target:
            # Immediately register the job as QUEUED so the dashboard shows it
            state_manager.set_job_state(job_id, "manual_run", "QUEUED", target, {"step": "queued", "keywords": keywords})
            celery_app.send_task("tasks.crawl.execute_crawl", args=[
                job_id, 
                "manual_run", 
                target, 
                keywords
            ])

@app.post("/api/start_job/prompt")
async def start_job_prompt(prompt: str = Form(...)):
    # Run sync Ollama call in a thread so it doesn't block the event loop
    loop = asyncio.get_event_loop()
    jobs = await loop.run_in_executor(None, extract_targets_from_text, prompt)
    enqueue_jobs(jobs)
    return {"status": "started", "jobs": len(jobs)}

@app.post("/api/start_job/csv")
async def start_job_csv(file: UploadFile = File(...)):
    contents = await file.read()
    text = contents.decode('utf-8', errors='ignore')
    # Run sync Ollama call in a thread so it doesn't block the event loop
    loop = asyncio.get_event_loop()
    jobs = await loop.run_in_executor(None, extract_targets_from_text, text)
    enqueue_jobs(jobs)
    return {"status": "started", "jobs": len(jobs)}

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
