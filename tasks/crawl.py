from celery_worker import app
from state_manager import state_manager
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import hashlib
from dorking_engine import DorkingEngine

def check_robots_sitemap(url):
    """Tier 1: Check robots.txt and sitemap, support ETag/Hash based dedup"""
    try:
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        # Check ETag of homepage
        head_resp = requests.head(base_url, timeout=5)
        etag = head_resp.headers.get("ETag") or head_resp.headers.get("Last-Modified")
        
        if etag:
            if state_manager.check_and_add_hash(f"etag_{base_url}_{etag}"):
                return True, "ETag unchanged, already crawled recently", True
                
        resp = requests.get(f"{base_url}/robots.txt", timeout=5)
        if resp.status_code == 200:
            return True, "Found robots.txt", False
            
        return False, "No robots.txt", False
    except Exception as e:
        return False, str(e), False

def crawl_homepage_bfs(url, keywords):
    """Tier 2: BFS root-level crawling for links with content dedup."""
    try:
        resp = requests.get(url, headers=DorkingEngine.get_random_headers(), timeout=10)
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        text_content = soup.get_text(separator=' ', strip=True)
        
        # Deduplication based on SHA-256
        content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()
        is_duplicate = state_manager.check_and_add_hash(content_hash)
        
        if is_duplicate:
            return "DUPLICATE"
            
        links = [a['href'] for a in soup.find_all('a', href=True) if any(kw.lower() in a['href'].lower() for kw in keywords)]
        
        return {
            "source": "bfs",
            "url": url,
            "text": text_content[:5000],
            "interesting_links": links
        }
    except Exception as e:
        return None

@app.task(bind=True, name="tasks.crawl.execute_crawl")
def execute_crawl(self, job_id, pipeline, target, keywords):
    """
    Executes the tiered crawl logic for a target.
    """
    state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "crawling"})
    
    is_url = target.startswith("http://") or target.startswith("https://")
    raw_data = None
    
    if is_url:
        # Tier 1
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "crawling", "tier": 1})
        has_sitemap, msg, is_unchanged = check_robots_sitemap(target)
        
        if is_unchanged:
            state_manager.set_job_state(job_id, pipeline, "COMPLETED", target, {"step": "skipped_unchanged_etag"})
            return f"Job {job_id} skipped: unchanged ETag."
            
        # Tier 2
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "crawling", "tier": 2})
        raw_data = crawl_homepage_bfs(target, keywords)
        
        if raw_data == "DUPLICATE":
            state_manager.set_job_state(job_id, pipeline, "COMPLETED", target, {"step": "skipped_duplicate_content"})
            return f"Job {job_id} skipped: duplicate HTML content."
    
    if not raw_data:
        # Tier 3
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "crawling", "tier": 3})
        raw_data = DorkingEngine.fallback_search(target, keywords)
        
    if raw_data:
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "crawl_completed"})
        # Send to AI Inference queue
        app.send_task("tasks.ai_inference.extract_structured_data", args=[job_id, pipeline, target, raw_data])
        return f"Job {job_id} crawled successfully, sent to AI."
    else:
        state_manager.set_job_state(job_id, pipeline, "FAILED", target, {"reason": "All tiers failed to extract data."})
        return f"Job {job_id} failed at all tiers."
