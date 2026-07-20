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
        
        # Check ETag of homepage with stealth headers
        head_resp = requests.head(base_url, headers=DorkingEngine.get_random_headers(), timeout=5)
        etag = head_resp.headers.get("ETag") or head_resp.headers.get("Last-Modified")
        
        if etag:
            if state_manager.check_and_add_hash(f"etag_{base_url}_{etag}"):
                return True, "ETag unchanged, already crawled recently", True
                
        resp = requests.get(f"{base_url}/robots.txt", headers=DorkingEngine.get_random_headers(), timeout=5)
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
def execute_crawl(self, job_id, pipeline, target, keywords=None, missing_fields=None, depth=0, original_target=None):
    """
    Executes the tiered crawl logic for a target.
    Supports infinite loop data extraction by taking `missing_fields` and `depth`.
    """
    if keywords is None:
        keywords = missing_fields if missing_fields else []
        
    actual_target = original_target if original_target else target
    state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target, {"step": "crawling", "depth": depth, "url": target})
    
    is_url = target.startswith("http://") or target.startswith("https://")
    raw_data = None
    
    if is_url:
        # Tier 1 - Skip robots/sitemap on deep crawls (depth > 0)
        if depth == 0:
            state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target, {"step": "crawling", "tier": 1})
            has_sitemap, msg, is_unchanged = check_robots_sitemap(target)
            
            if is_unchanged:
                state_manager.set_job_state(job_id, pipeline, "COMPLETED", actual_target, {"step": "skipped_unchanged_etag"})
                return f"Job {job_id} skipped: unchanged ETag."
            
        # Tier 2
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target, {"step": "crawling", "tier": 2, "depth": depth})
        
        # Merge missing fields into keywords for better link discovery
        search_kws = keywords[:]
        if missing_fields:
            search_kws.extend(missing_fields)
            
        raw_data = crawl_homepage_bfs(target, search_kws)
        
        if raw_data == "DUPLICATE":
            # If duplicate on a deep crawl, we might want to fallback to dorking instead of skipping
            if depth > 0:
                raw_data = None
            else:
                state_manager.set_job_state(job_id, pipeline, "COMPLETED", actual_target, {"step": "skipped_duplicate_content"})
                return f"Job {job_id} skipped: duplicate HTML content."
    
    if not raw_data:
        # Tier 3 - Dorking
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target, {"step": "crawling", "tier": 3, "depth": depth})
        
        # If we have missing fields, tailor the dorking query
        dork_keywords = keywords
        if missing_fields:
            dork_keywords = missing_fields
            
        raw_data = DorkingEngine.fallback_search(target, dork_keywords)
        
    if raw_data:
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target, {"step": "crawl_completed", "depth": depth})
        # Send to AI Inference queue, passing depth and missing_fields for the loop
        app.send_task("tasks.ai_inference.extract_structured_data", args=[job_id, pipeline, actual_target, raw_data, depth, missing_fields])
        return f"Job {job_id} crawled successfully, sent to AI."
    else:
        state_manager.set_job_state(job_id, pipeline, "FAILED", actual_target, {"reason": "All tiers failed to extract data."})
        return f"Job {job_id} failed at all tiers."
