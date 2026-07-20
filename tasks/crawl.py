from celery_worker import app
from state_manager import state_manager
import requests
from bs4 import BeautifulSoup
from duckduckgo_search import DDGS
from urllib.parse import urlparse

# We'll import ai_inference dynamically to avoid circular imports if any,
# or we can use app.send_task
from celery import chain

def check_robots_sitemap(url):
    """Tier 1: Check robots.txt and sitemap"""
    try:
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        # A real implementation would parse sitemap for last-modified dates.
        # For simplicity in this demo, we'll just check if we can fetch it.
        resp = requests.get(f"{base_url}/robots.txt", timeout=5)
        if resp.status_code == 200:
            return True, "Found robots.txt"
        
        resp = requests.get(f"{base_url}/sitemap.xml", timeout=5)
        if resp.status_code == 200:
            return True, "Found sitemap.xml"
            
        return False, "No robots.txt or sitemap.xml"
    except Exception as e:
        return False, str(e)

def crawl_homepage_bfs(url, keywords):
    """Tier 2: BFS root-level crawling for links."""
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Simple extraction of text content and matching links
        text_content = soup.get_text(separator=' ', strip=True)
        links = [a['href'] for a in soup.find_all('a', href=True) if any(kw.lower() in a['href'].lower() for kw in keywords)]
        
        return {
            "source": "bfs",
            "url": url,
            "text": text_content[:5000], # Limit text size
            "interesting_links": links
        }
    except Exception as e:
        return None

def google_dorking_fallback(target, keywords):
    """Tier 3: Free search dorking fallback (using DuckDuckGo HTML)"""
    query = f"{target} {' OR '.join(keywords)}"
    url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(resp.text, 'html.parser')
        results = []
        
        for a in soup.find_all('a', class_='result__snippet'):
            results.append(a.text)
            if len(results) >= 5:
                break
                
        if not results:
            return None
            
        return {
            "source": "dorking",
            "target": target,
            "results": results
        }
    except Exception as e:
        return None

@app.task(bind=True, name="tasks.crawl.execute_crawl")
def execute_crawl(self, job_id, pipeline, target, keywords):
    """
    Executes the tiered crawl logic for a target.
    """
    state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "crawling"})
    
    # Target can be a URL or a keyword/entity name.
    is_url = target.startswith("http://") or target.startswith("https://")
    
    raw_data = None
    
    if is_url:
        # Tier 1
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "crawling", "tier": 1})
        has_sitemap, msg = check_robots_sitemap(target)
        
        # Tier 2
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "crawling", "tier": 2})
        raw_data = crawl_homepage_bfs(target, keywords)
    
    if not raw_data:
        # Tier 3
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "crawling", "tier": 3})
        raw_data = google_dorking_fallback(target, keywords)
        
    if raw_data:
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", target, {"step": "crawl_completed"})
        # Send to AI Inference queue
        app.send_task("tasks.ai_inference.extract_structured_data", args=[job_id, pipeline, target, raw_data])
        return f"Job {job_id} crawled successfully, sent to AI."
    else:
        state_manager.set_job_state(job_id, pipeline, "FAILED", target, {"reason": "All tiers failed to extract data."})
        return f"Job {job_id} failed at all tiers."
