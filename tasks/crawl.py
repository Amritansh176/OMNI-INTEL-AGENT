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

def crawl_homepage_clean(url, keywords):
    """Tier 2: Crawl using Trafilatura for clean boilerplate-free text and BeautifulSoup for link discovery."""
    import trafilatura
    
    try:
        resp = requests.get(url, headers=DorkingEngine.get_random_headers(), timeout=10)
        if resp.status_code != 200:
            return None
        
        # Use Trafilatura to extract clean text (strips menus, footers, etc)
        text_content = trafilatura.extract(resp.text, include_links=True)
        
        # Fallback to BeautifulSoup if Trafilatura fails to extract
        soup = BeautifulSoup(resp.text, 'html.parser')
        if not text_content:
            text_content = soup.get_text(separator=' ', strip=True)
        
        if not text_content or len(text_content.strip()) < 100:
            return None
        
        # Deduplication based on SHA-256
        content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()
        is_duplicate = state_manager.check_and_add_hash(content_hash)
        
        if is_duplicate:
            return "DUPLICATE"
            
        links = [a['href'] for a in soup.find_all('a', href=True) if any(kw.lower() in a['href'].lower() for kw in keywords)]
        
        return {
            "source": "trafilatura_clean",
            "url": url,
            "text": text_content[:8000],
            "interesting_links": links
        }
    except Exception as e:
        print(f"Crawl wrapper error: {e}")
        return None


def deep_crawl_top_urls(urls, keywords, max_urls=3):
    """
    Actually crawl the top URLs from dorking results to get REAL page content.
    This is the key accuracy improvement — instead of sending search snippets to AI,
    we send actual webpage content.
    """
    import trafilatura
    
    crawled_pages = []
    all_links = []
    
    for url in urls[:max_urls]:
        try:
            # Skip non-HTTP URLs
            if not url.startswith("http"):
                continue
            
            # Skip PDFs and other binary files
            if any(url.lower().endswith(ext) for ext in ['.pdf', '.zip', '.xlsx', '.docx', '.pptx']):
                continue
                
            resp = requests.get(url, headers=DorkingEngine.get_random_headers(), timeout=10, allow_redirects=True)
            if resp.status_code != 200:
                continue
            
            # Extract clean text via trafilatura
            text = trafilatura.extract(resp.text, include_links=True)
            if not text:
                soup = BeautifulSoup(resp.text, 'html.parser')
                text = soup.get_text(separator=' ', strip=True)
            
            if text and len(text.strip()) > 200:
                # Dedup check
                content_hash = hashlib.sha256(text.encode('utf-8')).hexdigest()
                if not state_manager.check_and_add_hash(content_hash):
                    crawled_pages.append({"url": url, "text": text[:5000]})
                    
                    # Also grab interesting links from the page
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    page_links = [a['href'] for a in soup.find_all('a', href=True) 
                                  if any(kw.lower() in a.get('href', '').lower() for kw in 
                                         (keywords + ['about', 'team', 'contact', 'leadership', 'director', 
                                          'management', 'founders', 'people', 'board', 'executives', 
                                          'staff', 'partners', 'who-we-are', 'our-team']))]
                    all_links.extend(page_links[:5])
                    
        except Exception as e:
            print(f"Deep crawl error for {url}: {e}")
            continue
    
    if not crawled_pages:
        return None
    
    # Merge all crawled page texts
    merged_text = "\n\n--- PAGE BREAK ---\n\n".join(
        [f"[Source: {p['url']}]\n{p['text']}" for p in crawled_pages]
    )
    
    return {
        "source": "deep_crawl",
        "url": crawled_pages[0]["url"],
        "text": merged_text[:12000],
        "interesting_links": list(set(all_links)),
        "pages_crawled": len(crawled_pages)
    }


@app.task(bind=True, name="tasks.crawl.execute_crawl", time_limit=180, soft_time_limit=160)
def execute_crawl(self, job_id, pipeline, target, keywords=None, missing_fields=None, 
                  depth=0, original_target=None, query_strategy=None, parent_job_id=None):
    """
    Executes the tiered crawl logic for a target.
    Supports infinite loop data extraction by taking `missing_fields` and `depth`.
    """
    if keywords is None:
        keywords = missing_fields if missing_fields else []
        
    actual_target = original_target if original_target else target
    if not state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target, {"step": "crawling", "depth": depth, "url": target}):
        return f"Job {job_id} cancelled."
    
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
            
        raw_data = crawl_homepage_clean(target, search_kws)
        
        if raw_data == "DUPLICATE":
            # If duplicate on a deep crawl, we might want to fallback to dorking instead of skipping
            if depth > 0:
                raw_data = None
            else:
                state_manager.set_job_state(job_id, pipeline, "COMPLETED", actual_target, {"step": "skipped_duplicate_content"})
                return f"Job {job_id} skipped: duplicate HTML content."
    
    if not raw_data:
        # Tier 3 - Dorking → then Deep Crawl the found URLs
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target, {"step": "crawling", "tier": 3, "depth": depth})
        
        # If we have missing fields, tailor the dorking query
        dork_keywords = keywords
        if missing_fields:
            dork_keywords = missing_fields
            
        dork_result = DorkingEngine.fallback_search(target, dork_keywords)
        if dork_result:
            found_urls = dork_result.get("interesting_links", [])
            
            if found_urls:
                # DEEP CRAWL: Actually visit the top URLs to get real page content
                state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target, 
                    {"step": "deep_crawling_urls", "tier": "3b", "urls_found": len(found_urls)})
                
                crawl_keywords = keywords if keywords else [target]
                deep_result = deep_crawl_top_urls(found_urls, crawl_keywords, max_urls=3)
                
                if deep_result:
                    raw_data = deep_result
                else:
                    # Fallback: use dorking snippets if deep crawl failed
                    raw_data = dork_result
                    if "url" not in raw_data and found_urls:
                        raw_data["url"] = found_urls[0]
            else:
                # No URLs found, use dorking text snippets as fallback
                raw_data = dork_result
        
    if raw_data:
        state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target, {"step": "crawl_completed", "depth": depth})
        # Send to Semantic Filter
        app.send_task("tasks.semantic_filter.filter_and_chunk", args=[
            job_id, pipeline, target, raw_data
        ], kwargs={
            "keywords": search_kws if 'search_kws' in locals() else keywords,
            "depth": depth,
            "original_target": actual_target,
            "query_strategy": query_strategy,
            "parent_job_id": parent_job_id
        })
        return f"Job {job_id} crawled successfully, sent to semantic filter."
    else:
        state_manager.set_job_state(job_id, pipeline, "FAILED", actual_target, {"reason": "All tiers failed to extract data."})
        return f"Job {job_id} failed at all tiers."
