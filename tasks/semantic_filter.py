"""
Phase 2: Semantic Pre-Filtering
Strips boilerplate noise from crawled HTML and scores text chunks by relevance
before sending them to the expensive AI Extractor.
"""
from celery_worker import app
from state_manager import state_manager
from config import Config
from bs4 import BeautifulSoup, Comment
import json
import re


# Tags that almost never contain useful intelligence data
BOILERPLATE_TAGS = ['nav', 'footer', 'header', 'aside', 'script', 'style', 'noscript', 'iframe']
BOILERPLATE_CLASSES = ['cookie', 'consent', 'banner', 'sidebar', 'menu', 'navigation', 
                       'social', 'share', 'widget', 'ad', 'advertisement', 'popup']


def strip_boilerplate(html_content):
    """Remove navigation, footers, cookie banners, and other noise from HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove boilerplate tags entirely
    for tag_name in BOILERPLATE_TAGS:
        for tag in soup.find_all(tag_name):
            tag.decompose()
    
    # Remove elements with boilerplate class names
    for element in soup.find_all(True):
        classes = element.get('class', [])
        element_id = element.get('id', '')
        class_str = ' '.join(classes).lower() if classes else ''
        id_str = element_id.lower() if element_id else ''
        
        if any(bp in class_str or bp in id_str for bp in BOILERPLATE_CLASSES):
            element.decompose()
    
    # Remove HTML comments
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    
    return soup


def chunk_by_sections(soup):
    """Split cleaned HTML into semantic chunks based on structural tags."""
    chunks = []
    
    # Try to find semantic sections first
    section_tags = ['article', 'section', 'main', 'div']
    
    for tag_name in section_tags:
        sections = soup.find_all(tag_name, recursive=False) or soup.find_all(tag_name)
        for section in sections:
            text = section.get_text(separator=' ', strip=True)
            # Only keep chunks with meaningful content (at least 50 chars)
            if len(text) > 50:
                chunks.append({
                    "tag": tag_name,
                    "text": text[:2000],  # Cap each chunk
                    "length": len(text)
                })
    
    # If no sections found, fall back to paragraph-level chunking
    if not chunks:
        full_text = soup.get_text(separator=' ', strip=True)
        # Split into ~1000 char chunks at sentence boundaries
        sentences = re.split(r'(?<=[.!?])\s+', full_text)
        current_chunk = ""
        for sentence in sentences:
            if len(current_chunk) + len(sentence) > 1000:
                if current_chunk:
                    chunks.append({"tag": "paragraph", "text": current_chunk.strip(), "length": len(current_chunk)})
                current_chunk = sentence
            else:
                current_chunk += " " + sentence
        if current_chunk and len(current_chunk) > 50:
            chunks.append({"tag": "paragraph", "text": current_chunk.strip(), "length": len(current_chunk)})
    
    return chunks


def score_chunk_relevance(chunk_text, keywords):
    """
    Fast keyword-density scoring to rank chunks by relevance.
    Returns a 0-1 score based on how many keywords appear in the chunk.
    """
    if not keywords:
        return 0.5  # No keywords = neutral score
    
    text_lower = chunk_text.lower()
    hits = 0
    total_density = 0
    
    for kw in keywords:
        kw_lower = kw.lower()
        count = text_lower.count(kw_lower)
        if count > 0:
            hits += 1
            total_density += count
    
    # Score = (fraction of keywords found) * (density bonus)
    keyword_coverage = hits / len(keywords)
    density_bonus = min(total_density / 10, 1.0)  # Cap at 1.0
    
    # Also boost chunks containing high-value patterns
    high_value_patterns = ['email', 'phone', 'tel:', 'mailto:', 'ceo', 'founder', 
                           'director', 'manager', 'contact', '@', '.com', 'linkedin']
    pattern_hits = sum(1 for p in high_value_patterns if p in text_lower)
    pattern_bonus = min(pattern_hits / 5, 0.3)
    
    return min(keyword_coverage * 0.5 + density_bonus * 0.3 + pattern_bonus, 1.0)


@app.task(bind=True, name="tasks.semantic_filter.filter_and_chunk", time_limit=120, soft_time_limit=100)
def filter_and_chunk(self, job_id, pipeline, target, raw_data, keywords=None, 
                     depth=0, original_target=None, query_strategy=None, parent_job_id=None):
    """
    Receives raw crawl data, strips boilerplate, chunks intelligently,
    scores by relevance, and sends only the top-K chunks to the AI Extractor.
    """
    actual_target = original_target or target
    if not state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target, 
                                {"step": "semantic_filtering", "depth": depth, "strategy": query_strategy}):
        return f"Job {job_id} cancelled."

    # Handle different raw_data formats
    if isinstance(raw_data, dict):
        if "results" in raw_data and isinstance(raw_data["results"], list):
            html_or_text = "\n\n".join(raw_data["results"])
        else:
            html_or_text = raw_data.get("html", raw_data.get("text", ""))
        interesting_links = raw_data.get("interesting_links", [])
        source = raw_data.get("source", "unknown")
    elif isinstance(raw_data, str):
        html_or_text = raw_data
        interesting_links = []
        source = "raw_text"
    else:
        html_or_text = json.dumps(raw_data)
        interesting_links = []
        source = "serialized"

    # If it looks like HTML, do full filtering; otherwise just chunk text
    if '<' in html_or_text and '>' in html_or_text:
        clean_soup = strip_boilerplate(html_or_text)
        chunks = chunk_by_sections(clean_soup)
    else:
        # Already plain text (e.g., from dorking results)
        chunks = [{"tag": "text", "text": html_or_text[:2000], "length": len(html_or_text)}]

    # Score and sort chunks by relevance
    for chunk in chunks:
        chunk["relevance_score"] = score_chunk_relevance(chunk["text"], keywords or [])
    
    chunks.sort(key=lambda c: c["relevance_score"], reverse=True)
    
    # Keep only top-K chunks
    top_chunks = chunks[:Config.SEMANTIC_FILTER_TOP_K]
    
    # Merge top chunks into a single clean text payload
    filtered_text = "\n\n---\n\n".join([c["text"] for c in top_chunks])
    
    # Calculate filtering stats
    original_length = len(html_or_text)
    filtered_length = len(filtered_text)
    reduction_pct = round((1 - filtered_length / max(original_length, 1)) * 100, 1)

    state_manager.set_job_state(job_id, pipeline, "IN_PROGRESS", actual_target,
                                {"step": "filtering_complete", "chunks_kept": len(top_chunks),
                                 "total_chunks": len(chunks), "noise_reduction": f"{reduction_pct}%"})

    # Build clean data payload for the AI Extractor
    clean_data = {
        "source": source,
        "url": raw_data.get("url", target) if isinstance(raw_data, dict) else target,
        "text": filtered_text,
        "interesting_links": interesting_links,
        "filter_stats": {
            "original_chars": original_length,
            "filtered_chars": filtered_length,
            "chunks_scored": len(chunks),
            "chunks_kept": len(top_chunks),
            "reduction": f"{reduction_pct}%"
        }
    }

    # Send to Agentic AI Extractor
    app.send_task("tasks.ai_inference.extract_structured_data", args=[
        job_id, pipeline, actual_target, clean_data
    ], kwargs={
        "depth": depth,
        "query_strategy": query_strategy,
        "parent_job_id": parent_job_id
    })

    return f"Job {job_id}: Filtered {len(chunks)} chunks down to {len(top_chunks)} ({reduction_pct}% noise removed)."
