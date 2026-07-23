import requests
from bs4 import BeautifulSoup
import urllib.parse
from googlesearch import search
from fake_useragent import UserAgent
import concurrent.futures
import re

class DorkingEngine:
    # Initialize fake-useragent for stealth
    ua = UserAgent(os=['mac', 'windows'], browsers=['chrome', 'edge'])
    
    @staticmethod
    def get_random_headers():
        return {
            "User-Agent": DorkingEngine.ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-Ch-Ua": "\"Not A(Brand\";v=\"99\", \"Google Chrome\";v=\"121\", \"Chromium\";v=\"121\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"macOS\"",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1"
        }

    @staticmethod
    def clean_redirect_url(url):
        """Extract the real destination URL from Yahoo/Google redirect wrappers."""
        # Yahoo redirect: extract /RU=<encoded_url>/
        yahoo_match = re.search(r'/RU=(https?[^/]+)/', url)
        if yahoo_match:
            return urllib.parse.unquote(yahoo_match.group(1))
        
        # Google redirect: extract url= parameter
        if 'google.com/url' in url:
            parsed = urllib.parse.urlparse(url)
            params = urllib.parse.parse_qs(parsed.query)
            if 'url' in params:
                return params['url'][0]
            if 'q' in params:
                return params['q'][0]
        
        return url

    @staticmethod
    def search_yahoo_sync(query):
        url = "https://search.yahoo.com/search"
        try:
            resp = requests.get(url, params={"p": query}, headers=DorkingEngine.get_random_headers(), timeout=10)
            if resp.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, 'html.parser')
                results = []
                urls = []
                for r in soup.find_all('div', class_='algo-sr'):
                    text = r.get_text(separator=' ', strip=True)
                    a_tag = r.find('a', href=True)
                    if text:
                        results.append(text)
                        if a_tag:
                            clean_url = DorkingEngine.clean_redirect_url(a_tag['href'])
                            urls.append(clean_url)
                        if len(results) >= 10:
                            break
                return results, urls
        except Exception as e:
            print(f"Yahoo Sync Error: {e}")
        return [], []

    @staticmethod
    def search_google_sync(query):
        results = []
        urls = []
        try:
            for result in search(query, num_results=5, advanced=True):
                results.append(f"{result.title} - {result.description}")
                urls.append(result.url)
        except Exception as e:
            print(f"Google Sync Error: {e}")
        return results, urls

    @staticmethod
    def search_searxng(query):
        import random
        instances = [
            "https://searx.be/search",
            "https://paulgo.io/search",
            "https://search.mdosch.de/search",
            "https://priv.au/search"
        ]
        random.shuffle(instances)
        results = []
        urls = []
        for inst in instances:
            try:
                resp = requests.get(inst, params={'q': query, 'format': 'json'}, headers=DorkingEngine.get_random_headers(), timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    for r in data.get('results', []):
                        title = r.get('title', '')
                        content = r.get('content', '')
                        url = r.get('url', '')
                        if title and url:
                            results.append(f"{title} - {content}")
                            urls.append(url)
                    if results:
                        break # Found results, no need to try next instance
            except Exception as e:
                print(f"SearXNG Sync Error ({inst}): {e}")
        return results, urls

    @staticmethod
    def fallback_search(target, keywords):
        # Add random Jitter to prevent firing concurrent requests at the exact same millisecond
        import time
        import random
        time.sleep(random.uniform(2, 5))
        
        query = f"{target} {' OR '.join(keywords)}" if keywords else target
        
        # Run Google and Yahoo concurrently. If Google blocks us, it returns [] and we still get Yahoo's results!
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            google_future = executor.submit(DorkingEngine.search_google_sync, query)
            yahoo_future = executor.submit(DorkingEngine.search_yahoo_sync, query)
            searx_future = executor.submit(DorkingEngine.search_searxng, query)
            
            google_results, google_urls = google_future.result()
            yahoo_results, yahoo_urls = yahoo_future.result()
            searx_results, searx_urls = searx_future.result()
            
        all_results = list(set(google_results + yahoo_results + searx_results))
        all_urls = list(set(google_urls + yahoo_urls + searx_urls))
        
        # Clean all redirect URLs
        all_urls = [DorkingEngine.clean_redirect_url(u) for u in all_urls]
        # Remove duplicate URLs after cleaning
        all_urls = list(set(all_urls))
        
        if not all_results:
            return None
            
        return {
            "source": "multi_source_dorking",
            "target": target,
            "text": "\n".join(all_results),
            "results": all_results,
            "interesting_links": all_urls
        }
