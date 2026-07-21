import requests
from bs4 import BeautifulSoup
import urllib.parse
from googlesearch import search
from fake_useragent import UserAgent
import concurrent.futures

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
    def search_yahoo_sync(query):
        url = "https://search.yahoo.com/search"
        try:
            resp = requests.get(url, params={"p": query}, headers=DorkingEngine.get_random_headers(), timeout=10)
            if resp.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, 'html.parser')
                results = []
                for r in soup.find_all('div', class_='algo-sr'):
                    text = r.get_text(separator=' ', strip=True)
                    if text:
                        results.append(text)
                        if len(results) >= 10:
                            break
                return results
        except Exception as e:
            print(f"Yahoo Sync Error: {e}")
        return []

    @staticmethod
    def search_google_sync(query):
        results = []
        try:
            for result in search(query, num_results=5, advanced=True):
                results.append(f"{result.title} - {result.description}")
        except Exception as e:
            print(f"Google Sync Error: {e}")
        return results

    @staticmethod
    def fallback_search(target, keywords):
        # Add random Jitter to prevent firing concurrent requests at the exact same millisecond
        import time
        import random
        time.sleep(random.uniform(2, 5))
        
        query = f"{target} {' OR '.join(keywords)}" if keywords else target
        
        # Run Google and Yahoo concurrently. If Google blocks us, it returns [] and we still get Yahoo's results!
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            google_future = executor.submit(DorkingEngine.search_google_sync, query)
            yahoo_future = executor.submit(DorkingEngine.search_yahoo_sync, query)
            
            google_results = google_future.result()
            yahoo_results = yahoo_future.result()
            
        all_results = list(set(google_results + yahoo_results))
        
        if not all_results:
            return None
            
        return {
            "source": "multi_source_dorking",
            "target": target,
            "results": all_results
        }
