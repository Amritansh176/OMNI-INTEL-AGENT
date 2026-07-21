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
    def search_duckduckgo_sync(query):
        url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
        try:
            resp = requests.get(url, headers=DorkingEngine.get_random_headers(), timeout=10)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                results = []
                for a in soup.find_all('a', class_='result__snippet'):
                    results.append(a.text)
                    if len(results) >= 5:
                        break
                return results
        except Exception as e:
            print(f"DDG Sync Error: {e}")
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
        query = f"{target} {' OR '.join(keywords)}" if keywords else target
        
        # Run both searches concurrently using threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            google_future = executor.submit(DorkingEngine.search_google_sync, query)
            ddg_future = executor.submit(DorkingEngine.search_duckduckgo_sync, query)
            
            google_results = google_future.result()
            ddg_results = ddg_future.result()
            
        # Combine and deduplicate
        all_results = list(set(google_results + ddg_results))
        
        if not all_results:
            return None
            
        return {
            "source": "multi_source_dorking",
            "target": target,
            "results": all_results
        }
