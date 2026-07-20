import requests
from bs4 import BeautifulSoup
import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"
]

class DorkingEngine:
    @staticmethod
    def get_random_headers():
        return {"User-Agent": random.choice(USER_AGENTS)}

    @staticmethod
    def search_duckduckgo(query):
        url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"
        try:
            resp = requests.get(url, headers=DorkingEngine.get_random_headers(), timeout=10)
            soup = BeautifulSoup(resp.text, 'html.parser')
            results = []
            for a in soup.find_all('a', class_='result__snippet'):
                results.append(a.text)
                if len(results) >= 5:
                    break
            return results
        except Exception as e:
            return []

    @staticmethod
    def fallback_search(target, keywords):
        query = f"{target} {' OR '.join(keywords)}"
        
        # Primary provider: DDG
        results = DorkingEngine.search_duckduckgo(query)
        
        # Here we could implement other providers like SerpAPI as secondary fallback
        if not results:
            return None
            
        return {
            "source": "dorking",
            "target": target,
            "results": results
        }
