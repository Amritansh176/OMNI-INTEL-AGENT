import asyncio
import aiohttp
from bs4 import BeautifulSoup
import urllib.parse
from googlesearch import search
from fake_useragent import UserAgent

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
    async def search_duckduckgo_async(query, session):
        url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
        try:
            async with session.get(url, headers=DorkingEngine.get_random_headers(), timeout=10) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    results = []
                    for a in soup.find_all('a', class_='result__snippet'):
                        results.append(a.text)
                        if len(results) >= 5:
                            break
                    return results
        except Exception as e:
            print(f"DDG Async Error: {e}")
        return []

    @staticmethod
    def search_google_sync(query):
        results = []
        try:
            # googlesearch-python advanced=True returns objects with title, description, url
            for result in search(query, num_results=5, advanced=True):
                results.append(f"{result.title} - {result.description}")
        except Exception as e:
            print(f"Google Sync Error: {e}")
        return results

    @staticmethod
    async def fallback_search_async(target, keywords):
        query = f"{target} {' OR '.join(keywords)}"
        
        async with aiohttp.ClientSession() as session:
            # Run both searches concurrently (Multi-Source Aggregation)
            loop = asyncio.get_event_loop()
            google_task = loop.run_in_executor(None, DorkingEngine.search_google_sync, query)
            ddg_task = DorkingEngine.search_duckduckgo_async(query, session)
            
            google_results, ddg_results = await asyncio.gather(google_task, ddg_task)
            
        # Combine and deduplicate
        all_results = list(set(google_results + ddg_results))
        
        if not all_results:
            return None
            
        return {
            "source": "multi_source_dorking",
            "target": target,
            "results": all_results
        }
        
    @staticmethod
    def fallback_search(target, keywords):
        """Synchronous wrapper for Celery tasks to call easily"""
        return asyncio.run(DorkingEngine.fallback_search_async(target, keywords))
