import requests
from bs4 import BeautifulSoup

instances = [
    "https://searx.be/search",
    "https://paulgo.io/search",
    "https://search.mdosch.de/search",
    "https://priv.au/search"
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9"
}

for inst in instances:
    try:
        resp = requests.get(inst, params={'q': 'counter drone companies india'}, headers=headers, timeout=10)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            results = soup.find_all('div', class_='result')
            print(f"Success with {inst}: {len(results)} results")
            for r in results[:1]:
                h3 = r.find('h3')
                if h3:
                    print(f" - {h3.text.strip()}")
        else:
            print(f"Failed {inst}: {resp.status_code}")
    except Exception as e:
        print(f"Exception {inst}: {e}")
