import requests
from bs4 import BeautifulSoup

query = "counterdrones.com drones OR counter"
url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

try:
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')
    results = []
    for a in soup.find_all('a', class_='result__snippet'):
        results.append(a.text)
    print("Results:", results)
except Exception as e:
    print("Error:", e)
