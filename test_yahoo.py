import requests
from bs4 import BeautifulSoup

url = "https://search.yahoo.com/search"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}
resp = requests.get(url, params={"p": "counter drone companies in india"}, headers=headers)
if resp.status_code == 200:
    soup = BeautifulSoup(resp.text, 'html.parser')
    results = soup.find_all('div', class_='compTitle')
    print(f"Found {len(results)} on Yahoo")
    for r in results[:3]:
        h3 = r.find('h3')
        a = r.find('a')
        if h3 and a:
            print(f"- {h3.text.strip()} ({a['href']})")
else:
    print(f"Failed with {resp.status_code}")
