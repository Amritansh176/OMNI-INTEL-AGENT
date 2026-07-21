import requests
import random

instances = [
    "https://searx.be/search",
    "https://paulgo.io/search",
    "https://searx.tiekoetter.com/search",
    "https://search.mdosch.de/search"
]

for inst in instances:
    try:
        resp = requests.get(inst, params={'q': 'counter drone india', 'format': 'json'}, timeout=5)
        if resp.status_code == 200:
            print(f"Success with {inst}: {len(resp.json().get('results', []))} results")
    except Exception as e:
        print(f"Failed {inst}: {e}")
