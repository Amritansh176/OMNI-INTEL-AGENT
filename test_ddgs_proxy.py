import requests
from duckduckgo_search import DDGS
import random
import time

print("Fetching free proxies...")
resp = requests.get("https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all")
proxies = [p for p in resp.text.split('\n') if p.strip()]
print(f"Found {len(proxies)} proxies.")

random.shuffle(proxies)

for proxy in proxies[:10]:
    try:
        p_dict = {"http://": f"http://{proxy.strip()}", "https://": f"http://{proxy.strip()}"}
        print(f"Trying {proxy.strip()}...")
        # Need to set timeout so it fails fast
        ddgs = DDGS(proxies=p_dict, timeout=5)
        results = ddgs.text("counter drone companies in india", max_results=2)
        res_list = list(results)
        if len(res_list) > 0:
            print("SUCCESS!")
            print(res_list)
            break
    except Exception as e:
        print(f"Failed: {e}")
