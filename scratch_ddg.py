from duckduckgo_search import DDGS

query = "counterdrones.com drones OR counter"
try:
    results = []
    with DDGS() as ddgs:
        for r in ddgs.text(query, max_results=5):
            results.append(r)
    print("Results:", results)
except Exception as e:
    print("Error:", e)
