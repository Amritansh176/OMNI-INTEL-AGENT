from duckduckgo_search import DDGS

results = []
for res in DDGS().text("counter drone companies in India", max_results=5):
    results.append(f"{res['title']} - {res['body']}")
    
print("FOUND:", len(results))
print(results)
