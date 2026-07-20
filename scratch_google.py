from googlesearch import search
query = "counterdrones.com drones OR counter"
try:
    results = list(search(query, num_results=5, advanced=False))
    print("Results:", results)
except Exception as e:
    print("Error:", e)
