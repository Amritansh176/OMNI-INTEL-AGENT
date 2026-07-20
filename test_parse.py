import json
import ollama

def extract_targets_from_text(text: str):
    prompt = f"""
    You are an AI assistant helping to trigger web scraping jobs. 
    Analyze the following input text (which could be a CSV or a natural language prompt) and extract the target URLs (or company names/entities) and the relevant keywords to search for.
    Return ONLY a valid JSON object matching this schema:
    {{
        "jobs": [
            {{"target": "url_or_entity", "keywords": ["keyword1", "keyword2"]}}
        ]
    }}
    Input text:
    {text[:4000]}
    """
    try:
        print("Sending prompt to Ollama...")
        response = ollama.chat(model="llama3", messages=[{'role': 'user', 'content': prompt}])
        output = response['message']['content']
        print("Raw output:", output)
        start = output.find('{')
        end = output.rfind('}') + 1
        if start != -1 and end != -1:
            parsed = json.loads(output[start:end])
            print("Parsed JSON:", parsed)
            return parsed.get("jobs", [])
        else:
            print("No JSON found")
    except Exception as e:
        print("Error parsing with AI:", e)
    return []

print(extract_targets_from_text("Find the CEO and founders of openai.com"))
