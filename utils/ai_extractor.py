
import requests
import json

def extract_event_data_with_ollama(text, image_urls, model="mistral"):
    prompt = f"""
Extract a list of structured event entries in JSON format with the following fields:
- name
- venue_name
- venue_address
- start_datetime (ISO 8601)
- end_datetime (ISO 8601)
- short_description
- price
- host
- source_websites (list of URLs)
- hero_images (list of image URLs)

Text:
{text}

Image URLs:
{image_urls}

Return a list of events in JSON array format.
"""
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
            timeout=60
        )
        output = response.json().get("response", "").strip()
        try:
            return json.loads(output)
        except json.JSONDecodeError:
            print("⚠️ Could not parse JSON. Raw response:")
            print(output)
            return []
    except Exception as e:
        print(f"Error calling Ollama LLM: {e}")
        return []
