import requests
import json
import re
import pandas as pd
import time
from utils.html_scraper import fetch_page_text_and_images
from utils.text_tools import summarize_text

INPUT_FILE = "event_sources_input.csv"
OUTPUT_JSON = "output/events.json"
OUTPUT_CSV = "output/events.csv"

def load_sources_from_csv(path):
    try:
        df = pd.read_csv(path)
        return df["SourceURL"].dropna().tolist()
    except Exception as e:
        print(f"Error reading input CSV: {e}")
        return []

def extract_json_from_string(raw_text):
    try:
        # Extract everything between the first [ and the last ]
        match = re.search(r'\[\s*{.*?}\s*\]', raw_text, re.DOTALL)
        if match:
            json_str = match.group(0)
            return json.loads(json_str)
        else:
            print("⚠️ No JSON array found in the text.")
            return []
    except Exception as e:
        print(f"❌ Failed to parse JSON: {e}")
        return []


def extract_event_data(text, image_urls):
    text = summarize_text(text, max_sentences=10)
    image_urls = image_urls[:4] if len(image_urls) > 4 else image_urls
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

Return a list of events in JSON array format. If you din not find any relevent data then return blank array.Do not return any extra text except the json
"""
    try:
        print('prompt: ', prompt)
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={"model": "llama3.2", "prompt": prompt, "stream": False},
            timeout=60
        )
        output = response.json().get("response", "").strip()
        try:
            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', output)
            return extract_json_from_string(output)
            
        except json.JSONDecodeError:
            print("⚠️ Could not parse JSON. Raw response:")
            print(output)
            return []
    except Exception as e:
        print(f"Error calling Ollama LLM: {e}")
        return []

def main():
    urls = load_sources_from_csv(INPUT_FILE)
    all_events = []

    for url in urls:
        text, images = fetch_page_text_and_images(url)
        if text:
            # print(text, images)
            extracted = extract_event_data(text, images)
            print(extracted)
            for event in extracted:
                event["source"] = url
            all_events.extend(extracted)
            time.sleep(2)  # polite delay

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f_json:
        json.dump(all_events, f_json, indent=2)

    try:
        df = pd.DataFrame(all_events)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Saved {len(all_events)} events to {OUTPUT_JSON} and {OUTPUT_CSV}")
    except Exception as e:
        print(f"Error saving CSV: {e}")

if __name__ == "__main__":
    main()
