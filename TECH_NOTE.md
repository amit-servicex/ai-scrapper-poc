# 📄 Tech Note – Local Event Discovery POC

## 🔧 What This POC Delivers

This proof-of-concept implements a complete, scalable pipeline for extracting local event data using web scraping and AI-based enrichment.

### ✅ Key Functional Highlights

- Crawls multiple public event sources (HTML pages).
- Summarizes and extracts structured event fields using a local LLM (Mistral via Ollama).
- Accepts a city name as input and returns formatted event listings.
- Outputs both `events.json` and `events.csv` for downstream use.
- Includes CLI tooling for querying results by city.

### 🧠 Schema Extracted Per Event
- venue_name
- venue_address
- start_datetime (ISO 8601)
- end_datetime (ISO 8601)
- short_description
- price
- host
- source_websites (list of URLs)
- hero_images (list of image URLs)



## 🛠 What’s Implemented

- Modular code with `utils/` folder (`html_scraper.py`, `ai_extractor.py`, `schema.py`)
- Summarization before LLM prompt to reduce token load
- CLI interface via `get_events_by_city.py`
- Robust error handling for HTTP failures, JSON parsing, timeouts
- Fully local operation with `ollama` for cost-free execution


