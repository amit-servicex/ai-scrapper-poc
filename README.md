# 🗺️ Local Event Discovery POC

This is a proof-of-concept for a scalable, AI-augmented pipeline that extracts local events from city calendars and media pages using web scraping and large language models.

## ✅ What This Does

- Takes a city name as input.
- Scrapes real event pages listed in `event_sources_input.csv`.
- Extracts:
  - name
  - venue_name
  - venue_address
  - start_datetime, end_datetime (ISO 8601)
  - short_description
  - price
  - host
  - source_websites[]
  - hero_images[]
- Outputs structured JSON + CSV in under a few minutes.
- Uses [Ollama](https://ollama.com/) with Mistral or LLaMA 3 for schema-based extraction.

---

## 🚀 How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Start LLM locally (in separate terminal)
```bash
ollama run mistral
```

### 3. Run scraper
```bash
python ai_event_crawler.py
```

### 4. Query results by city
```bash
python get_events_by_city.py "miami"
```

---

## 📂 Project Structure

```
.
├── ai_event_crawler.py           # Main pipeline
├── get_events_by_city.py         # CLI tool to query results
├── event_sources_input.csv       # List of source URLs
├── output/
│   ├── events.json
│   └── events.csv
├── requirements.txt
└── utils/
    ├── html_scraper.py
    ├── ai_extractor.py
    └── schema.py
```

---

## 🔍 Sources Used

- City of Miami Events: `https://www.miamigov.com/Residents/Events-Calendar`
- TimeOut Events: `https://www.timeout.com/miami/events`
- Patch Calendar: `https://patch.com/calendar`
- DoStuff Media: `https://dostuffmedia.com/`
- Miami Beach Convention Center: `https://www.miamibeachconvention.com/events/`

---

## 🛠 Requirements

- Python 3.8+
- Ollama installed with `mistral` model downloaded
- Internet access for scraping and downloading models

---

## 📌 Notes

- This is a POC — no deduplication or advanced retries yet.
- Runtime is optimized for a 1-pass extraction using local LLM.
- Easily extendable for API-based sources (e.g. Eventbrite, Ticketmaster).