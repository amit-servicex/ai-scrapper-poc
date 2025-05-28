import sys
import json
import pandas as pd
from pathlib import Path

OUTPUT_JSON = "output/events.json"
#python get_events_by_city.py "miami"


FIELDS = [
    "venue_name",
    "venue_address",
    "start_datetime",
    "end_datetime",
    "short_description",
    "price",
    "host",
    "source_websites",
    "hero_images"
]

def print_event(event):
    for field in FIELDS:
        print(f"- {field}: {event.get(field, '')}")
    print("\n" + "-" * 60 + "\n")

def main():
    if len(sys.argv) < 2:
        print("Usage: python get_events_by_city.py <city_name>")
        sys.exit(1)

    city_name = sys.argv[1].lower()
    path = Path(OUTPUT_JSON)

    if not path.exists():
        print(f"❌ Event data file not found: {OUTPUT_JSON}")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        try:
            events = json.load(f)
        except json.JSONDecodeError:
            print("❌ Failed to load JSON.")
            sys.exit(1)

    filtered_events = [
    e for e in events if any(
        city_name in (e.get(field, "") or "").lower()
        for field in ["venue_name", "venue_address", "source"]
    )
    ]

    if not filtered_events:
        print(f"⚠️ No events found for city: {city_name}")
    else:
        print(f"✅ Found {len(filtered_events)} events in '{city_name}':\n")
        for event in filtered_events:
            print_event(event)

if __name__ == "__main__":
    main()
