
from dataclasses import dataclass, asdict
from typing import List

@dataclass
class Event:
    name: str
    venue_name: str
    venue_address: str
    start_datetime: str
    end_datetime: str
    short_description: str
    price: str
    host: str
    source_websites: List[str]
    hero_images: List[str]

    def to_dict(self):
        return asdict(self)
