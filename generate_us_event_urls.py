#!/usr/bin/env python3
"""
US Event URLs Generator Script
Converts existing event source URLs to cover all US states and major cities
Based on the provided CSV format with Category and SourceURL columns
"""

import pandas as pd
import csv
from urllib.parse import urlparse
import re

# US States and their major cities
US_STATES_CITIES = {
    'AL': {'name': 'Alabama', 'capital': 'Montgomery', 'major_cities': ['Birmingham', 'Mobile', 'Huntsville']},
    'AK': {'name': 'Alaska', 'capital': 'Juneau', 'major_cities': ['Anchorage', 'Fairbanks']},
    'AZ': {'name': 'Arizona', 'capital': 'Phoenix', 'major_cities': ['Tucson', 'Mesa', 'Chandler', 'Scottsdale']},
    'AR': {'name': 'Arkansas', 'capital': 'Little Rock', 'major_cities': ['Fort Smith', 'Fayetteville']},
    'CA': {'name': 'California', 'capital': 'Sacramento', 'major_cities': ['Los Angeles', 'San Francisco', 'San Diego', 'San Jose', 'Oakland', 'Fresno']},
    'CO': {'name': 'Colorado', 'capital': 'Denver', 'major_cities': ['Colorado Springs', 'Aurora', 'Fort Collins']},
    'CT': {'name': 'Connecticut', 'capital': 'Hartford', 'major_cities': ['Bridgeport', 'New Haven', 'Stamford']},
    'DE': {'name': 'Delaware', 'capital': 'Dover', 'major_cities': ['Wilmington', 'Newark']},
    'FL': {'name': 'Florida', 'capital': 'Tallahassee', 'major_cities': ['Miami', 'Tampa', 'Orlando', 'Jacksonville', 'St. Petersburg', 'Fort Lauderdale']},
    'GA': {'name': 'Georgia', 'capital': 'Atlanta', 'major_cities': ['Columbus', 'Augusta', 'Savannah', 'Athens']},
    'HI': {'name': 'Hawaii', 'capital': 'Honolulu', 'major_cities': ['Pearl City', 'Hilo']},
    'ID': {'name': 'Idaho', 'capital': 'Boise', 'major_cities': ['Nampa', 'Meridian']},
    'IL': {'name': 'Illinois', 'capital': 'Springfield', 'major_cities': ['Chicago', 'Aurora', 'Peoria', 'Rockford']},
    'IN': {'name': 'Indiana', 'capital': 'Indianapolis', 'major_cities': ['Fort Wayne', 'Evansville', 'South Bend']},
    'IA': {'name': 'Iowa', 'capital': 'Des Moines', 'major_cities': ['Cedar Rapids', 'Davenport', 'Sioux City']},
    'KS': {'name': 'Kansas', 'capital': 'Topeka', 'major_cities': ['Wichita', 'Overland Park', 'Kansas City']},
    'KY': {'name': 'Kentucky', 'capital': 'Frankfort', 'major_cities': ['Louisville', 'Lexington', 'Bowling Green']},
    'LA': {'name': 'Louisiana', 'capital': 'Baton Rouge', 'major_cities': ['New Orleans', 'Shreveport', 'Lafayette']},
    'ME': {'name': 'Maine', 'capital': 'Augusta', 'major_cities': ['Portland', 'Lewiston', 'Bangor']},
    'MD': {'name': 'Maryland', 'capital': 'Annapolis', 'major_cities': ['Baltimore', 'Frederick', 'Rockville']},
    'MA': {'name': 'Massachusetts', 'capital': 'Boston', 'major_cities': ['Worcester', 'Springfield', 'Cambridge']},
    'MI': {'name': 'Michigan', 'capital': 'Lansing', 'major_cities': ['Detroit', 'Grand Rapids', 'Warren', 'Sterling Heights']},
    'MN': {'name': 'Minnesota', 'capital': 'Saint Paul', 'major_cities': ['Minneapolis', 'Rochester', 'Duluth']},
    'MS': {'name': 'Mississippi', 'capital': 'Jackson', 'major_cities': ['Gulfport', 'Southaven', 'Hattiesburg']},
    'MO': {'name': 'Missouri', 'capital': 'Jefferson City', 'major_cities': ['Kansas City', 'St. Louis', 'Springfield', 'Columbia']},
    'MT': {'name': 'Montana', 'capital': 'Helena', 'major_cities': ['Billings', 'Missoula', 'Great Falls']},
    'NE': {'name': 'Nebraska', 'capital': 'Lincoln', 'major_cities': ['Omaha', 'Bellevue', 'Grand Island']},
    'NV': {'name': 'Nevada', 'capital': 'Carson City', 'major_cities': ['Las Vegas', 'Henderson', 'Reno']},
    'NH': {'name': 'New Hampshire', 'capital': 'Concord', 'major_cities': ['Manchester', 'Nashua', 'Rochester']},
    'NJ': {'name': 'New Jersey', 'capital': 'Trenton', 'major_cities': ['Newark', 'Jersey City', 'Paterson']},
    'NM': {'name': 'New Mexico', 'capital': 'Santa Fe', 'major_cities': ['Albuquerque', 'Las Cruces', 'Rio Rancho']},
    'NY': {'name': 'New York', 'capital': 'Albany', 'major_cities': ['New York City', 'Buffalo', 'Rochester', 'Yonkers', 'Syracuse']},
    'NC': {'name': 'North Carolina', 'capital': 'Raleigh', 'major_cities': ['Charlotte', 'Greensboro', 'Durham', 'Winston-Salem']},
    'ND': {'name': 'North Dakota', 'capital': 'Bismarck', 'major_cities': ['Fargo', 'Grand Forks', 'Minot']},
    'OH': {'name': 'Ohio', 'capital': 'Columbus', 'major_cities': ['Cleveland', 'Cincinnati', 'Toledo', 'Akron', 'Dayton']},
    'OK': {'name': 'Oklahoma', 'capital': 'Oklahoma City', 'major_cities': ['Tulsa', 'Norman', 'Lawton']},
    'OR': {'name': 'Oregon', 'capital': 'Salem', 'major_cities': ['Portland', 'Eugene', 'Bend', 'Gresham']},
    'PA': {'name': 'Pennsylvania', 'capital': 'Harrisburg', 'major_cities': ['Philadelphia', 'Pittsburgh', 'Allentown', 'Erie']},
    'RI': {'name': 'Rhode Island', 'capital': 'Providence', 'major_cities': ['Warwick', 'Cranston', 'Pawtucket']},
    'SC': {'name': 'South Carolina', 'capital': 'Columbia', 'major_cities': ['Charleston', 'North Charleston', 'Mount Pleasant']},
    'SD': {'name': 'South Dakota', 'capital': 'Pierre', 'major_cities': ['Sioux Falls', 'Rapid City', 'Aberdeen']},
    'TN': {'name': 'Tennessee', 'capital': 'Nashville', 'major_cities': ['Memphis', 'Knoxville', 'Chattanooga', 'Clarksville']},
    'TX': {'name': 'Texas', 'capital': 'Austin', 'major_cities': ['Houston', 'San Antonio', 'Dallas', 'Fort Worth', 'El Paso', 'Arlington']},
    'UT': {'name': 'Utah', 'capital': 'Salt Lake City', 'major_cities': ['West Valley City', 'Provo', 'West Jordan']},
    'VT': {'name': 'Vermont', 'capital': 'Montpelier', 'major_cities': ['Burlington', 'South Burlington', 'Rutland']},
    'VA': {'name': 'Virginia', 'capital': 'Richmond', 'major_cities': ['Virginia Beach', 'Norfolk', 'Chesapeake', 'Newport News']},
    'WA': {'name': 'Washington', 'capital': 'Olympia', 'major_cities': ['Seattle', 'Spokane', 'Tacoma', 'Vancouver', 'Bellevue']},
    'WV': {'name': 'West Virginia', 'capital': 'Charleston', 'major_cities': ['Huntington', 'Parkersburg', 'Morgantown']},
    'WI': {'name': 'Wisconsin', 'capital': 'Madison', 'major_cities': ['Milwaukee', 'Green Bay', 'Kenosha']},
    'WY': {'name': 'Wyoming', 'capital': 'Cheyenne', 'major_cities': ['Casper', 'Laramie', 'Gillette']}
}

class USEventURLGenerator:
    def __init__(self, input_csv_path):
        """Initialize with the input CSV file"""
        self.input_data = pd.read_csv(input_csv_path)
        self.generated_urls = []
        
    def clean_city_name(self, city_name):
        """Clean city name for URL generation"""
        return city_name.lower().replace(' ', '').replace('-', '').replace('.', '')
    
    def generate_government_urls(self, city, state_code):
        """Generate government event URLs for a city"""
        city_clean = self.clean_city_name(city)
        urls = []
        
        # Various government URL patterns
        patterns = [
            f"https://www.{city_clean}.gov/events/",
            f"https://www.{city_clean}.org/events/",
            f"https://www.cityof{city_clean}.gov/events/",
            f"https://www.cityof{city_clean}.com/events/",
            f"https://{city_clean}.gov/events/",
            f"https://{city_clean}gov.org/events/",
            f"https://www.{city_clean}.{state_code.lower()}.us/events/",
            f"https://www.{city_clean}texas.gov/event" if state_code == 'TX' else f"https://www.{city_clean}{US_STATES_CITIES[state_code]['name'].lower()}.gov/events/",
        ]
        
        for pattern in patterns:
            urls.append({
                'Category': 'City & Local Government',
                'SourceURL': pattern,
                'City': city,
                'State': state_code,
                'Generated': True
            })
        
        return urls
    
    def generate_chamber_urls(self, city, state_code):
        """Generate Chamber of Commerce URLs for a city"""
        city_clean = self.clean_city_name(city)
        urls = []
        
        patterns = [
            f"https://www.{city_clean}chamber.com/events/",
            f"https://www.{city_clean}chamber.org/events/",
            f"https://{city_clean}chamber.com/events/",
            f"https://www.chamberof{city_clean}.com/events/",
            f"https://www.greater{city_clean}chamber.com/events/",
            f"https://www.{city_clean}areachamber.com/events/",
        ]
        
        for pattern in patterns:
            urls.append({
                'Category': 'Business & Chamber of Commerce',
                'SourceURL': pattern,
                'City': city,
                'State': state_code,
                'Generated': True
            })
        
        return urls
    
    def generate_library_urls(self, city, state_code):
        """Generate library event URLs for a city"""
        city_clean = self.clean_city_name(city)
        urls = []
        
        patterns = [
            f"https://www.{city_clean}library.org/events/",
            f"https://www.{city_clean}publiclibrary.org/events/",
            f"https://{city_clean}library.org/events/",
            f"https://www.{city_clean}lib.org/events/",
            f"https://library.{city_clean}.gov/events/",
            f"https://www.{city_clean}.gov/library/events/",
        ]
        
        for pattern in patterns:
            urls.append({
                'Category': 'Libraries & Education',
                'SourceURL': pattern,
                'City': city,
                'State': state_code,
                'Generated': True
            })
        
        return urls
    
    def generate_university_urls(self, city, state_code):
        """Generate university event URLs for major cities"""
        city_clean = self.clean_city_name(city)
        urls = []
        
        # Major universities by city (simplified mapping)
        university_mapping = {
            'boston': ['harvard', 'mit', 'bu', 'northeastern'],
            'cambridge': ['harvard', 'mit'],
            'newyorkcity': ['nyu', 'columbia', 'fordham'],
            'newyork': ['nyu', 'columbia', 'fordham'],
            'losangeles': ['ucla', 'usc', 'caltech'],
            'chicago': ['uchicago', 'northwestern', 'uic'],
            'philadelphia': ['upenn', 'temple', 'drexel'],
            'atlanta': ['emory', 'gatech', 'gsu'],
            'seattle': ['washington', 'seattleu'],
            'miami': ['um', 'fiu'],
            'austin': ['utexas', 'austincc'],
            'houston': ['rice', 'uh', 'tsu'],
            'dallas': ['smu', 'utdallas'],
            'denver': ['du', 'ucdenver'],
            'portland': ['psu', 'up'],
        }
        
        if city_clean in university_mapping:
            for uni in university_mapping[city_clean]:
                urls.append({
                    'Category': 'University Calendars',
                    'SourceURL': f"https://www.{uni}.edu/events/",
                    'City': city,
                    'State': state_code,
                    'Generated': True
                })
        else:
            # Generic university patterns
            patterns = [
                f"https://www.{city_clean}cc.edu/events/",
                f"https://www.u{city_clean}.edu/events/",
            ]
            for pattern in patterns:
                urls.append({
                    'Category': 'University Calendars',
                    'SourceURL': pattern,
                    'City': city,
                    'State': state_code,
                    'Generated': True
                })
        
        return urls
    
    def generate_meetup_urls(self, city, state_code):
        """Generate Meetup URLs for all cities"""
        city_encoded = city.replace(' ', '%20')
        url = f"https://www.meetup.com/find/?location={city_encoded}%2C%20{state_code}"
        
        return [{
            'Category': 'Community & Social Platforms',
            'SourceURL': url,
            'City': city,
            'State': state_code,
            'Generated': True
        }]
    
    def generate_timeout_urls(self, city, state_code):
        """Generate TimeOut event URLs for major cities"""
        major_timeout_cities = ['newyork', 'losangeles', 'chicago', 'miami', 'boston', 
                               'washington', 'atlanta', 'philadelphia', 'denver', 'seattle']
        
        city_clean = self.clean_city_name(city)
        urls = []
        
        if city_clean in major_timeout_cities or city_clean == 'newyorkcity':
            timeout_city = 'newyork' if city_clean == 'newyorkcity' else city_clean
            urls.append({
                'Category': 'Local News & Community',
                'SourceURL': f"https://www.timeout.com/{timeout_city}/events",
                'City': city,
                'State': state_code,
                'Generated': True
            })
        
        return urls
    
    def generate_all_urls(self):
        """Generate URLs for all US states and major cities"""
        self.generated_urls = []
        
        # First, add the original URLs from the CSV
        for _, row in self.input_data.iterrows():
            self.generated_urls.append({
                'Category': row['Category'],
                'SourceURL': row['SourceURL'],
                'City': 'Original',
                'State': 'Original',
                'Generated': False
            })
        
        # Generate URLs for all states and cities
        for state_code, state_info in US_STATES_CITIES.items():
            # State capital
            capital = state_info['capital']
            self.generated_urls.extend(self.generate_government_urls(capital, state_code))
            self.generated_urls.extend(self.generate_chamber_urls(capital, state_code))
            self.generated_urls.extend(self.generate_library_urls(capital, state_code))
            self.generated_urls.extend(self.generate_meetup_urls(capital, state_code))
            self.generated_urls.extend(self.generate_timeout_urls(capital, state_code))
            
            if len(state_info['major_cities']) > 0:
                self.generated_urls.extend(self.generate_university_urls(capital, state_code))
            
            # Major cities
            for city in state_info['major_cities']:
                self.generated_urls.extend(self.generate_government_urls(city, state_code))
                self.generated_urls.extend(self.generate_chamber_urls(city, state_code))
                self.generated_urls.extend(self.generate_library_urls(city, state_code))
                self.generated_urls.extend(self.generate_university_urls(city, state_code))
                self.generated_urls.extend(self.generate_meetup_urls(city, state_code))
                self.generated_urls.extend(self.generate_timeout_urls(city, state_code))
    
    def add_api_sources(self):
        """Add API sources for each major platform"""
        api_sources = [
            {
                'Category': 'APIs and Structured Sources',
                'SourceURL': 'https://app.ticketmaster.com/discovery/v2/events.json?apikey={YOUR_KEY}',
                'City': 'National',
                'State': 'API',
                'Generated': False
            },
            {
                'Category': 'APIs and Structured Sources', 
                'SourceURL': 'https://www.eventbriteapi.com/v3/events/search/',
                'City': 'National',
                'State': 'API',
                'Generated': False
            },
            {
                'Category': 'APIs and Structured Sources',
                'SourceURL': 'https://www.predicthq.com/events/local-events',
                'City': 'National', 
                'State': 'API',
                'Generated': False
            }
        ]
        
        self.generated_urls.extend(api_sources)
    
    def save_to_csv(self, output_path='us_event_sources_complete.csv'):
        """Save all generated URLs to CSV file"""
        df = pd.DataFrame(self.generated_urls)
        
        # Reorder columns to match original format plus additional info
        columns_order = ['Category', 'SourceURL', 'City', 'State', 'Generated']
        df = df[columns_order]
        
        # Sort by Category, then State, then City
        df = df.sort_values(['Category', 'State', 'City'])
        
        df.to_csv(output_path, index=False)
        print(f"Generated {len(df)} URLs saved to {output_path}")
        
        # Print summary statistics
        print("\nSummary Statistics:")
        print(f"Total URLs: {len(df)}")
        print(f"Original URLs: {len(df[df['Generated'] == False])}")
        print(f"Generated URLs: {len(df[df['Generated'] == True])}")
        print("\nURLs by Category:")
        print(df['Category'].value_counts())
        
        return df
    
    def save_simple_format(self, output_path='us_event_sources_simple.csv'):
        """Save in original simple format (Category, SourceURL only)"""
        df = pd.DataFrame(self.generated_urls)
        simple_df = df[['Category', 'SourceURL']].copy()
        simple_df.to_csv(output_path, index=False)
        print(f"Simple format saved to {output_path}")
        return simple_df

def main():
    """Main function to run the URL generator"""
    
    # Initialize the generator with input CSV
    generator = USEventURLGenerator('event_sources_input.csv')
    
    # Generate all URLs
    print("Generating URLs for all US states and cities...")
    generator.generate_all_urls()
    
    # Add API sources
    generator.add_api_sources()
    
    # Save to CSV files
    print("Saving to CSV files...")
    complete_df = generator.save_to_csv('us_event_sources_complete.csv')
    simple_df = generator.save_simple_format('us_event_sources_simple.csv')
    
    print("\nDone! Files generated:")
    print("1. us_event_sources_complete.csv - Full details with city/state info")
    print("2. us_event_sources_simple.csv - Original format (Category, SourceURL)")
    
    # Show sample of generated URLs
    print("\nSample of generated URLs:")
    print(complete_df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()