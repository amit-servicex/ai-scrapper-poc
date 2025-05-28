#!/usr/bin/env python3
"""
URL Validator and Filter Script
Reads CSV data, checks URL availability, and creates filtered CSV with working sites only
Includes retry logic, timeout handling, and comprehensive logging
"""

import pandas as pd
import requests
from urllib.parse import urlparse
import time
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import warnings
warnings.filterwarnings('ignore', category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

class URLValidator:
    def __init__(self, input_csv_path, max_workers=10, timeout=10, delay=0.5):
        """
        Initialize URL validator
        
        Args:
            input_csv_path: Path to input CSV file
            max_workers: Number of concurrent threads
            timeout: Request timeout in seconds
            delay: Delay between requests to avoid rate limiting
        """
        self.input_csv_path = input_csv_path
        self.max_workers = max_workers
        self.timeout = timeout
        self.delay = delay
        self.session = self.create_session()
        self.results = []
        self.lock = threading.Lock()
        self.processed_count = 0
        self.total_count = 0
        
    def create_session(self):
        """Create a requests session with retry strategy"""
        session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Headers to appear more like a browser
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        return session
    
    def check_url(self, url):
        """
        Check if a URL is accessible
        
        Args:
            url: URL to check
            
        Returns:
            dict: Result dictionary with URL status information
        """
        result = {
            'url': url,
            'status': 'unknown',
            'status_code': None,
            'response_time': None,
            'error': None,
            'redirect_url': None,
            'content_length': None,
            'content_type': None
        }
        
        try:
            start_time = time.time()
            
            # Make request with timeout
            response = self.session.get(
                url, 
                timeout=self.timeout, 
                allow_redirects=True,
                verify=False  # Skip SSL verification for problematic sites
            )
            
            end_time = time.time()
            response_time = round(end_time - start_time, 2)
            
            result.update({
                'status_code': response.status_code,
                'response_time': response_time,
                'content_length': len(response.content) if response.content else 0,
                'content_type': response.headers.get('content-type', '').split(';')[0]
            })
            
            # Check if URL was redirected
            if response.url != url:
                result['redirect_url'] = response.url
            
            # Determine status based on response
            if response.status_code == 200:
                # Additional checks for valid content
                content = response.text.lower()
                
                # Check for common error indicators
                error_indicators = [
                    '404 not found', 'page not found', 'file not found',
                    'under construction', 'coming soon', 'temporarily unavailable',
                    'site maintenance', 'access denied', 'forbidden'
                ]
                
                if any(indicator in content for indicator in error_indicators):
                    result['status'] = 'error_page'
                    result['error'] = 'Page contains error indicators'
                elif len(content) < 100:  # Very short content might indicate an error
                    result['status'] = 'minimal_content'
                    result['error'] = 'Page has minimal content'
                else:
                    result['status'] = 'success'
            
            elif response.status_code in [301, 302, 303, 307, 308]:
                result['status'] = 'redirect'
            elif response.status_code == 404:
                result['status'] = 'not_found'
                result['error'] = 'Page not found'
            elif response.status_code == 403:
                result['status'] = 'forbidden'
                result['error'] = 'Access forbidden'
            elif response.status_code >= 500:
                result['status'] = 'server_error'
                result['error'] = f'Server error: {response.status_code}'
            else:
                result['status'] = 'other_error'
                result['error'] = f'HTTP {response.status_code}'
                
        except requests.exceptions.Timeout:
            result['status'] = 'timeout'
            result['error'] = f'Request timeout after {self.timeout}s'
        except requests.exceptions.ConnectionError:
            result['status'] = 'connection_error'
            result['error'] = 'Connection failed'
        except requests.exceptions.TooManyRedirects:
            result['status'] = 'redirect_error'
            result['error'] = 'Too many redirects'
        except requests.exceptions.SSLError:
            result['status'] = 'ssl_error'
            result['error'] = 'SSL certificate error'
        except Exception as e:
            result['status'] = 'unknown_error'
            result['error'] = str(e)
        
        return result
    
    def process_url(self, row_data):
        """
        Process a single URL and return combined result
        
        Args:
            row_data: Dictionary containing row data from CSV
            
        Returns:
            dict: Combined row data with URL check result
        """
        url = row_data['SourceURL']
        url_result = self.check_url(url)
        
        # Combine original row data with URL check result
        combined_result = {**row_data, **url_result}
        
        # Update progress
        with self.lock:
            self.processed_count += 1
            if self.processed_count % 50 == 0:
                print(f"Progress: {self.processed_count}/{self.total_count} URLs checked ({self.processed_count/self.total_count*100:.1f}%)")
        
        # Add delay to avoid rate limiting
        time.sleep(self.delay)
        
        return combined_result
    
    def validate_urls(self, input_df):
        """
        Validate all URLs in the dataframe using multithreading
        
        Args:
            input_df: Input DataFrame with URLs to validate
            
        Returns:
            list: List of validation results
        """
        self.total_count = len(input_df)
        self.processed_count = 0
        
        print(f"Starting validation of {self.total_count} URLs...")
        print(f"Using {self.max_workers} concurrent threads with {self.delay}s delay")
        
        # Convert DataFrame rows to dictionaries
        rows = input_df.to_dict('records')
        
        # Process URLs with thread pool
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_row = {executor.submit(self.process_url, row): row for row in rows}
            
            # Collect results as they complete
            for future in as_completed(future_to_row):
                try:
                    result = future.result()
                    self.results.append(result)
                except Exception as e:
                    print(f"Error processing URL: {e}")
        
        print(f"Validation complete! Processed {len(self.results)} URLs")
        return self.results
    
    def analyze_results(self, results):
        """
        Analyze and print validation results statistics
        
        Args:
            results: List of validation results
        """
        print("\n" + "="*60)
        print("VALIDATION RESULTS ANALYSIS")
        print("="*60)
        
        # Count results by status
        status_counts = {}
        successful_urls = 0
        
        for result in results:
            status = result['status']
            status_counts[status] = status_counts.get(status, 0) + 1
            if status in ['success', 'redirect']:
                successful_urls += 1
        
        print(f"Total URLs checked: {len(results)}")
        print(f"Successful URLs: {successful_urls} ({successful_urls/len(results)*100:.1f}%)")
        print(f"Failed URLs: {len(results) - successful_urls} ({(len(results) - successful_urls)/len(results)*100:.1f}%)")
        
        print(f"\nStatus Breakdown:")
        for status, count in sorted(status_counts.items()):
            percentage = count / len(results) * 100
            print(f"  {status:15}: {count:4} ({percentage:5.1f}%)")
        
        # Category breakdown for successful URLs
        category_success = {}
        for result in results:
            if result['status'] in ['success', 'redirect']:
                category = result.get('Category', 'Unknown')
                category_success[category] = category_success.get(category, 0) + 1
        
        if category_success:
            print(f"\nSuccessful URLs by Category:")
            for category, count in sorted(category_success.items()):
                print(f"  {category:30}: {count:4}")
        
        return status_counts, successful_urls
    
    def save_results(self, results, output_path='url_validation_results.csv'):
        """
        Save all validation results to CSV
        
        Args:
            results: List of validation results
            output_path: Output file path
        """
        if not results:
            print("No results to save!")
            return
        
        df = pd.DataFrame(results)
        
        # Reorder columns for better readability
        column_order = [
            'Category', 'SourceURL', 'status', 'status_code', 'response_time',
            'City', 'State', 'Generated', 'redirect_url', 'content_length',
            'content_type', 'error'
        ]
        
        # Only include columns that exist
        available_columns = [col for col in column_order if col in df.columns]
        other_columns = [col for col in df.columns if col not in column_order]
        final_columns = available_columns + other_columns
        
        df = df[final_columns]
        df.to_csv(output_path, index=False)
        print(f"All validation results saved to: {output_path}")
        
        return df
    
    def create_filtered_csv(self, results, output_path='working_event_sources.csv'):
        """
        Create filtered CSV with only working URLs
        
        Args:
            results: List of validation results
            output_path: Output file path for filtered results
        """
        # Filter for successful URLs
        working_results = [
            result for result in results 
            if result['status'] in ['success', 'redirect']
        ]
        
        if not working_results:
            print("No working URLs found!")
            return None
        
        # Create DataFrame with working URLs
        df = pd.DataFrame(working_results)
        
        # Keep original format columns plus some useful additional info
        filtered_columns = [
            'Category', 'SourceURL', 'City', 'State', 'status_code', 
            'response_time', 'redirect_url'
        ]
        
        # Only include columns that exist
        available_columns = [col for col in filtered_columns if col in df.columns]
        df_filtered = df[available_columns]
        
        # Sort by category and response time (faster sites first)
        df_filtered = df_filtered.sort_values(['Category', 'response_time'])
        
        # Save filtered results
        df_filtered.to_csv(output_path, index=False)
        
        print(f"\nFiltered results saved to: {output_path}")
        print(f"Working URLs: {len(working_results)}")
        
        # Create simple format (Category, SourceURL only)
        simple_output = output_path.replace('.csv', '_simple.csv')
        df_simple = df_filtered[['Category', 'SourceURL']].copy()
        df_simple.to_csv(simple_output, index=False)
        print(f"Simple format saved to: {simple_output}")
        
        return df_filtered
    
    def create_performance_report(self, results, output_path='url_performance_report.csv'):
        """
        Create a performance report of working URLs sorted by speed
        
        Args:
            results: List of validation results
            output_path: Output file path for performance report
        """
        working_results = [
            result for result in results 
            if result['status'] in ['success', 'redirect'] and result['response_time']
        ]
        
        if not working_results:
            return None
        
        df = pd.DataFrame(working_results)
        
        # Performance report columns
        performance_columns = [
            'Category', 'SourceURL', 'City', 'State', 'status_code',
            'response_time', 'content_length', 'content_type'
        ]
        
        available_columns = [col for col in performance_columns if col in df.columns]
        df_perf = df[available_columns]
        
        # Sort by response time (fastest first)
        df_perf = df_perf.sort_values('response_time')
        
        df_perf.to_csv(output_path, index=False)
        print(f"Performance report saved to: {output_path}")
        
        # Show top 10 fastest sites
        print(f"\nTop 10 Fastest Responding Sites:")
        print(df_perf.head(10)[['SourceURL', 'response_time', 'Category']].to_string(index=False))
        
        return df_perf

def main():
    """Main function to run URL validation"""
    
    # Configuration
    INPUT_CSV = 'us_event_sources_complete.csv'  # Change this to your input file
    MAX_WORKERS = 20  # Number of concurrent threads
    TIMEOUT = 15      # Request timeout in seconds
    DELAY = 0.3       # Delay between requests (seconds)
    
    print("URL Validation and Filtering Script")
    print("="*40)
    
    # Check if input file exists
    try:
        input_df = pd.read_csv(INPUT_CSV)
        print(f"Loaded {len(input_df)} URLs from {INPUT_CSV}")
    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_CSV}' not found!")
        print("Please make sure the CSV file exists and update the INPUT_CSV variable.")
        return
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    # Initialize validator
    validator = URLValidator(
        input_csv_path=INPUT_CSV,
        max_workers=MAX_WORKERS,
        timeout=TIMEOUT,
        delay=DELAY
    )
    
    # Validate URLs
    start_time = time.time()
    results = validator.validate_urls(input_df)
    end_time = time.time()
    
    print(f"\nValidation completed in {end_time - start_time:.1f} seconds")
    
    # Analyze results
    status_counts, successful_count = validator.analyze_results(results)
    
    # Save all results
    validator.save_results(results, 'url_validation_results.csv')
    
    # Create filtered CSV with working URLs only
    working_df = validator.create_filtered_csv(results, 'working_event_sources.csv')
    
    # Create performance report
    validator.create_performance_report(results, 'url_performance_report.csv')
    
    print(f"\n" + "="*60)
    print("FILES CREATED:")
    print("="*60)
    print("1. url_validation_results.csv     - All URLs with validation status")
    print("2. working_event_sources.csv      - Only working URLs (detailed)")
    print("3. working_event_sources_simple.csv - Only working URLs (Category, SourceURL)")
    print("4. url_performance_report.csv     - Working URLs sorted by speed")
    
    if successful_count > 0:
        print(f"\nSUCCESS: Found {successful_count} working event sources!")
        print("You can now use 'working_event_sources_simple.csv' for your scraping pipeline.")
    else:
        print(f"\nWARNING: No working URLs found. You may need to:")
        print("- Increase timeout value")
        print("- Reduce concurrent threads")
        print("- Check network connectivity")

if __name__ == "__main__":
    main()