
import requests
from bs4 import BeautifulSoup

import requests
from bs4 import BeautifulSoup



def fetch_page_text_and_images(url):
    try:
        print(f"Fetching: {url}")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/113.0.0.0 Safari/537.36"
            )
        }

        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text(separator="\n", strip=True)
        images = [
            img["src"] for img in soup.find_all("img", src=True)
            if img["src"].startswith("http")
        ]

        return text, images

    except requests.exceptions.Timeout:
        print(f"⏳ Timeout error: {url}")
    except requests.exceptions.HTTPError as e:
        print(f"📛 HTTP error {e.response.status_code}: {url}")
    except requests.exceptions.RequestException as e:
        print(f"🔌 Network error: {url} – {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {url} – {e}")

    return "", []












def fetch_page_text(url):
    try:
        print(f"Fetching: {url}")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/113.0.0.0 Safari/537.36"
            )
        }

        # Increase timeout to tolerate slow-loading pages
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract visible text
        text = soup.get_text(separator="\n", strip=True)

        # Extract high-res image URLs
        images = [
            img["src"] for img in soup.find_all("img", src=True)
            if img["src"].startswith("http")
        ]

        return text, images

    except requests.exceptions.Timeout:
        print(f"⏳ Timeout error: {url}")
    except requests.exceptions.HTTPError as e:
        print(f"📛 HTTP error {e.response.status_code}: {url}")
    except requests.exceptions.RequestException as e:
        print(f"🔌 Network error: {url} – {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {url} – {e}")

    return "", []
