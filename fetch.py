import os
import random
import string
import time
from typing import List, Dict, Optional
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class GoogleBooksAPI:
    """Handles Google Books API interactions with extended field coverage."""

    def __init__(self, api_keys: List[str]):
        self.base_url = "https://www.googleapis.com/books/v1/volumes"
        self.api_keys = api_keys
        self.current_key_index = 0

    def rotate_api_key(self):
        """Rotate to the next API key."""
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        print(f"Switching to API key index {self.current_key_index}")

    def get_current_api_key(self) -> str:
        """Retrieve the current API key."""
        return self.api_keys[self.current_key_index]

    def _api_request(self, params: Dict) -> Optional[requests.Response]:
        """Handle API requests with retries and key rotation."""
        retries, delay = 5, 1
        for attempt in range(retries):
            params["key"] = self.get_current_api_key()
            try:
                response = requests.get(self.base_url, params=params)
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate-limited
                    print(f"Rate limit reached for key {self.current_key_index}. Retrying...")
                    self.rotate_api_key()
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
            except requests.RequestException as e:
                print(f"Request error: {e}")
        print("All keys exhausted or maximum retries reached. Skipping this request.")
        return None

    def search_books(self, query: str, max_results: int = 10) -> List[Dict]:
        """Search for books using a query."""
        params = {"q": query, "maxResults": max_results, "projection": "full"}
        response = self._api_request(params)
        if not response:
            return []

        items = response.json().get("items", [])
        return [self._parse_book_data(item) for item in items]

    def fetch_book_data(self, isbn: str) -> Optional[Dict]:
        """Fetch detailed book data by ISBN."""
        params = {"q": f"isbn:{isbn}", "projection": "full"}
        response = self._api_request(params)
        if not response:
            return None
        return self._parse_book_data(response.json().get("items", [{}])[0])

    def _parse_book_data(self, item: Dict) -> Optional[Dict]:
        """Parse book data to extract required fields."""
        if not item:
            return None

        volume_info = item.get("volumeInfo", {})
        sale_info = item.get("saleInfo", {})
        access_info = item.get("accessInfo", {})

        return {
            "title": volume_info.get("title"),
            "subtitle": volume_info.get("subtitle"),
            "description": volume_info.get("description"),
            "authors": volume_info.get("authors", []),
            "publisher": volume_info.get("publisher"),
            "published_year": volume_info.get("publishedDate", "").split("-")[0],
            "isbn_10": next((i["identifier"] for i in volume_info.get("industryIdentifiers", [])
                             if i["type"] == "ISBN_10"), None),
            "isbn_13": next((i["identifier"] for i in volume_info.get("industryIdentifiers", [])
                             if i["type"] == "ISBN_13"), None),
            "page_count": volume_info.get("pageCount"),
            "categories": volume_info.get("categories", []),
            "language_code": volume_info.get("language"),
            "maturity_rating": volume_info.get("maturityRating"),
            "average_rating": volume_info.get("averageRating"),
            "ratings_count": volume_info.get("ratingsCount"),
            "physical_format": "Hardcover" if sale_info.get("isEbook", False) else "Paperback",
            "price_info": {

                "listPrice": sale_info.get("listPrice", {}).get("amount"),
                "retailPrice": sale_info.get("retailPrice", {}).get("amount"),
                "currency": sale_info.get("listPrice", {}).get("currencyCode"),
                "saleability": sale_info.get("saleability"),
                "buyLink": sale_info.get("buyLink"),
                "onSaleDate": sale_info.get("onSaleDate"),

            },
            "isEbook": sale_info.get("isEbook"),
            "google_books_id": item.get("id"),
            "google_preview_link": volume_info.get("previewLink"),
            "google_info_link": volume_info.get("infoLink"),
            "google_canonical_link": volume_info.get("canonicalVolumeLink"),
        }


class OpenLibraryAPI:
    """Handles Open Library API interactions with extended metadata parsing."""

    def __init__(self):
        self.base_url = "https://openlibrary.org"

    def fetch_by_isbn(self, isbn: str) -> Optional[Dict]:
        """Fetch book data by ISBN from Open Library."""
        url = f"{self.base_url}/api/books"
        params = {"bibkeys": f"ISBN:{isbn}", "format": "json", "jscmd": "data"}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            book_data = response.json().get(f"ISBN:{isbn}")
            if book_data:
                return self._parse_book_data(book_data)
        return None

    def _parse_book_data(self, book_data: Dict) -> Dict:
        """Parse Open Library book data."""
        authors = book_data.get("authors", [])

        author_details = [
            {"name": author.get("name"), "key": author.get("key").split("/")[-1] if author.get("key") else None}
            for author in authors
        ]

        preview_url = None
        if book_data.get("ebooks"):
            for ebook in book_data["ebooks"]:
                if ebook.get("preview_url"):
                 preview_url = ebook["preview_url"]
                 break

        return {
            "title": book_data.get("title"),
            "subtitle": book_data.get("subtitle"),
            "authors": author_details,
            "publisher": book_data.get("publishers", [{}])[0].get("name"),
            "published_year": book_data.get("publish_date", "").split()[-1],
            "isbn_10": book_data.get("identifiers", {}).get("isbn_10", [None])[0],
            "isbn_13": book_data.get("identifiers", {}).get("isbn_13", [None])[0],
            "page_count": book_data.get("number_of_pages"),
            "subjects": [subject.get("name") for subject in book_data.get("subjects", [])],
            "ebook_url": preview_url
        }


def fetch_and_merge_data(api_keys: List[str], query: str, num_books: int = 50) -> List[Dict]:
    """Fetch and merge data from Google Books and OpenLibrary."""
    google_books_api = GoogleBooksAPI(api_keys)
    open_library_api = OpenLibraryAPI()

    google_books = google_books_api.search_books(query, max_results=num_books)
    merged_books = []

    for book in google_books:
        isbn_13 = book.get("isbn_13")
        open_library_data = open_library_api.fetch_by_isbn(isbn_13) if isbn_13 else None

        # Merge data
        merged_data = {**book, **(open_library_data or {})}
        merged_books.append(merged_data)
        print(merged_data)

    return merged_books

