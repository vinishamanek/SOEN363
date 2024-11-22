import os
import time
import random
import string
from typing import List, Dict, Optional
import psycopg2
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def connect_to_db():
    """Establish a database connection."""
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        connection.autocommit = True
        print("Connected to the database.")
        return connection
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        return None


class GoogleBooksAPI:
    def __init__(self, api_key: str):
        self.base_url = "https://www.googleapis.com/books/v1/volumes"
        self.api_key = api_key

    def search_books_randomly(self, max_results: int = 40) -> List[str]:
        """Fetch random books using random characters as queries."""
        random_query = random.choice(string.ascii_lowercase + string.digits)
        params = {"q": random_query, "maxResults": max_results, "key": self.api_key}
        response = self._api_request(params)
        if not response:
            return []
        return self._extract_isbns(response.json())

    def fetch_book_data(self, isbn: str) -> Optional[Dict]:
        """Fetch book data by ISBN."""
        params = {"q": f"isbn:{isbn}", "key": self.api_key}
        response = self._api_request(params)
        if not response:
            return None
        return self._parse_book_data(response.json())

    def _extract_isbns(self, data: Dict) -> List[str]:
        """Extract ISBNs from Google Books API response."""
        isbns = []
        for item in data.get("items", []):
            for identifier in item.get("volumeInfo", {}).get("industryIdentifiers", []):
                if identifier.get("type") in ["ISBN_10", "ISBN_13"]:
                    isbns.append(identifier.get("identifier"))
        return list(set(isbns))

    def _parse_book_data(self, data: Dict) -> Optional[Dict]:
        """Parse book data from Google Books API response."""
        if not data.get("items"):
            return None
        volume_info = data["items"][0].get("volumeInfo", {})
        sale_info = data["items"][0].get("saleInfo", {})

        published_date = volume_info.get("publishedDate", "")
        publication_year = published_date[:4] if published_date.isdigit() else None

        return {
            "title": volume_info.get("title"),
            "authors": volume_info.get("authors", []),
            "publisher": volume_info.get("publisher", "Unknown Publisher"),
            "published_year": publication_year,
            "description": volume_info.get("description"),
            "isbn_10": self._get_isbn(volume_info, "ISBN_10"),
            "isbn_13": self._get_isbn(volume_info, "ISBN_13"),
            "page_count": volume_info.get("pageCount"),
            "categories": volume_info.get("categories", []),
            "average_rating": volume_info.get("averageRating"),
            "ratings_count": volume_info.get("ratingsCount"),
            "google_books_id": data["items"][0].get("id"),
            "maturity_rating": volume_info.get("maturityRating"),
            "image_links": volume_info.get("imageLinks", {}),
            "google_preview_link": volume_info.get("previewLink"),
            "google_info_link": volume_info.get("infoLink"),
            "google_canonical_link": volume_info.get("canonicalVolumeLink"),
            "physical_format": "Hardcover" if sale_info.get("isEbook", False) else "Paperback",
        }

    def _get_isbn(self, volume_info: Dict, isbn_type: str) -> Optional[str]:
        """Get the specified ISBN type from volume info."""
        for identifier in volume_info.get("industryIdentifiers", []):
            if identifier.get("type") == isbn_type:
                return identifier.get("identifier")
        return None

    def _api_request(self, params: Dict) -> Optional[requests.Response]:
        """Handle API requests with retries."""
        retries, delay = 3, 1
        for attempt in range(retries):
            try:
                response = requests.get(self.base_url, params=params)
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    print(f"Rate limit reached. Retrying in {delay}s...")
                    time.sleep(delay)
                    delay *= 2
            except requests.RequestException as e:
                print(f"Request error: {e}")
        return None


class PublisherDataCollector:
    def __init__(self):
        self.api_url = "https://openlibrary.org/search.json"

    def fetch_publisher(self, publisher_name: str) -> Optional[Dict]:
        """Fetch publisher details from Open Library."""
        if not publisher_name:
            return None
        try:
            response = requests.get(f"{self.api_url}?publisher={publisher_name}")
            if response.status_code == 200:
                docs = response.json().get("docs", [])
                if docs:
                    return {
                        "name": publisher_name,
                        "description": docs[0].get("text", [None])[0],
                    }
        except requests.RequestException as e:
            print(f"Error fetching publisher data: {e}")
        return None


def insert_or_update_book_data(connection, book_data: Dict, publisher_collector: PublisherDataCollector):
    """Insert or update book and publisher data in the database."""
    if not book_data.get("isbn_13"):
        print("Skipping book due to missing ISBN.")
        return

    publisher_data = publisher_collector.fetch_publisher(book_data["publisher"])
    try:
        with connection.cursor() as cursor:
            publisher_id = None
            if publisher_data:
                cursor.execute("""
                    INSERT INTO Publisher (name, description)
                    VALUES (%s, %s)
                    ON CONFLICT (name) DO NOTHING
                    RETURNING publisher_id
                """, (publisher_data["name"], publisher_data["description"]))
                publisher_id = cursor.fetchone()

            cursor.execute("""
                INSERT INTO Book (isbn, title, description, publication_year, 
                                  publisher_id, page_count, average_rating, ratings_count, google_books_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (isbn) DO NOTHING
            """, (
                book_data["isbn_13"], book_data["title"], book_data["description"],
                book_data["published_year"], publisher_id, book_data["page_count"],
                book_data["average_rating"], book_data["ratings_count"], book_data["google_books_id"]
            ))
            print(f"Book processed: {book_data['title']}")
    except psycopg2.Error as e:
        print(f"Database operation error: {e}")


def main():
    connection = connect_to_db()
    if not connection:
        return

    google_api = GoogleBooksAPI(api_key=os.getenv("GOOGLE_API_KEY"))
    publisher_collector = PublisherDataCollector()

    for _ in range(50):  # Adjust as needed
        isbns = google_api.search_books_randomly()
        for isbn in isbns:
            book_data = google_api.fetch_book_data(isbn)
            if book_data:
                insert_or_update_book_data(connection, book_data, publisher_collector)
    connection.close()


if __name__ == "__main__":
    main()
