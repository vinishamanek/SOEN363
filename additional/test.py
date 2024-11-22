import json
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
    """Handles Google Books API interactions."""

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
        retries, delay = 5, 1  # Increase retries to handle multiple keys
        for attempt in range(retries):
            params["key"] = self.get_current_api_key()
            try:
                response = requests.get(self.base_url, params=params)
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Too many requests
                    print(f"Rate limit reached for key {self.current_key_index}. Retrying...")
                    self.rotate_api_key()  # Switch to the next key
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
            except requests.RequestException as e:
                print(f"Request error: {e}")
        print("All keys exhausted or maximum retries reached. Skipping this request.")
        return None

    def search_books_randomly(self, max_results: int = 40) -> List[str]:
        """Fetch random books using random characters as queries."""
        random_query = random.choice(string.ascii_lowercase + string.digits)
        params = {"q": random_query, "maxResults": max_results}
        response = self._api_request(params)
        if not response:
            return []
        return self._extract_isbns(response.json())

    def fetch_book_data(self, isbn: str) -> Optional[Dict]:
        """Fetch book data by ISBN."""
        params = {"q": f"isbn:{isbn}"}
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
        dimensions = volume_info.get("dimensions", {})

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
            "dimensions": dimensions,
            "sale_info": sale_info,
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


class OpenLibraryAPI:
    """Handles Open Library API interactions."""

    def __init__(self):
        self.base_url = "https://openlibrary.org"

    def fetch_book_data(self, isbn: str) -> Optional[Dict]:
        """Fetch book data by ISBN from Open Library."""
        url = f"{self.base_url}/search.json"
        params = {"q": f"isbn:{isbn}"}
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data["docs"]:
                    return self._parse_book_data(data["docs"][0])
        except requests.RequestException as e:
            print(f"Open Library API request failed: {e}")
        return None

    def _parse_book_data(self, book_data: dict):
        """Parse book data from Open Library API response."""
        return {
            "title": book_data.get("title"),
            "authors": book_data.get("author_name", []),
            "publisher": book_data.get("publisher", ["Unknown Publisher"])[0],
            "published_year": book_data.get("first_publish_year"),
            "isbn_10": book_data.get("isbn", [None])[0],
            "isbn_13": book_data.get("isbn", [None])[1] if len(book_data.get("isbn", [])) > 1 else None,
            "language": book_data.get("language", ["en"])[0],
        }

    def fetch_author_data(self, author_name: str) -> Optional[Dict]:
        """Fetch author data from Open Library."""
        url = f"{self.base_url}/search/authors.json?q={author_name}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if data.get("docs"):
                    return self._parse_author_data(data["docs"][0])
        except requests.RequestException as e:
            print(f"Error fetching author data for {author_name}: {e}")
        return None

    def _parse_author_data(self, data: Dict) -> Dict:
        """Parse author data from Open Library API response."""
        return {
            "first_name": data.get("name", "").split(" ")[0],
            "last_name": " ".join(data.get("name", "").split(" ")[1:]),
            "birth_date": data.get("birth_date"),
            "bio": data.get("bio"),
            "wikipedia": data.get("wikipedia"),
            "remote_ids": data.get("remote_ids", {}),
            "alternate_names": data.get("alternate_names", [])
        }


def insert_or_update_book_data(connection, book_data: Dict, openlibrary_api):
    """Insert or update book and author data in the database."""
    try:
        # Ensure the book has a valid ISBN
        if not book_data.get("isbn_13") and not book_data.get("isbn_10"):
            print(f"Skipping book due to missing ISBN: {book_data.get('title')}")
            return

        with connection.cursor() as cursor:
            # Insert or update publisher information
            publisher_id = None
            if book_data.get("publisher"):
                cursor.execute("""
                    INSERT INTO Publisher (name)
                    VALUES (%s)
                    ON CONFLICT (name) DO NOTHING
                    RETURNING publisher_id
                """, (book_data["publisher"],))
                result = cursor.fetchone()
                publisher_id = result[0] if result else None

            # Insert or update book information
            cursor.execute("""
                INSERT INTO Book (
                    isbn10, isbn13, title, description, language_code, publication_year,
                    publisher_id, page_count, average_rating, ratings_count, maturity_rating,
                    google_books_id, google_preview_link, google_info_link, google_canonical_link
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (isbn13) DO NOTHING
            """, (
                book_data.get("isbn_10"), book_data.get("isbn_13"), book_data.get("title"),
                book_data.get("description"), book_data.get("language_code", "en"),
                book_data.get("published_year"), publisher_id, book_data.get("page_count"),
                book_data.get("average_rating"), book_data.get("ratings_count"),
                book_data.get("maturity_rating"), book_data.get("google_books_id"),
                book_data.get("google_preview_link"), book_data.get("google_info_link"),
                book_data.get("google_canonical_link")
            ))

            # Insert author data
            authors = book_data.get("authors", [])
            for author_name in authors:
                # Attempt to fetch detailed author data using Open Library
                author_data = openlibrary_api.fetch_author_data(author_name)
                if author_data:
                    cursor.execute("""
                        INSERT INTO Author (
                            first_name, last_name, birth_date, biography, website, wikipedia_url,
                            goodreads_author_id, alternate_names
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (first_name, last_name) DO NOTHING
                    """, (
                        author_data.get("first_name"), author_data.get("last_name"),
                        author_data.get("birth_date"), author_data.get("bio"),
                        author_data.get("website"), author_data.get("wikipedia"),
                        author_data.get("remote_ids", {}).get("goodreads"),
                        author_data.get("alternate_names")
                    ))

                    # Link the author to the book
                    cursor.execute("""
                        INSERT INTO BookAuthor (book_id, author_id, role)
                        VALUES (
                            (SELECT book_id FROM Book WHERE isbn13 = %s),
                            (SELECT author_id FROM Author WHERE first_name = %s AND last_name = %s),
                            'Author'
                        )
                    """, (
                        book_data.get("isbn_13"),
                        author_data.get("first_name"),
                        author_data.get("last_name")
                    ))

            # Insert genres/categories
            categories = book_data.get("categories", [])
            for category in categories:
                cursor.execute("""
                    INSERT INTO Genre (name)
                    VALUES (%s)
                    ON CONFLICT (name) DO NOTHING
                """, (category,))
                cursor.execute("""
                    INSERT INTO BookGenre (book_id, genre_id)
                    VALUES (
                        (SELECT book_id FROM Book WHERE isbn13 = %s),
                        (SELECT genre_id FROM Genre WHERE name = %s)
                    )
                    ON CONFLICT DO NOTHING
                """, (book_data.get("isbn_13"), category))

            # Insert physical book information if dimensions are provided
            dimensions = book_data.get("dimensions", {})
            # print(dimensions)
            if dimensions:
                cursor.execute("""
                    INSERT INTO PhysicalBook (
                        book_id, height_mm, width_mm, thickness_mm, format
                    )
                    VALUES (
                        (SELECT book_id FROM Book WHERE isbn13 = %s),
                        %s, %s, %s, %s
                    )
                """, (
                    book_data.get("isbn_13"), dimensions.get("height"), dimensions.get("width"),
                    dimensions.get("thickness"), book_data.get("physical_format", "Paperback")
                ))

            # Insert price history if sale information is provided
            sale_info = book_data.get("sale_info", {})
            list_price = sale_info.get("list_price", {})
            if list_price:
                cursor.execute("""
                    INSERT INTO PriceHistory (
                        book_id, price, currency_code, effective_date, source
                    )
                    VALUES (
                        (SELECT book_id FROM Book WHERE isbn13 = %s),
                        %s, %s, NOW(), 'Google Books'
                    )
                """, (
                    book_data.get("isbn_13"), list_price.get("amount"), list_price.get("currencyCode")
                ))

            print(f"Processed book: {book_data['title']}")
    except psycopg2.Error as e:
        print(f"Database operation error: {e}")

def merge_book_data(google_data: Optional[Dict], openlibrary_data: Optional[Dict]) -> Dict:
    """Merge book data from Google Books and Open Library APIs."""
    if not google_data and not openlibrary_data:
        return {}

    # Use Google Books data as the base
    merged_data = google_data or {}

    # Fill in missing fields with Open Library data
    if openlibrary_data:
        merged_data["title"] = merged_data.get("title") or openlibrary_data.get("title")
        merged_data["authors"] = merged_data.get("authors") or openlibrary_data.get("authors")
        merged_data["publisher"] = merged_data.get("publisher") or openlibrary_data.get("publisher")
        merged_data["published_year"] = merged_data.get("published_year") or openlibrary_data.get("published_year")
        merged_data["isbn_10"] = merged_data.get("isbn_10") or openlibrary_data.get("isbn_10")
        merged_data["isbn_13"] = merged_data.get("isbn_13") or openlibrary_data.get("isbn_13")
        merged_data["language_code"] = merged_data.get("language_code") or openlibrary_data.get("language")

    return merged_data

def main():
    connection = connect_to_db()
    if not connection:
        return

    api_keys = [
        os.getenv("GOOGLE_API_KEY_1"),
        os.getenv("GOOGLE_API_KEY_2"),
        os.getenv("GOOGLE_API_KEY_3"),
    ]  # Add as many keys as you have
    google_api = GoogleBooksAPI(api_keys=api_keys)
    openlibrary_api = OpenLibraryAPI()

    for _ in range(50):  # Adjust as needed
        try:
            isbns = google_api.search_books_randomly()
            for isbn in isbns:
                google_data = google_api.fetch_book_data(isbn)
                openlibrary_data = openlibrary_api.fetch_book_data(isbn)

                if google_data or openlibrary_data:
                    # Combine data from both APIs
                    combined_data = merge_book_data(google_data, openlibrary_data)
                    print(json.dumps(combined_data, indent=4, ensure_ascii=False))

                    insert_or_update_book_data(connection, combined_data, openlibrary_api)
                else:
                    print(f"No data found for ISBN: {isbn}")
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(10)  # Add a longer delay if an error occurs
    connection.close()

if __name__ == "__main__":
    main()
