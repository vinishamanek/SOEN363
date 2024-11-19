import requests
from typing import List, Dict, Any, Optional


class OpenLibraryDataCollector:
    def __init__(self):
        """Initialize the Open Library Data Collector."""
        self.book_api_url = "https://openlibrary.org/api/books"
        self.search_api_url = "https://openlibrary.org/search.json"
        self.author_api_url = "https://openlibrary.org/authors"
        self.works_api_url = "https://openlibrary.org/works"
        self.editions_api_url = "https://openlibrary.org/books"

    def fetch_book_by_isbn(self, isbn: str) -> Dict:
        """Fetch book details by ISBN."""
        params = {
            'bibkeys': f'ISBN:{isbn}',
            'format': 'json',
            'jscmd': 'data'
        }
        response = requests.get(self.book_api_url, params=params)
        if response.status_code == 200:
            return response.json().get(f'ISBN:{isbn}', {})
        else:
            print(f"Error fetching book by ISBN: {response.status_code}, {response.text}")
            return {}

    def fetch_book_details(self, query: str, max_results: int = 10) -> List[Dict]:
        """Fetch detailed book data including extended fields."""
        params = {'q': query, 'limit': max_results}
        response = requests.get(self.search_api_url, params=params)
        if response.status_code == 200:
            books_data = []
            for doc in response.json().get('docs', []):
                authors = [{"name": name, "key": key} for name, key in zip(
                    doc.get("author_name", []), doc.get("author_key", [])
                )]
                author_details = [self.fetch_author_details(author["key"]) for author in authors]

                book = {
                    "title": doc.get("title"),
                    "authors": authors,
                    "author_details": author_details,
                    "number_of_pages": doc.get("number_of_pages_median"),
                    "pagination": doc.get("pagination"),
                    "weight": doc.get("weight"),
                    "isbn_10": [isbn for isbn in doc.get("isbn", []) if len(isbn) == 10][-1:],
                    "isbn_13": [isbn for isbn in doc.get("isbn", []) if len(isbn) == 13][-1:],
                    "openlibrary_edition_id": doc.get("edition_key", [None])[0],
                    "openlibrary_work_id": doc.get("key").split('/')[-1],  
                    "google_books_id": doc.get("google_books_id"),
                    "goodreads_id": doc.get("goodreads_id"),
                    "librarything_id": doc.get("librarything_id"),
                    "subjects": doc.get("subject", []),
                    "publish_date": doc.get("first_publish_year"),
                    "publisher": doc.get("publisher", []),
                    "publish_place": doc.get("publish_place"),
                    "notes": doc.get("notes"),
                    "cover_urls": {
                        "small": f"https://covers.openlibrary.org/b/id/{doc.get('cover_i', 0)}-S.jpg",
                        "medium": f"https://covers.openlibrary.org/b/id/{doc.get('cover_i', 0)}-M.jpg",
                        "large": f"https://covers.openlibrary.org/b/id/{doc.get('cover_i', 0)}-L.jpg"
                    },
                    "preview_url": f"https://archive.org/details/{doc.get('ocaid')}" if doc.get("ocaid") else None,
                    "borrow_url": f"https://openlibrary.org/books/{doc.get('key')}/borrow" if doc.get("key") else None,
                    "availability": "Borrowable" if doc.get("ocaid") else "Not available"
                }
                books_data.append(book)
            return books_data
        else:
            print(f"Error fetching book details: {response.status_code}, {response.text}")
            return []

    def fetch_author_details(self, author_id: str) -> Dict:
        """Fetch author details by Open Library Author ID."""
        url = f"{self.author_api_url}/{author_id}.json"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return {
                "full_name": data.get("name"),
                "alternate_names": data.get("alternate_names", []),
                "personal_name": data.get("personal_name"),
                "birth_date": data.get("birth_date"),
                "title": data.get("title"),
                "bio": data.get("bio", {}).get("value") if isinstance(data.get("bio"), dict) else data.get("bio"),
                "entity_type": data.get("type", {}).get("key"),
                "wikipedia_url": data.get("wikipedia"),
                "official_website": next((link.get("url") for link in data.get("links", []) if "Official" in link.get("title", "")), None),
                "remote_identifiers": data.get("remote_ids", {}),
                "photos": [f"https://covers.openlibrary.org/b/id/{photo}-L.jpg" for photo in data.get("photos", [])],
                "openlibrary_key": data.get("key")
            }
        else:
            print(f"Error fetching author details: {response.status_code}, {response.text}")
            return {}

    def fetch_work_details(self, work_id: str) -> Dict:
        """Fetch work details by Open Library Work ID."""
        url = f"{self.works_api_url}/{work_id}.json"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching work details: {response.status_code}, {response.text}")
            return {}

    def fetch_edition_details(self, edition_id: str) -> Dict:
        """Fetch edition details by Open Library Edition ID."""
        url = f"{self.editions_api_url}/{edition_id}.json"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching edition details: {response.status_code}, {response.text}")
            return {}

    def fetch_cover_image(self, cover_id: int, size: str = "L") -> str:
        """Fetch cover image URL by cover ID and size."""
        return f"{self.covers_api_url}/id/{cover_id}-{size}.jpg"

    def collect_all_data_by_isbn(self, isbn: str) -> Dict:
        """Collect all relevant data for a given ISBN."""
        book_data = self.fetch_book_by_isbn(isbn)
        if not book_data:
            return {}

        edition_id = book_data.get('identifiers', {}).get('openlibrary', [None])[0]
        work_id = book_data.get('works', [{}])[0].get('key', '').split('/')[-1] if book_data.get('works') else None

        edition_data = self.fetch_edition_details(edition_id) if edition_id else {}
        work_data = self.fetch_work_details(work_id) if work_id else {}

        return {
            "book_data": book_data,
            "edition_data": edition_data,
            "work_data": work_data,
        }

    @staticmethod
    def format_book_data(book: Dict) -> str:
        """Format book data for display."""
        authors = ", ".join([f"{author['name']} (Key: {author.get('key', 'N/A')})" for author in book["authors"]])
        publishers = ", ".join(book.get("publisher", []))
        subjects = ", ".join(book.get("subjects", []))
        return f"""
        Book Information:
        -----------------
        Title: {book['title']}
        Authors: {authors}
        Number of Pages: {book['number_of_pages']}
        Pagination: {book['pagination']}
        Weight: {book['weight']}
        isbn_10: {book['isbn_10']},  
        isbn_13: {book['isbn_13']},  
        Open Library Edition ID: {book['openlibrary_edition_id']}
        Open Library Work ID: {book['openlibrary_work_id']}
        Google Books ID: {book['google_books_id']}
        Goodreads ID: {book['goodreads_id']}
        LibraryThing ID: {book['librarything_id']}
        Subjects: {subjects}
        Publish Date: {book['publish_date']}
        Publisher: {publishers}
        Publish Place: {book['publish_place']}
        Notes: {book['notes']}
        Cover URLs: {book['cover_urls']}
        Preview URL: {book['preview_url']}
        Borrow URL: {book['borrow_url']}
        Availability: {book['availability']}
        """
        
    def _insert_data(self, table: str, data: Dict):
        """Insert data into a PostgreSQL table."""
        if not self.cursor:
            print("Database connection not established.")
            return

        try:
            columns = ", ".join(data.keys())
            values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])
            insert_query = f"INSERT INTO {table} ({columns}) VALUES ({values})"
            self.cursor.execute(insert_query)
            self.connection.commit()
            print(f"Data inserted into {table} successfully.")
        except Exception as e:
            print(f"Error inserting data into {table}: {e}")

    @staticmethod
    def format_author_data(author: Dict) -> str:
        """Format author data for display."""
        alternate_names = ", ".join(author.get("alternate_names", []))
        identifiers = "\n".join([f"{key}: {value}" for key, value in author.get("remote_identifiers", {}).items()])
        photos = "\n".join(author.get("photos", []))
        return f"""
        Author Information:
        -------------------
        Full Name: {author['full_name']}
        Alternate Names: {alternate_names}
        Personal Name: {author['personal_name']}
        Birth Date: {author['birth_date']}
        Title: {author['title']}
        Bio: {author['bio']}
        Entity Type: {author['entity_type']}
        Wikipedia URL: {author['wikipedia_url']}
        Official Website: {author['official_website']}
        Remote Identifiers:
        {identifiers}
        Photos:
        {photos}
        Open Library Key: {author['openlibrary_key']}
        """

    @staticmethod
    def print_collected_data(data: Dict) -> None:
        """Print the collected data in a readable format."""
        if not data:
            print("No data found.")
            return
        
        print("\nEdition Data:")
        edition_data = data.get("edition_data", {})
        if edition_data:
            print(edition_data)
        else:
            print("No edition data available.")

def generate_isbn_range(start_isbn, end_isbn, prefix=''):
    """
    Generate a range of ISBNs with optional prefix
    
    Args:
        start_isbn (str): Starting ISBN
        end_isbn (str): Ending ISBN
        prefix (str, optional): Prefix to add to ISBNs
    
    Returns:
        List of ISBNs in the specified range
    """
    start = int(start_isbn)
    end = int(end_isbn)
    
    return [f"{prefix}{str(isbn).zfill(len(start_isbn))}" for isbn in range(start, end + 1)]


if __name__ == "__main__":
    collector = OpenLibraryDataCollector()

    isbns = generate_isbn_range('9780000000033', '9780000000035')

    for isbn in isbns:
        print(f"\n--- Processing ISBN: {isbn} ---")
        
        isbn_data = collector.collect_all_data_by_isbn(isbn)
        
        books = collector.fetch_book_details(query=isbn, max_results=1)
        
        if books:
            book = books[0]
            print(collector.format_book_data(book))
            
            for author in book["author_details"]:
                print(collector.format_author_data(author))
        
        collector.print_collected_data(isbn_data)
        