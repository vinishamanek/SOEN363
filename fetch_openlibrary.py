import requests
from typing import Optional, Dict, List, Any


class OpenLibraryDataCollector:
    def __init__(self):
        """Initialize the Open Library Data Collector."""
        self.book_api_url = "https://openlibrary.org/api/books"
        self.search_api_url = "https://openlibrary.org/search.json"
        self.author_api_url = "https://openlibrary.org/authors"

    def fetch_openlibrary_data(self, query: str, start_index: int = 0, max_results: int = 10) -> List[Dict]:
        """Fetch data from Open Library API."""
        params = {
            'q': query,
            'offset': start_index,
            'limit': max_results
        }

        print(f"Fetching data with query: {query}")
        response = requests.get(self.search_api_url, params=params)
        if response.status_code == 200:
            books_data = []
            data = response.json()

            for item in data.get('docs', []):
                # Extract book details
                book_data = {
                    "title": item.get("title"),
                    "authors": [{"name": author} for author in item.get("author_name", [])],
                    "publisher": item.get("publisher", []),
                    "publish_date": item.get("first_publish_year"),
                    "language_code": item.get("language", []),
                    "isbn_10": [isbn for isbn in item.get("isbn", []) if len(isbn) == 10],
                    "isbn_13": [isbn for isbn in item.get("isbn", []) if len(isbn) == 13],
                    "openlibrary_work_id": item.get("key"),
                    "number_of_pages": item.get("number_of_pages_median"),
                    "description": item.get("first_sentence"),
                    "subjects": item.get("subject", []),
                }

                books_data.append(book_data)

            return books_data
        else:
            print(f"Error fetching data: {response.status_code}, {response.text}")
            return []

    def fetch_author_details(self, author_id: str) -> Dict:
        """Fetch author details by Open Library Author ID."""
        url = f"{self.author_api_url}/{author_id}.json"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return {
                "name": data.get("name"),
                "alternate_names": data.get("alternate_names", []),
                "birth_date": data.get("birth_date", None),
                "bio": data.get("bio", {}).get("value") if isinstance(data.get("bio"), dict) else data.get("bio"),
                "wikipedia_url": data.get("wikipedia", None),
                "key": data.get("key"),
            }
        else:
            print(f"Error fetching author details: {response.status_code}, {response.text}")
            return {}

    @staticmethod
    def format_for_display(book: Dict) -> str:
        """Format book data for display."""
        authors = ", ".join([author['name'] for author in book['authors']])
        publishers = ", ".join(book.get('publisher', []))
        subjects = ", ".join(book.get('subjects', []))
        return f"""
        Book Information:
        -----------------
        Title: {book['title']}
        Authors: {authors}
        Publishers: {publishers}
        Publish Date: {book['publish_date']}
        Language Code: {", ".join(book['language_code'])}
        ISBN-10: {", ".join(book['isbn_10'])}
        ISBN-13: {", ".join(book['isbn_13'])}
        Open Library Work ID: {book['openlibrary_work_id']}
        Number of Pages: {book['number_of_pages']}
        Description: {book['description']}
        Subjects: {subjects}
        """

    @staticmethod
    def format_author_for_display(author: Dict) -> str:
        """Format author data for display."""
        alternate_names = ", ".join(author.get("alternate_names", []))
        return f"""
        Author Information:
        -------------------
        Name: {author['name']}
        Alternate Names: {alternate_names}
        Birth Date: {author['birth_date']}
        Bio: {author['bio']}
        Wikipedia URL: {author['wikipedia_url']}
        """

if __name__ == "__main__":
    collector = OpenLibraryDataCollector()

    books = collector.fetch_openlibrary_data(query="Harry Potter", max_results=3)
    for book in books:
        print(collector.format_for_display(book))

    author_id = "OL23919A"  # J.K. Rowling
    author = collector.fetch_author_details(author_id)
    print(collector.format_author_for_display(author))
