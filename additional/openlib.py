import requests
from typing import List, Dict, Any

class OpenLibraryDataCollector:
    def __init__(self):
        self.base_url = "https://openlibrary.org"
        self.book_api_url = f"{self.base_url}/api/books"
        self.search_api_url = f"{self.base_url}/search.json"
        self.author_api_url = f"{self.base_url}/authors"
        print("Initialized OpenLibraryDataCollector")

    def fetch_by_isbn(self, isbn: str) -> Dict:
        params = {
            'bibkeys': f'ISBN:{isbn}',
            'format': 'json',
            'jscmd': 'data'
        }
        print(f"Fetching data for ISBN: {isbn}")
        response = requests.get(self.book_api_url, params=params)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
            return {}

        book_data = response.json().get(f'ISBN:{isbn}', {})
        if not book_data:
            print(f"No data found for ISBN: {isbn}")
            return {}

        print(f"Fetched book data: {book_data}")
        authors_raw = book_data.get('authors', [])
        author_details = []
        
        for author in authors_raw:
            author_name = author.get('name', '')
            author_url = author.get('url', '')
            author_id = author_url.split('/')[-1] if author_url else None
            
            author_info = {"name": author_name}
            if author_id:
                print(f"Fetching details for author: {author_name}")
                additional_info = self.fetch_author_details(author_id)
                if additional_info:
                    author_info.update(additional_info)
            author_details.append(author_info)

        formatted_data = {
            "title": book_data.get('title'),
            "subtitle": book_data.get('subtitle'),
            "authors": author_details,
            "publisher": next((pub.get('name') for pub in book_data.get('publishers', [])), None),
            "publish_date": book_data.get('publish_date'),
            "pages": book_data.get('number_of_pages'),
            "cover_url": book_data.get('cover', {}).get('large'),
            "identifiers": book_data.get('identifiers', {}),
            "subjects": [subject.get('name') for subject in book_data.get('subjects', [])],
            "notes": book_data.get('notes'),
            "url": book_data.get('url')
        }

        print(f"Formatted data for ISBN: {isbn}: {formatted_data}")
        return formatted_data

    def fetch_author_details(self, author_id: str) -> Dict:
        url = f"{self.author_api_url}/{author_id}.json"
        print(f"Fetching author details from URL: {url}")
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                print(f"Fetched author details: {data}")
                return {
                    "birth_date": data.get("birth_date"),
                    "death_date": data.get("death_date"),
                    "bio": data.get("bio", {}).get("value") if isinstance(data.get("bio"), dict) else data.get("bio"),
                    "wikipedia_url": data.get("wikipedia")
                }
        except Exception as e:
            print(f"Error fetching author details: {e}")
        return {}

    @staticmethod
    def format_for_display(book: Dict) -> str:
        if not book:
            return "No book data found"
            
        author_names = [author.get('name', 'Unknown') for author in book.get('authors', [])]
        return f"""
        Title: {book.get('title', 'N/A')}
        Subtitle: {book.get('subtitle', 'N/A')}
        Authors: {', '.join(author_names)}
        Publisher: {book.get('publisher', 'N/A')}
        Published: {book.get('publish_date', 'N/A')}
        Pages: {book.get('pages', 'N/A')}
        Subjects: {', '.join(book.get('subjects', []))}
        ISBNs: {', '.join(f"{k}: {v}" for k, v in book.get('identifiers', {}).items())}
        URL: {book.get('url', 'N/A')}
        """

if __name__ == "__main__":
    collector = OpenLibraryDataCollector()
    book = collector.fetch_by_isbn("9780590353427")
    print(collector.format_for_display(book))