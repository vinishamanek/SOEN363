import requests
from typing import Dict, List
from dotenv import load_dotenv
import os

load_dotenv()

class GoogleBooksDataCollector:
    def __init__(self, api_key: str):
        self.base_url = "https://www.googleapis.com/books/v1/volumes"
        self.api_key = api_key

    def fetch_by_isbn(self, isbn: str) -> List[Dict]:
        return self.fetch_google_books_data(f'isbn:{isbn}')

    def fetch_google_books_data(self, query: str, start_index: int = 0, max_results: int = 10) -> List[Dict]:
        params = {
            'q': query,
            'startIndex': start_index,
            'maxResults': max_results,
            'key': self.api_key,
            'projection': 'full',
            'printType': 'all'
        }

        response = requests.get(self.base_url, params=params)
        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
            return []

        print("Fetched raw data:", response.json())

        books_data = []
        data = response.json()
        
        for item in data.get('items', []):
            volume_info = item.get('volumeInfo', {})
            sales_info = item.get('saleInfo', {})
            access_info = item.get('accessInfo', {})
            search_info = item.get('searchInfo', {})
            
            identifiers = {
                identifier.get('type'): identifier.get('identifier')
                for identifier in volume_info.get('industryIdentifiers', [])
            }

            print("Volume Info:", volume_info)
            print("Sales Info:", sales_info)
            print("Access Info:", access_info)
            print("Search Info:", search_info)
            print("Identifiers:", identifiers)

            author_details = [{
                "full_name": author,
                "first_name": author.split()[0] if author else None,
                "last_name": author.split()[-1] if author else None,
                "birth_date": None,
                "death_date": None,
                "profile_url": None,
                "biography": None,
                "other_works": []
            } for author in volume_info.get('authors', [])]

            print("Author Details:", author_details)

            book_data = {
                "google_books_id": item.get('id'),
                "etag": item.get('etag'),
                "self_link": item.get('selfLink'),
                "volume_info": {
                    "title": volume_info.get('title'),
                    "subtitle": volume_info.get('subtitle'),
                    "authors": author_details,
                    "publisher_info": {
                        "name": volume_info.get('publisher'),
                        "description": None,
                        "website": None,
                        "founded_year": None,
                        "location": None,
                        "imprint": None
                    },
                    "published_date": volume_info.get('publishedDate'),
                    "description": volume_info.get('description'),
                    "identifiers": identifiers,
                    "page_count": volume_info.get('pageCount'),
                    "dimensions": volume_info.get('dimensions'),
                    "categories": volume_info.get('categories', []),
                    "average_rating": volume_info.get('averageRating'),
                    "ratings_count": volume_info.get('ratingsCount'),
                    "language": volume_info.get('language'),
                    "preview_link": volume_info.get('previewLink'),
                    "info_link": volume_info.get('infoLink'),
                    "canonical_volume_link": volume_info.get('canonicalVolumeLink')
                },
                "edition_info": {
                    "name": volume_info.get('title'),
                    "edition_number": None,
                    "format": "ebook" if volume_info.get('isEbook', False) else "physical",
                    "print_type": volume_info.get('printType'),
                    "maturity_rating": volume_info.get('maturityRating'),
                    "main_category": volume_info.get('mainCategory'),
                    "dimensions": volume_info.get('dimensions', {}),
                    "print_edition_isbn": identifiers.get('ISBN_13') or identifiers.get('ISBN_10'),
                    "publication_date": volume_info.get('publishedDate'),
                    "content_version": volume_info.get('contentVersion')
                },
                "price_info": {
                    "list_price": sales_info.get('listPrice', {}),
                    "retail_price": sales_info.get('retailPrice', {}),
                    "saleability": sales_info.get('saleability'),
                    "on_sale_date": sales_info.get('onSaleDate'),
                    "currency": sales_info.get('listPrice', {}).get('currencyCode'),
                    "country": sales_info.get('country'),
                    "is_ebook": sales_info.get('isEbook', False),
                    "buy_link": sales_info.get('buyLink')
                },
                "metadata": {
                    "content_version": volume_info.get('contentVersion'),
                    "allow_anon_logging": volume_info.get('allowAnonLogging', False),
                    "language": volume_info.get('language'),
                    "preview_link": volume_info.get('previewLink'),
                    "info_link": volume_info.get('infoLink'),
                    "canonical_volume_link": volume_info.get('canonicalVolumeLink'),
                    "sample_page_count": access_info.get('samplePageCount'),
                    "text_snippet": search_info.get('textSnippet'),
                    "view_ability": access_info.get('viewability'),
                    "pdf_available": access_info.get('pdf', {}).get('isAvailable', False),
                    "pdf_link": access_info.get('pdf', {}).get('acsTokenLink'),
                    "epub_available": access_info.get('epub', {}).get('isAvailable', False),
                    "epub_link": access_info.get('epub', {}).get('acsTokenLink'),
                    "web_reader_link": access_info.get('webReaderLink')
                },
                "reviews": volume_info.get('reviews', []),
                "related_books": volume_info.get('relatedBooks', [])
            }
            print("Book Data:", book_data)
            books_data.append(book_data)
        return books_data

    @staticmethod
    def format_for_display(book: Dict) -> str:
        return f"""
        Title: {book['volume_info']['title']}
        Subtitle: {book['volume_info']['subtitle']}
        Authors: {', '.join([author['full_name'] for author in book['volume_info']['authors']])}
        Publisher: {book['volume_info']['publisher_info']['name']}
        Published: {book['volume_info']['published_date']}
        ISBN: {book['volume_info']['identifiers']}
        Pages: {book['volume_info']['page_count']}
        Language: {book['volume_info']['language']}
        Categories: {', '.join(book['volume_info']['categories'])}
        Rating: {book['volume_info']['average_rating']} ({book['volume_info']['ratings_count']} ratings)
        Preview: {book['metadata']['preview_link']}
        """

if __name__ == "__main__":
    API_KEY = os.getenv("GOOGLE_API_KEY")
    if not API_KEY:
        raise ValueError("Missing GOOGLE_API_KEY environment variable")
        
    collector = GoogleBooksDataCollector(api_key=API_KEY)
    books = collector.fetch_by_isbn("9780590353427")
    
    for book in books:
        print(collector.format_for_display(book))