import requests
from typing import Optional, Dict, List, Any
from datetime import datetime
from dotenv import load_dotenv
import os

# to load environment variables from .env file
load_dotenv()

class GoogleBooksDataCollector:
    def __init__(self, api_key: str):
        """Initialize the Google Books Data Collector with an API key."""
        self.base_url = "https://www.googleapis.com/books/v1/volumes"
        self.api_key = api_key

    def fetch_google_books_data(self, query: str, start_index: int = 0, max_results: int = 10) -> List[Dict]:
        """Fetch data from Google Books API."""
        params = {
            'q': query,
            'startIndex': start_index,
            'maxResults': max_results,
            'key': self.api_key,
            'projection': 'full',  
            'printType': 'all',    
            'langRestrict': None   
        }

        print(f"Fetching data with query: {query}")
        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            books_data = []
            data = response.json()
            
            for item in data.get('items', []):
                volume_info = item.get('volumeInfo', {})
                sales_info = item.get('saleInfo', {})
                access_info = item.get('accessInfo', {})
                search_info = item.get('searchInfo', {})
                
                # extract all identifiers
                industry_identifiers = volume_info.get('industryIdentifiers', [])
                identifiers = {
                    identifier.get('type'): identifier.get('identifier')
                    for identifier in industry_identifiers
                }

                # author details with additional fields
                authors = volume_info.get('authors', [])
                author_details = []
                for author in authors:
                    author_info = {
                        "full_name": author,
                        "first_name": author.split()[0] if author else None,
                        "last_name": author.split()[-1] if author else None,
                        "birth_date": None,  # unable to get from Google Books API
                        "death_date": None,  # unable to get from Google Books API
                        "profile_url": None,  # unable to get from Google Books API
                        "biography": None,    # unable to get from Google Books API
                        "other_works": []     # unable to get from Google Books API
                    }
                    author_details.append(author_info)

                # edition information
                edition_info = {
                    "name": volume_info.get('title'),
                    "edition_number": None,  # extract from subtitle if available
                    "format": "ebook" if volume_info.get('isEbook', False) else "physical",
                    "print_type": volume_info.get('printType'),
                    "maturity_rating": volume_info.get('maturityRating'),
                    "main_category": volume_info.get('mainCategory'),
                    "dimensions": volume_info.get('dimensions', {}),
                    "print_edition_isbn": identifiers.get('ISBN_13') or identifiers.get('ISBN_10'),
                    "publication_date": volume_info.get('publishedDate'),
                    "content_version": volume_info.get('contentVersion')
                }

                # Enhanced publisher information
                publisher_info = {
                    "name": volume_info.get('publisher'),
                    "description": None,  # unable to get from Google Books API
                    "website": None,      # unable to get from Google Books API
                    "founded_year": None, # unable to get from Google Books API
                    "location": None,     ## unable to get from Google Books API
                    "imprint": None       # unable to get from Google Books API
                }

                # price and availability information
                price_info = {
                    "list_price": sales_info.get('listPrice', {}),
                    "retail_price": sales_info.get('retailPrice', {}),
                    "saleability": sales_info.get('saleability'),
                    "on_sale_date": sales_info.get('onSaleDate'),
                    "currency": sales_info.get('listPrice', {}).get('currencyCode'),
                    "country": sales_info.get('country'),
                    "is_ebook": sales_info.get('isEbook', False),
                    "buy_link": sales_info.get('buyLink')
                }

                # chapter information (if available)
                chapter_info = []
                if 'tableOfContents' in volume_info:
                    for i, chapter in enumerate(volume_info['tableOfContents'], 1):
                        chapter_data = {
                            "number": i,
                            "title": chapter if isinstance(chapter, str) else chapter.get('title'),
                            "subtitle": None,
                            "start_page": None,
                            "end_page": None,
                            "preview_available": False
                        }
                        chapter_info.append(chapter_data)

                # more metadata
                metadata = {
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
                }

                # reviews + related books (if available)
                reviews = volume_info.get('reviews', [])
                related_books = volume_info.get('relatedBooks', [])

                # combining all data
                book_data = {
                    "google_books_id": item.get('id'),
                    "etag": item.get('etag'),
                    "self_link": item.get('selfLink'),
                    "volume_info": {
                        "title": volume_info.get('title'),
                        "subtitle": volume_info.get('subtitle'),
                        "authors": author_details,
                        "publisher_info": publisher_info,
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
                    "edition_info": edition_info,
                    "chapter_info": chapter_info,
                    "price_info": price_info,
                    "metadata": metadata,
                    "reviews": reviews,
                    "related_books": related_books
                }
                books_data.append(book_data)
            return books_data
        else:
            print(f"Error fetching data: {response.status_code}, {response.text}")
            return []

    @staticmethod
    def format_for_display(book: Dict) -> str:
        """printing book data"""
        return f"""
        book info:
        -----------------
        Title: {book['volume_info']['title']}
        Subtitle: {book['volume_info']['subtitle']}
        Google Books ID: {book['google_books_id']}
        Authors: {', '.join([author['full_name'] for author in book['volume_info']['authors']])}
        Identifiers: {', '.join([f"{k}: {v}" for k, v in book['volume_info']['identifiers'].items()])}
        
        publisher info:
        --------------------
        Publisher: {book['volume_info']['publisher_info']['name']}
        Publication Date: {book['volume_info']['published_date']}
        
        content info:
        ---------------
        Page Count: {book['volume_info']['page_count']}
        Language: {book['volume_info']['language']}
        Categories: {', '.join(book['volume_info']['categories'])}
        
        ratings and reviews (if available):
        --------
        Average Rating: {book['volume_info']['average_rating']}
        Ratings Count: {book['volume_info']['ratings_count']}
        Reviews: {', '.join([review['text'] for review in book['reviews']])}

        price/sale info:
        ----------------
        Saleability: {book['price_info']['saleability']}
        Buy Link: {book['price_info']['buy_link']}
        
        Related Books:
        --------------
        {', '.join([related_book['title'] for related_book in book['related_books']])}
        
        Additional Information:
        ---------------------
        Preview Link: {book['metadata']['preview_link']}
        PDF Available: {book['metadata']['pdf_available']}
        EPUB Available: {book['metadata']['epub_available']}
        Sample Page Count: {book['metadata']['sample_page_count']}
        """

if __name__ == "__main__":
    API_KEY = os.getenv("GOOGLE_API_KEY")
    if not API_KEY:
        raise ValueError("need to set the GOOGLE_API_KEY environment variable.")
    collector = GoogleBooksDataCollector(api_key=API_KEY)
    books = collector.fetch_google_books_data(query="software engineering", max_results=1)
    
    for book in books:
        print(collector.format_for_display(book))
