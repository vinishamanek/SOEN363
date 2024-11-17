import requests
import pandas as pd
import time

class OpenLibraryDataCollector:
    def __init__(self):
        """Initialize the collector with API configuration"""
        self.base_url = "https://openlibrary.org/search.json"

    def fetch_openlibrary_data(self, start_index: int = 100, batch_size: int = 10):
        """Fetch data from OpenLibrary API"""
        books_data = []

        try:
            params = {
                'q': '*:*',  # fetching all books
                'limit': batch_size,
                'offset': start_index
            }
            
            print(f"Requesting URL: {self.base_url} with params: {params}")  # Debugging info
            response = requests.get(self.base_url, params=params)
            if response.status_code == 200:
                works = response.json()
                
                for doc in works.get('docs', []):
                    # extracting only the necessary fields
                    book_info = {
                        'Title': doc.get('title'),
                        'Author': ', '.join([author for author in doc.get('author_name', [])]),
                        'First Publish Year': doc.get('first_publish_year'),
                        'ISBN': ', '.join(doc.get('isbn', [])) if doc.get('isbn') else None,
                        'Subjects': ', '.join(doc.get('subject', [])) if doc.get('subject') else None
                    }
                    books_data.append(book_info)

                return books_data
            else:
                print(f"Error: Received status code {response.status_code} with response: {response.text}")  # Improved error logging
                return []
        except Exception as e:
            print(f"Error fetching OpenLibrary data: {e}")
            return []

    def fetch_and_print(self, total_records: int = 10, batch_size: int = 1):
        """Fetch data in batches and print"""
        all_books = []

        for offset in range(0, total_records, batch_size):
            print(f"Fetching records {offset} to {offset + batch_size}...")
            books = self.fetch_openlibrary_data(start_index=offset, batch_size=batch_size)
            all_books.extend(books)

            # printing each book individually
            for book in books:
                print(book)

            # to respect API rate limits, setting a sleep time of 1 second
            time.sleep(1)

        # convert to pandas
        df = pd.DataFrame(all_books)
        print(df)

# main script
if __name__ == "__main__":
    collector = OpenLibraryDataCollector()
    collector.fetch_and_print(total_records=10, batch_size=1)