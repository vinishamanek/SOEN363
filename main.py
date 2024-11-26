import os
import logging
from typing import List, Dict
from fetch import GoogleBooksAPI, OpenLibraryAPI
from insert import connect_to_db, insert_data
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BookDataPipeline:
    """
    Manages the entire pipeline for fetching book data from APIs and storing in PostgreSQL.
    Handles API connections, data enrichment, and database operations.
    """
    def __init__(self):
        # load env variables + establish connections
        load_dotenv()
        self.connection = connect_to_db()
        
        # initialize API keys for rotation 
        self.api_keys = [
            os.getenv("GOOGLE_API_KEY_1"),
            os.getenv("GOOGLE_API_KEY_2"),
            os.getenv("GOOGLE_API_KEY_3"),
        ]
        
        # initialize API clients
        self.google_books_api = GoogleBooksAPI(self.api_keys)
        self.open_library_api = OpenLibraryAPI()

    def enrich_books(self, books: List[Dict]) -> List[Dict]:
        """
        Enhances book data from Google Books with additional information from OpenLibrary.
        Falls back to original data if enrichment fails.
        
        Args:
            books: List of book dictionaries from Google Books API
        Returns:
            List of enriched book dictionaries
        """
        enriched_books = []
        for book in books:
            if isbn13 := book.get("isbn_13"):  
                try:
                    openlib_data = self.open_library_api.fetch_by_isbn(isbn13)
                    enriched_books.append({**book, **(openlib_data or {})})
                except Exception as e:
                    logger.error(f"Error enriching book {isbn13}: {e}")
                    enriched_books.append(book)  
            else:
                enriched_books.append(book)
        return enriched_books

    def process_batch(self, max_results: int = 40, pages: int = 1) -> bool:
        """
        Processes a single batch of books: fetches, enriches, and stores them.
        
        Args:
            max_results: Number of results per page
            pages: Number of pages to fetch
        Returns:
            bool: True if batch was processed successfully, False otherwise
        """
        try:
            # fetch random books from Google Books API
            logger.info("Fetching random books with pagination...")
            books = self.google_books_api.search_books_randomly_with_pagination(
                max_results=max_results, 
                pages=pages
            )
            
            if not books:
                logger.warning("No books fetched. Skipping batch.")
                return False

            # enrich with OpenLibrary data and store in database
            enriched_books = self.enrich_books(books)
            logger.info("Inserting enriched books into database...")
            insert_data(self.connection, enriched_books)
            logger.info(f"Successfully processed batch of {len(enriched_books)} books")
            return True

        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return False

    def run(self, batch_limit: int = None):
        """
        Main execution loop. Continuously processes batches until interrupted
        or batch limit is reached.
        
        Args:
            batch_limit: Optional maximum number of batches to process
        """
        if not self.connection:
            logger.error("Failed to connect to database")
            return

        try:
            batch_count = 0
            while batch_limit is None or batch_count < batch_limit:
                if self.process_batch():
                    batch_count += 1

        except KeyboardInterrupt:
            logger.info("Process stopped by user")
        finally:
            # close db connection properly
            if self.connection:
                self.connection.close()
                logger.info("Database connection closed")

def main():
    # create and run the pipeline
    pipeline = BookDataPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()