import os
from fetch import GoogleBooksAPI, OpenLibraryAPI
from insert import connect_to_db, insert_data
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main():
    # Connect to the database
    connection = connect_to_db()
    if not connection:
        print("Failed to connect to the database")
        return

    # API keys for Google Books API
    api_keys = [
        os.getenv("GOOGLE_API_KEY_1"),
        os.getenv("GOOGLE_API_KEY_2"),
        os.getenv("GOOGLE_API_KEY_3"),
    ]

    google_books_api = GoogleBooksAPI(api_keys)
    open_library_api = OpenLibraryAPI()

    try:
        while True:  # Infinite loop to continuously fetch books
            print("Fetching random books with pagination...")
            books = google_books_api.search_books_randomly_with_pagination(max_results=10, pages=1)  

            enriched_books = []
            for book in books:
                if book.get("isbn_13"):
                    openlib_data = open_library_api.fetch_by_isbn(book["isbn_13"])
                    enriched_books.append({**book, **(openlib_data or {})})
                    print(enriched_books)
                else:
                    enriched_books.append(book)

            if not enriched_books:
                print("No books were fetched this time. Retrying...")
                continue

            print("\nInserting enriched books into the database...")
            insert_data(connection, enriched_books)
            print("Batch processed. Continuing to fetch more books...\n")

    except KeyboardInterrupt:
        print("Process stopped by user.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if connection:
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
