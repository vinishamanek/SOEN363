# import os
# from fetch import GoogleBooksAPI
# from insert import connect_to_db, insert_data
#
# def main():
#     connection = connect_to_db()
#     if not connection:
#         print("Failed to connect to database")
#         return
#
#     api_keys = [
#         os.getenv("GOOGLE_API_KEY_1"),
#         os.getenv("GOOGLE_API_KEY_2"),
#         os.getenv("GOOGLE_API_KEY_3"),
#     ]
#
#     google_books_api = GoogleBooksAPI(api_keys)
#
#     try:
#         print("Fetching random books...")
#         books = google_books_api.search_books_randomly_with_pagination(max_results=10, pages=5)  # Fetch up to 50 books
#
#         print("\nData to be inserted:")
#         for idx, book in enumerate(books, 1):
#             print(f"\nBook {idx}:")
#             print(f"Title: {book.get('title')}")
#             print(f"Authors: {book.get('authors')}")
#             print(f"ISBN13: {book.get('isbn_13')}")
#             print("-" * 50)
#
#         if not books:
#             print("No books were fetched!")
#             return
#
#         print("\nInserting books into the database...")
#         insert_data(connection, books)
#
#     except Exception as e:
#         print(f"An error occurred: {e}")
#     finally:
#         if connection:
#             connection.close()
#             print("Database connection closed.")
#
# if __name__ == "__main__":
#     main()
import os
from fetch import GoogleBooksAPI
from insert import connect_to_db, insert_data
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main():
    # Connect to the database
    connection = connect_to_db()
    if not connection:
        print("Failed to connect to database")
        return

    # API keys for Google Books API
    api_keys = [
        os.getenv("GOOGLE_API_KEY_1"),
        os.getenv("GOOGLE_API_KEY_2"),
        os.getenv("GOOGLE_API_KEY_3"),
    ]

    google_books_api = GoogleBooksAPI(api_keys)

    try:
        while True:  # Infinite loop to continuously fetch books
            print("Fetching random books with pagination...")
            books = google_books_api.search_books_randomly_with_pagination(max_results=10, pages=5)  # Fetch up to 50 books

            if not books:
                print("No books were fetched this time. Retrying...")
                continue

            print("\nInserting books into the database...")
            insert_data(connection, books)
            print("Batch processed. Continuing to fetch more books...\n")

    except KeyboardInterrupt:
        # Gracefully handle manual stop
        print("Process stopped by user.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if connection:
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
