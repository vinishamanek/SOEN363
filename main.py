import os
from fetch import fetch_and_merge_data
from insert import connect_to_db, insert_data

def main():
    connection = connect_to_db()
    if not connection:
        print("Failed to connect to database")
        return

    api_keys = [
        os.getenv("GOOGLE_API_KEY_1"),
        os.getenv("GOOGLE_API_KEY_2"),
        os.getenv("GOOGLE_API_KEY_3"),
    ]

    try:
        print("Fetching book data...")
        merged_books = fetch_and_merge_data(api_keys, query="harry potter", num_books=5)
        
        print("\nData to be inserted:")
        for idx, book in enumerate(merged_books, 1):
            print(f"\nBook {idx}:")
            print(f"Title: {book.get('title')}")
            print(f"Authors: {book.get('authors')}")
            print(f"ISBN13: {book.get('isbn_13')}")
            print("-" * 50)
        
        if not merged_books:
            print("No books were fetched!")
            return

        print("\nInserting data into database...")
        insert_data(connection, merged_books)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if connection:
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()