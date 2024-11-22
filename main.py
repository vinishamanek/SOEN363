import os

from fetch import fetch_and_merge_data


def main():
    # Replace these with your actual Google Books API keys
    api_keys = [
        os.getenv("GOOGLE_API_KEY_1"),
        os.getenv("GOOGLE_API_KEY_2"),
        os.getenv("GOOGLE_API_KEY_3"),
    ]

    # Define a query for testing (e.g., "fiction")
    test_query = "harry potter"

    # Initialize APIs and fetch data
    print("Fetching and merging book data...")
    merged_books = fetch_and_merge_data(api_keys, query=test_query, num_books=5)

    # Display the merged data
    print("\nMerged Book Data:")
    for idx, book in enumerate(merged_books, start=1):
        print(f"\nBook {idx}:")
        print(book)

if __name__ == "__main__":
    main()
