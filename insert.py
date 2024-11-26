import psycopg2
from typing import Dict, List, Optional, Union
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

def connect_to_db():
    """Establish a connection to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        print("Connected to the database.")
        return connection
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        return None

def format_year(year_str: str) -> Optional[int]:
    """Format the year string to an integer if valid."""
    if not year_str:
        return None
    try:
        year = int(year_str.split('-')[0])
        current_year = datetime.now().year
        return year if 1400 <= year <= current_year else None
    except ValueError:
        return None

def map_maturity_rating(rating: str) -> str:
    """Map the maturity rating to the database enum."""
    return 'MATURE' if rating == 'MATURE' else 'NOT_MATURE'

def insert_publisher(cursor, publisher_name: str) -> Optional[int]:
    """Insert a publisher into the database."""
    if not publisher_name:
        return None
    try:
        cursor.execute("""
            INSERT INTO Publisher (name)
            VALUES (%s)
            ON CONFLICT (name) DO UPDATE 
            SET name = EXCLUDED.name
            RETURNING publisher_id;
        """, (publisher_name,))
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error inserting publisher: {e}")
        return None

def insert_author(cursor, authors: List[Union[str, Dict]]) -> List[int]:
    """Insert authors into the database and return their IDs."""
    author_ids = []
    for author in authors:
        if not author:
            continue
        author_name = author['name'] if isinstance(author, dict) else author
        try:
            cursor.execute("""
                INSERT INTO Author (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
                RETURNING author_id;
            """, (author_name,))
            result = cursor.fetchone()
            if result:
                author_ids.append(result[0])
        except Exception as e:
            print(f"Error inserting author {author_name}: {e}")
    return author_ids

def insert_category(cursor, categories: List[str]) -> List[int]:
    """Insert categories into the database and return their IDs."""
    category_ids = []
    for category in categories:
        if not category:
            continue
        try:
            cursor.execute("""
                INSERT INTO Category (name)
                VALUES (%s)
                ON CONFLICT (name) DO UPDATE 
                SET name = EXCLUDED.name
                RETURNING category_id;
            """, (category,))
            result = cursor.fetchone()
            if result:
                category_ids.append(result[0])
        except Exception as e:
            print(f"Error inserting category {category}: {e}")
    return category_ids

def insert_subject(cursor, subjects: List[str]) -> List[int]:
    """Insert subjects into the database and return their IDs."""
    subject_ids = []
    for subject in subjects:
        if not subject:
            continue
        try:
            cursor.execute("""
                INSERT INTO Subject (name)
                VALUES (%s)
                ON CONFLICT (name) DO UPDATE 
                SET name = EXCLUDED.name
                RETURNING subject_id;
            """, (subject,))
            result = cursor.fetchone()
            if result:
                subject_ids.append(result[0])
        except Exception as e:
            print(f"Error inserting subject {subject}: {e}")
    return subject_ids

def insert_book(cursor, book_data: Dict) -> Optional[int]:
    """Insert or update a book with rating attributes directly in the Book table."""
    
    if not book_data.get("isbn_10") or not book_data.get("isbn_13"):
        print(f"Skipping book insertion due to missing ISBN: {book_data}")
        return None
    
    try:
        # print(f"Inserting book: {book_data}")

        cursor.execute("""
            INSERT INTO Book (
                isbn10, isbn13, title, subtitle, description,
                language_code, publication_year, page_count,
                maturity_rating, google_books_id, google_preview_link,
                google_info_link, google_canonical_link
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (isbn13) WHERE isbn13 IS NOT NULL DO
            UPDATE SET 
                title = EXCLUDED.title,
                subtitle = EXCLUDED.subtitle,
                description = EXCLUDED.description
            RETURNING book_id;
        """, (
            book_data.get("isbn_10"),
            book_data.get("isbn_13"),
            book_data.get("title"),
            book_data.get("subtitle"),
            book_data.get("description"),
            book_data.get("language_code"),
            format_year(book_data.get("published_year")),
            book_data.get("page_count"),
            map_maturity_rating(book_data.get("maturity_rating")),
            book_data.get("google_books_id"),
            book_data.get("google_preview_link"),
            book_data.get("google_info_link"),
            book_data.get("google_canonical_link"),
        ))

        return cursor.fetchone()[0]

    except Exception as e:
        print(f"Error inserting book {book_data.get('title')}: {e}")
        return None

def insert_rating(cursor, book_id: int, avg_rating: float, ratings_count: int) -> None:
    """Insert or update a rating in the Ratings table."""
    try:
        cursor.execute("""
            INSERT INTO Ratings (book_id, avg_rating, ratings_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (book_id) DO UPDATE 
            SET avg_rating = EXCLUDED.avg_rating,
                ratings_count = EXCLUDED.ratings_count;
        """, (book_id, avg_rating, ratings_count))
    except Exception as e:
        print(f"Error inserting rating for book {book_id}: {e}")

def insert_price(cursor, book_id: int, price_data: Dict) -> Optional[int]:
    """Insert or update price data for a book."""
    if not price_data or not book_id:
        return None
    try:
        cursor.execute("""
            INSERT INTO Price (
                book_id, country, on_sale_date, saleability,
                list_price, retail_price,
                list_price_currency_code, retail_price_currency_code,
                buy_link
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (book_id, country, on_sale_date) DO UPDATE
            SET list_price = EXCLUDED.list_price,
                retail_price = EXCLUDED.retail_price
            RETURNING price_id;
        """, (
            book_id,
            price_data.get('country', 'USD'),
            datetime.now().date(),
            price_data.get('saleability'),
            price_data.get('listPrice'),
            price_data.get('retailPrice'),
            price_data.get('currency', 'USD'),
            price_data.get('currency', 'USD'),
            price_data.get('buyLink')
        ))
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error inserting price: {e}")
        return None

def handle_book_format(cursor, book_id: int, book_data: Dict):
    """Handle the book's format (PhysicalBook or EBook)."""
    try:
        if book_data.get("isEbook"):
            ebook_url = book_data.get("ebook_url")
            if not ebook_url:
                ebook_url = "https://example.com/default-ebook-url"
            cursor.execute("""
                INSERT INTO EBook (book_id, ebook_url)
                VALUES (%s, %s)
                ON CONFLICT (book_id) DO UPDATE
                SET ebook_url = EXCLUDED.ebook_url;
            """, (book_id, ebook_url))
        else:
            format_value = book_data.get("physical_format", "Hardcover").capitalize()
            if format_value not in ['Hardcover', 'Paperback']:
                format_value = 'Hardcover'
            cursor.execute("""
                INSERT INTO PhysicalBook (book_id, format)
                VALUES (%s, %s::format_type)
                ON CONFLICT (book_id) DO UPDATE
                SET format = EXCLUDED.format;
            """, (book_id, format_value))
    except Exception as e:
        print(f"Error handling book format: {e}")

def insert_data(connection, books: List[Dict]):
    """Insert all book-related data into the database."""
    with connection.cursor() as cursor:
        for book in books:
            try:
                cursor.execute("BEGIN;")
                book_id = insert_book(cursor, book)
                if not book_id:
                    cursor.execute("ROLLBACK;")
                    continue

                author_ids = insert_author(cursor, book.get("authors", []))
                publisher_id = insert_publisher(cursor, book.get("publisher"))
                category_ids = insert_category(cursor, book.get("categories", []))
                subject_ids = insert_subject(cursor, book.get("subjects", []))

                for author_id in author_ids:
                    cursor.execute("""
                        INSERT INTO BookAuthor (book_id, author_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (book_id, author_id))
                if publisher_id:
                    cursor.execute("""
                        INSERT INTO BookPublisher (book_id, publisher_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (book_id, publisher_id))
                for category_id in category_ids:
                    cursor.execute("""
                        INSERT INTO BookCategory (book_id, category_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (book_id, category_id))
                for subject_id in subject_ids:
                    cursor.execute("""
                        INSERT INTO BookSubject (book_id, subject_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (book_id, subject_id))

                handle_book_format(cursor, book_id, book)

                if book.get("price_info"):
                    insert_price(cursor, book_id, book["price_info"])

                if book.get("average_rating") is not None:
                    insert_rating(
                        cursor,
                        book_id,
                        book.get("average_rating", 0.0),
                        book.get("ratings_count", 0)
                    )

                cursor.execute("COMMIT;")
                print(f"Successfully processed book: {book.get('title')}")
            except Exception as e:
                cursor.execute("ROLLBACK;")
                print(f"Error processing book {book.get('title')}: {e}")
