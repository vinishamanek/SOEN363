import psycopg2
from psycopg2 import sql
from typing import Dict, List, Optional
import os


def connect_to_db():
    """Establish a database connection."""
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )

        connection.autocommit = True
        print("Connected to the database.")
        return connection
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        return None


def insert_publisher(cursor, publisher: str) -> Optional[int]:
    """Insert publisher into the database."""
    cursor.execute("""
        INSERT INTO Publisher (name)
        VALUES (%s)
        ON CONFLICT (name) DO NOTHING
        RETURNING publisher_id;
    """, (publisher,))
    result = cursor.fetchone()
    return result[0] if result else None


def insert_author(cursor, authors: List[str]) -> List[int]:
    """Insert authors into the database."""
    author_ids = []
    for author in authors:
        cursor.execute("""
            INSERT INTO Author (full_name)
            VALUES (%s)
            ON CONFLICT (full_name) DO NOTHING
            RETURNING author_id;
        """, (author,))
        result = cursor.fetchone()
        if result:
            author_ids.append(result[0])
    return author_ids


def insert_category(cursor, categories: List[str]) -> List[int]:
    """Insert categories into the database."""
    category_ids = []
    for category in categories:
        cursor.execute("""
            INSERT INTO Category (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING
            RETURNING category_id;
        """, (category,))
        result = cursor.fetchone()
        if result:
            category_ids.append(result[0])
    return category_ids


def insert_subject(cursor, subjects: List[str]) -> List[int]:
    """Insert subjects into the database."""
    subject_ids = []
    for subject in subjects:
        cursor.execute("""
            INSERT INTO Subject (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING
            RETURNING subject_id;
        """, (subject,))
        result = cursor.fetchone()
        if result:
            subject_ids.append(result[0])
    return subject_ids


def insert_rating(cursor, book_id: int, avg_rating: float, rating_count: int) -> Optional[int]:
    """Insert rating into the database."""
    cursor.execute("""
        INSERT INTO Rating (book_id, avg_rating, rating_count)
        VALUES (%s, %s, %s)
        ON CONFLICT (book_id) DO UPDATE 
        SET avg_rating = EXCLUDED.avg_rating,
            rating_count = EXCLUDED.rating_count
        RETURNING rating_id;
    """, (book_id, avg_rating, rating_count))
    result = cursor.fetchone()
    return result[0] if result else None


def insert_price(cursor, book_id: int, price_data: Dict) -> Optional[int]:
    """Insert price into the database."""
    cursor.execute("""
        INSERT INTO Price (
            country, saleability, book_id, listPrice, retailPrice,
            listPrice_currency_code, retailPrice_currency_code, buyLink
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING price_id;
    """, (
        price_data.get('country'),
        price_data.get('saleability'),
        book_id,
        price_data.get('listPrice'),
        price_data.get('retailPrice'),
        price_data.get('listPrice_currency_code'),
        price_data.get('retailPrice_currency_code'),
        price_data.get('buyLink')
    ))
    result = cursor.fetchone()
    return result[0] if result else None


def insert_book(cursor, book_data: Dict, publisher_id: Optional[int]) -> Optional[int]:
    """Insert book into the database."""
    cursor.execute("""
        INSERT INTO Book (
            isbn10, isbn13, title, subtitle, description, language_code, publication_year,
            book_publisher_id, page_count, maturity_rating, content_rating, google_books_id,
            google_preview_link, google_info_link, google_canonical_link
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (isbn13) DO NOTHING
        RETURNING book_id;
    """, (
        book_data.get("isbn_10"), book_data.get("isbn_13"), book_data.get("title"),
        book_data.get("subtitle"), book_data.get("description"), book_data.get("language_code"),
        book_data.get("published_year"), publisher_id, book_data.get("page_count"),
        book_data.get("maturity_rating"), book_data.get("content_rating"),
        book_data.get("google_books_id"), book_data.get("google_preview_link"),
        book_data.get("google_info_link"), book_data.get("google_canonical_link")
    ))
    result = cursor.fetchone()
    return result[0] if result else None


def link_book_author(cursor, book_id: int, author_ids: List[int]):
    """Link book to authors in the BookAuthor table."""
    for author_id in author_ids:
        cursor.execute("""
            INSERT INTO BookAuthor (book_id, author_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        """, (book_id, author_id))


def link_book_category(cursor, book_id: int, category_ids: List[int]):
    """Link book to categories in the BookCategory table."""
    for category_id in category_ids:
        cursor.execute("""
            INSERT INTO BookCategory (book_id, category_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        """, (book_id, category_id))


def link_book_subject(cursor, book_id: int, subject_ids: List[int]):
    """Link book to subjects in the BookSubject table."""
    for subject_id in subject_ids:
        cursor.execute("""
            INSERT INTO BookSubject (book_id, subject_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        """, (book_id, subject_id))


def insert_data(connection, books: List[Dict]):
    with connection.cursor() as cursor:
        for book in books:
            publisher_id = insert_publisher(cursor, book.get("publisher"))
            author_ids = insert_author(cursor, book.get("authors", []))
            book_id = insert_book(cursor, book, publisher_id)

            if book_id:
                category_ids = insert_category(cursor, book.get("categories", []))
                subject_ids = insert_subject(cursor, book.get("subjects", []))

                link_book_author(cursor, book_id, author_ids)
                link_book_category(cursor, book_id, category_ids)
                link_book_subject(cursor, book_id, subject_ids)

                if book.get("average_rating") and book.get("ratings_count"):
                    insert_rating(cursor, book_id, book["average_rating"], book["ratings_count"])

                if book.get("price_info"):
                    insert_price(cursor, book_id, book["price_info"])

        print("Data insertion complete.")


if __name__ == "__main__":
    connection = connect_to_db()
    if not connection:
        exit()

    connection.close()
