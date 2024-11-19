import psycopg2
from psycopg2 import sql
from datetime import datetime
from typing import Optional, Dict, Any
from fetch_openlibrary import OpenLibraryDataCollector

def connect_to_db():
    """Connect to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            dbname="my_database",
            user="vinishamanek",
            password="",
            host="localhost",
            port="5432"
        )
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def parse_date(date_str: Optional[str]) -> Optional[str]:
    """Convert various date formats to YYYY-MM-DD."""
    if not date_str:
        return None
    try:
        for fmt in ('%Y-%m-%d', '%Y', '%B %d, %Y', '%d %B %Y'):
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None
    except Exception:
        return None

def insert_publisher(connection, publisher_name: str) -> Optional[int]:
    """Insert publisher and return publisher_id."""
    if not publisher_name:
        return None
        
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO publishers (name)
            VALUES (%s)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING publisher_id
        """, (publisher_name,))
        publisher_id = cursor.fetchone()[0]
        connection.commit()
        return publisher_id
    except Exception as e:
        print(f"Error inserting publisher: {e}")
        connection.rollback()
        return None
    finally:
        cursor.close()

def insert_author(connection, author_data: Dict[str, Any]) -> Optional[int]:
    """Insert author and return author_id."""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO authors (
                openlibrary_key, full_name, personal_name, birth_date,
                bio, wikipedia_url, official_website
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (openlibrary_key) DO UPDATE SET
                full_name = EXCLUDED.full_name,
                personal_name = EXCLUDED.personal_name,
                birth_date = EXCLUDED.birth_date,
                bio = EXCLUDED.bio,
                wikipedia_url = EXCLUDED.wikipedia_url,
                official_website = EXCLUDED.official_website
            RETURNING author_id
        """, (
            author_data.get('openlibrary_key'),
            author_data.get('full_name'),
            author_data.get('personal_name'),
            parse_date(author_data.get('birth_date')),
            author_data.get('bio'),
            author_data.get('wikipedia_url'),
            author_data.get('official_website')
        ))
        author_id = cursor.fetchone()[0]
        connection.commit()

        # Insert external IDs
        if remote_ids := author_data.get('remote_identifiers'):
            for id_type, id_value in remote_ids.items():
                insert_author_external_id(connection, author_id, id_type, id_value)

        return author_id
    except Exception as e:
        print(f"Error inserting author: {e}")
        connection.rollback()
        return None
    finally:
        cursor.close()

def insert_author_external_id(connection, author_id: int, id_type: str, id_value: str):
    """Insert author's external identifier."""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO author_external_ids (author_id, identifier_type, identifier_value)
            VALUES (%s, %s, %s)
            ON CONFLICT (author_id, identifier_type) DO UPDATE SET
                identifier_value = EXCLUDED.identifier_value
        """, (author_id, id_type, id_value))
        connection.commit()
    except Exception as e:
        print(f"Error inserting author external ID: {e}")
        connection.rollback()
    finally:
        cursor.close()

def insert_book(connection, book_data: Dict[str, Any], publisher_id: Optional[int]) -> Optional[int]:
    """Insert book and return book_id."""
    try:
        cursor = connection.cursor()
        availability = 'Available' if book_data.get('availability') == 'Borrowable' else 'Not Available'
        
        cursor.execute("""
            INSERT INTO books (
                openlibrary_work_id, title, first_publish_year, number_of_pages,
                isbn_10, isbn_13, publisher_id, availability, preview_url, borrow_url
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::book_availability, %s, %s)
            ON CONFLICT (openlibrary_work_id) DO UPDATE SET
                title = EXCLUDED.title,
                first_publish_year = EXCLUDED.first_publish_year,
                number_of_pages = EXCLUDED.number_of_pages,
                isbn_10 = EXCLUDED.isbn_10,
                isbn_13 = EXCLUDED.isbn_13,
                publisher_id = EXCLUDED.publisher_id,
                availability = EXCLUDED.availability,
                preview_url = EXCLUDED.preview_url,
                borrow_url = EXCLUDED.borrow_url
            RETURNING book_id
        """, (
            book_data.get('openlibrary_work_id'),
            book_data.get('title'),
            book_data.get('publish_date'),
            book_data.get('number_of_pages'),
            book_data.get('isbn_10')[0] if book_data.get('isbn_10') else None,
            book_data.get('isbn_13')[0] if book_data.get('isbn_13') else None,
            publisher_id,
            availability,
            book_data.get('preview_url'),
            book_data.get('borrow_url')
        ))
        book_id = cursor.fetchone()[0]
        connection.commit()

        # Insert subjects
        if subjects := book_data.get('subjects'):
            for subject in subjects:
                insert_book_subject(connection, book_id, subject)

        return book_id
    except Exception as e:
        print(f"Error inserting book: {e}")
        connection.rollback()
        return None
    finally:
        cursor.close()

def insert_book_author(connection, book_id: int, author_id: int):
    """Insert book-author relationship."""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO book_authors (book_id, author_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (book_id, author_id))
        connection.commit()
    except Exception as e:
        print(f"Error inserting book-author relationship: {e}")
        connection.rollback()
    finally:
        cursor.close()

def insert_book_subject(connection, book_id: int, subject: str):
    """Insert book subject."""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO book_subjects (book_id, subject)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (book_id, subject))
        connection.commit()
    except Exception as e:
        print(f"Error inserting book subject: {e}")
        connection.rollback()
    finally:
        cursor.close()

def process_book(connection, book_data: Dict[str, Any]):
    """Process a complete book entry with all related data."""
    # Insert publisher if available
    publisher_id = None
    if publishers := book_data.get('publisher'):
        publisher_id = insert_publisher(connection, publishers[0])

    # Insert book
    book_id = insert_book(connection, book_data, publisher_id)
    if not book_id:
        return None, None

    # Process authors
    for author_data in book_data.get('author_details', []):
        author_id = insert_author(connection, author_data)
        if author_id:
            insert_book_author(connection, book_id, author_id)

    return book_id, publisher_id

def main():
    # Initialize the collector from fetch.py
    collector = OpenLibraryDataCollector()
    
    # Connect to the database
    connection = connect_to_db()
    if not connection:
        return

    try:
        # Example: process a range of ISBNs
        isbns = ['9780000000033', '9780000000035']  # You can modify this list
        for isbn in isbns:
            print(f"\nProcessing ISBN: {isbn}")
            
            # Fetch book details using the collector
            books = collector.fetch_book_details(query=isbn, max_results=1)
            
            if books:
                book_data = books[0]
                book_id, publisher_id = process_book(connection, book_data)
                if book_id:
                    print(f"Successfully processed book ID: {book_id}")
            else:
                print(f"No data found for ISBN: {isbn}")

    except Exception as e:
        print(f"Error in main process: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    main()