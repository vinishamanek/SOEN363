import psycopg2
from psycopg2 import sql
from fetch_googlebooks import GoogleBooksDataCollector
from fetch_openlibrary import OpenLibraryDataCollector
from fetch_publisherInfo import PublisherDataCollector
import re
import os

def connect_to_db():
    """ Establish a connection to the PostgreSQL database. """
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("books"),
            user='postgres',
            # password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        return connection
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")
        return None

def insert_publisher(connection, publisher_data):
    """ Insert publisher data into the publishers table. """
    try:
        name = publisher_data.get('name', 'Unknown Publisher')
        founding_year = publisher_data.get('founded_year', None)
        country_code = publisher_data.get('country_code', 'Unknown')

        print(f"Inserting publisher data: name={name}, founding_year={founding_year}, country_code={country_code}")  # Debug print
        with connection.cursor() as cursor:
            insert_query = sql.SQL("""
                INSERT INTO publishers (name, founding_year, country)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET
                    founding_year = EXCLUDED.founding_year,
                    country = EXCLUDED.country
                RETURNING publisher_id;
            """)
            cursor.execute(insert_query, (
                name,
                founding_year,
                country_code
            ))
            result = cursor.fetchone()
            connection.commit()
            return result[0] if result else None
    except psycopg2.Error as e:
        print(f"Error inserting publisher: {e}")
        connection.rollback()
        return None


def insert_author(connection, author_data):
    """ Insert author data into the authors table. """
    with connection.cursor() as cursor:
        # Check for each key in author_data and set default if not present
        openlibrary_key = author_data.get('openlibrary_key', None)
        full_name = author_data.get('full_name', None)
        birth_date = author_data.get('birth_date', None)
        death_date = author_data.get('death_date', None)
        status = author_data.get('status', 'Unknown')  # Defaulting status to 'Unknown' if not provided
        wikipedia_url = author_data.get('wikipedia_url', None)
        official_website = author_data.get('official_website', None)

        insert_query = sql.SQL("""
            INSERT INTO authors (openlibrary_key, full_name, birth_date, death_date, status, wikipedia_url, official_website)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (openlibrary_key) DO NOTHING
            RETURNING author_id;
        """)
        cursor.execute(insert_query, (
            openlibrary_key, 
            full_name, 
            birth_date, 
            death_date, 
            status, 
            wikipedia_url, 
            official_website
        ))
        result = cursor.fetchone()
        connection.commit()
        return result[0] if result else None


def insert_book(connection, book_data, publisher_id, genre_id):
    """ Insert book data into the books table. """
    with connection.cursor() as cursor:
        # Validate ISBN-10
        isbn_10 = book_data['volume_info']['identifiers'].get('ISBN_10')
        # Regex checks for nine digits followed by either a digit or 'X'
        if isbn_10 and not re.match(r'^\d{9}[\dX]$', isbn_10):
            print(f"Invalid ISBN-10 format: {isbn_10}")
            isbn_10 = None  # Set to None if invalid
        else:
            print(f"Valid ISBN-10: {isbn_10}")

        # Extracting year from publication_date assuming it's formatted as 'YYYY-MM-DD'
        publish_year = None
        if book_data['edition_info']['publication_date']:
            publish_year = int(book_data['edition_info']['publication_date'].split('-')[0])  # Extracting the year as integer

        # Debug print to check values
        print(f"Inserting book data: title={book_data['volume_info']['title']}, publish_year={publish_year}, page_count={book_data['volume_info']['page_count']}, isbn_10={isbn_10}, isbn_13={book_data['volume_info']['identifiers'].get('ISBN_13')}, publisher_id={publisher_id}, genre_id={genre_id}, availability={'Available' if book_data['price_info']['saleability'] == 'FOR_SALE' else 'Not Available'}")

        insert_query = sql.SQL("""
            INSERT INTO books (title, first_publish_year, number_of_pages, isbn_10, isbn_13, publisher_id, genre_id, availability)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (openlibrary_work_id) DO NOTHING
            RETURNING book_id;
        """)
        cursor.execute(insert_query, (
            book_data['volume_info']['title'], 
            publish_year,  
            book_data['volume_info']['page_count'], 
            isbn_10,  # Using sanitized ISBN-10
            book_data['volume_info']['identifiers'].get('ISBN_13'), 
            publisher_id, 
            genre_id, 
            'Available' if book_data['price_info']['saleability'] == 'FOR_SALE' else 'Not Available'
        ))
        book_id = cursor.fetchone()
        connection.commit()
        return book_id[0] if book_id else None

def insert_genre(connection, genre_name):
    """ Insert genre data into the genres table. """
    try:
        with connection.cursor() as cursor:
            insert_query = sql.SQL("""
                INSERT INTO genres (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
                RETURNING genre_id;
            """)
            cursor.execute(insert_query, (genre_name,))
            result = cursor.fetchone()
            connection.commit()
            return result[0] if result else None
    except psycopg2.Error as e:
        print(f"Error inserting genre: {e}")
        connection.rollback()
        return None

def insert_subject(connection, book_id, subject):
    """ Insert subject data into the book_subjects table. """
    try:
        with connection.cursor() as cursor:
            insert_query = sql.SQL("""
                INSERT INTO book_subjects (book_id, subject)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            """)
            cursor.execute(insert_query, (book_id, subject))
            connection.commit()
    except psycopg2.Error as e:
        print(f"Error inserting subject: {e}")
        connection.rollback()

def main():
    connection = connect_to_db()
    if not connection:
        print("Database connection could not be established.")
        return

    # Fetch data from APIs
    google_books_collector = GoogleBooksDataCollector(api_key=os.getenv("GOOGLE_API_KEY"))
    open_library_collector = OpenLibraryDataCollector()
    publisher_collector = PublisherDataCollector()

    google_books_data = google_books_collector.fetch_google_books_data("Fiction", max_results=10)
    publisher_data = publisher_collector.aggregate_publisher_data("Penguin Random House", "Random_House")

    # Insert data into the database
    if publisher_data:
        publisher_id = insert_publisher(connection, publisher_data)
    else:
        publisher_id = None

    for book in google_books_data:
        if publisher_id:
            genre_id = None
            if book['volume_info'].get('categories'):
                genre_name = book['volume_info']['categories'][0]  # Assuming the first category as genre
                genre_id = insert_genre(connection, genre_name)

            book_id = insert_book(connection, book, publisher_id, genre_id)
            for author in book['volume_info']['authors']:
                author_id = insert_author(connection, author)
                # Assuming a relationship table exists to link books and authors
                link_book_author(connection, book_id, author_id)

            for subject in book['volume_info'].get('subjects', []):
                insert_subject(connection, book_id, subject)

    connection.close()

def link_book_author(connection, book_id, author_id):
    """ Link book and author in the book_authors table. """
    with connection.cursor() as cursor:
        insert_query = sql.SQL("""
            INSERT INTO book_authors (book_id, author_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        """)
        cursor.execute(insert_query, (book_id, author_id))
        connection.commit()

if __name__ == "__main__":
    main()
