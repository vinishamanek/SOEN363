SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';

DROP TABLE IF EXISTS book_subjects;
DROP TABLE IF EXISTS book_authors;
DROP TABLE IF EXISTS books;
DROP TABLE IF EXISTS author_external_ids;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS authors;
DROP TABLE IF EXISTS publishers;

DROP DOMAIN IF EXISTS email_address;
DROP DOMAIN IF EXISTS isbn_type_13;
DROP DOMAIN IF EXISTS isbn_type_10;
DROP DOMAIN IF EXISTS url_type;
DROP DOMAIN IF EXISTS isbn_type;

DROP TYPE IF EXISTS book_availability;
DROP TYPE IF EXISTS author_status;

SELECT domain_name
FROM information_schema.domains
WHERE domain_schema = 'public';

SELECT * FROM publishers;
SELECT * FROM authors;
SELECT * FROM genres;
SELECT * FROM books;

CREATE DOMAIN email_address AS VARCHAR(255)
    CHECK (VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$');

CREATE DOMAIN isbn_type_13 AS CHAR(13)
    CHECK (VALUE ~ '^\d{13}$');

CREATE DOMAIN isbn_type_10 AS CHAR(10)
    CHECK (VALUE ~ '^\d{9}[\dX]$');

CREATE DOMAIN url_type AS VARCHAR(2048)
    CHECK (VALUE SIMILAR TO 'https?://%');

CREATE TYPE book_availability AS ENUM ('Available', 'Not Available', 'Limited');
CREATE TYPE author_status AS ENUM ('Active', 'Deceased', 'Unknown');

CREATE TABLE publishers (
    publisher_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    founding_year INTEGER,
    country VARCHAR(100)
);

CREATE TABLE authors (
    author_id SERIAL PRIMARY KEY,
    openlibrary_key VARCHAR(100) UNIQUE,
    full_name VARCHAR(255) NOT NULL,
    personal_name VARCHAR(255),
    birth_date DATE,
    death_date DATE,
    status author_status,
    bio TEXT,
    wikipedia_url url_type,
    official_website url_type
);

CREATE TABLE genres (
    genre_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE,
    parent_genre_id INTEGER REFERENCES genres(genre_id) ON DELETE SET NULL
);

CREATE TABLE author_external_ids (
    author_id INTEGER REFERENCES authors(author_id) ON DELETE CASCADE,
    identifier_type VARCHAR(50),
    identifier_value VARCHAR(255),
    PRIMARY KEY (author_id, identifier_type)
);

CREATE TABLE books (
    book_id SERIAL PRIMARY KEY,
    openlibrary_work_id VARCHAR(100) UNIQUE,
    title VARCHAR(500) NOT NULL,
    first_publish_year INTEGER,
    number_of_pages INTEGER,
    isbn_10 isbn_type_10,
    isbn_13 isbn_type_13,
    publisher_id INTEGER REFERENCES publishers(publisher_id) ON DELETE SET NULL,
    genre_id INTEGER REFERENCES genres(genre_id) ON DELETE SET NULL,
    availability book_availability DEFAULT 'Not Available',
    preview_url url_type,
    borrow_url url_type
);

CREATE TABLE book_authors (
    book_id INTEGER REFERENCES books(book_id) ON DELETE CASCADE,
    author_id INTEGER REFERENCES authors(author_id) ON DELETE CASCADE,
    PRIMARY KEY (book_id, author_id)
);

CREATE TABLE book_subjects (
    book_id INTEGER REFERENCES books(book_id) ON DELETE CASCADE,
    subject VARCHAR(255),
    PRIMARY KEY (book_id, subject)
);

SELECT pg_size_pretty( pg_database_size('my_database') );