-- Drop all existing objects if they exist
-- DROP TABLE IF EXISTS Contributor CASCADE;
DROP TABLE IF EXISTS BookAuthor CASCADE;
DROP TABLE IF EXISTS BookGenre CASCADE;
DROP TABLE IF EXISTS BookSeries CASCADE;
DROP TABLE IF EXISTS EBook CASCADE;
DROP TABLE IF EXISTS PhysicalBook CASCADE;
DROP TABLE IF EXISTS Book CASCADE;
DROP TABLE IF EXISTS Author CASCADE;
DROP TABLE IF EXISTS Publisher CASCADE;
DROP TABLE IF EXISTS Category CASCADE;
DROP TABLE IF EXISTS Subject CASCADE;
DROP TABLE IF EXISTS BookSubject CASCADE;
DROP TABLE IF EXISTS BookCategory CASCADE;
DROP TABLE IF EXISTS BookAuthor CASCADE;
DROP TABLE IF EXISTS BookPublisher CASCADE;
DROP TABLE IF EXISTS Price CASCADE;

DROP VIEW IF EXISTS AdminBookInfo CASCADE;
DROP VIEW IF EXISTS PublicBookInfo CASCADE;

DROP DOMAIN IF EXISTS ISBN_TYPE10 CASCADE;
DROP DOMAIN IF EXISTS ISBN_TYPE13 CASCADE;
DROP DOMAIN IF EXISTS RATING_TYPE CASCADE;
DROP DOMAIN IF EXISTS URL_TYPE CASCADE;

DROP TYPE IF EXISTS format_type CASCADE;
DROP TYPE IF EXISTS MATURITY_RATING CASCADE;
DROP TYPE IF EXISTS content_rating CASCADE;

-- Create domains and custom types
CREATE TYPE FORMAT_TYPE AS ENUM ('Hardcover', 'Paperback', 'Ebook');
CREATE TYPE MATURITY_RATING AS ENUM ('G', 'PG', 'Teen', 'Mature', 'Adult');

-- CREATE DOMAIN ISBN_TYPE10 AS VARCHAR(10)
--     CHECK (VALUE ~ '^(?:\d{10}|\d{15})$');
-- CREATE DOMAIN ISBN_TYPE13 AS VARCHAR(13)
--     CHECK (VALUE ~ '^(?:\d{13}|\d{20})$');

CREATE DOMAIN ISBN_TYPE10 AS VARCHAR(10)
    CHECK (VALUE ~ '^[0-9]{9}[0-9X]$');

CREATE DOMAIN ISBN_TYPE13 AS VARCHAR(13)
    CHECK (VALUE ~ '^[0-9]{13}$');

CREATE DOMAIN RATING_TYPE AS DECIMAL(2, 1)
    CHECK (VALUE >= 0.0 AND VALUE <= 5.0);

CREATE DOMAIN URL_TYPE AS VARCHAR(2048)
    CHECK (VALUE ~ '^https?://');

CREATE TABLE Publisher (
    publisher_id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL UNIQUE
);

CREATE TABLE Author (
    author_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    author_openlib_id VARCHAR(15)
);

CREATE TABLE Book (
    book_id SERIAL PRIMARY KEY,
    isbn10 ISBN_TYPE10 NOT NULL UNIQUE,
    isbn13 ISBN_TYPE13 NOT NULL UNIQUE,
    title VARCHAR(500) NOT NULL,
    subtitle VARCHAR(500),
    description TEXT,
    language_code CHAR(3),
    publication_year INTEGER CHECK (publication_year >= 1400 AND publication_year <= EXTRACT(YEAR FROM CURRENT_DATE)),
    page_count INTEGER CHECK (page_count > 0),
    avg_rating RATING_TYPE,
    rating_count INTEGER CHECK (rating_count >= 0),
    maturity_rating MATURITY_RATING,
    openlibrary_work_id VARCHAR(50),
    openlibrary_edition_id VARCHAR(50),
    google_books_id VARCHAR(50),
    google_preview_link URL_TYPE,
    google_info_link URL_TYPE,
    google_canonical_link URL_TYPE
);

CREATE TABLE PhysicalBook (
    book_id INTEGER PRIMARY KEY REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    format FORMAT_TYPE NOT NULL
);

CREATE TABLE EBook (
    book_id SERIAL PRIMARY KEY REFERENCES Book (book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    ebook_url URL_TYPE
);

-- -- Contributor Table
-- CREATE TABLE Contributor (
--                              contributor_id SERIAL PRIMARY KEY,
--                              name VARCHAR(200) NOT NULL,
--                              role VARCHAR(100), -- e.g., Illustrator, Editor
--                              book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE
-- );


CREATE TABLE Category (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE Subject (
    subject_id SERIAL PRIMARY KEY,
    name VARCHAR(500) NOT NULL UNIQUE
);


CREATE TABLE BookSubject (
    book_id INTEGER REFERENCES Book (book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    subject_id INTEGER REFERENCES Subject (subject_id) ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY(book_id, subject_id)
);

CREATE TABLE BookCategory (
    book_id INTEGER REFERENCES Book (book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    category_id INTEGER REFERENCES Category (category_id) ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY(book_id, category_id)
);

CREATE TABLE BookAuthor (
    book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    author_id INTEGER REFERENCES Author(author_id) ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY(book_id, author_id)
);

CREATE TABLE BookPublisher (
    book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    publisher_id INTEGER REFERENCES Publisher(publisher_id) ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY(book_id, publisher_id)
);

CREATE TABLE Price (
    price_id SERIAL PRIMARY KEY,
    book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    country CHAR(3),
    on_sale_date DATE,
    saleability VARCHAR(20),
    list_price DECIMAL(10, 2) CHECK (list_price >= 0),
    retail_price DECIMAL(10, 2) CHECK (retail_price >= 0),
    list_price_currency_code CHAR(3),
    retail_price_currency_code CHAR(3),
    buy_link URL_TYPE,
    UNIQUE(book_id, country, on_sale_date)
);


-- -- Public Book Info View
-- CREATE VIEW PublicBookInfo AS
-- SELECT b.title,
--        b.subtitle,
--        b.publication_year,
--        b.average_rating,
--        b.ratings_count,
--        p.name                                                        AS publisher,
--        string_agg(DISTINCT a.first_name || ' ' || a.last_name, '; ') AS authors,
--        string_agg(DISTINCT g.name, ', ')                             AS genres
-- FROM Book b
--          LEFT JOIN Publisher p ON b.publisher_id = p.publisher_id
--          LEFT JOIN BookAuthor ba ON b.book_id = ba.book_id
--          LEFT JOIN Author a ON ba.author_id = a.author_id
--          LEFT JOIN BookGenre bg ON b.book_id = bg.book_id
--          LEFT JOIN Genre g ON bg.genre_id = g.genre_id
-- WHERE b.content_rating != 'Adult'
-- GROUP BY b.book_id, p.publisher_id;
--
-- -- Admin Book Info View
-- CREATE VIEW AdminBookInfo AS
-- SELECT b.book_id,
--        b.isbn10,
--        b.isbn13,
--        b.title,
--        b.subtitle,
--        b.description                                                 AS book_description,
--        b.language_code,
--        b.publication_year,
--        b.page_count,
--        b.average_rating,
--        b.ratings_count,
--        b.content_rating,
--        b.openlibrary_work_id,
--        b.openlibrary_edition_id,
--        b.google_books_id,
--        p.publisher_id,
--        p.name                                                        AS publisher_name,
--        p.description                                                 AS publisher_description,
--        p.founding_year,
--        p.website                                                     AS publisher_website,
--        p.country_code,
--        p.openlibrary_publisher_id,
--        string_agg(DISTINCT a.first_name || ' ' || a.last_name, '; ') AS authors,
--        string_agg(DISTINCT g.name, ', ')                             AS genres,
--        ph.price                                                      AS current_price,
--        ph.currency_code
-- FROM Book b
--          LEFT JOIN Publisher p ON b.publisher_id = p.publisher_id
--          LEFT JOIN BookAuthor ba ON b.book_id = ba.book_id
--          LEFT JOIN Author a ON ba.author_id = a.author_id
--          LEFT JOIN BookGenre bg ON b.book_id = bg.book_id
--          LEFT JOIN Genre g ON bg.genre_id = g.genre_id
--          LEFT JOIN PriceHistory ph ON b.book_id = ph.book_id
-- WHERE ph.end_date IS NULL -- Filter active prices
-- GROUP BY b.book_id, p.publisher_id, ph.price, ph.currency_code;
