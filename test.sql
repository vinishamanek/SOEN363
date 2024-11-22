-- Drop all existing objects if they exist
DROP TABLE IF EXISTS Contributor CASCADE;
DROP TABLE IF EXISTS BookAuthor CASCADE;
DROP TABLE IF EXISTS BookGenre CASCADE;
DROP TABLE IF EXISTS BookSeries CASCADE;
DROP TABLE IF EXISTS Chapter CASCADE;
DROP TABLE IF EXISTS PriceHistory CASCADE;
DROP TABLE IF EXISTS AudioBook CASCADE;
DROP TABLE IF EXISTS EBook CASCADE;
DROP TABLE IF EXISTS PhysicalBook CASCADE;
DROP TABLE IF EXISTS Book CASCADE;
DROP TABLE IF EXISTS Series CASCADE;
DROP TABLE IF EXISTS Genre CASCADE;
DROP TABLE IF EXISTS Author CASCADE;
DROP TABLE IF EXISTS Publisher CASCADE;

DROP VIEW IF EXISTS AdminBookInfo CASCADE;
DROP VIEW IF EXISTS PublicBookInfo CASCADE;

DROP DOMAIN IF EXISTS ISBN_TYPE CASCADE;
DROP DOMAIN IF EXISTS RATING_TYPE CASCADE;
DROP DOMAIN IF EXISTS URL_TYPE CASCADE;

DROP TYPE IF EXISTS format_type CASCADE;
DROP TYPE IF EXISTS series_status CASCADE;
DROP TYPE IF EXISTS content_rating CASCADE;

-- Create domains and custom types
CREATE TYPE format_type AS ENUM ('Hardcover', 'Paperback', 'Ebook');
CREATE TYPE series_status AS ENUM ('Ongoing', 'Completed');
CREATE TYPE content_rating AS ENUM ('G', 'PG', 'Teen', 'Mature', 'Adult');

-- CREATE DOMAIN ISBN_TYPE10 AS VARCHAR(15)
--     CHECK (VALUE ~ '^(?:\d{10}|\d{15})$');
-- CREATE DOMAIN ISBN_TYPE13 AS VARCHAR(20)
--     CHECK (VALUE ~ '^(?:\d{10}|\d{20})$');
CREATE DOMAIN RATING_TYPE AS DECIMAL(2,1)
    CHECK (VALUE >= 0.0 AND VALUE <= 5.0);
CREATE DOMAIN URL_TYPE AS VARCHAR(2048)
    CHECK (VALUE ~ '^https?://');

-- Publisher Table
CREATE TABLE Publisher (
                           publisher_id SERIAL PRIMARY KEY,
                           name VARCHAR(200) NOT NULL UNIQUE,
                           description TEXT,
                           founding_year INTEGER CHECK (founding_year BETWEEN 1400 AND EXTRACT(YEAR FROM CURRENT_DATE)),
                           website URL_TYPE,
                           country_code CHAR(2),
                           openlibrary_publisher_id VARCHAR(50)
);

-- Author Table
CREATE TABLE Author (
                        author_id SERIAL PRIMARY KEY,
                        first_name VARCHAR(100),
                        last_name VARCHAR(100) NOT NULL,
                        alternate_names TEXT[],
                        birth_date DATE,
                        death_date DATE,
                        biography TEXT,
                        website URL_TYPE,
                        openlibrary_author_id VARCHAR(50),
                        goodreads_author_id VARCHAR(50),
                        wikipedia_url URL_TYPE,
--    ratings_breakdown JSONB, -- Optional, unavailable for now
--    total_works INTEGER, -- Optional, unavailable for now
--    profile_image URL_TYPE, -- Optional, unavailable for now
                        CONSTRAINT author_dates_check CHECK (death_date IS NULL OR birth_date IS NULL OR death_date > birth_date),
                        CONSTRAINT unique_author UNIQUE (first_name, last_name)
);

-- Series Table
CREATE TABLE Series (
                        series_id SERIAL PRIMARY KEY,
                        title VARCHAR(500) NOT NULL UNIQUE,
                        description TEXT,
                        status series_status DEFAULT 'Ongoing',
                        planned_volumes INTEGER,
                        openlibrary_series_id VARCHAR(50),
                        goodreads_series_id VARCHAR(50)
);

-- Book Table
CREATE TABLE Book (
                      book_id SERIAL PRIMARY KEY,
                      isbn10 ISBN_TYPE10 NOT NULL UNIQUE,
                      isbn13 ISBN_TYPE13 NOT NULL UNIQUE,
                      title VARCHAR(500) NOT NULL,
                      subtitle VARCHAR(500),
                      description TEXT,
                      language_code CHAR(3),
                      publication_year INTEGER CHECK (publication_year >= 1400 AND publication_year <= EXTRACT(YEAR FROM CURRENT_DATE)),
                      publisher_id INTEGER REFERENCES Publisher(publisher_id) ON DELETE SET NULL ON UPDATE CASCADE,
                      page_count INTEGER CHECK (page_count > 0),
                      average_rating RATING_TYPE DEFAULT 0.0,
                      maturity_rating VARCHAR(50),
                      ratings_count INTEGER DEFAULT 0 CHECK (ratings_count >= 0),
                      content_rating content_rating,
                      openlibrary_work_id VARCHAR(50),
                      openlibrary_edition_id VARCHAR(50),
                      google_books_id VARCHAR(50),
                      google_preview_link URL_TYPE,
                      google_info_link URL_TYPE,
                      google_canonical_link URL_TYPE,
--    text_snippet TEXT, -- Optional, unavailable for now
--    reading_modes JSONB, -- Optional, unavailable for now
--    panelization_summary JSONB, -- Optional, unavailable for now
--    ratings_breakdown JSONB, -- Optional, unavailable for now
                      series_id INTEGER REFERENCES Series(series_id) ON DELETE SET NULL
);

-- Physical Book Table
CREATE TABLE PhysicalBook (
                              book_id INTEGER PRIMARY KEY REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
                              weight_grams DECIMAL(7,2) CHECK (weight_grams > 0),
                              height_mm DECIMAL(5,2) CHECK (height_mm > 0),
                              width_mm DECIMAL(5,2) CHECK (width_mm > 0),
                              thickness_mm DECIMAL(5,2) CHECK (thickness_mm > 0),
                              format format_type NOT NULL,
--    printing_house VARCHAR(200), -- Optional, unavailable for now
--    binding_type VARCHAR(50) -- Optional, unavailable for now
);

-- EBook Table
CREATE TABLE EBook (
                       book_id INTEGER PRIMARY KEY REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
                       file_size_bytes BIGINT CHECK (file_size_bytes > 0),
                       drm_protected BOOLEAN DEFAULT true,
--    supported_devices TEXT[], -- Optional, unavailable for now
--    download_format VARCHAR(50)[] -- Optional, unavailable for now
);

-- AudioBook Table
CREATE TABLE AudioBook (
                           book_id INTEGER PRIMARY KEY REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
                           duration_seconds INTEGER CHECK (duration_seconds > 0),
--    narrator VARCHAR(200), -- Optional, unavailable for now
--    audio_format VARCHAR(50)[], -- Optional, unavailable for now
--    sample_rate INTEGER -- Optional, unavailable for now
);

-- Contributor Table
CREATE TABLE Contributor (
                             contributor_id SERIAL PRIMARY KEY,
                             name VARCHAR(200) NOT NULL,
                             role VARCHAR(100), -- e.g., Illustrator, Editor
                             book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Chapter Table
CREATE TABLE Chapter (
                         book_id INTEGER,
                         chapter_number INTEGER CHECK (chapter_number > 0),
                         title VARCHAR(500),
                         start_page INTEGER,
                         end_page INTEGER,
                         word_count INTEGER,
                         PRIMARY KEY (book_id, chapter_number),
                         FOREIGN KEY (book_id) REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
                         CONSTRAINT chapter_pages_check CHECK (end_page >= start_page)
);

-- Book-Series Relationship
CREATE TABLE BookSeries (
                            book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
                            series_id INTEGER REFERENCES Series(series_id) ON DELETE CASCADE ON UPDATE CASCADE,
                            volume_number INTEGER CHECK (volume_number > 0),
                            PRIMARY KEY (book_id, series_id)
);

-- Genre Table
CREATE TABLE Genre (
                       genre_id SERIAL PRIMARY KEY,
                       name VARCHAR(100) NOT NULL UNIQUE,
                       description TEXT,
                       parent_genre_id INTEGER REFERENCES Genre(genre_id) ON DELETE SET NULL
);

-- Book-Genre Relationship
CREATE TABLE BookGenre (
                           book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
                           genre_id INTEGER REFERENCES Genre(genre_id) ON DELETE CASCADE ON UPDATE CASCADE,
                           PRIMARY KEY (book_id, genre_id)
);

-- Book-Author Relationship
CREATE TABLE BookAuthor (
                            book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
                            author_id INTEGER REFERENCES Author(author_id) ON DELETE CASCADE ON UPDATE CASCADE,
                            role VARCHAR(50) DEFAULT 'Author',
                            PRIMARY KEY (book_id, author_id)
);

-- Price History Table
CREATE TABLE PriceHistory (
                              price_id SERIAL PRIMARY KEY,
                              book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
                              price DECIMAL(10,2) CHECK (price >= 0),
                              currency_code CHAR(3),
                              effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                              end_date TIMESTAMP, -- Optional, uncommented for use in views
                              source VARCHAR(50),
                              CONSTRAINT price_dates_check CHECK (end_date IS NULL OR end_date > effective_date)
);

-- Reviews Table
CREATE TABLE Reviews (
                         review_id SERIAL PRIMARY KEY,
                         book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE,
--    user_id INTEGER, -- Optional, unavailable for now
                         rating DECIMAL(2,1) CHECK (rating >= 0 AND rating <= 5),
                         text TEXT,
                         spoiler BOOLEAN DEFAULT FALSE,
--    review_date DATE -- Optional, unavailable for now
);

-- Digital Content Table
CREATE TABLE DigitalContent (
                                content_id SERIAL PRIMARY KEY,
                                book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE,
                                source VARCHAR(100), -- e.g., "Project Gutenberg"
                                content_url URL_TYPE,
--    format VARCHAR(50), -- Optional, unavailable for now
--    file_size INTEGER -- Optional, unavailable for now
);

-- Public Book Info View
CREATE VIEW PublicBookInfo AS
SELECT
    b.title,
    b.subtitle,
    b.publication_year,
    b.average_rating,
    b.ratings_count,
    p.name AS publisher,
    string_agg(DISTINCT a.first_name || ' ' || a.last_name, '; ') AS authors,
    string_agg(DISTINCT g.name, ', ') AS genres
FROM Book b
         LEFT JOIN Publisher p ON b.publisher_id = p.publisher_id
         LEFT JOIN BookAuthor ba ON b.book_id = ba.book_id
         LEFT JOIN Author a ON ba.author_id = a.author_id
         LEFT JOIN BookGenre bg ON b.book_id = bg.book_id
         LEFT JOIN Genre g ON bg.genre_id = g.genre_id
WHERE b.content_rating != 'Adult'
GROUP BY b.book_id, p.publisher_id;

-- Admin Book Info View
CREATE VIEW AdminBookInfo AS
SELECT
    b.book_id,
    b.isbn10,
    b.isbn13,
    b.title,
    b.subtitle,
    b.description AS book_description,
    b.language_code,
    b.publication_year,
    b.page_count,
    b.average_rating,
    b.ratings_count,
    b.content_rating,
    b.openlibrary_work_id,
    b.openlibrary_edition_id,
    b.google_books_id,
    p.publisher_id,
    p.name AS publisher_name,
    p.description AS publisher_description,
    p.founding_year,
    p.website AS publisher_website,
    p.country_code,
    p.openlibrary_publisher_id,
    string_agg(DISTINCT a.first_name || ' ' || a.last_name, '; ') AS authors,
    string_agg(DISTINCT g.name, ', ') AS genres,
    ph.price AS current_price,
    ph.currency_code
FROM Book b
         LEFT JOIN Publisher p ON b.publisher_id = p.publisher_id
         LEFT JOIN BookAuthor ba ON b.book_id = ba.book_id
         LEFT JOIN Author a ON ba.author_id = a.author_id
         LEFT JOIN BookGenre bg ON b.book_id = bg.book_id
         LEFT JOIN Genre g ON bg.genre_id = g.genre_id
         LEFT JOIN PriceHistory ph ON b.book_id = ph.book_id
WHERE ph.end_date IS NULL -- Filter active prices
GROUP BY b.book_id, p.publisher_id, ph.price, ph.currency_code;
