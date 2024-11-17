-- drop tables
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

-- drop views
DROP VIEW IF EXISTS AdminBookInfo CASCADE;
DROP VIEW IF EXISTS PublicBookInfo CASCADE;

-- drop custom types + domains
DROP DOMAIN IF EXISTS ISBN_TYPE, RATING_TYPE, URL_TYPE CASCADE;
DROP TYPE IF EXISTS format_type, series_status, content_rating CASCADE;


-- custom types + domains
CREATE TYPE format_type AS ENUM ('Hardcover', 'Paperback', 'Ebook');
CREATE TYPE series_status AS ENUM ('Ongoing', 'Completed');
CREATE TYPE content_rating AS ENUM ('G', 'PG', 'Teen', 'Mature', 'Adult');

CREATE DOMAIN ISBN_TYPE AS VARCHAR(13)
    CHECK (VALUE ~ '^(?:\d{10}|\d{13})$');
CREATE DOMAIN RATING_TYPE AS DECIMAL(2,1)
    CHECK (VALUE >= 0.0 AND VALUE <= 5.0);
CREATE DOMAIN URL_TYPE AS VARCHAR(2048)
    CHECK (VALUE ~ '^https?://');

-- publisher table
CREATE TABLE Publisher (
    publisher_id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL UNIQUE,
    description TEXT,
    founding_year INTEGER CHECK (founding_year BETWEEN 1400 AND EXTRACT(YEAR FROM CURRENT_DATE)),
    website URL_TYPE,
    country_code CHAR(2),
    openlibrary_publisher_id VARCHAR(50)
);

-- author table
CREATE TABLE Author (
    author_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100) NOT NULL,
    birth_date DATE,
    death_date DATE,
    biography TEXT,
    website URL_TYPE,
    openlibrary_author_id VARCHAR(50),
    goodreads_author_id VARCHAR(50),
    wikipedia_url URL_TYPE,
    CONSTRAINT author_dates_check 
        CHECK (death_date IS NULL OR birth_date IS NULL OR death_date > birth_date)
);

-- book table
CREATE TABLE Book (
    book_id SERIAL PRIMARY KEY,
    isbn ISBN_TYPE NOT NULL UNIQUE,
    title VARCHAR(500) NOT NULL,
    subtitle VARCHAR(500),
    description TEXT,
    language_code CHAR(3),
    publication_date DATE,
    publisher_id INTEGER REFERENCES Publisher(publisher_id) ON DELETE SET NULL ON UPDATE CASCADE,
    page_count INTEGER CHECK (page_count > 0),
    average_rating RATING_TYPE DEFAULT 0.0,
    ratings_count INTEGER DEFAULT 0 CHECK (ratings_count >= 0),
    content_rating content_rating,
    openlibrary_work_id VARCHAR(50),
    openlibrary_edition_id VARCHAR(50),
    google_books_id VARCHAR(50)
);

-- physical book table (hardcover, softcover enum?)
-- IS-A relationship with book
CREATE TABLE PhysicalBook (
    book_id INTEGER PRIMARY KEY REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    weight_grams DECIMAL(7,2) CHECK (weight_grams > 0),
    height_mm DECIMAL(5,2) CHECK (height_mm > 0),
    width_mm DECIMAL(5,2) CHECK (width_mm > 0),
    thickness_mm DECIMAL(5,2) CHECK (thickness_mm > 0),
    format format_type NOT NULL,
    printing_house VARCHAR(200),
    binding_type VARCHAR(50)
);

-- ebook table
-- IS-A relationship with book
CREATE TABLE EBook (
    book_id INTEGER PRIMARY KEY REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    file_size_bytes BIGINT CHECK (file_size_bytes > 0),
    drm_protected BOOLEAN DEFAULT true,
    supported_devices TEXT[],
    download_format VARCHAR(50)[]
);

-- audiobook table
-- IS-A relationship with book
CREATE TABLE AudioBook (
    book_id INTEGER PRIMARY KEY REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    duration_seconds INTEGER CHECK (duration_seconds > 0),
    narrator VARCHAR(200),
    audio_format VARCHAR(50)[],
    sample_rate INTEGER
);

-- weak entities and relationships
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

CREATE TABLE Series (
    series_id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL UNIQUE,
    description TEXT,
    status series_status DEFAULT 'Ongoing',
    planned_volumes INTEGER,
    openlibrary_series_id VARCHAR(50),
    goodreads_series_id VARCHAR(50)
);

CREATE TABLE BookSeries (
    book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    series_id INTEGER REFERENCES Series(series_id) ON DELETE CASCADE ON UPDATE CASCADE,
    volume_number INTEGER CHECK (volume_number > 0),
    PRIMARY KEY (book_id, series_id)
);

CREATE TABLE Genre (
    genre_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    parent_genre_id INTEGER REFERENCES Genre(genre_id) ON DELETE SET NULL
);

CREATE TABLE BookGenre (
    book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    genre_id INTEGER REFERENCES Genre(genre_id) ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (book_id, genre_id)
);

CREATE TABLE BookAuthor (
    book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    author_id INTEGER REFERENCES Author(author_id) ON DELETE CASCADE ON UPDATE CASCADE,
    role VARCHAR(50) DEFAULT 'Author',
    PRIMARY KEY (book_id, author_id)
);

CREATE TABLE PriceHistory (
    price_id SERIAL PRIMARY KEY,
    book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE ON UPDATE CASCADE,
    price DECIMAL(10,2) CHECK (price >= 0),
    currency_code CHAR(3),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP,
    source VARCHAR(50),
    CONSTRAINT price_dates_check CHECK (end_date IS NULL OR end_date > effective_date)
);

-- views
-- view with all public book details, authors, genres, publisher, etc
CREATE VIEW PublicBookInfo AS
SELECT 
    b.title,
    b.subtitle,
    b.publication_date,
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

-- view with all publisher and book details, current price and currency codes, etc
-- fix as needed
CREATE VIEW AdminBookInfo AS
SELECT 
    b.*,
    p.*,
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
WHERE ph.end_date IS NULL
GROUP BY b.book_id, p.publisher_id, ph.price, ph.currency_code;

