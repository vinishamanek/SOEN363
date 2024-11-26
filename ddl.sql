-- DROP TABLES
-- DROP TABLE IF EXISTS Ratings CASCADE;
-- DROP TABLE IF EXISTS BookSubject CASCADE;
-- DROP TABLE IF EXISTS BookCategory CASCADE;
-- DROP TABLE IF EXISTS BookAuthor CASCADE;
-- DROP TABLE IF EXISTS BookPublisher CASCADE;
-- DROP TABLE IF EXISTS Price CASCADE;
-- DROP TABLE IF EXISTS PhysicalBook CASCADE;
-- DROP TABLE IF EXISTS EBook CASCADE;
-- DROP TABLE IF EXISTS Book CASCADE;
-- DROP TABLE IF EXISTS Subject CASCADE;
-- DROP TABLE IF EXISTS Category CASCADE;
-- DROP TABLE IF EXISTS Author CASCADE;
-- DROP TABLE IF EXISTS Publisher CASCADE;

-- DROP DOMAIN IF EXISTS ISBN_TYPE10 CASCADE;
-- DROP DOMAIN IF EXISTS ISBN_TYPE13 CASCADE;
-- DROP DOMAIN IF EXISTS RATING_TYPE CASCADE;
-- DROP DOMAIN IF EXISTS URL_TYPE CASCADE;

-- DROP TYPE IF EXISTS FORMAT_TYPE CASCADE;
-- DROP TYPE IF EXISTS MATURITY_RATING CASCADE;

-- SELECT TABLES
SELECT * FROM Book;
SELECT * FROM Price;
SELECT * FROM PhysicalBook;
SELECT * FROM EBook;
SELECT * FROM Ratings;
SELECT * FROM BookSubject;
SELECT * FROM BookCategory;
SELECT * FROM BookAuthor;
SELECT * FROM BookPublisher;
SELECT * FROM Publisher;
SELECT * FROM Author;
SELECT * FROM Category;
SELECT * FROM Subject;

-- SELECT SIZE OF DB
SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size;
SELECT pg_database_size(current_database()) / 1024 AS database_size_kb;

-- SELECT VIEWS
SELECT * FROM AdminBookInfo;
SELECT * FROM PublicBookInfo;

-- CREATE TYPES
CREATE TYPE FORMAT_TYPE AS ENUM ('Hardcover', 'Paperback', 'Ebook');
CREATE TYPE MATURITY_RATING AS ENUM ('NOT_MATURE', 'MATURE');

-- CREATE DOMAINS
CREATE DOMAIN ISBN_TYPE10 AS VARCHAR(13)
    CONSTRAINT valid_isbn10 CHECK (VALUE ~ '^[0-9]{9}[0-9X]$');

CREATE DOMAIN ISBN_TYPE13 AS VARCHAR(13)
    CONSTRAINT valid_isbn13 CHECK (VALUE ~ '^[0-9]{13}$');

CREATE DOMAIN RATING_TYPE AS DECIMAL(3, 1)
    CHECK (VALUE >= 0.0 AND VALUE <= 5.0);

CREATE DOMAIN URL_TYPE AS VARCHAR(2048)
    CHECK (VALUE ~ '^https?://');

CREATE TABLE Publisher (
    publisher_id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL UNIQUE
);

CREATE TABLE Author (
    author_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE
);

CREATE TABLE Category (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE Subject (
    subject_id SERIAL PRIMARY KEY,
    name VARCHAR(500) NOT NULL UNIQUE
);

CREATE TABLE Book (
    book_id SERIAL PRIMARY KEY,
    isbn10 ISBN_TYPE10 DEFAULT NULL UNIQUE,
    isbn13 ISBN_TYPE13 DEFAULT NULL UNIQUE,
    title VARCHAR(500) NOT NULL,
    subtitle VARCHAR(500),
    description TEXT,
    language_code CHAR(3),
    publication_year INTEGER CHECK (publication_year >= 1400 AND publication_year <= EXTRACT(YEAR FROM CURRENT_DATE)),
    page_count INTEGER CHECK (page_count IS NULL OR page_count > 0),
    maturity_rating MATURITY_RATING,
    google_books_id VARCHAR(50) UNIQUE,
    google_preview_link URL_TYPE,
    google_info_link URL_TYPE,
    google_canonical_link URL_TYPE
);

-- WEAK ENTITY
CREATE TABLE Ratings (
    book_id INTEGER PRIMARY KEY REFERENCES Book(book_id) ON DELETE CASCADE,
    avg_rating RATING_TYPE DEFAULT NULL,
    ratings_count INTEGER DEFAULT 0 CHECK (ratings_count >= 0)
);

-- ISA BOOK
CREATE TABLE PhysicalBook (
    book_id INTEGER PRIMARY KEY REFERENCES Book(book_id) ON DELETE CASCADE,
    format FORMAT_TYPE
);

-- ISA BOOK
CREATE TABLE EBook (
    book_id INTEGER PRIMARY KEY REFERENCES Book(book_id) ON DELETE CASCADE,
    ebook_url URL_TYPE NOT NULL
);

CREATE TABLE Price (
    price_id SERIAL PRIMARY KEY,
    book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE,
    country CHAR(3) NOT NULL,
    on_sale_date DATE NOT NULL,
    saleability VARCHAR(20),
    list_price DECIMAL(10, 2),
    retail_price DECIMAL(10, 2),
    list_price_currency_code CHAR(3),
    retail_price_currency_code CHAR(3),
    buy_link URL_TYPE,
    CONSTRAINT valid_list_price CHECK (list_price IS NULL OR list_price >= 0),
    CONSTRAINT valid_retail_price CHECK (retail_price IS NULL OR retail_price >= 0),
    CONSTRAINT unique_price UNIQUE (book_id, country, on_sale_date)
);

CREATE TABLE BookAuthor (
    book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE,
    author_id INTEGER REFERENCES Author(author_id) ON DELETE CASCADE,
    PRIMARY KEY(book_id, author_id)
);

CREATE TABLE BookPublisher (
    book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE,
    publisher_id INTEGER REFERENCES Publisher(publisher_id) ON DELETE CASCADE,
    PRIMARY KEY(book_id, publisher_id)
);

CREATE TABLE BookCategory (
    book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE,
    category_id INTEGER REFERENCES Category(category_id) ON DELETE CASCADE,
    PRIMARY KEY (book_id, category_id)
);

CREATE TABLE BookSubject (
    book_id INTEGER REFERENCES Book(book_id) ON DELETE CASCADE,
    subject_id INTEGER REFERENCES Subject(subject_id) ON DELETE CASCADE,
    PRIMARY KEY (book_id, subject_id)
);

-- CREATE VIEW AdminBookInfo
CREATE OR REPLACE VIEW AdminBookInfo AS
SELECT
    b.book_id,
    b.isbn10,
    b.isbn13,
    b.title,
    b.subtitle,
    b.description,
    b.language_code,
    b.publication_year,
    b.page_count,
    b.maturity_rating,
    r.avg_rating,
    r.ratings_count,
    p.publisher_id,
    p.name AS publisher_name,
    string_agg(DISTINCT a.name, '; ') AS authors,
    string_agg(DISTINCT c.name, ', ') AS categories,
    string_agg(DISTINCT s.name, ', ') AS subjects
FROM
    Book b
        LEFT JOIN Ratings r ON b.book_id = r.book_id
        LEFT JOIN BookPublisher bp ON b.book_id = bp.book_id
        LEFT JOIN Publisher p ON bp.publisher_id = p.publisher_id
        LEFT JOIN BookAuthor ba ON b.book_id = ba.book_id
        LEFT JOIN Author a ON ba.author_id = a.author_id
        LEFT JOIN BookCategory bc ON b.book_id = bc.book_id
        LEFT JOIN Category c ON bc.category_id = c.category_id
        LEFT JOIN BookSubject bs ON b.book_id = bs.book_id
        LEFT JOIN Subject s ON bs.subject_id = s.subject_id
GROUP BY
    b.book_id, r.avg_rating, r.ratings_count, p.publisher_id, p.name;


-- CREATE VIEW PublicBookInfo
CREATE OR REPLACE VIEW PublicBookInfo AS
SELECT
    b.book_id,
    b.title,
    b.subtitle,
    b.publication_year,
    r.avg_rating,
    r.ratings_count,
    string_agg(DISTINCT a.name, '; ') AS authors
FROM
    Book b
        LEFT JOIN Ratings r ON b.book_id = r.book_id
        LEFT JOIN BookAuthor ba ON b.book_id = ba.book_id
        LEFT JOIN Author a ON ba.author_id = a.author_id
WHERE
        b.maturity_rating = 'NOT_MATURE'
GROUP BY
    b.book_id, r.avg_rating, r.ratings_count;


-- CREATE TRIGGER
CREATE OR REPLACE FUNCTION validate_price()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.retail_price > NEW.list_price THEN
        RAISE EXCEPTION 'Retail price (%s) cannot be higher than list price (%s)', NEW.retail_price, NEW.list_price;
    END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_validate_price
    BEFORE INSERT OR UPDATE ON Price
                         FOR EACH ROW
                         EXECUTE FUNCTION validate_price();

