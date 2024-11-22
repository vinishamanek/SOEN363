-- 1. Basic select with simple where clause
-- Find all books published after 2000 with more than 300 pages
SELECT title,
    publication_year,
    page_count
FROM Book
WHERE publication_year > 2000
    AND page_count > 300;
-- 2a. Basic select with simple group by clause (without having)
-- Count books published per year
SELECT publication_year,
    COUNT(*) as book_count
FROM Book
GROUP BY publication_year
ORDER BY publication_year DESC;
-- 3a. Simple join query
-- List all books with their authors
SELECT b.title,
    a.name as author_name
FROM Book b
    JOIN BookAuthor ba ON b.book_id = ba.book_id
    JOIN Author a ON ba.author_id = a.author_id;
-- 3b. Equivalent using cartesian product and where clause
SELECT b.title,
    a.name as author_name
FROM Book b,
    BookAuthor ba,
    Author a
WHERE b.book_id = ba.book_id
    AND ba.author_id = a.author_id;
-- 4a. Inner Join
-- Books and their publishers
SELECT b.title,
    p.name as publisher_name
FROM Book b
    INNER JOIN BookPublisher bp ON b.book_id = bp.book_id
    INNER JOIN Publisher p ON bp.publisher_id = p.publisher_id;
-- 4b. Left Outer Join
-- All books and their publishers (including books without publishers)
SELECT b.title,
    p.name as publisher_name
FROM Book b
    LEFT JOIN BookPublisher bp ON b.book_id = bp.book_id
    LEFT JOIN Publisher p ON bp.publisher_id = p.publisher_id;
-- 4c. Right Outer Join
-- All publishers and their books (including publishers without books)
SELECT b.title,
    p.name as publisher_name
FROM Book b
    RIGHT JOIN BookPublisher bp ON b.book_id = bp.book_id
    RIGHT JOIN Publisher p ON bp.publisher_id = p.publisher_id;
-- 4d. Full Outer Join
-- All books and publishers (including unmatched on both sides)
SELECT b.title,
    p.name as publisher_name
FROM Book b
    FULL OUTER JOIN BookPublisher bp ON b.book_id = bp.book_id
    FULL OUTER JOIN Publisher p ON bp.publisher_id = p.publisher_id;
-- 5. Demonstrating NULL values
-- Find books with missing ISBNs or page counts
SELECT title,
    isbn10,
    isbn13,
    page_count
FROM Book
WHERE isbn10 IS NULL
    OR isbn13 IS NULL
    OR page_count IS NULL;
-- 6a. Correlated query example 1
-- Find books with above-average page count for their publication year
SELECT b1.title,
    b1.page_count,
    b1.publication_year
FROM Book b1
WHERE b1.page_count > (
        SELECT AVG(b2.page_count)
        FROM Book b2
        WHERE b2.publication_year = b1.publication_year
    );
-- 6b. Correlated query example 2
-- Find authors who have written more books than the average author
SELECT a.name,
    COUNT(*) as book_count
FROM Author a
    JOIN BookAuthor ba ON a.author_id = ba.author_id
GROUP BY a.author_id,
    a.name
HAVING COUNT(*) > (
        SELECT AVG(book_count)
        FROM (
                SELECT COUNT(*) as book_count
                FROM BookAuthor
                GROUP BY author_id
            ) as avg_books
    );
-- 7a. Set operation: INTERSECT
-- Find books that are both ebooks and have physical copies
SELECT b.book_id,
    b.title
FROM Book b
    JOIN EBook e ON b.book_id = e.book_id
INTERSECT
SELECT b.book_id,
    b.title
FROM Book b
    JOIN PhysicalBook p ON b.book_id = p.book_id;
-- 7b. Equivalent without INTERSECT
SELECT DISTINCT b.book_id,
    b.title
FROM Book b
    JOIN EBook e ON b.book_id = e.book_id
    JOIN PhysicalBook p ON b.book_id = p.book_id;
-- 7c. Set operation: UNION
-- Get all unique categories and subjects
SELECT name
FROM Category
UNION
SELECT name
FROM Subject;
-- 7d. Set operation: EXCEPT (DIFFERENCE)
-- Find categories that aren't used by any book
SELECT c.name
FROM Category c
EXCEPT
SELECT c.name
FROM Category c
    JOIN BookCategory bc ON c.category_id = bc.category_id;
-- 8. View with hard-coded criteria
CREATE OR REPLACE VIEW RecentPopularBooks AS
SELECT b.title,
    b.publication_year,
    r.avg_rating,
    r.rating_count
FROM Book b
    JOIN Rating r ON r.rating_id = b.book_id
WHERE b.publication_year >= 2020 -- Hard-coded year
    AND r.avg_rating >= 4.0 -- Hard-coded rating threshold
ORDER BY r.rating_count DESC;
-- 9a. Overlap constraint example
-- Ensure no book is both an ebook and a physical book
ALTER TABLE EBook
ADD CONSTRAINT no_overlap_physical CHECK NOT EXISTS (
        SELECT 1
        FROM PhysicalBook p
        WHERE p.book_id = EBook.book_id
    );
-- 9b. Covering constraint example
-- Ensure every book is either an ebook or a physical book
CREATE OR REPLACE FUNCTION check_book_format() RETURNS TRIGGER AS $$ BEGIN IF NOT EXISTS (
        SELECT 1
        FROM EBook e
        WHERE e.book_id = NEW.book_id
        UNION
        SELECT 1
        FROM PhysicalBook p
        WHERE p.book_id = NEW.book_id
    ) THEN RAISE EXCEPTION 'Book must be either electronic or physical';
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER ensure_book_format
AFTER
INSERT ON Book FOR EACH ROW EXECUTE FUNCTION check_book_format();
-- 10a. Division using NOT IN
-- Find authors who have written books in all categories
SELECT a.name
FROM Author a
WHERE NOT EXISTS (
        SELECT c.category_id
        FROM Category c
        WHERE c.category_id NOT IN (
                SELECT bc.category_id
                FROM Book b
                    JOIN BookAuthor ba ON b.book_id = ba.book_id
                    JOIN BookCategory bc ON b.book_id = bc.book_id
                WHERE ba.author_id = a.author_id
            )
    );
-- 10b. Division using NOT EXISTS and EXCEPT
SELECT a.name
FROM Author a
WHERE NOT EXISTS (
        SELECT c.category_id
        FROM Category c
        EXCEPT
        SELECT bc.category_id
        FROM Book b
            JOIN BookAuthor ba ON b.book_id = ba.book_id
            JOIN BookCategory bc ON b.book_id = bc.book_id
        WHERE ba.author_id = a.author_id
    );