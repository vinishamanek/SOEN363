-- DML QUERIES

-- 1. basic select with simple where clause

SELECT title, publication_year, page_count
FROM Book
WHERE publication_year > 2010 AND page_count > 200;

-- 2. basic select with simple group by clause (with and without having clause)

-- without having clause, number of books published each year 
SELECT publication_year, COUNT(*) as book_count
FROM Book
GROUP BY publication_year
ORDER BY publication_year DESC;

-- with having clauses, publishers with more than 5 published books
SELECT p.name, COUNT(bp.book_id) as published_books
FROM Publisher p
JOIN BookPublisher bp ON p.publisher_id = bp.publisher_id
GROUP BY p.publisher_id, p.name
HAVING COUNT(bp.book_id) > 5;

-- 3. a simple join query and equivalent implementation using cartesian product and where clause

-- simple join query, books and their authors
SELECT b.title, a.name as author_name
FROM Book b
JOIN BookAuthor ba ON b.book_id = ba.book_id
JOIN Author a ON ba.author_id = a.author_id;

-- equivalent using cartesian product and where clause, books and their authors
SELECT b.title, a.name as author_name
FROM Book b, BookAuthor ba, Author a
WHERE b.book_id = ba.book_id 
  AND ba.author_id = a.author_id;


-- 4. a few queries to demonstrate various join types on the same tables

-- inner join, books that have a corresponding rating
SELECT b.book_id, b.title, r.avg_rating
FROM Book b
INNER JOIN Ratings r ON b.book_id = r.book_id;

-- left join, all books with their respective rating (rating may be null)
SELECT b.book_id, b.title, r.avg_rating
FROM Book b
LEFT JOIN Ratings r ON b.book_id = r.book_id;

-- right join, all ratings with their respective books (in our case, equivalent to inner join) 
SELECT b.book_id, b.title, r.avg_rating
FROM Book b
RIGHT JOIN Ratings r ON b.book_id = r.book_id;

-- full outer join, all books and all ratings 
-- (equivalent to left join in our case, since there can't be a rating without the book) 
SELECT b.book_id, b.title, r.avg_rating
FROM Book b
FULL OUTER JOIN Ratings r ON b.book_id = r.book_id;

-- 5.  queries to demonstrate use of null values for undefined/non-applicable.

-- null descriptions
SELECT title, description
FROM Book
WHERE description IS NULL;

-- null ratings
SELECT b.title, r.avg_rating, r.ratings_count
FROM Book b
LEFT JOIN Ratings r ON b.book_id = r.book_id
WHERE r.avg_rating IS NULL;

-- null prices
SELECT b.title, pr.list_price, pr.retail_price
FROM Book b
LEFT JOIN Price pr ON b.book_id = pr.book_id
WHERE pr.list_price IS NULL OR pr.retail_price IS NULL;

-- 6. examples of correlated queries

-- example 1, books with higher rating than the average: 
SELECT b.title, r.avg_rating AS "book rating",
    (SELECT AVG(avg_rating) FROM Ratings) AS "overall average rating"
FROM Book b
JOIN Ratings r ON b.book_id = r.book_id
WHERE r.avg_rating > (SELECT AVG(avg_rating) FROM Ratings)
ORDER BY r.avg_rating DESC;

-- example 2, authors that have books published after 2020
SELECT a.name AS author_name,
       (SELECT b.publication_year
        FROM BookAuthor ba
        JOIN Book b ON ba.book_id = b.book_id
        WHERE ba.author_id = a.author_id
        AND b.publication_year > 2020) AS publication_year
FROM Author a
WHERE EXISTS (
    SELECT 1
    FROM BookAuthor ba
    JOIN Book b ON ba.book_id = b.book_id
    WHERE ba.author_id = a.author_id
    AND b.publication_year > 2020
);

-- example 3, books with price is higher than the average price of books in the same category and country
SELECT b.title, c.name AS category_name, p.country, p.retail_price,(
           SELECT AVG(p2.retail_price)
           FROM Price p2
           JOIN Book b2 ON p2.book_id = b2.book_id
           JOIN BookCategory bc2 ON b2.book_id = bc2.book_id
           WHERE bc2.category_id = bc.category_id
           AND p2.country = p.country
       ) AS category_avg_price
FROM Book b
JOIN BookCategory bc ON b.book_id = bc.book_id
JOIN Category c ON bc.category_id = c.category_id
JOIN Price p ON b.book_id = p.book_id
WHERE p.retail_price > (
    SELECT AVG(p2.retail_price)
    FROM Price p2
    JOIN Book b2 ON p2.book_id = b2.book_id
    JOIN BookCategory bc2 ON b2.book_id = bc2.book_id
    WHERE bc2.category_id = bc.category_id
    AND p2.country = p.country
)
ORDER BY c.name, p.country, p.retail_price DESC;

-- 7. One example per set operations: intersect, union, and difference vs. their equivalences
-- without using set operations.

-- insersect
-- equivalent to intersect
-- 1. INTERSECT Example
-- Find books that have both ratings AND categories, showing detailed information
SELECT b.book_id, b.title, b.publication_year, r.avg_rating, r.ratings_count
FROM Book b
JOIN Ratings r ON b.book_id = r.book_id
INTERSECT
SELECT b.book_id, b.title, b.publication_year, r.avg_rating, r.ratings_count
FROM Book b
JOIN BookCategory bc ON b.book_id = bc.book_id
JOIN Ratings r ON b.book_id = r.book_id;


--equvalent without INTERSECT
SELECT DISTINCT b.book_id, b.title, b.publication_year, r.avg_rating, r.ratings_count
FROM Book b
JOIN Ratings r ON b.book_id = r.book_id
WHERE EXISTS (SELECT 1 FROM BookCategory bc WHERE bc.book_id = b.book_id
);

-- union
-- equivalent to union
-- UNION Example: Find books that are either physical books OR have high ratings (> 4.0)
SELECT 
    b.book_id,
    b.title,
    b.publication_year,
    p.format as physical_format,
    NULL as rating,
    'Physical Book' as source
FROM Book b
JOIN PhysicalBook p ON b.book_id = p.book_id
UNION
SELECT 
    b.book_id,
    b.title,
    b.publication_year,
    NULL as physical_format,
    r.avg_rating as rating,
    'Highly Rated' as source
FROM Book b
JOIN Ratings r ON b.book_id = r.book_id
WHERE r.avg_rating > 4.0;

-- Equivalent without UNION (using OR conditions and CASE expressions)
SELECT 
    b.book_id,
    b.title,
    b.publication_year,
    CASE 
        WHEN pb.format IS NOT NULL THEN pb.format
        ELSE NULL
    END as physical_format,
    CASE 
        WHEN r.avg_rating > 4.0 THEN r.avg_rating
        ELSE NULL
    END as rating,
    CASE 
        WHEN pb.format IS NOT NULL THEN 'Physical Book'
        ELSE 'Highly Rated'
    END as source
FROM Book b
LEFT JOIN PhysicalBook pb ON b.book_id = pb.book_id
LEFT JOIN Ratings r ON b.book_id = r.book_id
WHERE pb.book_id IS NOT NULL 
   OR (r.avg_rating IS NOT NULL AND r.avg_rating > 4.0);

-- difference
-- equivalent to difference
-- EXCEPT (DIFFERENCE) Example
-- Find books that have ratings but don't have any categories assigned
-- Using EXCEPT
SELECT b.book_id, b.title
FROM Book b
JOIN Ratings r ON b.book_id = r.book_id
EXCEPT
SELECT b.book_id, b.title
FROM Book b
JOIN BookCategory bc ON b.book_id = bc.book_id;

-- Equivalent without EXCEPT
SELECT b.book_id, b.title
FROM Book b
JOIN Ratings r ON b.book_id = r.book_id
WHERE NOT EXISTS (
    SELECT 1 
    FROM BookCategory bc 
    WHERE bc.book_id = b.book_id
);







-- 8. a view with hard-coded criteria




-- 9. a few examples of constraints: overlap and covering constraints

-- Query 1: Check for Overlap Constraint Violation 
-- This query finds books that violate the mutually exclusive constraint
-- by appearing in both PhysicalBook and EBook tables
SELECT b.book_id, b.title, p.format AS physical_format, e.ebook_url
FROM Book b
JOIN PhysicalBook p ON b.book_id = p.book_id
JOIN EBook e ON b.book_id = e.book_id;

-- Query 2: Check for Covering Constraint Violation
-- This query finds books that violate the covering constraint
-- by not appearing in either PhysicalBook or EBook tables
SELECT b.book_id, b.title
FROM Book b
WHERE b.book_id NOT IN (
    SELECT book_id FROM PhysicalBook
    UNION
    SELECT book_id FROM EBook
);


-- Find books that appear in related categories and subjects overlap?? idk if valid
SELECT b.book_id, b.title,
       STRING_AGG(DISTINCT c.name, ', ') as categories,
       STRING_AGG(DISTINCT s.name, ', ') as subjects
FROM Book b
JOIN BookCategory bc ON b.book_id = bc.book_id
JOIN Category c ON bc.category_id = c.category_id
JOIN BookSubject bs ON b.book_id = bs.book_id
JOIN Subject s ON bs.subject_id = s.subject_id
GROUP BY b.book_id, b.title
HAVING COUNT(DISTINCT c.category_id) > 0 
AND COUNT(DISTINCT s.subject_id) > 0;

-- Query 2: Check for Books without Publishers (Covering)
-- Shows books that don't have any publisher assigned,
-- which might be missing important information
SELECT b.book_id, b.title, bp.publisher_id
FROM Book b
LEFT JOIN BookPublisher bp ON b.book_id = bp.book_id
WHERE bp.publisher_id IS NULL;




-- 10. a few examples of division queries using NOT IN and NOT EXISTS-- First, let's verify what subjects exist for reference






















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