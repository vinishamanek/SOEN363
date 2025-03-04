-- DML QUERIES

-- 1. basic select with simple where clause

SELECT title, publication_year, page_count
FROM Book
WHERE publication_year > 2020 AND page_count > 2000;


-- 2. basic select with simple group by clause (with and without having clause)

-- 2.1 without having clause, number of books published each year 
SELECT publication_year, COUNT(*) as book_count
FROM Book
GROUP BY publication_year
ORDER BY publication_year DESC;

-- 2.2 with having clause, publishers with more than 5 published books
SELECT p.name, COUNT(bp.book_id) as published_books
FROM Publisher p
JOIN BookPublisher bp ON p.publisher_id = bp.publisher_id
GROUP BY p.publisher_id, p.name
HAVING COUNT(bp.book_id) > 5;


-- 3. a simple join query and equivalent implementation using cartesian product and where clause

-- 3.1 simple join query, books and their authors
SELECT b.title, a.name as author_name
FROM Book b
JOIN BookAuthor ba ON b.book_id = ba.book_id
JOIN Author a ON ba.author_id = a.author_id;

-- 3.2 equivalent using cartesian product and where clause, books and their authors
SELECT b.title, a.name as author_name
FROM Book b, BookAuthor ba, Author a
WHERE b.book_id = ba.book_id 
  AND ba.author_id = a.author_id;


-- 4. a few queries to demonstrate various join types on the same tables

-- 4.1 inner join, books that have a corresponding rating
SELECT b.book_id, b.title, r.avg_rating
FROM Book b
INNER JOIN Ratings r ON b.book_id = r.book_id;

-- 4.2 left join, all books with their respective rating (rating may be null)
SELECT b.book_id, b.title, r.avg_rating
FROM Book b
LEFT JOIN Ratings r ON b.book_id = r.book_id;

-- 4.3 right join, all ratings with their respective books (in our case, equivalent to inner join) 
SELECT b.book_id, b.title, r.avg_rating
FROM Book b
RIGHT JOIN Ratings r ON b.book_id = r.book_id;

-- 4.4 full outer join, all books and all ratings 
-- (equivalent to left join in our case, since there can't be a rating without the book) 
SELECT b.book_id, b.title, r.avg_rating
FROM Book b
FULL OUTER JOIN Ratings r ON b.book_id = r.book_id;


-- 5.  queries to demonstrate use of null values for undefined/non-applicable.

-- 5.1 null descriptions
SELECT title, description
FROM Book
WHERE description IS NULL;

-- 5.2 null ratings
SELECT b.title, r.avg_rating, r.ratings_count
FROM Book b
LEFT JOIN Ratings r ON b.book_id = r.book_id
WHERE r.avg_rating IS NULL;

-- 5.3 null prices
SELECT b.title, pr.list_price, pr.retail_price
FROM Book b
LEFT JOIN Price pr ON b.book_id = pr.book_id
WHERE pr.list_price IS NULL OR pr.retail_price IS NULL;


-- 6. correlated queries

-- 6.1 books with higher rating than the average: 
SELECT b.title, r.avg_rating AS "book rating",
    (SELECT AVG(avg_rating) FROM Ratings) AS "overall average rating"
FROM Book b
JOIN Ratings r ON b.book_id = r.book_id
WHERE r.avg_rating > (SELECT AVG(avg_rating) FROM Ratings)
ORDER BY r.avg_rating DESC;

-- 6.2 authors that have books published after 2020
SELECT DISTINCT 
    a.name AS author_name,
    b.publication_year
FROM Author a
JOIN BookAuthor ba ON a.author_id = ba.author_id
JOIN Book b ON ba.book_id = b.book_id
WHERE b.publication_year > 2020
ORDER BY a.name;

-- 7. set operations

-- 7.1 intersect:

-- books with both authors and publishers, using INTERSECT
SELECT b.book_id, b.title
FROM Book b
JOIN BookAuthor ba ON b.book_id = ba.book_id
INTERSECT
SELECT b.book_id, b.title
FROM Book b
JOIN BookPublisher bp ON b.book_id = bp.book_id;

-- equivalent (books with both authors and publishers), without INTERSECT
SELECT DISTINCT b.book_id, b.title
FROM Book b
JOIN BookAuthor ba ON b.book_id = ba.book_id
JOIN BookPublisher bp ON b.book_id = bp.book_id;

-- 7.2 union:

-- books with either authors or publishers, using UNION
SELECT b.book_id, b.title 
FROM Book b 
JOIN BookAuthor ba ON b.book_id = ba.book_id
UNION
SELECT b.book_id, b.title
FROM Book b 
JOIN BookPublisher bp ON b.book_id = bp.book_id;

-- equivalent (books with either authors or publishers), without UNION
SELECT DISTINCT b.book_id, b.title
FROM Book b 
LEFT JOIN BookAuthor ba ON b.book_id = ba.book_id
LEFT JOIN BookPublisher bp ON b.book_id = bp.book_id
WHERE ba.book_id IS NOT NULL 
   OR bp.book_id IS NOT NULL;

-- 7.3 difference:

-- books with authors but no publishers, using EXCEPT
SELECT b.book_id, b.title 
FROM Book b 
JOIN BookAuthor ba ON b.book_id = ba.book_id
EXCEPT
SELECT b.book_id, b.title
FROM Book b 
JOIN BookPublisher bp ON b.book_id = bp.book_id;

-- equivalent (books with authors but no publishers), without EXCEPT
SELECT DISTINCT b.book_id, b.title
FROM Book b 
JOIN BookAuthor ba ON b.book_id = ba.book_id
LEFT JOIN BookPublisher bp ON b.book_id = bp.book_id
WHERE bp.book_id IS NULL;

-- 8. a view with hard-coded criteria

CREATE OR REPLACE VIEW books_after_2020 AS
SELECT 
    b.book_id,
    b.title,
    b.publication_year,
    string_agg(DISTINCT a.name, '; ') AS authors
FROM 
    Book b
    LEFT JOIN BookAuthor ba ON b.book_id = ba.book_id
    LEFT JOIN Author a ON ba.author_id = a.author_id
WHERE 
    b.publication_year > 2020  -- hard-coded criteria
GROUP BY 
    b.book_id;

SELECT * FROM books_after_2020;


-- 9. a few examples of constraints: overlap and covering constraints

-- 9.1 overlap constraint:
-- finds books that violate mutual exclusion by appearing in both PhysicalBook and EBook tables
-- note that the isbn10 and isbn13 columns are different for a physical book and its corresponding ebook
SELECT b1.title, b1.book_id as physical_id, b2.book_id as ebook_id
FROM Book b1
JOIN PhysicalBook p ON b1.book_id = p.book_id
JOIN Book b2 ON b1.title = b2.title AND b1.book_id != b2.book_id
JOIN EBook e ON b2.book_id = e.book_id;

-- 9.2 covering constraint:
-- finds books that violate covering constraint by not appearing in either PhysicalBook or EBook tables
-- (in our case currently, nothing should be returned since there is complete coverage)
SELECT b.book_id, b.title
FROM Book b
WHERE NOT EXISTS (SELECT 1 FROM PhysicalBook p WHERE p.book_id = b.book_id)
  AND NOT EXISTS (SELECT 1 FROM EBook e WHERE e.book_id = b.book_id);

-- equvalent query for covering constraint using LEFT JOIN
SELECT b.book_id, b.title
FROM Book b
LEFT JOIN PhysicalBook p ON b.book_id = p.book_id
LEFT JOIN EBook e ON b.book_id = e.book_id
WHERE p.book_id IS NULL AND e.book_id IS NULL;

-- 10. implementation of division operator queries using NOT IN and NOT EXISTS

-- 10.1 regular nested query using NOT IN
SELECT DISTINCT b.book_id, b.title 
FROM Book b
WHERE b.book_id NOT IN (
    SELECT DISTINCT b2.book_id
    FROM Book b2
    WHERE EXISTS (
        (SELECT s.subject_id 
         FROM Subject s
         WHERE s.name IN ('Mathematics', 'Computer science'))
        EXCEPT
        (SELECT bs.subject_id 
         FROM BookSubject bs
         WHERE bs.book_id = b2.book_id)
    )
);

-- 10.2 correlated nested query using NOT EXISTS and EXCEPT
SELECT DISTINCT b.book_id, b.title 
FROM Book AS b
WHERE NOT EXISTS (
           (SELECT s.subject_id 
            FROM Subject AS s
            WHERE s.name IN ('Mathematics', 'Computer science'))
            EXCEPT
           (SELECT bs.subject_id 
            FROM BookSubject AS bs 
            WHERE bs.book_id = b.book_id)
);

-- extra 10. terrible performance querying using not in (really bad performance)
SELECT DISTINCT b.book_id, b.title 
FROM Book AS b
WHERE b.book_id NOT IN (
    SELECT b2.book_id 
    FROM Book AS b2, Subject AS s
    WHERE s.name IN ('Mathematics', 'Computer science')
    AND s.subject_id NOT IN (
        SELECT bs.subject_id 
        FROM BookSubject AS bs
        WHERE bs.book_id = b2.book_id
    )
);