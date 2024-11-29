import psycopg2
import time
from typing import List, Dict, Any, Tuple

class PostgresQuerier:
    def __init__(self, dbname: str, user: str, password: str, host: str, port: str):
        """Initialize database connection"""
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

    def close(self):
        """Close database connection"""
        self.conn.close()

    def measure_query_time(self, query: str, params: Dict = None) -> Tuple[List[Dict[str, Any]], float]:
        """Execute a query and measure its execution time"""
        with self.conn.cursor() as cur:
            start_time = time.time()
            cur.execute(query, params or {})
            try:
                result = cur.fetchall()
            except psycopg2.ProgrammingError:
                # For queries that don't return results (like CREATE INDEX)
                result = []
            execution_time = time.time() - start_time
            
            # Get column names for dictionary creation
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in result], execution_time
            return [], execution_time

    def create_indexes(self):
        """Create indexes for better query performance"""
        with self.conn.cursor() as cur:
            # Index for book titles (using gin for full text search)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS book_title_idx 
                ON Book USING gin(to_tsvector('english', title));
            """)
            
            # Index for ratings
            cur.execute("""
                CREATE INDEX IF NOT EXISTS ratings_avg_idx 
                ON Ratings(avg_rating);
            """)
            
            # Index for book prices
            cur.execute("""
                CREATE INDEX IF NOT EXISTS price_list_idx 
                ON Price(list_price);
            """)
            
            # Index for category names
            cur.execute("""
                CREATE INDEX IF NOT EXISTS category_name_idx 
                ON Category(name);
            """)
            
            # Index for publication year
            cur.execute("""
                CREATE INDEX IF NOT EXISTS book_year_idx 
                ON Book(publication_year);
            """)
            
            # Index for language code
            cur.execute("""
                CREATE INDEX IF NOT EXISTS book_language_idx 
                ON Book(language_code);
            """)
            
        self.conn.commit()

    def demonstrate_queries(self):
        """Run all query types and measure their performance"""
        queries = {
            "basic search on attribute value": """
                SELECT title, publication_year
                FROM Book
                WHERE publication_year > 2023;
            """,
            
            "aggregation paperback": """
                SELECT COUNT(*)
                FROM PhysicalBook
                WHERE format = 'Paperback';
            """,
            
            "aggregation hardcover": """
                SELECT COUNT(*)
                FROM PhysicalBook
                WHERE format = 'Hardcover';
            """,
            
            "aggregation ebook": """
                SELECT COUNT(*)
                FROM Ebook
            """,
            
            "top n entities satisfying criteria": """
                SELECT title, publication_year, language_code
                FROM Book
                WHERE language_code = 'en ' 
                AND publication_year IS NOT NULL 
                AND page_count > 10000
                ORDER BY publication_year DESC
                LIMIT 300;
            """,
            
            "books group by publication year": """
                SELECT publication_year as year, COUNT(*) as number_of_books
                FROM Book
                WHERE publication_year IS NOT NULL
                GROUP BY publication_year
                ORDER BY year DESC;
            """,
            
            "full text title search": """
                SELECT b.title, 
                       string_agg(a.name, '; ') as authors,
                       r.avg_rating,
                       ts_rank(to_tsvector('english', b.title), 
                              to_tsquery('english', %(search_term)s)) as relevance
                FROM Book b
                LEFT JOIN Ratings r ON b.book_id = r.book_id
                LEFT JOIN BookAuthor ba ON b.book_id = ba.book_id
                LEFT JOIN Author a ON ba.author_id = a.author_id
                WHERE to_tsvector('english', b.title) @@ to_tsquery('english', %(search_term)s)
                GROUP BY b.book_id, b.title, r.avg_rating
                ORDER BY relevance DESC
                LIMIT 5;
            """
        }

        # Test queries before creating indexes
        print("\nBefore creating indexes:")
        self._run_queries(queries)

        # Create indexes
        print("\nCreating indexes...")
        self.create_indexes()

        # Test queries after creating indexes
        print("\nAfter creating indexes:")
        self._run_queries(queries)

    def _run_queries(self, queries: Dict[str, str]):
        """Helper method to run queries and print results"""
        for name, query in queries.items():
            params = {"search_term": "python & programming"} if "full text" in name.lower() else None
            results, execution_time = self.measure_query_time(query, params)
            print(f"\n{name}:")
            print(f"Execution time: {execution_time:.6f} seconds")
            print(f"Sample results: {results[:2]}")

def main():
    # Database connection details
    dbname = "bookdatabase"
    user = "postgres"
    password = ""  # Set your password here
    host = "localhost"
    port = "5432"

    # Create querier instance and run demonstrations
    querier = PostgresQuerier(dbname, user, password, host, port)
    try:
        querier.demonstrate_queries()
    finally:
        querier.close()

if __name__ == "__main__":
    main()