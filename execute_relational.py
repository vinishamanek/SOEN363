import psycopg2
import time
from typing import List, Dict, Any, Tuple
import os
from dotenv import load_dotenv

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
            # index for ratings
            cur.execute("""
                CREATE INDEX IF NOT EXISTS ratings_avg_idx 
                ON Ratings(avg_rating);
            """)
            
            # index for book prices
            cur.execute("""
                CREATE INDEX IF NOT EXISTS price_list_idx 
                ON Price(list_price);
            """)
            
            # index for category names
            cur.execute("""
                CREATE INDEX IF NOT EXISTS category_name_idx 
                ON Category(name);
            """)
            
            # index for publication year
            cur.execute("""
                CREATE INDEX IF NOT EXISTS book_year_idx 
                ON Book(publication_year);
            """)
            
            # index for language code
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
            """
        }

        # test queries before creating indexes
        print("\nBefore creating indexes:")
        self._run_queries(queries)

        # create indexes
        print("\nCreating indexes...")
        self.create_indexes()

        # test queries after creating indexes
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
    
    # load environment variables
    load_dotenv()
    
    # relational db connection details
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")

    # create querier instance and run demonstrations
    querier = PostgresQuerier(dbname, user, password, host, port)
    try:
        querier.demonstrate_queries()
    finally:
        querier.close()

if __name__ == "__main__":
    main()