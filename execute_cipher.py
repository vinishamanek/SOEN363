# execute_cipher.py

from neo4j import GraphDatabase
import time
from typing import List, Dict, Any

class Neo4jQuerier:
    def __init__(self, uri: str, username: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        self.driver.close()

    def measure_query_time(self, query: str, params: Dict = None) -> tuple[List[Dict[str, Any]], float]:
        """Execute a query and measure its execution time"""
        with self.driver.session() as session:
            start_time = time.time()
            result = list(session.run(query, params or {}))
            execution_time = time.time() - start_time
            return [dict(record) for record in result], execution_time

# COMMENTING OUT THE CREATE INDEXES FOR NOW, WILL NEED TO FIX
    # def create_indexes(self):
    #     """Create indexes for better query performance"""
    #     with self.driver.session() as session:
    #         # Full-text search index for book titles
    #         session.run("""
    #             CREATE FULLTEXT INDEX book_title_index IF NOT EXISTS
    #             FOR (b:Book) ON EACH [b.title]
    #         """)
            
    #         # Index for book ratings
    #         session.run("""
    #             CREATE INDEX book_rating_index IF NOT EXISTS
    #             FOR (b:Book) ON b.avg_rating
    #         """)
            
    #         # Index for book prices
    #         session.run("""
    #             CREATE INDEX price_amount_index IF NOT EXISTS
    #             FOR (p:Price) ON p.list_price
    #         """)
            
    #         # Composite index for category name and book count
    #         session.run("""
    #             CREATE INDEX category_name_index IF NOT EXISTS
    #             FOR (c:Category) ON c.name
    #         """)

    def demonstrate_queries(self):
        """Run all query types and measure their performance"""
        queries = {
            "basic search on attribute value": """
                MATCH (b:Book)
                WHERE b.publication_year > 2023
                RETURN b.title, b.publication_year
            """,
            
            "aggregation paperback": """
                MATCH (b:Book)
                WHERE b.format = "Paperback"
                RETURN count(b)
            """,
            
            "aggregation hardcover": """
                MATCH (b:Book)
                WHERE b.format = "Hardcover"
                RETURN count(b)
            """,
            
            "aggregation ebook": """
                MATCH (b:Book)
                WHERE b.ebook_url IS NOT NULL
                RETURN count(b)
            """,
            
            # add description of exactly what the query is
            "top n entities satisfying a criteria, sorted by an attribute ": """
                MATCH (b:Book)
                WHERE b.language_code = "en " and b.publication_year IS NOT NULL and b.page_count > 10000
                RETURN b.title, b.publication_year, b.language_code
                ORDER BY b.publication_year DESC
                LIMIT 300
            """,
            
            # is group by a thing in cipher? need to check, but this is a good example query
            # "group books by year of publication": """
            #     MATCH (b:Book)
            #     WHERE b.publication_year IS NOT NULL
            #     RETURN b.publication_year as year, count(*) as number_of_books
            #     ORDER BY year DESC
            # """,
            
            
            # "full text search": """
                
            # """
        }

        # test queries before creating indexes
        print("\nBefore creating indexes:")
        for name, query in queries.items():
            params = {"search_term": "python programming"} if name == "Full Text Search" else None
            results, execution_time = self.measure_query_time(query, params)
            print(f"\n{name}:")
            print(f"Execution time: {execution_time:.4f} seconds")
            print(f"Sample results: {results[:2]}")

        # # create indexes
        # print("\nCreating indexes...")
        # self.create_indexes()

        # # test queries after creating indexes
        # print("\nAfter creating indexes:")
        # for name, query in queries.items():
        #     params = {"search_term": "python programming"} if name == "Full Text Search" else None
        #     results, execution_time = self.measure_query_time(query, params)
        #     print(f"\n{name}:")
        #     print(f"Execution time: {execution_time:.4f} seconds")
        #     print(f"Sample results: {results[:2]}")

def main():
    # connection details
    uri = "bolt://localhost:7687"
    username = "neo4j"
    password = "soen363!"  

    querier = Neo4jQuerier(uri, username, password)
    try:
        querier.demonstrate_queries()
    finally:
        querier.close()

if __name__ == "__main__":
    main()