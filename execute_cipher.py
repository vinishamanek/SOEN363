from neo4j import GraphDatabase
import time
from typing import List, Dict, Any

class Neo4jQuerier:
   def __init__(self, uri: str, username: str, password: str):
       self.driver = GraphDatabase.driver(uri, auth=(username, password))

   def close(self):
       self.driver.close()

   def measure_query_time(self, query: str, params: Dict = None) -> tuple[List[Dict[str, Any]], float]:
       with self.driver.session() as session:
           start_time = time.time()
           result = list(session.run(query, params or {}))
           execution_time = time.time() - start_time
           return [dict(record) for record in result], execution_time

   def create_indexes(self):
       with self.driver.session() as session:
           
           session.run("""
               DROP INDEX book_rating_index IF EXISTS;
           """)

           session.run("""
               DROP INDEX price_index IF EXISTS;
           """)

           session.run("""
               DROP INDEX year_index IF EXISTS;
           """)

           session.run("""
               DROP INDEX book_search IF EXISTS;
           """)

           session.run("""
               CREATE FULLTEXT INDEX book_search FOR (b:Book) ON EACH [b.title, b.description];
           """)
           
           session.run("""
               CREATE INDEX book_rating_index FOR (b:Book) ON (b.avg_rating);
           """)
           
           session.run("""
               CREATE INDEX price_index FOR (p:Price) ON (p.list_price);
           """)
           
           session.run("""
               CREATE INDEX year_index FOR (b:Book) ON (b.publication_year);
           """)

   def demonstrate_queries(self):
       queries = {
           "basic search on attribute value": """
               MATCH (b:Book)
               WHERE b.publication_year IS NOT NULL
               RETURN b.title, b.publication_year
               ORDER BY b.publication_year DESC
               LIMIT 5
           """,
           
           "aggregation by format and price": """
               MATCH (b:Book)-[:HAS_PRICE]->(p:Price)
               WHERE p.list_price IS NOT NULL AND b.format IS NOT NULL
               RETURN b.format as format, 
                      COUNT(b) as book_count,
                      ROUND(AVG(p.list_price), 2) as avg_price
               ORDER BY avg_price DESC
           """,
           
           "top n rated books": """
               MATCH (b:Book)
               WHERE b.avg_rating IS NOT NULL 
               RETURN b.title, 
                      b.avg_rating,
                      COALESCE(b.ratings_count, 0) as ratings_count
               ORDER BY b.avg_rating DESC
               LIMIT 5
           """,
           
           "group by publisher statistics": """
               MATCH (b:Book)-[:PUBLISHER]->(p:Publisher)
               RETURN p.name as publisher,
                      COUNT(b) as number_of_books,
                      ROUND(AVG(COALESCE(b.avg_rating, 0)), 2) as average_rating
               ORDER BY number_of_books DESC
           """
       }

       print("\nBefore creating indexes:")
       for name, query in queries.items():
           results, execution_time = self.measure_query_time(query)
           print(f"\n{name}:")
           print(f"Execution time: {execution_time:.10f} seconds")
           print(f"Sample results: {results[:2]}")

       print("\nCreating indexes...")
       self.create_indexes()
       time.sleep(2)

       print("\nAfter creating indexes:")
       for name, query in queries.items():
           results, execution_time = self.measure_query_time(query)
           print(f"\n{name}:")
           print(f"Execution time: {execution_time:.10f} seconds")
           print(f"Sample results: {results[:2]}")

       print("\nFull-text search with index:")
       fulltext_query = (
           "CALL db.index.fulltext.queryNodes('book_search', $search_term) "
           "YIELD node, score "
           "WHERE node.title IS NOT NULL "
           "RETURN node.title as title, score "
           "LIMIT 5"
       )
       results, execution_time = self.measure_query_time(fulltext_query, {"search_term": "science technology"})
       print(f"\nFull-text search:")
       print(f"Execution time: {execution_time:.10f} seconds")
       print(f"Sample results: {results[:2]}")

def main():
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