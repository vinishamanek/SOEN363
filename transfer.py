from dotenv import load_dotenv
import os
import psycopg2
from neo4j import GraphDatabase
from tqdm import tqdm

load_dotenv()

# postgreSQL connection
PG_HOST = os.getenv("DB_HOST")
PG_USER = os.getenv("DB_USER")
PG_DB = os.getenv("DB_NAME")  
PG_PASSWORD = os.getenv("DB_PASSWORD")
PG_PORT = os.getenv("DB_PORT")

pg_conn = psycopg2.connect(
    host=PG_HOST,
    database=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD,
    port=PG_PORT
)
pg_cursor = pg_conn.cursor()

# neo4j connection
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

def create_indexes(session):
    """Create indexes for all node types to optimize relationship creation"""
    print("Creating indexes...")
    session.run("CREATE INDEX book_id IF NOT EXISTS FOR (b:Book) ON (b.id)")
    session.run("CREATE INDEX price_id IF NOT EXISTS FOR (p:Price) ON (p.id)")
    session.run("CREATE INDEX author_id IF NOT EXISTS FOR (a:Author) ON (a.id)")
    session.run("CREATE INDEX publisher_id IF NOT EXISTS FOR (p:Publisher) ON (p.id)")
    session.run("CREATE INDEX category_id IF NOT EXISTS FOR (c:Category) ON (c.id)")
    session.run("CREATE INDEX subject_id IF NOT EXISTS FOR (s:Subject) ON (s.id)")

def create_publisher_nodes(tx, publishers):
    tx.run("""
        UNWIND $publishers AS pub
        CREATE (p:Publisher {
            id: pub.id,
            name: pub.name
        })
    """, publishers=publishers)

def create_author_nodes(tx, authors):
    tx.run("""
        UNWIND $authors AS auth
        CREATE (a:Author {
            id: auth.id,
            name: auth.name
        })
    """, authors=authors)

def create_category_nodes(tx, categories):
    tx.run("""
        UNWIND $categories AS cat
        CREATE (c:Category {
            id: cat.id,
            name: cat.name
        })
    """, categories=categories)

def create_subject_nodes(tx, subjects):
    tx.run("""
        UNWIND $subjects AS sub
        CREATE (s:Subject {
            id: sub.id,
            name: sub.name
        })
    """, subjects=subjects)

def create_book_nodes(tx, books):
    tx.run("""
        UNWIND $books AS book
        CREATE (b:Book {
            id: book.id,
            isbn10: book.isbn10,
            isbn13: book.isbn13,
            title: book.title,
            subtitle: book.subtitle,
            description: book.description,
            language_code: book.language_code,
            publication_year: book.publication_year,
            page_count: book.page_count,
            maturity_rating: book.maturity_rating,
            google_books_id: book.google_books_id,
            google_preview_link: book.google_preview_link,
            google_info_link: book.google_info_link,
            google_canonical_link: book.google_canonical_link,
            avg_rating: book.avg_rating,
            ratings_count: book.ratings_count,
            format: book.format,
            ebook_url: book.ebook_url
        })
    """, books=books)

def create_price_nodes(tx, prices):
    # create all price nodes in batch
    tx.run("""
        UNWIND $prices AS price
        CREATE (p:Price {
            id: price.id,
            country: price.country,
            on_sale_date: price.on_sale_date,
            saleability: price.saleability,
            list_price: price.list_price,
            retail_price: price.retail_price,
            list_price_currency_code: price.list_price_currency_code,
            retail_price_currency_code: price.retail_price_currency_code,
            buy_link: price.buy_link
        })
    """, prices=prices)
    
    # create all price relationships in batch using indexes
    tx.run("""
        UNWIND $prices AS price
        MATCH (b:Book), (p:Price)
        WHERE b.id = price.book_id AND p.id = price.id
        CREATE (b)-[:PRICED_AT]->(p)
    """, prices=prices)

def create_relationships(tx, relationships, rel_type, node_type):
    tx.run(f"""
        UNWIND $rels AS rel
        MATCH (b:Book), (e:{node_type})
        WHERE b.id = rel.book_id AND e.id = rel.entity_id
        CREATE (b)-[:{rel_type}]->(e)
    """, rels=relationships)
    
def main():
    with driver.session() as session:
        # database reset
        session.run("MATCH (n) DETACH DELETE n")
        
        # create indexes first (better performance)
        create_indexes(session)
        
        print("transferring publishers...")
        pg_cursor.execute("SELECT * FROM Publisher")
        publishers = [{"id": row[0], "name": row[1]} for row in pg_cursor.fetchall()]
        session.execute_write(create_publisher_nodes, publishers)
        print(f"{len(publishers)} publishers transferred successfully.")

        print("transferring authors...")
        pg_cursor.execute("SELECT * FROM Author")
        authors = [{"id": row[0], "name": row[1]} for row in pg_cursor.fetchall()]
        session.execute_write(create_author_nodes, authors)
        print(f"{len(authors)} authors transferred successfully.")

        print("transferring categories...")
        pg_cursor.execute("SELECT * FROM Category")
        categories = [{"id": row[0], "name": row[1]} for row in pg_cursor.fetchall()]
        session.execute_write(create_category_nodes, categories)
        print(f"{len(categories)} categories transferred successfully.")

        print("transferring subjects...")
        pg_cursor.execute("SELECT * FROM Subject")
        subjects = [{"id": row[0], "name": row[1]} for row in pg_cursor.fetchall()]
        session.execute_write(create_subject_nodes, subjects)
        print(f"{len(subjects)} subjects transferred successfully.")

        print("transferring books...")
        pg_cursor.execute("""
            SELECT b.*, r.avg_rating, r.ratings_count, 
                   pb.format, eb.ebook_url
            FROM Book b
            LEFT JOIN Ratings r ON b.book_id = r.book_id
            LEFT JOIN PhysicalBook pb ON b.book_id = pb.book_id
            LEFT JOIN EBook eb ON b.book_id = eb.book_id
        """)
        books = []
        for row in pg_cursor.fetchall():
            books.append({
                "id": row[0],
                "isbn10": row[1],
                "isbn13": row[2],
                "title": row[3],
                "subtitle": row[4],
                "description": row[5],
                "language_code": row[6],
                "publication_year": row[7],
                "page_count": row[8],
                "maturity_rating": row[9],
                "google_books_id": row[10],
                "google_preview_link": row[11],
                "google_info_link": row[12],
                "google_canonical_link": row[13],
                "avg_rating": float(row[14]) if row[14] else None,
                "ratings_count": row[15],
                "format": row[16],
                "ebook_url": row[17]
            })
        session.execute_write(create_book_nodes, books)
        print(f"{len(books)} books transferred successfully.")

        print("transferring prices...")
        pg_cursor.execute("SELECT * FROM Price")
        prices = [
            {
                "id": row[0],
                "book_id": row[1],
                "country": row[2],
                "on_sale_date": str(row[3]),
                "saleability": row[4],
                "list_price": float(row[5]) if row[5] else None,
                "retail_price": float(row[6]) if row[6] else None,
                "list_price_currency_code": row[7],
                "retail_price_currency_code": row[8],
                "buy_link": row[9]
            }
            for row in pg_cursor.fetchall()
        ]
        session.execute_write(create_price_nodes, prices)
        print(f"{len(prices)} prices transferred successfully.")

        print("creating relationships...")
        # Book-Author relationships
        pg_cursor.execute("SELECT * FROM BookAuthor")
        author_rels = [{"book_id": row[0], "entity_id": row[1]} for row in pg_cursor.fetchall()]
        session.execute_write(create_relationships, author_rels, "AUTHORED_BY", "Author")
        print(f"{len(author_rels)} book-author relationships created successfully.")

        # Book-Publisher relationships
        pg_cursor.execute("SELECT * FROM BookPublisher")
        publisher_rels = [{"book_id": row[0], "entity_id": row[1]} for row in pg_cursor.fetchall()]
        session.execute_write(create_relationships, publisher_rels, "PUBLISHED_BY", "Publisher")
        print(f"{len(publisher_rels)} book-publisher relationships created successfully.")

        # Book-Category relationships
        pg_cursor.execute("SELECT * FROM BookCategory")
        category_rels = [{"book_id": row[0], "entity_id": row[1]} for row in pg_cursor.fetchall()]
        session.execute_write(create_relationships, category_rels, "CATEGORIZED_AS", "Category")
        print(f"{len(category_rels)} book-category relationships created successfully.")

        # Book-Subject relationships
        pg_cursor.execute("SELECT * FROM BookSubject")
        subject_rels = [{"book_id": row[0], "entity_id": row[1]} for row in pg_cursor.fetchall()]
        session.execute_write(create_relationships, subject_rels, "HAS_SUBJECT", "Subject")
        print(f"{len(subject_rels)} book-subject relationships created successfully.")

if __name__ == "__main__":
    main()
    print("Migration completed successfully!")