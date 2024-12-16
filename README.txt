# Biblio-Tech (Books Database)

**Fall 2024 - SOEN 363: Data Systems for Software Engineers**  

## Overview
Biblio-Tech is a books database project that integrates data from the Google Books API and Open Library API into both relational and NoSQL databases.

---

## Project Structure

### **Relational Database (Phase 1)**

This phase focuses on implementing a relational database using PostgreSQL.  

**Files:**
- `ddl.sql`: Contains all table/view definitions and the database schema.  
- `dml.sql`: Implements all required SQL queries.  
- `fetch.py`: Fetches data from the Google Books and Open Library APIs.  
- `insert.py`: Inserts fetched data into the PostgreSQL database.  
- `main.py`: The main script for the data pipeline, coordinating fetching and insertion.  
- `relationaldiagram.png`: Entity Relationship Diagram (ERD) for the relational database.  
- `execute_relational.py`: Script for performance testing of indexes.*  

---

### **NoSQL Database (Phase 2)**

This phase migrates data to a Neo4j graph database and implements graph queries using Cypher.

**Files:**
- `transfer.py`: Migrates data from PostgreSQL to Neo4j.  
- `cypher.txt`: Cypher queries for the Neo4j database.  
- `execute_cypher.py`: Executes Cypher queries and performance testing of indexes.  
- `nosqldiagram.png`: Graph database diagram for Neo4j.  
- `bookdatabase_backup.sql`: Backup of the database.  
  **[Download Link (260MB)](https://drive.google.com/file/d/1IjdChQXsLHD2RjP2efJ109vx9HWiEcG3/view?usp=drive_link)**  

---

## Data Migration

**Source:** PostgreSQL Relational Database  
**Destination:** Neo4j NoSQL Graph Database  
**Migration Tool:** Custom Python script (`transfer.py`)  

