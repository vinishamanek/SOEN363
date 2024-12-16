# Biblio-Tech (Books Database)

**Fall 2024 - SOEN 363**
## Overview
Biblio-Tech is a books database project which integrates data from the Google Books API and Open Library API into both relational and NoSQL databases.

## Project Structure

### **Relational Database (Phase 1)**

This phase focuses on implementing a relational database using PostgreSQL.  

**Files:**
- `ddl.sql`: Table/view definitions and the database schema.  
- `dml.sql`: SQL queries.  
- `fetch.py`: Fetches data from the Google Books and Open Library APIs.  
- `insert.py`: Inserts fetched data into the PostgreSQL database.  
- `main.py`: The main script for the data pipeline (coordinating fetching and insertion).
- `execute_relational.py`: Script for performance testing of indexes.  
- `relationaldiagram.png`: Entity Relationship Diagram (ERD) for the relational database.  

---

### **NoSQL Database (Phase 2)**

This phase migrates data to a Neo4j graph database and implements graph queries using Cypher.

**Files:**
- `transfer.py`: Migrates data from PostgreSQL to Neo4j.  
- `cypher.txt`: Cypher queries.  
- `execute_cypher.py`: Script for performance testing of indexes. 
- `nosqldiagram.png`: Graph database diagram for Neo4j.  

---

### Database Backup
**[Download Link](https://drive.google.com/file/d/1IjdChQXsLHD2RjP2efJ109vx9HWiEcG3/view?usp=drive_link)**  

---

## Data Migration

**Source:** PostgreSQL Relational Database  
**Destination:** Neo4j NoSQL Graph Database  
**Migration Tool:** Python script (`transfer.py`)  

