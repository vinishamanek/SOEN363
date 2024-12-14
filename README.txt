Biblio-Tech (Books Database)
Fall 2024 - SOEN 363: Data Systems for Software Engineers

Data Sources:
-Google Books API
-Open Library API

Project Structure

Relational Database - Phase 1
- ddl.sql: Contains all table/view definitions and database schema
- dml.sql: Contains all required query implementations
- fetch.py: Script for fetching data from APIs
- insert.py: Script for inserting data into database
- main.py: Main script for data pipeline
- relationaldiagram.png: Database schema diagram (ERD)
- execute_relational.py: Additional script for performance testing (not a requirement for
phase 1 submission)

NoSQL Database - Phase 2
Project Structure
- transfer.py: Script for migrating data from PostgreSQL to Neo4j
- cypher.txt: Contains all Cypher query implementations
- execute_cypher.py: Script for performance testing and query execution
- nosqldiagram.png: Neo4j database (graph) diagram
- bookdatabase_backup.sql: https://drive.google.com/file/d/1IjdChQXsLHD2RjP2efJ109vx9HWiEcG3/view?usp=drive_link (260MB)

Data Migration
- Source: PostgreSQL relational database from Phase 1
- Destination: Neo4j graph database
- Migration tool: Custom Python script (transfer.py)