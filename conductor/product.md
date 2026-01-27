# Product Definition

## Initial Concept
LeakHarvester is a high-performance breach data ingestion and search engine powered by ClickHouse. It is designed to handle massive datasets with speed and security, providing tools for researchers and administrators to analyze breach data effectively.

## Core Value Proposition
LeakHarvester solves the challenge of managing and searching through vast amounts of unstructured breach data. It leverages the power of ClickHouse for rapid ingestion and retrieval, while ensuring data security through robust encryption standards.

## Target Audience
- **Security Researchers & Threat Intelligence Analysts:** For investigating breaches, identifying trends, and gathering intelligence.
- **System Administrators:** For managing large-scale datasets and ensuring efficient data storage and retrieval.
- **Cybersecurity Professionals:** For performing breach assessments and verifying compromised credentials.

## Key Features
- **High-Performance Ingestion:**  Rapidly bulk loads CSV/JSON data into ClickHouse with automatic deduplication.
- **Secure Storage & Transport:**  Utilizes military-grade encryption (Argon2id + XChaCha20) and ZSTD compression for secure import/export of data containers (.lh).
- **Smart Indexing:**  Automatically manages storage-efficient Inverted Indexes on String columns for sub-second full-text search capabilities.
- **Search CLI:**  Provides instant substring and full-text search capabilities with regex support for flexible data exploration.
- **Auto-Repair:**  Includes mechanisms to detect and fix index inconsistencies to ensure data integrity.
- **Docker Integration:**  Simplifies deployment and database management via Docker and Docker Compose.
- **Database Maintenance:**  Consolidated `db` command for file management and database reset/wiping operations.
