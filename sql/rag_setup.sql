-- Use your existing context
USE DATABASE LEARNING_DB;
USE SCHEMA ECOMMERCE;

-- Create table for PDF chunks and embeddings
CREATE OR REPLACE TABLE PDF_DOC_CHUNKS (
	DOC_ID STRING,
	FILENAME STRING,
	CHUNK_INDEX INTEGER,
	CONTENT STRING,
	EMBEDDING VECTOR(FLOAT, 768)
);

-- Optional: Vector index (requires feature enablement). Skipped if unsupported.
