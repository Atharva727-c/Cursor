# CRH_POC

## Snowflake connection in this project

Follow these steps to connect your Snowflake account and verify the connection from this workspace.

### 1) Create your environment file
1. Duplicate `env.example` to `.env` (same folder).
2. Fill in values from your Snowflake account:
   - `SNOWFLAKE_ACCOUNT` (e.g., `xy12345.us-east-1`)
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_ROLE`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_DATABASE`
   - `SNOWFLAKE_SCHEMA`

### 2) Install dependencies
On Windows PowerShell:

```powershell
cd C:\DS_Training\CRH_POC
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3) Test the connection

```powershell
python .\snowflake_connect.py
```

Expected output:
```
Connected to Snowflake. Current version: X.Y.Z
```

### Using in your code
Import and reuse the connection:

```python
from snowflake_connect import connect_snowflake

with connect_snowflake() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        print(cur.fetchone())
```

## RAG over PDFs using Snowflake Cortex

This project includes a minimal RAG pipeline that:
- Stores PDF chunks and embeddings in Snowflake
- Retrieves top-k chunks via vector similarity
- Calls Snowflake Cortex COMPLETE to generate answers with sources

### 1) Ensure dependencies are installed
```powershell
cd C:\DS_Training\CRH_POC
. .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2) Run SQL setup in Snowflake
Execute:

```powershell
python .\run_sql_file.py sql\\rag_setup.sql
```

This creates `PDF_DOC_CHUNKS` with a vector index in `LEARNING_DB.ECOMMERCE`.

### 3) Add your PDFs
Place PDFs under `PDF Data/` (create the folder if it doesn't exist).

### 4) Ingest PDFs
```powershell
python .\ingest_pdfs.py
```
This reads each PDF, chunks text, inserts rows, and computes embeddings using Snowflake Cortex.

### 5) Ask questions
```powershell
python .\rag_query.py "What are the key findings?"
```
Outputs the answer and source chunk references.

Notes:
- Embedding model used: `e5-base-v2` via `SNOWFLAKE.CORTEX.EMBED_TEXT_768`.
- Generation model used: `snowflake-arctic` via `SNOWFLAKE.CORTEX.COMPLETE`.
- Adjust K, chunk size, models as needed in `ingest_pdfs.py` and `rag_query.py`.
