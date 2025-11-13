# Unified Query System Integration Summary

## Overview
This system integrates three components:
1. **EPAM DIAL Orchestrator** - Routes queries to appropriate systems
2. **Cortex Analyst** - Handles structured data queries (tables: orders, customers, products, etc.)
3. **RAG System** - Handles unstructured document queries (PDFs)

## Architecture

```
User Query
    ↓
EPAM DIAL Orchestrator (GPT-4)
    ↓
    ├─→ Analytics Query → Cortex Analyst → SQL Generation → Results
    ├─→ Document Query → RAG System → PDF Search → Answer
    └─→ Hybrid Query → Both Systems → Combined Results
```

## Files Created

### Core Modules
- `orchestrator.py` - EPAM DIAL-based query router
- `cortex_analyst_wrapper.py` - Wrapper for Cortex Analyst queries
- `rag_wrapper.py` - Wrapper for RAG queries
- `cortex_analyst_relationships.yaml` - Table relationships configuration

### Integration
- `streamlit_app.py` - Updated with orchestrator integration
- `test_integration.py` - Test script for the complete system

## Query Routing Logic

### Analytics Queries (→ Cortex Analyst)
Examples:
- "What are the top 5 customers by revenue?"
- "Show me sales by product category"
- "Which customers have the highest order values?"

### Document Queries (→ RAG)
Examples:
- "What does the sustainability report say about emissions?"
- "What are the key findings in the PDF?"
- "Summarize the earnings call transcript"

### Hybrid Queries (→ Both)
Examples:
- "Compare our sales data with what the report says"
- "What do our documents say about the numbers in our database?"

## Usage

### Streamlit App
```bash
python -m streamlit run streamlit_app.py
```

Access at: http://localhost:8501

### Test Script
```bash
python test_integration.py
```

## Configuration

### EPAM DIAL API
- API Key: `dial-j7r9nwg4xmk9spkibd3xrp4hjdg`
- Endpoint: `https://chat.epam.com`
- Model: `gpt-4`

### Snowflake
Configure in `.env` file:
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_USER
- SNOWFLAKE_PASSWORD
- SNOWFLAKE_ROLE
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_DATABASE
- SNOWFLAKE_SCHEMA

## Features

1. **Intelligent Routing** - Uses GPT-4 to analyze query intent
2. **Automatic SQL Generation** - Cortex generates SQL from natural language
3. **Document Search** - RAG retrieves relevant PDF chunks
4. **Combined Results** - Hybrid queries get results from both systems
5. **Visual Feedback** - Streamlit shows routing decisions and sources

## Response Format

```json
{
  "query": "user question",
  "classification": {
    "query_type": "analytics|document|hybrid",
    "route": "CORTEX_ANALYST|RAG|BOTH",
    "reasoning": "explanation",
    "confidence": 0.0-1.0
  },
  "results": {
    "cortex_analyst": {...},
    "rag": {...}
  },
  "combined_response": "formatted answer"
}
```

