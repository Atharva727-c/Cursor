"""
Wrapper module for RAG queries
"""

from typing import List, Tuple, Dict, Any
from rag_query import retrieve_context, build_prompt
from snowflake_connect import connect_snowflake


def query_rag_wrapper(question: str, k: int = 5) -> Dict[str, Any]:
    """
    Wrapper function to query RAG system.
    
    Args:
        question: Natural language question
        k: Number of context chunks to retrieve
        
    Returns:
        Dictionary with results
    """
    try:
        # Retrieve relevant contexts
        contexts = retrieve_context(question, k=k)
        if not contexts:
            return {
                "system": "RAG",
                "answer": "I couldn't find any relevant information in the documents. Please try rephrasing your question.",
                "sources": [],
                "success": False
            }
        
        # Build prompt
        prompt = build_prompt(question, contexts)
        
        # Get LLM response
        with connect_snowflake() as conn, conn.cursor() as cur:
            # Try multiple models in order of preference
            models = ['llama3-8b', 'mistral-7b', 'mixtral-8x7b', 'snowflake-arctic']
            answer = None
            last_error = None
            for model in models:
                try:
                    cur.execute(
                        f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', %s)",
                        (prompt,),
                    )
                    (answer,) = cur.fetchone()
                    break
                except Exception as e:
                    last_error = str(e)
                    continue
            
            if answer is None:
                raise Exception(f"All models failed. Last error: {last_error}")
        
        # Clean up the answer - remove any chunk references that might have slipped through
        import re
        # Remove references like "chunk [1]", "in chunk 2", "chunk [3]", etc.
        answer = re.sub(r'\bchunk\s*\[?\d+\]?', '', answer, flags=re.IGNORECASE)
        answer = re.sub(r'\bin\s+chunk\s+\[?\d+\]?', '', answer, flags=re.IGNORECASE)
        answer = re.sub(r'\[?\d+\]?\s*\(file:.*?\)', '', answer)
        answer = re.sub(r'\(file:.*?\)', '', answer)
        # Clean up extra spaces and newlines
        answer = re.sub(r'\n\s*\n\s*\n+', '\n\n', answer)  # Multiple newlines to double
        answer = answer.strip()
        
        # Format sources
        sources = []
        for i, (ctx, fn, ci, sim) in enumerate(contexts, 1):
            sources.append({
                "index": i,
                "filename": fn,
                "chunk_index": ci,
                "similarity": float(sim),
                "content": ctx[:200] + "..." if len(ctx) > 200 else ctx
            })
        
        return {
            "system": "RAG",
            "question": question,
            "answer": answer,
            "sources": sources,
            "source_count": len(sources),
            "success": True
        }
    
    except Exception as e:
        return {
            "system": "RAG",
            "error": str(e),
            "success": False
        }
