"""
Test script for the complete integration
"""

from orchestrator import create_orchestrator
from cortex_analyst_wrapper import query_cortex_analyst_wrapper
from rag_wrapper import query_rag_wrapper

def test_integration():
    """Test the complete integration."""
    print("=" * 70)
    print("Testing Complete Integration")
    print("=" * 70)
    
    orchestrator = create_orchestrator()
    
    test_queries = [
        "What are the top 5 customers by total order value?",
        "What does the sustainability report say about carbon emissions?",
        "Show me all products in the database"
    ]
    
    for query in test_queries:
        print(f"\n{'='*70}")
        print(f"Query: {query}")
        print(f"{'='*70}\n")
        
        try:
            result = orchestrator.execute_query(
                question=query,
                cortex_analyst_func=query_cortex_analyst_wrapper,
                rag_query_func=query_rag_wrapper,
                k=5
            )
            
            print(f"Route: {result['classification']['route']}")
            print(f"Query Type: {result['classification']['query_type']}")
            print(f"Confidence: {result['classification']['confidence']:.2f}")
            print(f"\nReasoning: {result['classification']['reasoning']}")
            print(f"\nResponse Preview:\n{result['combined_response'][:200]}...")
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    test_integration()

