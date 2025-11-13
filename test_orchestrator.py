"""
Test script for the orchestrator integration
"""

from orchestrator import create_orchestrator
from cortex_analyst_wrapper import query_cortex_analyst_wrapper
from rag_wrapper import query_rag_wrapper

def main():
    print("=" * 70)
    print("Testing Orchestrator Integration")
    print("=" * 70)
    
    # Create orchestrator
    orchestrator = create_orchestrator()
    
    # Test queries
    test_queries = [
        "What are the top 5 customers by total order value?",  # Analytics
        "What information is in the sustainability reports?",   # Document
        "How do our sales compare to what's mentioned in the reports?"  # Hybrid
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{'='*70}")
        print(f"Test {i}: {query}")
        print(f"{'='*70}\n")
        
        try:
            result = orchestrator.route_query(
                user_query=query,
                cortex_analyst_func=query_cortex_analyst_wrapper,
                rag_query_func=lambda q: query_rag_wrapper(q, k=5)
            )
            
            print(f"Query Type: {result['classification']['query_type']}")
            print(f"Confidence: {result['classification']['confidence']:.2f}")
            print(f"Reasoning: {result['classification']['reasoning']}")
            print(f"\nResponse Preview: {result['combined_response'][:200]}...")
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()

