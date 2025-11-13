"""
Orchestrator LLM Module
Routes user queries to Cortex Analyst (structured data) or RAG (unstructured PDFs)
"""

from typing import Dict, Any, Optional, Literal
from openai import AzureOpenAI
import json
import os


class QueryOrchestrator:
    """
    Orchestrator that uses EPAM DIAL LLM to route queries to appropriate systems.
    """
    
    def __init__(self, api_key: str = None):
        """
        Initialize the orchestrator with EPAM DIAL API.
        
        Args:
            api_key: EPAM DIAL API key. If None, reads from environment variable DIAL_API_KEY
        """
        self.api_key = api_key or os.getenv("DIAL_API_KEY", "dial-j7r9nwg4xmk9spkibd3xrp4hjdg")
        
        self.client = AzureOpenAI(
            api_version="2023-12-01-preview",
            azure_endpoint="https://chat.epam.com",
            api_key=self.api_key
        )
        
        self.routing_prompt = """You are an intelligent query router. Analyze the user's question and determine which system(s) should handle it.

Available systems:
1. CORTEX_ANALYST - For analytical queries about structured data (orders, customers, products, payments, order items). Examples:
   - "What are the top 5 customers by revenue?"
   - "Show me sales by product category"
   - "Which customers have the highest order values?"
   - "What's the total revenue this month?"

2. RAG - For questions about documents, PDFs, reports, unstructured content. Examples:
   - "What does the sustainability report say about carbon emissions?"
   - "What are the key findings in the document?"
   - "Summarize the construction cost report"
   - "What did the earnings call mention about revenue?"

3. BOTH - For hybrid queries that need both structured data and document information. Examples:
   - "Compare our sales data with what the report says about market trends"
   - "What do our financial reports say about the numbers in our database?"

Respond ONLY with a JSON object in this exact format:
{
  "route": "CORTEX_ANALYST" | "RAG" | "BOTH",
  "reasoning": "Brief explanation of why this route was chosen"
}

User question: {question}"""
    
    def route_query(self, question: str) -> Dict[str, Any]:
        """
        Route a user query to the appropriate system(s).
        
        Args:
            question: User's question/query
            
        Returns:
            Dictionary with routing decision:
            {
                "route": "CORTEX_ANALYST" | "RAG" | "BOTH",
                "reasoning": "Explanation",
                "confidence": float (0-1)
            }
        """
        try:
            messages = [
                {
                    "role": "system",
                    "content": "You are a query routing assistant. Respond only with valid JSON."
                },
                {
                    "role": "user",
                    "content": self.routing_prompt.format(question=question)
                }
            ]
            
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=messages,
                temperature=0.1,  # Low temperature for consistent routing
                max_tokens=200
            )
            
            # Extract response
            content = response.choices[0].message.content.strip()
            
            # Try to parse JSON from response
            # Sometimes LLM wraps JSON in markdown code blocks
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()
            
            # Try to extract JSON object
            import re
            json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
            if json_match:
                try:
                    routing_decision = json.loads(json_match.group())
                except json.JSONDecodeError:
                    routing_decision = self._fallback_routing(question)
            else:
                try:
                    routing_decision = json.loads(content)
                except json.JSONDecodeError:
                    # Fallback routing based on keywords
                    routing_decision = self._fallback_routing(question)
            
            # Validate route
            route = routing_decision.get("route", "").upper()
            if route not in ["CORTEX_ANALYST", "RAG", "BOTH"]:
                routing_decision = self._fallback_routing(question)
            
            routing_decision["confidence"] = 0.9 if route in ["CORTEX_ANALYST", "RAG", "BOTH"] else 0.5
            
            return routing_decision
            
        except Exception as e:
            print(f"Orchestrator error: {e}")
            # Fallback to keyword-based routing
            return self._fallback_routing(question)
    
    def execute_query(
        self,
        question: str,
        cortex_analyst_func=None,
        rag_query_func=None,
        k: int = 5
    ) -> Dict[str, Any]:
        """
        Route and execute a query using the appropriate system(s).
        
        Args:
            question: User's question
            cortex_analyst_func: Function to call for Cortex Analyst queries
            rag_query_func: Function to call for RAG queries
            k: Number of context chunks for RAG
            
        Returns:
            Dictionary with routing info, results, and combined response
        """
        # Route the query
        routing = self.route_query(question)
        route = routing.get("route", "RAG")
        
        results = {}
        combined_response_parts = []
        
        # Execute based on route
        if route == "CORTEX_ANALYST" or route == "BOTH":
            if cortex_analyst_func:
                try:
                    cortex_result = cortex_analyst_func(question)
                    results["cortex_analyst"] = cortex_result
                    
                    if cortex_result.get("success"):
                        # Format Cortex Analyst response
                        if cortex_result.get("results"):
                            combined_response_parts.append("## Analytics Results\n\n")
                            
                            # Create a summary
                            row_count = cortex_result.get("row_count", 0)
                            combined_response_parts.append(f"Found {row_count} result(s).\n\n")
                            
                            # Show first few results as text
                            for i, row in enumerate(cortex_result["results"][:5], 1):
                                row_text = ", ".join([f"{k}: {v}" for k, v in row.items()])
                                combined_response_parts.append(f"{i}. {row_text}\n")
                            
                            if row_count > 5:
                                combined_response_parts.append(f"\n... and {row_count - 5} more results (see Analytics Results below)")
                        else:
                            combined_response_parts.append("Analytics query completed but returned no results.")
                    else:
                        combined_response_parts.append(f"Analytics query error: {cortex_result.get('error', 'Unknown error')}")
                except Exception as e:
                    results["cortex_analyst"] = {"error": str(e), "success": False}
                    combined_response_parts.append(f"Analytics query failed: {str(e)}")
        
        if route == "RAG" or route == "BOTH":
            if rag_query_func:
                try:
                    rag_result = rag_query_func(question)
                    results["rag"] = rag_result
                    
                    if rag_result.get("success"):
                        combined_response_parts.append(rag_result.get("answer", "No answer generated"))
                    else:
                        combined_response_parts.append(f"Document query error: {rag_result.get('error', 'Unknown error')}")
                except Exception as e:
                    results["rag"] = {"error": str(e), "success": False}
                    combined_response_parts.append(f"Document query failed: {str(e)}")
        
        # Combine responses
        if route == "BOTH" and len(combined_response_parts) > 1:
            combined_response = "\n\n---\n\n".join(combined_response_parts)
            combined_response = f"## Combined Results\n\n{combined_response}"
        elif combined_response_parts:
            combined_response = "\n\n".join(combined_response_parts)
        else:
            combined_response = "No results generated. Please try rephrasing your question."
        
        # Map route to query type for display
        query_type_map = {
            "CORTEX_ANALYST": "analytics",
            "RAG": "document",
            "BOTH": "hybrid"
        }
        
        return {
            "query": question,
            "classification": {
                "query_type": query_type_map.get(route, "unknown"),
                "route": route,
                "reasoning": routing.get("reasoning", ""),
                "confidence": routing.get("confidence", 0.0)
            },
            "results": results,
            "combined_response": combined_response
        }
    
    def _fallback_routing(self, question: str) -> Dict[str, Any]:
        """
        Fallback routing based on keywords if LLM fails.
        
        Args:
            question: User's question
            
        Returns:
            Routing decision dictionary
        """
        question_lower = question.lower()
        
        # Keywords for structured data queries
        analytics_keywords = [
            "customer", "order", "product", "payment", "revenue", "sales",
            "total", "sum", "count", "average", "top", "highest", "lowest",
            "by", "group", "aggregate", "database", "table"
        ]
        
        # Keywords for document queries
        document_keywords = [
            "report", "document", "pdf", "sustainability", "earnings",
            "transcript", "statement", "findings", "mentioned", "says",
            "according to", "in the document", "in the report"
        ]
        
        analytics_score = sum(1 for keyword in analytics_keywords if keyword in question_lower)
        document_score = sum(1 for keyword in document_keywords if keyword in question_lower)
        
        if analytics_score > document_score and analytics_score > 0:
            route = "CORTEX_ANALYST"
            reasoning = "Query contains analytics/database keywords"
        elif document_score > analytics_score and document_score > 0:
            route = "RAG"
            reasoning = "Query contains document/report keywords"
        elif analytics_score > 0 and document_score > 0:
            route = "BOTH"
            reasoning = "Query contains both analytics and document keywords"
        else:
            # Default to RAG for ambiguous queries
            route = "RAG"
            reasoning = "Default routing - query is ambiguous"
        
        return {
            "route": route,
            "reasoning": reasoning,
            "confidence": 0.6
        }


def create_orchestrator(api_key: str = None) -> QueryOrchestrator:
    """
    Factory function to create an orchestrator instance.
    
    Args:
        api_key: Optional API key. If None, uses default or environment variable.
        
    Returns:
        QueryOrchestrator instance
    """
    return QueryOrchestrator(api_key=api_key)


def test_orchestrator():
    """Test function for the orchestrator."""
    orchestrator = create_orchestrator()
    
    test_queries = [
        "What are the top 5 customers by total order value?",
        "What does the sustainability report say about carbon emissions?",
        "Compare our sales data with what the earnings report mentions",
        "Show me all products",
        "What are the key findings in the PDF documents?"
    ]
    
    print("=" * 70)
    print("Orchestrator Test")
    print("=" * 70)
    
    for query in test_queries:
        result = orchestrator.route_query(query)
        print(f"\nQuery: {query}")
        print(f"Route: {result['route']}")
        print(f"Reasoning: {result['reasoning']}")
        print(f"Confidence: {result.get('confidence', 'N/A')}")
        print("-" * 70)


if __name__ == "__main__":
    test_orchestrator()
