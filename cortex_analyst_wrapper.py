"""
Wrapper module for Cortex Analyst queries
"""

import os
from typing import Dict, Any, Optional, List
import json
import yaml
from snowflake_connect import connect_snowflake


def load_relationships_yaml(yaml_path: str = "cortex_analyst_relationships.yaml") -> Dict[str, Any]:
    """Load the relationships YAML file."""
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"Relationships YAML file not found: {yaml_path}")
    
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f)


def get_table_schema(conn, table_name: str) -> List[Dict[str, str]]:
    """Get column information for a table."""
    with conn.cursor() as cur:
        try:
            cur.execute(f"DESCRIBE TABLE {table_name}")
            columns = []
            for row in cur.fetchall():
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[3] if len(row) > 3 else "Y"
                })
            return columns
        except Exception as e:
            return []


def query_cortex_analyst_wrapper(
    prompt: str,
    relationships_yaml: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Wrapper function to query Cortex Analyst.
    
    Args:
        prompt: Natural language question
        relationships_yaml: Optional relationships configuration
        
    Returns:
        Dictionary with results
    """
    import os
    
    # Load relationships if not provided
    if relationships_yaml is None:
        try:
            relationships_yaml = load_relationships_yaml()
        except:
            relationships_yaml = None
    
    conn = connect_snowflake()
    
    try:
        # Get table schemas
        tables = ["ORDERS", "CUSTOMERS", "ORDER_ITEMS", "PRODUCTS", "PAYMENTS"]
        table_schemas = {}
        for table in tables:
            schema = get_table_schema(conn, table)
            if schema:
                table_schemas[table] = schema
        
        # Build enhanced context
        context = "You are a SQL expert for Snowflake. Generate a valid SQL query.\n\n"
        context += "Available tables and their columns:\n"
        for table, columns in table_schemas.items():
            context += f"\n{table}:\n"
            for col in columns:
                context += f"  - {col['name']} ({col['type']})\n"
        
        if relationships_yaml:
            context += "\nTable relationships:\n"
            for rel in relationships_yaml.get('relationships', []):
                left_col = rel['relationshipColumns'][0]['leftColumn']
                right_col = rel['relationshipColumns'][0]['rightColumn']
                context += f"- {rel['leftTable']}.{left_col} -> {rel['rightTable']}.{right_col} ({rel['relationshipType']})\n"
        
        context += f"\nQuestion: {prompt}\n"
        context += "Generate ONLY the SQL SELECT query. No explanations, no markdown, just SQL."
        
        # Generate SQL using Cortex
        with conn.cursor() as cur:
            models = ['llama3-8b', 'mistral-7b', 'snowflake-arctic']
            sql_query = None
            
            for model in models:
                try:
                    cur.execute(
                        f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', %s)",
                        (context,)
                    )
                    result = cur.fetchone()
                    if result and result[0]:
                        sql_query = result[0].strip()
                        # Clean up
                        for prefix in ["```sql", "```", "SQL:", "Query:"]:
                            if sql_query.startswith(prefix):
                                sql_query = sql_query[len(prefix):].strip()
                        if sql_query.endswith("```"):
                            sql_query = sql_query[:-3].strip()
                        sql_query = sql_query.strip().rstrip(';')
                        break
                except Exception:
                    continue
            
            if not sql_query:
                return {
                    "error": "Could not generate SQL query using Cortex",
                    "system": "CORTEX_ANALYST"
                }
            
            # Execute the SQL
            try:
                cur.execute(sql_query)
                results = cur.fetchall()
                columns = [desc[0] for desc in cur.description] if cur.description else []
                
                # Format results
                formatted_results = []
                for row in results:
                    row_dict = {}
                    for i, col in enumerate(columns):
                        row_dict[col] = row[i]
                    formatted_results.append(row_dict)
                
                return {
                    "system": "CORTEX_ANALYST",
                    "prompt": prompt,
                    "sql_query": sql_query,
                    "results": formatted_results,
                    "row_count": len(formatted_results),
                    "columns": columns,
                    "success": True
                }
            except Exception as e:
                return {
                    "error": f"SQL execution failed: {str(e)}",
                    "sql_query": sql_query,
                    "system": "CORTEX_ANALYST",
                    "success": False
                }
    finally:
        conn.close()
