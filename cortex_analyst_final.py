"""
Snowflake Cortex Analyst Integration - Final Version
Uses SQL generation with table schema awareness
"""

import os
import sys
from typing import Dict, Any, Optional, List
import json
import yaml

# Fix Windows console encoding
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Create and return a Snowflake connection."""
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    role = os.getenv("SNOWFLAKE_ROLE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    
    if not all([account, user, password, role, warehouse, database, schema]):
        raise ValueError("Missing required Snowflake environment variables")
    
    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema
    )


def load_relationships_yaml(yaml_path: str = "cortex_analyst_relationships.yaml") -> Dict[str, Any]:
    """Load the relationships YAML file."""
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"Relationships YAML file not found: {yaml_path}")
    
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f)


def get_table_schema(conn: snowflake.connector.SnowflakeConnection, table_name: str) -> List[Dict[str, str]]:
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
            print(f"   ⚠️  Could not get schema for {table_name}: {e}")
            return []


def query_cortex_analyst(
    conn: snowflake.connector.SnowflakeConnection,
    prompt: str,
    relationships_yaml: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Query using Cortex to generate SQL, then execute it.
    """
    print(f"\n📝 Processing prompt: {prompt}\n")
    
    # Get table schemas
    print("   Gathering table schemas...")
    tables = ["ORDERS", "CUSTOMERS", "ORDER_ITEMS", "PRODUCTS", "PAYMENTS"]
    table_schemas = {}
    for table in tables:
        schema = get_table_schema(conn, table)
        if schema:
            table_schemas[table] = schema
            print(f"   ✅ {table}: {len(schema)} columns")
    
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
    print("\n   Generating SQL using Cortex...")
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
                    print(f"   ✅ Generated SQL using {model}")
                    break
            except Exception as e:
                continue
        
        if not sql_query:
            return {"error": "Could not generate SQL query using Cortex"}
        
        print(f"\n   Generated SQL:\n   {sql_query}\n")
        
        # Execute the SQL
        print("   Executing SQL query...")
        try:
            cur.execute(sql_query)
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            
            # Format results (handle Decimal and other non-serializable types)
            import decimal
            formatted_results = []
            for row in results:
                row_dict = {}
                for i, col in enumerate(columns):
                    value = row[i]
                    # Convert Decimal to float for JSON serialization
                    if isinstance(value, decimal.Decimal):
                        value = float(value)
                    # Convert other non-serializable types to string
                    elif not isinstance(value, (str, int, float, bool, type(None))):
                        value = str(value)
                    row_dict[col] = value
                formatted_results.append(row_dict)
            
            return {
                "prompt": prompt,
                "sql_query": sql_query,
                "results": formatted_results,
                "row_count": len(formatted_results),
                "columns": columns
            }
        except Exception as e:
            return {
                "error": f"SQL execution failed: {str(e)}",
                "sql_query": sql_query
            }


def main():
    """Main function."""
    print("=" * 70)
    print("Snowflake Cortex Analyst Integration")
    print("=" * 70)
    
    try:
        # Load relationships
        print("\n1. Loading relationships configuration...")
        relationships = load_relationships_yaml()
        print(f"   ✅ Loaded {len(relationships.get('relationships', []))} relationships")
        
        # Connect
        print("\n2. Connecting to Snowflake...")
        conn = get_snowflake_connection()
        print("   ✅ Connected successfully")
        
        # Run query
        print("\n3. Running Cortex Analyst query...")
        sample_prompt = "What are the top 5 customers by total order value?"
        
        result = query_cortex_analyst(
            conn=conn,
            prompt=sample_prompt,
            relationships_yaml=relationships
        )
        
        print("\n" + "=" * 70)
        print("CORTEX ANALYST RESPONSE:")
        print("=" * 70)
        print(json.dumps(result, indent=2, default=str))
        print("=" * 70)
        
        conn.close()
        print("\n✅ Query completed!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

