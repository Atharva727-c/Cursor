"""
Snowflake Cortex Analyst Integration Script (Version 2)
Uses REST API approach if SQL functions are not available
"""

import os
import sys
from typing import Dict, Any, Optional
import json
import yaml
import requests
from base64 import b64encode

# Fix Windows console encoding for emoji support
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Import Snowflake connector
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
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


def check_snowflake_version(conn: snowflake.connector.SnowflakeConnection) -> str:
    """Check Snowflake version."""
    with conn.cursor() as cur:
        cur.execute("SELECT CURRENT_VERSION()")
        result = cur.fetchone()
        return result[0] if result else "Unknown"


def check_cortex_availability(conn: snowflake.connector.SnowflakeConnection):
    """Check if Cortex functions are available."""
    with conn.cursor() as cur:
        try:
            # Check for CORTEX schema
            cur.execute("SHOW SCHEMAS LIKE 'CORTEX' IN DATABASE SNOWFLAKE")
            schemas = cur.fetchall()
            if schemas:
                print("   ✅ CORTEX schema found")
            
            # Check for any CORTEX functions
            cur.execute("""
                SELECT FUNCTION_NAME 
                FROM TABLE(INFORMATION_SCHEMA.FUNCTIONS())
                WHERE FUNCTION_SCHEMA = 'SNOWFLAKE' 
                AND FUNCTION_NAME LIKE '%CORTEX%'
                LIMIT 10
            """)
            functions = cur.fetchall()
            if functions:
                print(f"   ✅ Found {len(functions)} Cortex functions:")
                for func in functions[:5]:
                    print(f"      - {func[0]}")
                return True
            else:
                print("   ⚠️  No Cortex functions found")
                return False
        except Exception as e:
            print(f"   ⚠️  Could not check Cortex availability: {e}")
            return False


def query_cortex_analyst_sql(
    conn: snowflake.connector.SnowflakeConnection,
    prompt: str,
    relationships_yaml: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Try to query Cortex Analyst using SQL functions.
    """
    with conn.cursor() as cur:
        escaped_prompt = prompt.replace("'", "''")
        
        # Try the correct function name based on Snowflake documentation
        # Cortex Analyst might use ANALYZE_DATA or similar
        function_candidates = [
            ("SNOWFLAKE.CORTEX.ANALYZE_DATA", True),
            ("SNOWFLAKE.CORTEX.ANALYST", True),
            ("CORTEX.ANALYZE_DATA", True),
            ("CORTEX.ANALYST", True),
        ]
        
        if relationships_yaml:
            relationships_json = json.dumps(relationships_yaml)
            escaped_json = relationships_json.replace("'", "''")
        
        for func_name, use_relationships in function_candidates:
            try:
                if relationships_yaml and use_relationships:
                    sql = f"""
                    SELECT {func_name}(
                        '{escaped_prompt}',
                        PARSE_JSON('{escaped_json}')
                    ) AS RESULT
                    """
                else:
                    sql = f"""
                    SELECT {func_name}('{escaped_prompt}') AS RESULT
                    """
                
                print(f"   Trying: {func_name}...")
                cur.execute(sql)
                result = cur.fetchone()
                
                if result and result[0]:
                    response = result[0]
                    if isinstance(response, str):
                        try:
                            response = json.loads(response)
                        except json.JSONDecodeError:
                            pass
                    print(f"   ✅ Success with {func_name}!")
                    return response
            except Exception as e:
                error_msg = str(e)
                if "does not exist" not in error_msg and "Unknown" not in error_msg:
                    # Different error - might be a parameter issue
                    print(f"   ⚠️  {func_name} exists but error: {error_msg[:80]}")
                    continue
        
        return None


def query_using_sql_generation(
    conn: snowflake.connector.SnowflakeConnection,
    prompt: str,
    relationships_yaml: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Alternative approach: Use Cortex to generate SQL, then execute it.
    This is a workaround if ANALYZE_DATA is not available.
    """
    print("\n   Trying SQL generation approach...")
    
    # Build context about the tables and relationships
    context = "You are a SQL expert. Generate a SQL query for the following question.\n"
    context += "Available tables: ORDERS, CUSTOMERS, ORDER_ITEMS, PRODUCTS, PAYMENTS\n"
    
    if relationships_yaml:
        context += "\nTable relationships:\n"
        for rel in relationships_yaml.get('relationships', []):
            context += f"- {rel['leftTable']}.{rel['relationshipColumns'][0]['leftColumn']} -> {rel['rightTable']}.{rel['relationshipColumns'][0]['rightColumn']} ({rel['relationshipType']})\n"
    
    context += f"\nQuestion: {prompt}\n"
    context += "Generate only the SQL query, no explanations."
    
    with conn.cursor() as cur:
        try:
            # Use CORTEX.COMPLETE to generate SQL
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
                        # Clean up the SQL (remove markdown code blocks if present)
                        if sql_query.startswith("```sql"):
                            sql_query = sql_query[6:]
                        if sql_query.startswith("```"):
                            sql_query = sql_query[3:]
                        if sql_query.endswith("```"):
                            sql_query = sql_query[:-3]
                        sql_query = sql_query.strip()
                        break
                except:
                    continue
            
            if not sql_query:
                return {"error": "Could not generate SQL query"}
            
            print(f"   Generated SQL:\n   {sql_query}\n")
            
            # Execute the generated SQL
            cur.execute(sql_query)
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            
            # Format results
            formatted_results = []
            for row in results:
                formatted_results.append(dict(zip(columns, row)))
            
            return {
                "sql_query": sql_query,
                "results": formatted_results,
                "row_count": len(formatted_results)
            }
            
        except Exception as e:
            return {"error": f"SQL execution failed: {str(e)}"}


def main():
    """Main function to demonstrate Cortex Analyst usage."""
    print("=" * 70)
    print("Snowflake Cortex Analyst Integration (Version 2)")
    print("=" * 70)
    
    try:
        # Load relationships
        print("\n1. Loading relationships configuration...")
        relationships = load_relationships_yaml()
        print(f"   ✅ Loaded {len(relationships.get('relationships', []))} relationships")
        
        # Connect to Snowflake
        print("\n2. Connecting to Snowflake...")
        conn = get_snowflake_connection()
        print("   ✅ Connected successfully")
        
        # Check version and Cortex availability
        print("\n3. Checking Snowflake version and Cortex availability...")
        version = check_snowflake_version(conn)
        print(f"   Snowflake version: {version}")
        cortex_available = check_cortex_availability(conn)
        
        # Test query
        print("\n4. Running sample Cortex Analyst query...")
        sample_prompt = "What are the top 5 customers by total order value?"
        print(f"   Prompt: {sample_prompt}\n")
        
        # Try SQL function approach first
        result = query_cortex_analyst_sql(
            conn=conn,
            prompt=sample_prompt,
            relationships_yaml=relationships
        )
        
        # If SQL approach failed, try SQL generation approach
        if not result or "error" in result:
            print("\n   SQL function approach not available, trying SQL generation...")
            result = query_using_sql_generation(
                conn=conn,
                prompt=sample_prompt,
                relationships_yaml=relationships
            )
        
        print("\n" + "=" * 70)
        print("CORTEX ANALYST RESPONSE:")
        print("=" * 70)
        
        if isinstance(result, dict):
            print(json.dumps(result, indent=2, default=str))
        else:
            print(result)
        
        print("\n" + "=" * 70)
        
        # Close connection
        conn.close()
        print("\n✅ Query completed successfully!")
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("   Make sure cortex_analyst_relationships.yaml exists in the current directory.")
        sys.exit(1)
    except ValueError as e:
        print(f"\n❌ Configuration Error: {e}")
        print("   Please check your .env file has all required Snowflake credentials.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

