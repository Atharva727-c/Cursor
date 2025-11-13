"""
Snowflake Cortex Analyst Integration Script

This script demonstrates how to use Snowflake Cortex Analyst to query
structured tables using natural language prompts.
"""

import os
import sys
from typing import Dict, Any, Optional
import json
import yaml

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


def create_cortex_analyst_session(
    conn: snowflake.connector.SnowflakeConnection,
    relationships_yaml: Dict[str, Any],
    session_name: Optional[str] = None
) -> str:
    """
    Create a Cortex Analyst session with the provided relationships.
    
    Returns the session ID.
    """
    if session_name is None:
        session_name = "cortex_analyst_session"
    
    # Convert relationships to JSON string for the SQL function
    relationships_json = json.dumps(relationships_yaml)
    
    with conn.cursor() as cur:
        # Create the Cortex Analyst session
        # Note: The exact SQL syntax may vary based on Snowflake version
        # This uses the CORTEX.ANALYST_CREATE_SESSION function
        create_session_sql = f"""
        SELECT SNOWFLAKE.CORTEX.ANALYST_CREATE_SESSION(
            '{session_name}',
            PARSE_JSON('{relationships_json}')
        ) AS SESSION_ID
        """
        
        try:
            cur.execute(create_session_sql)
            result = cur.fetchone()
            session_id = result[0] if result else None
            
            if session_id:
                print(f"✅ Created Cortex Analyst session: {session_id}")
                return session_id
            else:
                raise Exception("Failed to create session - no session ID returned")
        except Exception as e:
            # If the function name is different, try alternative syntax
            error_msg = str(e)
            if "does not exist" in error_msg or "not found" in error_msg.lower():
                print(f"⚠️  Note: Direct session creation may not be available.")
                print(f"   Using alternative approach with CORTEX.ANALYST function...")
                return None
            else:
                raise


def query_cortex_analyst(
    conn: snowflake.connector.SnowflakeConnection,
    prompt: str,
    relationships_yaml: Optional[Dict[str, Any]] = None,
    session_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Query Cortex Analyst with a natural language prompt.
    
    Args:
        conn: Snowflake connection
        prompt: Natural language question/prompt
        relationships_yaml: Optional relationships configuration
        session_id: Optional existing session ID
    
    Returns:
        Dictionary with the response from Cortex Analyst
    """
    with conn.cursor() as cur:
        try:
            # Method 1: Using CORTEX.ANALYST function directly
            # This is the most common approach
            if relationships_yaml:
                relationships_json = json.dumps(relationships_yaml)
                sql = f"""
                SELECT SNOWFLAKE.CORTEX.ANALYST(
                    '{prompt}',
                    PARSE_JSON('{relationships_json}')
                ) AS RESULT
                """
            else:
                sql = f"""
                SELECT SNOWFLAKE.CORTEX.ANALYST('{prompt}') AS RESULT
                """
            
            print(f"\n📝 Executing Cortex Analyst query...")
            print(f"   Prompt: {prompt}\n")
            
            cur.execute(sql)
            result = cur.fetchone()
            
            if result and result[0]:
                response = result[0]
                # Parse the JSON response if it's a string
                if isinstance(response, str):
                    try:
                        response = json.loads(response)
                    except json.JSONDecodeError:
                        pass
                
                return response
            else:
                return {"error": "No response from Cortex Analyst"}
                
        except Exception as e:
            error_msg = str(e)
            
            # Try alternative method if the first one fails
            if "does not exist" in error_msg or "CORTEX.ANALYST" in error_msg:
                print(f"⚠️  Trying alternative method...")
                
                # Alternative: Use CORTEX.ANALYST_QUERY or similar function
                try:
                    if relationships_yaml:
                        relationships_json = json.dumps(relationships_yaml)
                        alt_sql = f"""
                        SELECT SNOWFLAKE.CORTEX.ANALYST_QUERY(
                            '{prompt}',
                            PARSE_JSON('{relationships_json}')
                        ) AS RESULT
                        """
                    else:
                        alt_sql = f"""
                        SELECT SNOWFLAKE.CORTEX.ANALYST_QUERY('{prompt}') AS RESULT
                        """
                    
                    cur.execute(alt_sql)
                    result = cur.fetchone()
                    
                    if result and result[0]:
                        response = result[0]
                        if isinstance(response, str):
                            try:
                                response = json.loads(response)
                            except json.JSONDecodeError:
                                pass
                        return response
                except Exception as e2:
                    print(f"❌ Alternative method also failed: {e2}")
            
            raise Exception(f"Cortex Analyst query failed: {error_msg}")


def main():
    """Main function to demonstrate Cortex Analyst usage."""
    print("=" * 70)
    print("Snowflake Cortex Analyst Integration")
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
        
        # Test query
        print("\n3. Running sample Cortex Analyst query...")
        sample_prompt = "What are the top 5 customers by total order value?"
        
        result = query_cortex_analyst(
            conn=conn,
            prompt=sample_prompt,
            relationships_yaml=relationships
        )
        
        print("\n" + "=" * 70)
        print("CORTEX ANALYST RESPONSE:")
        print("=" * 70)
        
        if isinstance(result, dict):
            print(json.dumps(result, indent=2))
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

