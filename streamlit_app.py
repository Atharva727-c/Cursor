import streamlit as st
from typing import List, Tuple, Dict, Any
import sys
import os
from dotenv import load_dotenv

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, '.env')

# Load environment variables from .env file with explicit path
if os.path.exists(ENV_PATH):
    load_dotenv(dotenv_path=ENV_PATH, override=True)
    
    # Workaround: Manually read .env file if SNOWFLAKE_ACCOUNT is not loaded
    if not os.getenv('SNOWFLAKE_ACCOUNT'):
        try:
            with open(ENV_PATH, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")
                        if key == 'SNOWFLAKE_ACCOUNT' and value:
                            os.environ[key] = value
                            break
        except Exception as e:
            print(f"Warning: Could not manually load SNOWFLAKE_ACCOUNT: {e}")
else:
    # Fallback to default .env lookup
    load_dotenv(override=True)

# Add the current directory to the path to import local modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from snowflake_connect import connect_snowflake
from orchestrator import create_orchestrator
from cortex_analyst_wrapper import query_cortex_analyst_wrapper
from rag_wrapper import query_rag_wrapper

# Page configuration
st.set_page_config(
    page_title="CRH EKO-AI 2",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .chat-message {
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        display: flex;
        align-items: flex-start;
    }
    .user-message {
        background-color: #e3f2fd;
        margin-left: 20%;
    }
    .assistant-message {
        background-color: #f5f5f5;
        margin-right: 20%;
    }
    .source-badge {
        display: inline-block;
        padding: 0.25rem 0.5rem;
        margin: 0.25rem;
        background-color: #2196F3;
        color: white;
        border-radius: 0.25rem;
        font-size: 0.85rem;
    }
    .stTextInput > div > div > input {
        font-size: 1.1rem;
    }
    </style>
    """, unsafe_allow_html=True)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

if "sources" not in st.session_state:
    st.session_state.sources = []

if "orchestrator" not in st.session_state:
    st.session_state.orchestrator = create_orchestrator()


def get_orchestrated_response(question: str, k: int = 5) -> Dict[str, Any]:
    """
    Get response using the orchestrator to route to appropriate system(s).
    
    Returns:
        Dictionary with routing info, results, and combined response
    """
    try:
        orchestrator = st.session_state.orchestrator
        
        # Execute the query through orchestrator
        result = orchestrator.execute_query(
            question=question,
            cortex_analyst_func=query_cortex_analyst_wrapper,
            rag_query_func=lambda q: query_rag_wrapper(q, k=k),
            k=k
        )
        
        return result
    
    except Exception as e:
        return {
            "query": question,
            "classification": {
                "query_type": "error",
                "reasoning": f"Error in orchestration: {str(e)}",
                "confidence": 0.0
            },
            "results": {},
            "combined_response": f"Error: {str(e)}"
        }


# Sidebar
with st.sidebar:
    st.title("⚙️ Settings")
    
    # Connection status
    try:
        with connect_snowflake() as conn:
            st.success("✅ Connected to Snowflake")
    except ValueError as e:
        error_msg = str(e)
        st.error(f"❌ Configuration Error: {error_msg}")
        st.info("""
        **To fix this:**
        1. Copy `env.example` to `.env` in the project root
        2. Fill in your Snowflake credentials in the `.env` file
        3. Restart the Streamlit app
        """)
        st.stop()
    except Exception as e:
        st.error(f"❌ Connection Error: {str(e)}")
        st.stop()
    
    st.divider()
    
    # Configuration
    st.subheader("📊 Configuration")
    k_chunks = st.slider("Number of context chunks (K)", min_value=3, max_value=10, value=5, step=1)
    
    st.divider()
    
    # Instructions
    st.subheader("📖 How to use")
    st.markdown("""
    **Ask questions about:**
    - 📊 **Analytics**: Orders, customers, products, sales data
    - 📄 **Documents**: PDF content and information
    - 🔀 **Both**: Combined insights
    
    """)
    
    # Clear chat button
    if st.button("🗑️ Clear Chat", type="secondary"):
        st.session_state.messages = []
        st.session_state.sources = []
        st.rerun()


# Main header
st.markdown('<div class="main-header">🤖 CRH EKO-AI 2 - Unified Query Interface</div>', unsafe_allow_html=True)
st.markdown("### Ask questions about your data (Analytics, Documents,Etc)")

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        # Display routing information
        if message["role"] == "assistant" and message.get("routing_info"):
            routing = message["routing_info"]
            query_type = routing.get("query_type", "unknown")
            route = routing.get("route", "")
            
            # Show routing badge
            if query_type == "analytics" or route == "CORTEX_ANALYST":
                st.info(f"🔍 Routed to: **Cortex Analyst** (Confidence: {routing.get('confidence', 0):.2f})")
            elif query_type == "document" or route == "RAG":
                st.info(f"📄 Routed to: **RAG/Documents** (Confidence: {routing.get('confidence', 0):.2f})")
            elif query_type == "hybrid" or route == "BOTH":
                st.info(f"🔀 Routed to: **Both Systems** (Confidence: {routing.get('confidence', 0):.2f})")
            
            if routing.get("reasoning"):
                with st.expander("ℹ️ Routing Reasoning"):
                    st.markdown(routing["reasoning"])
        
        # Display sources for RAG responses
        if message["role"] == "assistant" and message.get("sources"):
            with st.expander("📚 View Document Sources"):
                for i, source in enumerate(message["sources"], 1):
                    if isinstance(source, dict):
                        st.markdown(f"**Source {i}:** `{source.get('filename', 'Unknown')}` (Chunk {source.get('chunk_index', 'N/A')}, Similarity: {source.get('similarity', 0):.4f})")
                        st.markdown(f"*{source.get('content_preview', '')}*")
                    else:
                        # Legacy format
                        ctx, fn, ci, sim = source
                        st.markdown(f"**Source {i}:** `{fn}` (Chunk {ci}, Similarity: {sim:.4f})")
                        st.markdown(f"*{ctx[:200]}...*" if len(ctx) > 200 else f"*{ctx}*")
                    st.divider()
        
        # Display analytics results
        if message["role"] == "assistant" and message.get("analytics_results"):
            with st.expander("📊 View Analytics Results"):
                analytics = message["analytics_results"]
                if "sql_query" in analytics:
                    st.markdown("**Generated SQL:**")
                    st.code(analytics["sql_query"], language="sql")
                if "results" in analytics and analytics["results"]:
                    st.markdown(f"**Results ({analytics.get('row_count', 0)} rows):**")
                    import pandas as pd
                    df = pd.DataFrame(analytics["results"])
                    st.dataframe(df, use_container_width=True)

# Chat input
if prompt := st.chat_input("Ask a question (analytics, documents, or both)..."):
    # Add user message to chat
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Get orchestrated response
    with st.chat_message("assistant"):
        with st.spinner("Routing query and processing..."):
            result = get_orchestrated_response(prompt, k=k_chunks)
            
            # Display the combined response
            st.markdown(result.get("combined_response", "No response generated"))
            
            # Store routing info and results for display
            routing_info = result.get("classification", {})
            rag_results = result.get("results", {}).get("rag", {})
            cortex_results = result.get("results", {}).get("cortex_analyst", {})
            
            # Display sources if RAG was used
            sources = rag_results.get("sources", []) if rag_results else []
            
            # Display analytics results if Cortex Analyst was used
            analytics_results = cortex_results if cortex_results and "error" not in cortex_results else None
    
    # Add assistant message to chat history
    st.session_state.messages.append({
        "role": "assistant", 
        "content": result.get("combined_response", "No response"),
        "routing_info": routing_info,
        "sources": sources,
        "analytics_results": analytics_results
    })

# Footer
st.divider()
st.markdown(
    "<div style='text-align: center; color: #666; padding: 1rem;'>"
    "Powered by EPAM DIAL Orchestrator | Snowflake Cortex Analyst | RAG over PDF Documents"
    "</div>",
    unsafe_allow_html=True
)



