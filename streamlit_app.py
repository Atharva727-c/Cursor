import streamlit as st
from typing import List, Tuple
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

from rag_query import retrieve_context, build_prompt
from snowflake_connect import connect_snowflake

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


def get_llm_response(question: str, k: int = 5) -> Tuple[str, List[Tuple[str, str, int, float]]]:
    """
    Get LLM response using Snowflake Cortex.
    Returns the answer and source contexts.
    """
    try:
        # Retrieve relevant contexts
        contexts = retrieve_context(question, k=k)
        if not contexts:
            return "I couldn't find any relevant information in the documents. Please try rephrasing your question.", []
        
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
        
        return answer, contexts
    
    except Exception as e:
        return f"Error: {str(e)}", []


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
    1. Type your question in the chat input
    2. Press Enter or click Send
    3. View the AI response and sources
    4. Sources show which PDF chunks were used
    """)
    
    # Clear chat button
    if st.button("🗑️ Clear Chat", type="secondary"):
        st.session_state.messages = []
        st.session_state.sources = []
        st.rerun()


# Main header
st.markdown('<div class="main-header">🤖 CRH POC - RAG Chat Interface</div>', unsafe_allow_html=True)
st.markdown("### Ask questions about your PDF documents")

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        # Display sources for assistant messages
        if message["role"] == "assistant" and message.get("sources"):
            with st.expander("📚 View Sources"):
                for i, (ctx, fn, ci, sim) in enumerate(message["sources"], 1):
                    st.markdown(f"**Source {i}:** `{fn}` (Chunk {ci}, Similarity: {sim:.4f})")
                    st.markdown(f"*{ctx[:200]}...*" if len(ctx) > 200 else f"*{ctx}*")
                    st.divider()

# Chat input
if prompt := st.chat_input("Ask a question about your documents..."):
    # Add user message to chat
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Get assistant response
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            answer, sources = get_llm_response(prompt, k=k_chunks)
            st.markdown(answer)
            
            # Display sources
            if sources:
                with st.expander("📚 View Sources"):
                    for i, (ctx, fn, ci, sim) in enumerate(sources, 1):
                        st.markdown(f"**Source {i}:** `{fn}` (Chunk {ci}, Similarity: {sim:.4f})")
                        st.markdown(f"*{ctx[:200]}...*" if len(ctx) > 200 else f"*{ctx}*")
                        st.divider()
    
    # Add assistant message to chat history
    st.session_state.messages.append({
        "role": "assistant", 
        "content": answer,
        "sources": sources
    })

# Footer
st.divider()
st.markdown(
    "<div style='text-align: center; color: #666; padding: 1rem;'>"
    "Powered by Snowflake Cortex & Streamlit | RAG over PDF Documents"
    "</div>",
    unsafe_allow_html=True
)



