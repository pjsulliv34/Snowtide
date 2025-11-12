import streamlit as st
from snowflake.core import Root
from snowflake.cortex import Complete
from snowflake.snowpark.context import get_active_session

# --- Constants ---
DB = "SANDIEGO_AI"
SCHEMA = "MUNI_CODE"
SERVICE = "MUNI_CODE_SEARCH"
MODELS = ["mistral-large2", "llama3.1-70b", "llama3.1-8b"]

# --- Session state ---
def init_messages():
    if st.session_state.get("clear_conversation") or "messages" not in st.session_state:
        st.session_state.messages = []

# --- Layout ---
def init_layout():
    st.title("📘 Municipal Code Chatbot")
    st.markdown(f"Querying service: `{DB}.{SCHEMA}.{SERVICE}`".replace('"', ''))

# --- Query MUNI_CODE_SEARCH using your `.search()` method ---
def query_cortex_search_service(query, filter={}, columns=None, limit=5):
    if columns is None:
        columns = ["CHUNK", "RELATIVE_PATH", "PDF_URL", "CHAPTER_URL"]
    
    cortex_search_service = (
        Root(session)
        .databases[DB]
        .schemas[SCHEMA]
        .cortex_search_services[SERVICE]
    )
    
    context_documents = cortex_search_service.search(
        query=query,
        columns=columns,
        filter=filter,
        limit=limit
    )
    
    return context_documents.results

# --- Build RAG prompt ---
def create_prompt(user_question, context_chunks):
    context_str = "\n\n".join([chunk.get("CHUNK","") for chunk in context_chunks])
    prompt = f"""
    [INST]
    You are a helpful AI assistant with RAG capabilities.
    Use the context below to answer the question concisely and accurately.
    If the answer is not in the context, say "I don't know the answer".

    <context>
    {context_str}
    </context>
    <question>
    {user_question}
    </question>
    [/INST]
    Answer:
    """
    return prompt

# --- Chat ---
def main():
    st.set_page_config(page_title="Municipal Code Chatbot", layout="wide")
    init_layout()
    init_messages()

    # Sidebar config
    st.sidebar.selectbox("Select model:", MODELS, key="model_name")
    st.sidebar.number_input("Number of context chunks", value=5, key="num_retrieved_chunks", min_value=1, max_value=10)

    # Display chat history
    icons = {"assistant": "❄️", "user": "👤"}
    for message in st.session_state.messages:
        with st.chat_message(message["role"], avatar=icons[message["role"]]):
            st.markdown(message["content"])

    if question := st.chat_input("Ask a question..."):
        st.session_state.messages.append({"role": "user", "content": question})
        with st.chat_message("user", avatar=icons["user"]):
            st.markdown(question)

        with st.chat_message("assistant", avatar=icons["assistant"]):
            message_placeholder = st.empty()
            
            # Retrieve context chunks from MUNI_CODE_SEARCH using `.search()`
            context_chunks = query_cortex_search_service(
                query=question,
                limit=st.session_state.num_retrieved_chunks
            )

            # Build RAG prompt
            prompt = create_prompt(question, context_chunks)

            # Generate answer using Cortex model
            with st.spinner("Thinking..."):
                generated_response = Complete(st.session_state.model_name, prompt).replace("$", "\$")

                # Build references table
                # Build references table
                markdown_table = "###### References \n\n| PDF Title | PDF URL | Chapter URL |\n|-------|-----|------------|\n"
                for ref in context_chunks:
                    pdf_url = ref.get('PDF_URL', '')
                    chapter_url = ref.get('CHAPTER_URL', '')
                    markdown_table += f"| {ref.get('RELATIVE_PATH','')} | [Link]({pdf_url}) | [Link]({chapter_url}) |\n"

                message_placeholder.markdown(generated_response + "\n\n" + markdown_table)

            st.session_state.messages.append({"role": "assistant", "content": generated_response})

if __name__ == "__main__":
    session = get_active_session()
    root = Root(session)
    main()
