# Import python packages
import streamlit as st
from snowflake.core import Root
from snowflake.snowpark.context import get_active_session

# --- Constants for your service ---
DB = "SANDIEGO_AI"
SCHEMA = "MUNI_CODE"
SERVICE = "MUNI_CODE_SEARCH"
ARRAY_ATTRIBUTES = set()


# --- Helper: Get metadata about the search service ---
def get_column_specification():
    session = get_active_session()
    search_service_result = session.sql(
        f"DESC CORTEX SEARCH SERVICE {DB}.{SCHEMA}.{SERVICE}"
    ).collect()[0]
    st.session_state.attribute_columns = search_service_result.attribute_columns.split(",")
    st.session_state.search_column = search_service_result.search_column
    st.session_state.columns = search_service_result.columns.split(",")


# --- Layout setup ---
def init_layout():
    st.title("📘 Municipal Code Search")
    st.markdown(f"Querying service: `{DB}.{SCHEMA}.{SERVICE}`".replace('"', ''))


# --- Query the Cortex Search Service ---
def query_cortex_search_service(query, filter={}):
    session = get_active_session()
    cortex_search_service = (
        Root(session)
        .databases[DB]
        .schemas[SCHEMA]
        .cortex_search_services[SERVICE]
    )
    context_documents = cortex_search_service.search(
        query,
        columns=["CHUNK", "RELATIVE_PATH", "PDF_URL", "CHAPTER_URL"],
        filter=filter,
        limit=st.session_state.limit
    )
    return context_documents.results



# --- UI Inputs ---
def init_search_input():
    st.session_state.query = st.text_input(
        "🔍 Enter your query", placeholder="e.g. animal control regulations"
    )


def init_limit_input():
    st.session_state.limit = st.number_input("Result limit", min_value=1, value=5)


def init_context_limit_input():
    st.session_state.context_limit = st.select_slider(
        "Context length per result",
        options=[100, 200, 400, 800, 1600],
        value=400,
        help="Adjust how much of each result’s text is shown."
    )


def init_attribute_selection():
    st.session_state.attributes = {}


# --- Display Results ---
def display_search_results(results, chunk_limit=400):
    st.subheader("Search Results")
    if not results:
        st.info("No results found.")
        return

    for i, result in enumerate(results):
        result = dict(result)
        container = st.expander(f"Result {i+1}: {result.get('RELATIVE_PATH', '')}", expanded=True)

        chunk = result.get("CHUNK", "")
        if len(chunk) > chunk_limit:
            short_chunk = chunk[:chunk_limit].rsplit(" ", 1)[0] + "..."
            container.markdown(f"📝 **Excerpt:** {short_chunk}")
            if container.button(f"Show full context for Result {i+1}", key=f"full_{i}"):
                container.markdown(f"**Full Excerpt:** {chunk}")
        else:
            container.markdown(f"📝 **Excerpt:** {chunk}")

        # Links
        pdf_url = result.get("PDF_URL", "")
        chapter_url = result.get("CHAPTER_URL", "")
        if pdf_url or chapter_url:
            link_text = " | ".join(
                filter(None, [
                    f"[📄 View PDF]({pdf_url})" if pdf_url else "",
                    f"[📘 Open Chapter]({chapter_url})" if chapter_url else ""
                ])
            )
            container.markdown(link_text)


# --- Filter Builder ---
def create_filter_object(attributes):
    and_clauses = []
    for column, column_values in attributes.items():
        if len(column_values) == 0:
            continue
        if column in ARRAY_ATTRIBUTES:
            for attr_value in column_values:
                and_clauses.append({"@contains": {column: attr_value}})
        else:
            or_clauses = [{"@eq": {column: attr_value}} for attr_value in column_values]
            and_clauses.append({"@or": or_clauses})
    return {"@and": and_clauses} if and_clauses else {}


# --- Main ---
def main():
    st.set_page_config(page_title="Municipal Code Search", layout="wide")
    init_layout()
    get_column_specification()
    init_attribute_selection()
    init_limit_input()
    init_context_limit_input()
    init_search_input()

    if not st.session_state.get("query"):
        return

    results = query_cortex_search_service(
        st.session_state.query,
        filter=create_filter_object(st.session_state.attributes)
    )
    display_search_results(results, chunk_limit=st.session_state.context_limit)


if __name__ == "__main__":
    main()
