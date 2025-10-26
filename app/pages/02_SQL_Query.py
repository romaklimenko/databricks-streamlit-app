import streamlit as st
from common import run_sql as sqlQuery


st.set_page_config(page_title="SQL Query", layout="wide")

st.title("Run a SQL query")
with st.form("sql_form"):
    default_query = st.session_state.get(
        "last_query",
        "select * from samples.nyctaxi.trips limit 50",
    )
    query = st.text_area("SQL", value=default_query, height=150)
    # Ensure type is str for type checkers
    if query is None:
        query = ""
    run = st.form_submit_button("Run query")

if run:
    st.session_state["last_query"] = query
    with st.spinner("Running query..."):
        try:
            df = sqlQuery(query)
        except Exception as e:
            st.error(f"Query failed: {e}")
            df = None
    if df is not None:
        st.success(f"Returned {len(df)} rows")
        st.dataframe(df)
