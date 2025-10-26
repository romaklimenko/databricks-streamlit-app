import os
from databricks import sql
from databricks.sdk.core import Config
from dotenv import load_dotenv
import streamlit as st
import pandas as pd

load_dotenv()

for v in ["DATABRICKS_WAREHOUSE_ID"]:
    assert os.getenv(v), f"{v} must be set"


def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config()
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()


st.set_page_config(page_title="SQL Query", layout="wide")

st.title("Run a SQL query")
with st.form("sql_form"):
    default_query = (
        st.session_state.get(
            "last_query",
            "select * from samples.nyctaxi.trips limit 50",
        )
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
        st.dataframe(df, use_container_width=True)
