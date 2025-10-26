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


st.set_page_config(page_title="Taxi Fares", layout="wide")


@st.cache_data(ttl=30)
def getData():
    return sqlQuery("select * from samples.nyctaxi.trips limit 5000")


data = getData()

st.header("Taxi fare distribution")
col1, col2 = st.columns([3, 1])
with col1:
    st.scatter_chart(
        data=data, height=400, width=700, y="fare_amount", x="trip_distance"
    )
with col2:
    st.subheader("Predict fare")
    pickup = st.text_input("From (zipcode)", value="10003")
    dropoff = st.text_input("To (zipcode)", value="11238")
    d = data[
        (data["pickup_zip"] == int(pickup)) & (data["dropoff_zip"] == int(dropoff))
    ]
    st.write(f"# **${d['fare_amount'].mean() if len(d) > 0 else 99:.2f}**")

st.dataframe(data=data, height=600)
