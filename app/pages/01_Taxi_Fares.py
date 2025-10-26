import streamlit as st
from common import run_sql as sqlQuery


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
