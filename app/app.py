from dotenv import load_dotenv
import streamlit as st

load_dotenv()

st.set_page_config(page_title="Databricks Streamlit App", layout="wide")

"# Welcome to Databricks + Streamlit App!"

st.html('<video autoplay loop src="//i.imgflip.com/6ypc3w.mp4" type="video/mp4">')

if st.button("Yay!"):
    st.balloons()
