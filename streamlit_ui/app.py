import streamlit as st

st.set_page_config(
    page_title="Weather Pipeline Dashboard",
    page_icon="🌦️",
    layout="wide"
)

st.markdown("""
# 🌦️ Weather Pipeline Dashboard  
A mini control center for **Airflow + AWS (S3 / Glue / Athena)**  
Use the sidebar to navigate.
""")

st.info("Go to **Home** to trigger DAGs and monitor status. Check **About** and **How To Use** for project explanation.")
