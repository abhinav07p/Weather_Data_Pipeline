import streamlit as st

st.markdown("## 📘 About This Project")

st.markdown("""
## 🌦️ Real-Time Weather Data Engineering Pipeline (Airflow + AWS + Analytics UI)  
**end-to-end data engineering workflow**:
___________________
- This project is a production-style data pipeline that converts live OpenWeather API signals into a queryable analytics layer on AWS — orchestrated with Apache Airflow and validated end-to-end via Athena.
- The pipeline runs on a schedule, persists raw + curated data into a partitioned S3 data lake design, and enables serverless SQL analysis (Athena) without provisioning databases.
- A Streamlit control panel provides a lightweight “ops + demo” layer: view DAG status, trigger runs, and jump directly into Airflow/AWS consoles for monitoring and validation.
- Enable cloud analytics via **Glue + Athena + QuickSight**
""")

st.markdown("### 🎯 Significance (why this matters)")
st.markdown("""
✅ Demonstrates real data engineering skills: orchestration, data lake patterns, partitioning, cloud integration, and observability.  
✅ Mirrors real-world workflow: ingest → transform → store → query → validate → monitor.
""")

st.markdown("### 🧰 Tech Stack")
st.markdown("""
- **Airflow (DAG orchestration), Docker Compose (local reproducibility)**
- **AWS S3 (data lake), AWS Glue/Athena (metadata + serverless SQL)**
- **Streamlit UI (operational dashboard / demo surface)**s
""")
