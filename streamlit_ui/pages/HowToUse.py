import streamlit as st

st.markdown("## 🧭 How To Use")

st.markdown("""
### 1) Start Docker Airflow
In your project folder (where docker-compose.yaml is):
- Run: `docker compose up -d`
- Open Airflow UI: http://localhost:8080

### 2) Add Required Secrets (Airflow Variables / Connections)
- Add Variable: `OPENWEATHER_API_KEY`
- Add AWS Connection: `aws_default` with Access Key + Secret Key

### 3) Run the DAG
- Go to **Home page** → click **Trigger DAG Run**
- Refresh and verify:
  - fetch_weather_task ✅
  - transform_weather_task ✅
  - upload_to_s3_task ✅

### 4) Verify output in AWS
- S3 → confirm file created under:
  `weather/date=YYYY-MM-DD/run_id=<run_id>.json`

### 5) (Next steps)
- Run Glue Crawler → create table
- Query in Athena
- Build QuickSight dashboard
""")

st.info("Tip: Use the **Home** page buttons to open Airflow and AWS directly.")
