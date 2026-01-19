import streamlit as st
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from datetime import datetime

# ---- CONFIG ----
AIRFLOW_BASE = "http://localhost:8080"
DAG_ID = "weather_live_dag"

AIRFLOW_USER = "airflow"
AIRFLOW_PASS = "airflow"
auth = HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASS)

AWS_REGION = "us-east-2"
S3_BUCKET = "big-data-abhi07"

AIRFLOW_UI_LINK = f"{AIRFLOW_BASE}/home"
S3_BUCKET_LINK = f"https://s3.console.aws.amazon.com/s3/buckets/{S3_BUCKET}?region={AWS_REGION}&tab=objects"
ATHENA_LINK = f"https://{AWS_REGION}.console.aws.amazon.com/athena/home?region={AWS_REGION}#/query-editor"
GLUE_LINK = f"https://{AWS_REGION}.console.aws.amazon.com/gluestudio/home?region={AWS_REGION}#/catalog"

# ---- UI HEADER ----
st.markdown("""
<style>
.big-title {font-size: 34px; font-weight: 800;}
.sub {opacity: 0.85; font-size: 15px;}
.card {
    padding: 16px;
    border-radius: 16px;
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.10);
}
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="big-title">🚀 Home — Pipeline Control Center</div>', unsafe_allow_html=True)
st.markdown('<div class="sub">Trigger Airflow DAG runs, watch task progress, and jump to AWS services.</div>', unsafe_allow_html=True)
st.write("")

# ---- QUICK LINKS ----
st.subheader("🔗 Quick Links")
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.link_button("Open Airflow UI", AIRFLOW_UI_LINK, use_container_width=True)
with c2:
    st.link_button("Open S3 Bucket", S3_BUCKET_LINK, use_container_width=True)
with c3:
    st.link_button("Open Athena", ATHENA_LINK, use_container_width=True)
with c4:
    st.link_button("Open Glue Catalog", GLUE_LINK, use_container_width=True)

st.divider()

# ---- Helpers ----
def airflow_get(path: str, params=None):
    r = requests.get(f"{AIRFLOW_BASE}{path}", auth=auth, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def airflow_post(path: str, payload: dict):
    r = requests.post(f"{AIRFLOW_BASE}{path}", auth=auth, json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

def latest_run():
    runs = airflow_get(f"/api/v1/dags/{DAG_ID}/dagRuns", params={"limit": 1, "order_by": "-execution_date"})
    return runs["dag_runs"][0] if runs.get("dag_runs") else None

# ---- CONTROL PANEL ----
st.subheader("🕹️ Controls")
left, mid, right = st.columns([1, 1, 2])

with left:
    if st.button("▶ Trigger DAG Run", use_container_width=True):
        run_id = f"streamlit__{datetime.utcnow().isoformat()}"
        resp = airflow_post(f"/api/v1/dags/{DAG_ID}/dagRuns", {"dag_run_id": run_id})
        st.success(f"Triggered: {resp.get('dag_run_id')}")
        st.rerun()

with mid:
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()

with right:
    st.caption("Trigger once → Refresh to watch tasks move from queued → running → success.")

st.divider()

# ---- STATUS ----
st.subheader("📊 Status")

try:
    dag_info = airflow_get(f"/api/v1/dags/{DAG_ID}")
    run = latest_run()

    m1, m2, m3, m4 = st.columns(4)

    m1.metric("DAG", dag_info["dag_id"])
    m2.metric("Paused", str(dag_info["is_paused"]))
    m3.metric("Latest Run State", (run["state"] if run else "No runs"))
    m4.metric("Schedule", dag_info.get("schedule_interval", {}).get("value", "N/A"))

    if run:
        st.markdown(f"**Latest Run ID:** `{run['dag_run_id']}`  \n**Execution:** `{run['execution_date']}`")

        tis = airflow_get(f"/api/v1/dags/{DAG_ID}/dagRuns/{run['dag_run_id']}/taskInstances")
        rows = [{"task_id": t["task_id"], "state": t["state"]} for t in tis.get("task_instances", [])]
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

        exec_date = run["execution_date"][:10]
        #s3_expected = f"s3://{S3_BUCKET}/weather/date={exec_date}/run_id={run['dag_run_id']}.json"
        #st.info(f"Expected S3 output (your key pattern): **{s3_expected}**")
        with st.expander("📦 Expected S3 Output Path", expanded=True):
            st.code(f"s3://{S3_BUCKET}/weather/date={exec_date}/run_id={run['dag_run_id']}.json", language="text")


except Exception as e:
    st.error(f"❌ Airflow not reachable / DAG missing: {e}")
    st.caption("Make sure Docker Airflow is running on http://localhost:8080 and the DAG exists.")
