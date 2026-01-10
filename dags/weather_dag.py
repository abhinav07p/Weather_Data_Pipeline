from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import json
import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ✅ Docker-safe base path inside Airflow containers
BASE_DIR = "/opt/airflow/data"
RAW_DIR = os.path.join(BASE_DIR, "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")

def fetch_weather(**context):
    api_key = Variable.get("OPENWEATHER_API_KEY")
    city = "Boston,US"

    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?q={city}&appid={api_key}&units=metric"
    )

    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    # ---------- SAVE RAW FILE ----------
    os.makedirs(RAW_DIR, exist_ok=True)

    execution_date = context["ds"]    # e.g., 2026-01-10
    file_path = os.path.join(RAW_DIR, f"weather_{execution_date}.json")

    with open(file_path, "w") as f:
        json.dump(data, f)

    print(f"Saved raw weather to: {file_path}")
    return file_path


def transform_weather(**context):
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    ti = context["ti"]
    raw_file_path = ti.xcom_pull(task_ids="fetch_weather_task")

    with open(raw_file_path, "r") as f:
        data = json.load(f)

    transformed = {
        "city": data["name"],
        "country": data["sys"]["country"],
        "temperature_C": data["main"]["temp"],
        "feels_like_C": data["main"]["feels_like"],
        "humidity": data["main"]["humidity"],
        "condition": data["weather"][0]["main"],
        "description": data["weather"][0]["description"],
        "wind_speed": data["wind"]["speed"]
    }

    processed_path = os.path.join(PROCESSED_DIR, "processed_weather.json")

    # You were writing JSONL (one JSON per line) — keeping it the same:
    with open(processed_path, "w") as f:
        f.write(json.dumps(transformed) + "\n")

    print(f"Processed weather saved to: {processed_path}")
    return processed_path


def upload_to_s3(**context):
    s3 = S3Hook(aws_conn_id="aws_default")
    bucket_name = "big-data-abhi07"

    local_file = os.path.join(PROCESSED_DIR, "processed_weather.json")

    execution_date = context["data_interval_end"].strftime("%Y-%m-%d")
    run_id = context["run_id"]
    s3_key = f"weather/date={execution_date}/run_id={run_id}.json"

    s3.load_file(
        filename=local_file,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )

    print(f"Uploaded {local_file} to s3://{bucket_name}/{s3_key}")


with DAG(
    dag_id="weather_live_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False
) as dag:

    fetch_weather_task = PythonOperator(
        task_id="fetch_weather_task",
        python_callable=fetch_weather
    )

    transform_task = PythonOperator(
        task_id="transform_weather_task",
        python_callable=transform_weather
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3_task",
        python_callable=upload_to_s3
    )

    fetch_weather_task >> transform_task >> upload_task
