from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging
from impala.dbapi import connect
from dotenv import load_dotenv
import os

# Load environment variables dari .env
load_dotenv("/app/mount/resource-tomy/.env")

API_KEY = os.getenv("OPENWEATHER_API_KEY")
IMPALA_HOST = os.getenv("IMPALA_HOST")
IMPALA_PORT = int(os.getenv("IMPALA_PORT", 21050))
IMPALA_DB = os.getenv("IMPALA_DB")
IMPALA_TABLE = os.getenv("IMPALA_TABLE")
CITY_NAME = os.getenv("CITY_NAME")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def connect_to_impala_ssl():
    return connect(
        host=IMPALA_HOST,
        port=IMPALA_PORT,
        auth_mechanism="GSSAPI",
        use_ssl=True,
        kerberos_service_name="impala",
    )

def fetch_and_insert_weather():
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY_NAME}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()

    # Logging API response
    logging.info(f"Weather API Response: {data}")

    # Validasi response
    if "main" not in data:
        raise ValueError(f"Weather API error: {data}")

    temp = data["main"]["temp"]
    humidity = data["main"]["humidity"]
    pressure = data["main"].get("pressure", 0)
    weather_desc = data["weather"][0]["description"] if "weather" in data else "unknown"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Insert ke Kudu lewat Impala
    conn = connect_to_impala_ssl()
    cursor = conn.cursor()
    query = f"""
        INSERT INTO {IMPALA_DB}.{IMPALA_TABLE}
        VALUES ('{CITY_NAME}', {temp}, {humidity}, {pressure}, '{weather_desc}', CAST('{ts}' AS TIMESTAMP))
    """
    logging.info(f"Executing query: {query}")
    cursor.execute(query)

    cursor.close()
    conn.close()
    logging.info("Data inserted into Kudu successfully.")

with DAG(
    dag_id="weather_to_kudu",
    default_args=default_args,
    description="Fetch weather API and insert into Kudu every hour",
    schedule_interval="@hourly",
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["weather", "kudu"],
) as dag:

    task_fetch_insert = PythonOperator(
        task_id="fetch_and_insert_weather",
        python_callable=fetch_and_insert_weather,
    )

    task_fetch_insert
