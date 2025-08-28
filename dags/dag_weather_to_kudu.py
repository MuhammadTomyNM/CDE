from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging
from impala.dbapi import connect
import os

# Load environment variables
API_KEY = os.getenv("WEATHER_API_KEY")
IMPALA_HOST = os.getenv("IMPALA_HOST")
IMPALA_PORT = int(os.getenv("IMPALA_PORT", 21050))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def fetch_and_insert_weather():
    url = f"http://api.openweathermap.org/data/2.5/weather?q=Jakarta&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()

    # Logging API response
    logging.info(f"Weather API Response: {data}")

    # Validasi response
    if "main" not in data:
        raise ValueError(f"Weather API error: {data}")

    temp = data["main"]["temp"]
    humidity = data["main"]["humidity"]

    # Insert ke Kudu lewat Impala
    conn = connect(
        host=IMPALA_HOST,
        port=IMPALA_PORT,
        auth_mechanism="GSSAPI",
        use_ssl=True,
        kerberos_service_name="impala",
    )
    cursor = conn.cursor()
    cursor.execute(
        f"INSERT INTO weather_data (city, temp, humidity, ts) "
        f"VALUES ('Jakarta', {temp}, {humidity}, NOW())"
    )
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
