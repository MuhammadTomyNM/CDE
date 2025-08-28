from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import boto3
import os
from dotenv import load_dotenv
from pathlib import Path

print(">> DAG started parsing...")
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
print(">> Loaded API key:", os.getenv("OPENWEATHER_API_KEY"))

# --- Konfigurasi dari .env ---
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET = os.getenv("AWS_S3_BUCKET")
CITY_NAME = os.getenv("CITY_NAME")

# --- Fungsi: Ambil data cuaca dari API ---
def fetch_weather_data():
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY_NAME}&appid={OPENWEATHER_API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()

    # Simpan sementara ke file lokal
    with open('/tmp/weather_data.json', 'w') as f:
        json.dump(data, f)

    print(f"Data cuaca untuk {CITY_NAME} berhasil diambil.")

# --- Fungsi: Upload ke S3 ---
def upload_to_s3():
    s3_key = f"weather_data/{CITY_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    with open('/tmp/weather_data.json', 'rb') as f:
        s3.upload_fileobj(f, S3_BUCKET, s3_key)
    print(f"Berhasil upload ke S3: {S3_BUCKET}/{s3_key}")

# --- DAG Definition ---
with DAG(
    dag_id='weather_to_s3_dag',
    start_date=datetime(2024, 1, 1),
    schedule='* * * * *',  # Setiap menit
    catchup=False,
    tags=['weather', 'api', 's3']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_data
    )

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    fetch_task >> upload_task