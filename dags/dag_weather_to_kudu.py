from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import os
from dotenv import load_dotenv
from pathlib import Path
from impala.dbapi import connect

print(">> DAG started parsing...")

# Path .env di CDE (resource mount path)
load_dotenv("/app/mount/resource-tomy/.env")

# --- Konfigurasi dari .env ---
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY_NAME = os.getenv("CITY_NAME")

IMPALA_HOSTS = os.getenv("IMPALA_HOSTS", "").split(",")
IMPALA_PORT = int(os.getenv("IMPALA_PORT", "21050"))
IMPALA_DB = os.getenv("IMPALA_DB")
IMPALA_TABLE = os.getenv("IMPALA_TABLE")

# --- Helper: Koneksi ke Impala dengan failover ---
def get_impala_connection():
    for host in IMPALA_HOSTS:
        try:
            conn = connect(
                    host=host,
                    port=IMPALA_PORT,
                    database=IMPALA_DB,
                    auth_mechanism='GSSAPI'
                )

            print(f"✅ Connected to Impala host: {host}")
            return conn
        except Exception as e:
            print(f"⚠️ Gagal konek ke {host}: {e}")
    raise Exception("❌ Tidak bisa konek ke salah satu host Impala")

# --- Fungsi: Ambil data cuaca dari API ---
def fetch_weather_data():
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY_NAME}&appid={OPENWEATHER_API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()

    with open('/tmp/weather_data.json', 'w') as f:
        json.dump(data, f)

    print(f"✅ Data cuaca untuk {CITY_NAME} berhasil diambil.")

# --- Fungsi: Simpan ke Kudu via Impala (INSERT) ---
def insert_to_kudu():
    with open('/tmp/weather_data.json', 'r') as f:
        data = json.load(f)

    city = data.get("name", "")
    temp = data.get("main", {}).get("temp") or 0
    humidity = data.get("main", {}).get("humidity") or 0
    pressure = data.get("main", {}).get("pressure") or 0
    weather = data.get("weather", [{}])[0].get("description", "").replace("'", "''")

    dt_unix = data.get("dt")
    if dt_unix:
        dt = datetime.utcfromtimestamp(dt_unix).strftime("%Y-%m-%d %H:%M:%S")
    else:
        dt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    conn = get_impala_connection()
    cursor = conn.cursor()

    insert_sql = f"""
        INSERT INTO {IMPALA_DB}.{IMPALA_TABLE} 
        (city, dt, temp, humidity, pressure, weather)
        VALUES ('{city}', '{dt}', {temp}, {humidity}, {pressure}, '{weather}')
    """
    cursor.execute(insert_sql)

    print(f"✅ Berhasil INSERT data cuaca {city} ke {IMPALA_TABLE}.")

    cursor.close()
    conn.close()

# --- DAG Definition ---
with DAG(
    dag_id='weather_to_kudu_insert_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['weather', 'api', 'kudu']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_data
    )

    insert_task = PythonOperator(
        task_id='insert_to_kudu',
        python_callable=insert_to_kudu
    )

    fetch_task >> insert_task
