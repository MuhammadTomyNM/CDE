from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
from impala.dbapi import connect

# =============================
IMPALA_HOST = os.getenv("IMPALA_HOST")
IMPALA_PORT = int(os.getenv("IMPALA_PORT", "21050"))
CITY_NAME = os.getenv("CITY_NAME", "Jakarta")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# =============================
def fetch_and_insert_weather():
    # 1. Ambil data dari API
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY_NAME}&appid={OPENWEATHER_API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()

    # 2. Ambil field penting
    temp = data['main']['temp']
    humidity = data['main']['humidity']
    pressure = data['main']['pressure']
    description = data['weather'][0]['description']
    ts = datetime.utcfromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S')

    # 3. Koneksi ke Impala (SSL + Kerberos)
    conn = connect(
        host=IMPALA_HOST,
        port=IMPALA_PORT,
        auth_mechanism='GSSAPI',
        use_ssl=True,
        kerberos_service_name='impala'
    )
    cursor = conn.cursor()

    # 4. Insert langsung ke tabel kudu (pastikan tabel sudah ada)
    insert_sql = f"""
        INSERT INTO weather_data (city, temp, humidity, pressure, description, ts)
        VALUES ('{CITY_NAME}', {temp}, {humidity}, {pressure}, '{description}', TIMESTAMP '{ts}')
    """
    cursor.execute(insert_sql)
    conn.close()

    print(f"✅ Data cuaca {CITY_NAME} berhasil dimasukkan ke Kudu.")

# =============================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# =============================
with DAG(
    dag_id='weather_to_kudu_dag',
    default_args=default_args,
    description='Ambil cuaca dari API dan insert langsung ke Kudu',
    schedule_interval='@hourly',  # jalan tiap 1 jam
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['weather', 'kudu', 'impala'],
) as dag:

    fetch_and_insert = PythonOperator(
        task_id='fetch_and_insert_weather',
        python_callable=fetch_and_insert_weather
    )

    fetch_and_insert
