from pyspark.sql import SparkSession
import requests
from datetime import datetime

spark = SparkSession.builder.appName("WeatherToKudu").getOrCreate()

API_KEY = "API"
CITY = "Jakarta"

url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
data = requests.get(url).json()

if "main" not in data:
    raise ValueError(f"API error: {data}")

record = [
    (
        CITY,
        data["main"]["temp"],
        data["main"]["humidity"],
        data["main"].get("pressure", 0),
        data["weather"][0]["description"] if "weather" in data else "unknown",
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
]

columns = ["city", "temp", "humidity", "pressure", "weather_desc", "ts"]

df = spark.createDataFrame(record, columns)

# Tulis ke Kudu
df.write \
  .format("kudu") \
  .option("kudu.master", "kudu") \
  .option("kudu.table", "weather_data") \
  .mode("append") \
  .save()
