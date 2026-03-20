from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year, month, col, unix_timestamp, lit,
    count, avg, sum as spark_sum, max as spark_max, min as spark_min
)
import sys
import re
from pyspark.sql.functions import hour

# =========================
# 1. Parámetros de entrada
# =========================
input_path = sys.argv[1]

# =========================
# 2. Spark Session
# =========================
spark = SparkSession.builder \
    .appName("Taxis Processing") \
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ) \
    .getOrCreate()

##spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "xxx")
##spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "xxx") 
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com") 
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# =========================
# 3. Leer datos
# =========================
df = spark.read.parquet(input_path)

# =========================
# 4. Detectar tipo de taxi
# =========================
file_name = input_path.split("/")[-1].lower()
print("Archivo recibido:", file_name)

if "yellow" in file_name:
    taxi_type = "yellow"
elif "green" in file_name:
    taxi_type = "green"
else:
    taxi_type = "unknown"

df = df.withColumn("taxi_type", lit(taxi_type))

# =========================
# 5. Extraer año/mes del archivo
# =========================
match = re.search(r"(\d{4})-(\d{2})", file_name)
if not match:
    raise ValueError(f"No se pudo extraer año y mes del nombre del archivo: {file_name}")

year_file = int(match.group(1))
month_file = int(match.group(2))

# =========================
# 6. Normalizar columnas
# =========================
COLUMN_CANDIDATES = {
    "pickup_datetime": ["tpep_pickup_datetime", "lpep_pickup_datetime"],
    "dropoff_datetime": ["tpep_dropoff_datetime", "lpep_dropoff_datetime"],
    "passenger_count": ["passenger_count"],
    "trip_distance": ["trip_distance"],
    "fare_amount": ["fare_amount"],
    "total_amount": ["total_amount"],
}

def find_existing_column(df, candidates):
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None

resolved_columns = {}

for standard_name, candidates in COLUMN_CANDIDATES.items():
    found = find_existing_column(df, candidates)
    resolved_columns[standard_name] = found

required_columns = [
    "pickup_datetime",
    "dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
]

missing_required = [c for c in required_columns if resolved_columns[c] is None]
if missing_required:
    raise ValueError(f"Faltan columnas requeridas: {missing_required}. Columnas disponibles: {df.columns}")

for standard_name, original_name in resolved_columns.items():
    if original_name and original_name != standard_name:
        df = df.withColumnRenamed(original_name, standard_name)

# Si passenger_count no existe, lo creamos
if resolved_columns["passenger_count"] is None:
    df = df.withColumn("passenger_count", lit(1))

# =========================
# 7. Filtrar datos correctos
# =========================
df = df.filter(
    (year(col("pickup_datetime")) == year_file) &
    (month(col("pickup_datetime")) == month_file)
)

# =========================
# 8. Crear columnas derivadas
# =========================
df = df.withColumn("year", year("pickup_datetime")) \
       .withColumn("month", month("pickup_datetime"))

df = df.withColumn(
    "trip_duration",
    (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60
)

df = df.withColumn("pickup_hour", hour(col("pickup_datetime")))

# =========================
# 9. Limpieza
# =========================
df = df.filter(col("trip_duration") > 0)
df = df.filter(col("trip_distance") > 0)
df = df.filter(col("trip_distance") < 100)
df = df.filter(col("fare_amount") > 0)
df = df.filter(col("total_amount") > 0)

df = df.fillna({
    "passenger_count": 1
})

# =========================
# 10. Guardar PROCESSED
# =========================
df.write.mode("append") \
    .partitionBy("taxi_type", "year", "month") \
    .parquet("s3a://xideralaws-curso-eduardo/processed/taxis/")

# =========================
# 11. Agregaciones
# =========================
df_agg = df.groupBy("year", "month", "taxi_type").agg(
    count("*").alias("total_trips"),
    avg("trip_distance").alias("avg_distance"),
    avg("fare_amount").alias("avg_fare"),
    spark_sum("total_amount").alias("total_revenue"),
    spark_max("trip_distance").alias("max_distance"),
    spark_min("trip_distance").alias("min_distance")
)



# =========================
# 12. Guardar OUTPUT
# =========================
df_agg.write.mode("append") \
    .partitionBy("year", "month", "taxi_type") \
    .parquet("s3a://xideralaws-curso-eduardo/output/taxis/")

spark.stop()