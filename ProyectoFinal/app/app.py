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
(venv) ➜  ~ cat app.py          
import streamlit as st
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
import pandas as pd
# =========================================
# 1. CONFIGURACIÓN GENERAL
# =========================================
BUCKET_NAME = "xideralaws-curso-eduardo"

ARCHIVE_PREFIX = "archive/taxis/"
RAW_PREFIX = "raw/taxis/"
OUTPUT_PREFIX = "output/taxis/"

# =========================================
# 2. CLIENTE S3
# =========================================
s3 = boto3.client("s3")


# s3 = boto3.client(
#     "s3",
#     aws_access_key_id="TU_ACCESS_KEY",
#     aws_secret_access_key="TU_SECRET_KEY",
#     region_name="us-west-1"
# )

# =========================================
# 3. SPARK SESSION
# =========================================
@st.cache_resource
def get_spark():
    spark = SparkSession.builder \
        .appName("Taxis Dashboard") \
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ) \
        .getOrCreate()

    ##En esta seccion será necesario ingresar las ceredenciales correspondientes, este repositoprio no las incluye por seguridad
    ##spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "xxxx")
    ##spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "xxxx")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    return spark


spark = get_spark()

# =========================================
# 4. FUNCIONES AUXILIARES
# =========================================
def build_file_name(year, month, taxi_type):
    month_str = str(month).zfill(2)
    return f"{taxi_type}_tripdata_{year}-{month_str}.parquet"


def build_s3_keys(year, month, taxi_type):
    file_name = build_file_name(year, month, taxi_type)

    archive_key = f"{ARCHIVE_PREFIX}{file_name}"
    raw_key = f"{RAW_PREFIX}{file_name}"
    output_prefix = f"{OUTPUT_PREFIX}year={year}/month={int(month)}/taxi_type={taxi_type}/"

    return {
        "file_name": file_name,
        "archive_key": archive_key,
        "raw_key": raw_key,
        "output_prefix": output_prefix
    }


def object_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ["404", "NoSuchKey", "NotFound"]:
            return False
        raise


def prefix_has_files(bucket, prefix):
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1
    )
    return "Contents" in response


def check_availability(year, month, taxi_type):
    paths = build_s3_keys(year, month, taxi_type)

    archive_exists = object_exists(BUCKET_NAME, paths["archive_key"])
    output_exists = prefix_has_files(BUCKET_NAME, paths["output_prefix"])

    return {
        "year": year,
        "month": month,
        "taxi_type": taxi_type,
        "file_name": paths["file_name"],
        "archive_key": paths["archive_key"],
        "raw_key": paths["raw_key"],
        "output_prefix": paths["output_prefix"],
        "archive_exists": archive_exists,
        "output_exists": output_exists
    }


def copy_archive_to_raw(year, month, taxi_type):
    paths = build_s3_keys(year, month, taxi_type)

    if not object_exists(BUCKET_NAME, paths["archive_key"]):
        return {
            "success": False,
            "message": "El archivo no existe en archive.",
            "source": paths["archive_key"],
            "destination": paths["raw_key"]
        }

    s3.copy_object(
        Bucket=BUCKET_NAME,
        CopySource={
            "Bucket": BUCKET_NAME,
            "Key": paths["archive_key"]
        },
        Key=paths["raw_key"]
    )

    return {
        "success": True,
        "message": "Archivo copiado a raw/taxis/ correctamente.",
        "source": paths["archive_key"],
        "destination": paths["raw_key"]
    }


def build_output_path(year, month, taxi_type):
    return (
        f"s3a://{BUCKET_NAME}/"
        f"{OUTPUT_PREFIX}year={year}/month={int(month)}/taxi_type={taxi_type}/"
    )


def load_output_data(year, month, taxi_type):
    output_path = build_output_path(year, month, taxi_type)
    df = spark.read.parquet(output_path)
    return df

def get_output_preview(year, month, taxi_type, limit=10):
    df = load_output_data(year, month, taxi_type)
    pdf = df.limit(limit).toPandas()
    return pdf

def get_output_metrics(year, month, taxi_type):
    df = load_output_data(year, month, taxi_type)
    row = df.first()

    if row is None:
        return None

    row = row.asDict()

    return {
        "year": row.get("year"),
        "month": row.get("month"),
        "taxi_type": row.get("taxi_type"),
        "total_trips": row.get("total_trips"),
        "avg_distance": row.get("avg_distance"),
        "avg_fare": row.get("avg_fare"),
        "total_revenue": row.get("total_revenue"),
        "max_distance": row.get("max_distance"),
        "min_distance": row.get("min_distance")
    }


def resolve_taxi_request(year, month, taxi_type):
    status = check_availability(year, month, taxi_type)

    if status["output_exists"]:
        metrics = get_output_metrics(year, month, taxi_type)
        return {
            "status": "ready",
            "message": "Las estadísticas solicitadas están listas para consultarse.",
            "details": status,
            "metrics": metrics
        }

    if status["archive_exists"]:
        copy_result = copy_archive_to_raw(year, month, taxi_type)
        return {
            "status": "processing_triggered",
            "message": "Las estadísticas solicitadas estarán disponibles en un momento.",
            "details": status,
            "copy_result": copy_result
        }

    return {
        "status": "not_found",
        "message": "Las estadísticas solcitadas no se encuentran en los archivos.",
        "details": status
    }

# =========================================
# 5. INTERFAZ STREAMLIT
# =========================================
st.set_page_config(page_title="Taxi Dashboard", layout="wide")

st.title("Dashboard de Taxis NYC")
st.write("Consulta estadísticas por año, mes y tipo de taxi.")

years = list(range(2015, 2026))
months = list(range(1, 13))
taxi_types = ["yellow", "green"]

col1, col2, col3 = st.columns(3)

with col1:
    selected_year = st.selectbox("Año", years, index=len(years)-1)

with col2:
    selected_month = st.selectbox("Mes", months, index=0)

with col3:
    selected_taxi_type = st.selectbox("Tipo de taxi", taxi_types, index=0)

if st.button("Consultar"):
    with st.spinner("Procesando solicitud..."):
        result = resolve_taxi_request(selected_year, selected_month, selected_taxi_type)

    status = result["status"]

    if status == "ready":
        st.success(result["message"])

        metrics = result["metrics"]

        if metrics is not None:
            m1, m2, m3 = st.columns(3)
            m4, m5, m6 = st.columns(3)

            with m1:
                st.metric("Total de viajes", f"{metrics['total_trips']:,}" if metrics["total_trips"] is not None else "N/A")

            with m2:
                st.metric("Distancia promedio", f"{metrics['avg_distance']:.2f}" if metrics["avg_distance"] is not None else "N/A")

            with m3:
                st.metric("Tarifa promedio", f"{metrics['avg_fare']:.2f}" if metrics["avg_fare"] is not None else "N/A")

            with m4:
                st.metric("Ingreso total", f"{metrics['total_revenue']:.2f}" if metrics["total_revenue"] is not None else "N/A")

            with m5:
                st.metric("Distancia máxima", f"{metrics['max_distance']:.2f}" if metrics["max_distance"] is not None else "N/A")

            with m6:
                st.metric("Distancia mínima", f"{metrics['min_distance']:.2f}" if metrics["min_distance"] is not None else "N/A")

            st.subheader("Vista previa de datos")
            preview_df = get_output_preview(selected_year, selected_month, selected_taxi_type)
            st.dataframe(preview_df, use_container_width=True)

            st.subheader("Gráfica de métricas")
            chart_df = pd.DataFrame({
                "Métrica": ["Total viajes", "Distancia promedio", "Tarifa promedio", "Ingreso total"],
                "Valor": [
                    metrics["total_trips"] if metrics["total_trips"] is not None else 0,
                    metrics["avg_distance"] if metrics["avg_distance"] is not None else 0,
                    metrics["avg_fare"] if metrics["avg_fare"] is not None else 0,
                    metrics["total_revenue"] if metrics["total_revenue"] is not None else 0
                ]
            })

            st.bar_chart(chart_df.set_index("Métrica")) 
            st.subheader("Detalles")
            st.json(metrics)

        else:
            st.warning("Se encontró output, pero no fue posible obtener métricas.")

    elif status == "processing_triggered":
        st.warning(result["message"])
        st.info("El procesamiento fue lanzado. Vuelve a consultar en un momento.")

        st.subheader("Detalle")
        st.json(result["copy_result"])

    elif status == "not_found":
        st.error(result["message"])
        st.subheader("Detalle")
        st.json(result["details"])
