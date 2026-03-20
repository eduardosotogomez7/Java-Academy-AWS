import boto3
import json
import subprocess
import time

# Configuración
BUCKET = "xideralaws-curso-eduardo"
QUEUE_PREFIX = "queue/"          # Carpeta donde Lambda pone los JSON
PROCESSED_PREFIX = "processed/"  # Carpeta donde moveremos los JSON procesados
SCRIPT_PATH = "/home/ubuntu/process_taxis.py"

s3 = boto3.client("s3")

def listar_json_queue():
    """Lista todos los archivos JSON en queue/"""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=QUEUE_PREFIX)
    if "Contents" not in resp:
        return []
    return [obj["Key"] for obj in resp["Contents"] if obj["Key"].endswith(".json")]

def leer_json_s3(key):
    """Descarga y retorna el contenido del JSON como dict"""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read())

def mover_a_procesados(key):
    """Mueve el JSON de queue/ a processed/"""
    new_key = key.replace(QUEUE_PREFIX, PROCESSED_PREFIX)
    s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key}, Key=new_key)
    s3.delete_object(Bucket=BUCKET, Key=key)
    print(f"JSON movido a processed/: {new_key}")

def ejecutar_spark(input_path):
    """Ejecuta el script process_taxis.py con Spark"""
    cmd = [
        "spark-submit",
        "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        SCRIPT_PATH,
        input_path
    ]
    print(f"Ejecutando Spark: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print("Error en Spark:", result.stderr)

def main_loop():
    print("Watcher iniciado. Esperando archivos en queue/ ...")
    while True:
        json_files = listar_json_queue()
        if not json_files:
            time.sleep(10)  # espera 10s antes de revisar otra vez
            continue

        for key in json_files:
            try:
                data = leer_json_s3(key)
                input_path = data.get("input_path")
                if not input_path:
                    print(f"No se encontró 'input_path' en {key}, saltando")
                    mover_a_procesados(key)
                    continue

                ejecutar_spark(input_path)
                mover_a_procesados(key)
            except Exception as e:
                print(f"Error procesando {key}: {e}")

if __name__ == "__main__":
    main_loop()