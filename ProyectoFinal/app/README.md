
 # 📄 DESCRIPCIÓN DEL ARCHIVO: app.py (Procesamiento con Spark)
 

 Este script es el encargado de procesar datos crudos de viajes de taxi
 en formato Parquet almacenados en AWS S3.

 FUNCIONALIDAD PRINCIPAL:
 - Lee archivos Parquet desde S3 (capa RAW)
 - Detecta automáticamente el tipo de taxi (yellow o green)
 - Extrae año y mes desde el nombre del archivo
 - Normaliza nombres de columnas para distintos esquemas
 - Limpia datos inválidos (distancias, tarifas, duración, etc.)
 - Genera columnas derivadas como:
     - duración del viaje (trip_duration)
     - hora de pickup (pickup_hour)
 - Guarda los datos limpios en la capa PROCESSED
 - Calcula métricas agregadas como:
     - total de viajes
     - distancia promedio
     - tarifa promedio
     - ingreso total
     - distancia máxima y mínima
 - Guarda los resultados agregados en la capa OUTPUT

 ENTRADA:
 - Ruta S3 de archivo parquet (recibida como argumento)

 SALIDA:
 - Datos procesados en:
     s3://<bucket>/processed/taxis/
 - Métricas agregadas en:
     s3://<bucket>/output/taxis/

 TECNOLOGÍAS:
 - PySpark
 - AWS S3 (s3a)

 



 
 ## 📄 DESCRIPCIÓN DEL ARCHIVO: app.py (Streamlit Dashboard)
 

 Este script implementa una aplicación web interactiva usando Streamlit
 para consultar estadísticas de viajes de taxi almacenadas en AWS S3.

 FUNCIONALIDAD PRINCIPAL:
 - Permite al usuario seleccionar:
     - Año
     - Mes
     - Tipo de taxi (yellow / green)

 - Verifica si las métricas ya existen en la capa OUTPUT:
     ✔ Si existen:
         - Carga los datos con Spark
         - Muestra métricas clave
         - Muestra tabla preview
         - Muestra gráfica de barras

     ❗ Si no existen pero el archivo está en ARCHIVE:
         - Copia el archivo a RAW
         - Dispara el pipeline automático (Lambda / procesamiento)

     ❌ Si no existe en ningún lado:
         - Notifica al usuario

 FUNCIONES CLAVE:
 - check_availability → verifica existencia en S3
 - copy_archive_to_raw → copia archivos dentro del bucket
 - load_output_data → carga resultados desde S3 con Spark
 - get_output_metrics → obtiene métricas agregadas
 - get_output_preview → muestra datos en tabla

 VISUALIZACIÓN:
 - KPIs con st.metric
 - Tabla de datos (preview)
 - Gráfica de barras de métricas

 TECNOLOGÍAS:
 - Streamlit
 - Boto3
 - PySpark
 - AWS S3
 



 
 ## ARQUITECTURA GENERAL DEL PROYECTO
 

 El proyecto sigue una arquitectura tipo Data Pipeline en AWS:

 1. ARCHIVE (datos crudos)
    - Contiene todos los archivos originales (.parquet)

 2. RAW (trigger de procesamiento)
    - Cuando un archivo se copia aquí:
        → se activa una AWS Lambda

 3. PROCESSED
    - Datos limpios y transformados

 4. OUTPUT
    - Métricas agregadas listas para consumo

 5. STREAMLIT APP
    - Consume datos desde OUTPUT
    - Dispara procesamiento si no existen resultados

 FLUJO:

 Usuario → Streamlit →
     (¿ya existe en OUTPUT?)
         ✔ Sí → mostrar dashboard
         ❗ No → copiar a RAW →
                Lambda →
                Spark →
                OUTPUT →
                (luego usuario consulta otra vez)

 



 
 ## ☁️ SERVICIOS AWS UTILIZADOS
 

 - Amazon S3 → almacenamiento de datos
 - AWS Lambda → disparo automático de procesamiento
 - EC2 → ejecución de Spark + Streamlit

 



 
 ## 🚀 EJECUCIÓN DEL PROYECTO
 

 1. Procesamiento (Spark):
    python app.py s3://ruta/al/archivo.parquet

 2. Dashboard (Streamlit):
    streamlit run app.py --server.address 0.0.0.0 --server.port 8501

 