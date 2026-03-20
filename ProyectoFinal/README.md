#  Taxi Data Pipeline & Dashboard

##  Descripción

Pipeline de datos en la nube para el procesamiento y análisis de viajes de taxis de NYC.  
El sistema ingiere archivos Parquet desde un bucket en S3, los procesa con PySpark para limpieza y generación de métricas, y almacena los resultados de forma particionada.

Incluye una aplicación en Streamlit que permite consultar dinámicamente estadísticas por año, mes y tipo de taxi, activando el procesamiento automáticamente si los datos aún no han sido generados.

---

##  Arquitectura del proyecto

El flujo completo del sistema es el siguiente:

S3 (archive)  
→ S3 (raw)  
→ AWS Lambda  
→ PySpark Processing (EC2 / EMR)  
→ S3 (processed / output)  
→ Streamlit Dashboard  

---

##  Funcionamiento

1. El usuario selecciona:
   - Año
   - Mes
   - Tipo de taxi (yellow / green)

2. El sistema verifica:
   - Si los datos ya existen en `output/`
     → Se muestran inmediatamente en el dashboard

   - Si no existen pero están en `archive/`
     → Se copian a `raw/`
     → Se dispara automáticamente el procesamiento

3. El pipeline:
   - Limpia los datos
   - Normaliza columnas
   - Calcula métricas agregadas
   - Guarda resultados en S3

4. Streamlit muestra:
   - Métricas principales
   - Vista previa de datos
   - Gráficas simples

---

## 📊 Métricas generadas

- Total de viajes
- Distancia promedio
- Tarifa promedio
- Ingreso total
- Distancia máxima
- Distancia mínima

---

## 🛠️ Tecnologías utilizadas

- Python
- PySpark
- AWS S3
- AWS Lambda
- Streamlit
- Boto3

---

## 📁 Estructura del proyecto



 ```
 taxi-data-pipeline/
 │
 ├── app/
 │   └── app.py
 │
 ├── pipeline/
     └── watcher.py
 │   └── process_taxis.py
 │
 ├── images/
 │   └── dashboard.png
 │
 ├── requirements.txt
 └── README.md
 ```

---

## Ejecución local (Streamlit)

```bash
pip install -r requirements.txt
streamlit run app.py
```


## Notas importantes

Las credenciales de AWS no están incluidas en este repositorio.

Se recomienda usar IAM Roles o variables de entorno para autenticación.

Los archivos de datos (Parquet) no se incluyen por su tamaño.