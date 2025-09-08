import json
import boto3
import pymysql
import os
from datetime import datetime

# Cliente global de S3 (sí puede inicializarse aquí)
s3_client = boto3.client("s3")

def lambda_handler(event, context):
    """
    Lambda que lee un archivo JSON desde S3 (trigger) 
    y lo inserta en MySQL convirtiendo timestamp(ms) a DATETIME.
    """
    try:
        print("inicio lamdba handler")
        # --- Variables de entorno cargadas en tiempo de ejecución ---
        DB_HOST = os.environ.get("DB_HOST", "localhost")
        DB_USER = os.environ.get("DB_USER", "user")
        DB_PASS = os.environ.get("DB_PASS", "pass")
        DB_NAME = os.environ.get("DB_NAME", "db")
        TABLE_NAME = os.environ.get("TABLE_NAME", "dolar_trm")

        # --- Obtener bucket y key desde el evento ---
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]

        # --- Descargar archivo de S3 ---
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response["Body"].read().decode("utf-8")
        data = json.loads(file_content)   # lista de listas

        # --- Conectar a MySQL ---
        print("conectando a la basedatos")
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            cursorclass=pymysql.cursors.Cursor,
        )

        inserted = 0  # contador de filas insertadas
        with conn.cursor() as cursor:
            sql = f"""
                INSERT INTO {TABLE_NAME} (fechahora, valor)
                VALUES (%s, %s)
            """

            for row in data:
                ts_ms = int(row[0])
                fechahora = datetime.utcfromtimestamp(ts_ms / 1000.0)  # convertir a UTC
                valor = float(row[1])

                cursor.execute(sql, (fechahora, valor))
                inserted += 1

            conn.commit()

        conn.close()

        print(f"Archivo {key} insertado en {TABLE_NAME} con {inserted} filas")
    
        return {"status": "ok", "rows_inserted": inserted}

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
