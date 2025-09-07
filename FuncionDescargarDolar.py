import requests
import boto3
import time
from botocore.exceptions import NoCredentialsError


s3_client = boto3.client("s3")
bucket_name = "dolar-raw-parcial1bigdata"

def descargar_y_guardar_json():
    url = "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"
    
   
    
    try:
        # Descargar JSON desde el link
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Crear timestamp
        timestamp = int(time.time())
        object_name = f"dolar-{timestamp}.json"
        # Guardar en S3
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=str(data).encode("utf-8"),
            ContentType="application/json"
        )



        print(f"Archivo {object_name} guardado en el bucket {bucket_name} exitosamente.")
        return{"status":"ok","file":object_name}
        
    except NoCredentialsError:
        print("Error: No se encontraron credenciales de AWS configuradas.")
    except Exception as e:
        print(f"Error: {e}")