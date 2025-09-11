from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pymysql
from datetime import datetime
import os
from dotenv import load_dotenv  # <-- NUEVO

# -----------------------
# CARGAR VARIABLES DESDE .env
# -----------------------
load_dotenv()  # Busca un archivo .env en la raíz del proyecto

# -----------------------
# CONFIG (lee del .env)
# -----------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "usuario")
DB_PASS = os.getenv("DB_PASS", "clave")
DB_NAME = os.getenv("DB_NAME", "basedatos")

# -----------------------
# MODELO DE REQUEST
# -----------------------
class RangoFechas(BaseModel):
    fecha_inicio: str  # "2025-09-05 17:57:43"
    fecha_fin: str     # "2025-09-06 17:57:43"

# -----------------------
# FastAPI
# -----------------------
app = FastAPI(title="Consulta TRM (MySQL)")

@app.post("/trm/rango")
def get_trm_rango(rango: RangoFechas):
    try:
        # Validar formato de fecha
        try:
            inicio = datetime.strptime(rango.fecha_inicio, "%Y-%m-%d %H:%M:%S")
            fin = datetime.strptime(rango.fecha_fin, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Formato de fecha inválido. Use YYYY-MM-DD HH:MM:SS"
            )

        # Conectar a MySQL
        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5
        )

        with conn:
            with conn.cursor() as cur:
                sql = f"""
                    SELECT fechahora, valor
                    FROM dolar_trm
                    WHERE fechahora BETWEEN %s AND %s
                    ORDER BY fechahora ASC
                """
                cur.execute(sql, (inicio, fin))
                rows = cur.fetchall()

        # Serializar resultados
        items = [
            {
                "fechahora": r["fechahora"].isoformat()
                if isinstance(r["fechahora"], datetime) else str(r["fechahora"]),
                "valor": float(r["valor"])
            }
            for r in rows
        ]

        return {"count": len(items), "items": items}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

@app.get("/big")
def mensaje():
    return("bigdata")