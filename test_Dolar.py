import pytest
from unittest.mock import patch, MagicMock
import json

from FuncionDescargarDolar import descargar_y_guardar_json  

@patch("FuncionDescargarDolar.s3_client")
@patch("FuncionDescargarDolar.requests.get")
def test_descargar_y_guardar_json(mock_get, mock_s3):
    # Simular respuesta de requests
    fake_json = {"TRM": 4000}
    mock_response = MagicMock()
    mock_response.json.return_value = fake_json
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    # Simular cliente de S3
    mock_s3.put_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

    # Ejecutar la función
    result = descargar_y_guardar_json()

    # Validar respuesta
    assert result["status"] == "ok"
    assert result["file"].startswith("dolar-")
    assert result["file"].endswith(".json")

    # Verificar que se llamó requests.get con la URL correcta
    mock_get.assert_called_once_with(
        "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"
    )

    # Verificar que se subió a S3 con el contenido esperado
    args, kwargs = mock_s3.put_object.call_args
    assert kwargs["Bucket"] == "dolar-raw-parcial1bigdata"
    assert kwargs["ContentType"] == "application/json"
    assert json.loads(kwargs["Body"])== fake_json
