import json
import pytest
from unittest.mock import patch, MagicMock
from puntodos import lambda_handler

def test_lambda_handler_logic():
    # Evento simulado de S3
    fake_event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "fake-bucket"},
                    "object": {"key": "fake.json"},
                }
            }
        ]
    }

    # Contenido JSON simulado: lista de [timestamp(ms), valor]
    fake_data = [[1736352000000, 4200.5], [1736438400000, 4210.0]]
    fake_json = json.dumps(fake_data)

    # Mock del cliente de S3
    mock_s3_client = MagicMock()
    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=lambda: fake_json.encode("utf-8"))
    }

    # Mock de cursor MySQL
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("puntodos.s3_client", mock_s3_client), \
         patch("pymysql.connect", return_value=mock_conn):

        result = lambda_handler(fake_event, None)

        # Verificar lógica
        assert result["status"] == "ok"
        assert result["rows_inserted"] == len(fake_data)

        # Asegurar que se llamaron los métodos esperados
        assert mock_cursor.execute.call_count == len(fake_data)
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
