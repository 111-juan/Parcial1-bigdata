from FuncionDescargarDolar import descargar_y_guardar_json


def f(event,context):
    print("lambda ejecutado")
    dolar = descargar_y_guardar_json()
    return dolar