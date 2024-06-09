import requests

# URL de la API de CovidTracking
API_URL = "https://api.covidtracking.com/v1"

# Función para realizar una solicitud GET
def realizar_solicitud_get(url: str, parametros=None):
    respuesta = requests.get(url, params=parametros)
    return respuesta

def guardar_en_archivo(
    contenido: str | None = None, nombre_salida: str = "salida", extension: str = "json"
):
    try:
        if extension.startswith("."):
            extension = extension[1:]

        with open(f"{nombre_salida}.{extension}", "w") as archivo:
            if contenido is None:
                raise BaseException("El contenido no puede ser de tipo None, debe ser str.")
            else:
                archivo.write(contenido)
    except Exception as error:
        print(f"Error al intentar crear un archivo: {error}")
