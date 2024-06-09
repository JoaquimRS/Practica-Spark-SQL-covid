import json
import pandas as pd
from typing import List, Optional

def leer_archivo(ruta: Optional[str] = None, es_json: bool = False):
    try:
        if not ruta:
            raise ValueError("La ruta del archivo no puede ser None, debe ser una cadena válida")
        with open(ruta, "r") as archivo:
            contenido = json.load(archivo) if es_json else archivo.read()
        return contenido
    except FileNotFoundError:
        print(f"Error: Archivo no encontrado en '{ruta}'")
    except Exception as error:
        print(f"Error al leer el archivo: {error}")
        raise error

def generar_fechas(fecha: int, cantidad: int = 120) -> List[int]:
    try:
        fecha_str = str(fecha)
        año, mes, día = int(fecha_str[:4]), int(fecha_str[4:6]), int(fecha_str[6:])
        lista_fechas = []

        for _ in range(cantidad):
            día += 1
            if mes in {1, 3, 5, 7, 8, 10, 12}:
                días_en_mes = 31
            elif mes in {4, 6, 9, 11}:
                días_en_mes = 30
            else:
                días_en_mes = 29 if (año % 4 == 0 and año % 100 != 0) or (año % 400 == 0) else 28

            if día > días_en_mes:
                día -= días_en_mes
                mes += 1
                if mes > 12:
                    mes = 1
                    año += 1

            nueva_fecha = int(f"{año}{mes:02d}{día:02d}")
            lista_fechas.append(nueva_fecha)

        return lista_fechas
    except Exception as error:
        print(f"Error al generar fechas: {error}")

def convertir_csv_a_parquet(ruta_csv: Optional[str] = None, delimitador: str = ",") -> bool:
    try:
        if not ruta_csv:
            raise ValueError("La ruta del CSV no puede ser None")
        
        partes_ruta = ruta_csv.split("/")
        ruta_carpeta = "/".join(partes_ruta[:-1])
        nuevo_nombre = partes_ruta[-1].replace(".csv", ".parquet")
        
        pd.read_csv(ruta_csv, delimiter=delimitador).to_parquet(f"{ruta_carpeta}/{nuevo_nombre}")
        print("Transformación completada :)")
        return True
    except Exception as error:
        print(f"Error: {error}")
        return False
