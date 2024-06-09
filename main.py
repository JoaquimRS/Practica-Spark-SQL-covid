import os
import sys
import random
import pandas as pd
import numpy as np
import warnings
from utils import generar_fechas
from generate_cities_countries_csv import crear_csv_ciudades_paises
from get_json_files import realizar_solicitud_get, guardar_en_archivo, API_URL
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    NumericType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType
)

# Constantes
COVID_DATE = '20200501'
FOLDER_PATH = os.getcwd()
SINGLE_DAY = True
DURATION = 120
TOTAL_COUNTRIES = 6


def main():
    # Obtener datos diarios de Covid y guardarlos en un archivo JSON
    covid_data_url = f"{API_URL}/us/daily.json" if not SINGLE_DAY else f"{API_URL}/us/{COVID_DATE}.json"
    response = realizar_solicitud_get(url=covid_data_url)
    guardar_en_archivo(response.text, "procesado/daily_covid", "json")
    
    # Iniciar sesión de Spark
    spark = SparkSession.builder.appName("CovidDataProcessor").getOrCreate()
    
    # Generar CSV de ciudades y países
    crear_csv_ciudades_paises(spark=spark)

    # Leer el archivo JSON con Spark y crear un DataFrame
    json_path = os.path.join(FOLDER_PATH, "procesado/daily_covid.json")
    covid_df = spark.read.option("multiline", True).json(json_path)
    covid_df.createOrReplaceTempView("covid_data")

    # Identificar columnas numéricas
    numeric_columns = [field.name for field in covid_df.schema.fields if isinstance(field.dataType, NumericType)]

    # Definir el esquema del DataFrame final
    final_schema = StructType(
        [
            *[
                StructField(name, covid_df.schema[name].dataType, True)
                for name in numeric_columns + ["dateChecked"]
            ],
            StructField("city_id", IntegerType(), True),
            StructField("factor", FloatType(), True),
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
            StructField("day", StringType(), True),
            StructField("processed", BooleanType(), True)
        ]
    )
    
    try:
        new_rows = []
        
        # Procesar datos para cada país
        for country_index in range(TOTAL_COUNTRIES):
            city = country_index
            data_row = covid_df.select(*numeric_columns, "dateChecked").collect()[0]
            data_dict = data_row.asDict()
            
            # Generar fechas para los próximos 4 meses
            generated_dates = generar_fechas(fecha=data_dict["date"], cantidad=DURATION)
            new_data = {}

            # Procesar cada fecha generada
            for index in range(DURATION):
                factor = random.uniform(1, 1.5)
                for key, value in data_dict.items():
                    generated_date = str(generated_dates[index])
                    year = generated_date[:4]
                    month = generated_date[4:6]
                    day = generated_date[6:]
                    
                    if key == "date":
                        new_data[key] = generated_dates[index]
                    elif key == "dateChecked":
                        new_data[key] = value
                    else:
                        new_data[key] = int(np.floor(value * factor))

                # Crear y añadir nueva fila
                new_data.update({
                    "city_id": city + 1,
                    "factor": factor,
                    "year": year,
                    "month": month,
                    "day": day,
                    "processed": True
                })
                new_rows.append(Row(**new_data))

        # Crear DataFrame de Spark y guardar como archivo Parquet
        final_df = spark.createDataFrame(new_rows, final_schema)
        final_df.createOrReplaceTempView("final_data")
        final_df.toPandas().to_parquet("procesado/processed_covid_data.parquet")
        print("Archivo Parquet guardado con éxito: processed_covid_data.parquet")
    except Exception as e:
        print(f"Error al guardar los datos en formato Parquet: {e}")
    
if __name__ == "__main__":
    main()
