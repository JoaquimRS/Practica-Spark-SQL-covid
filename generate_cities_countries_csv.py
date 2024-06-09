from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

def crear_csv_ciudades_paises(spark):
    lista_ciudades = [
        Row(
            id=1,
            country_id=1,
            name="Valencia",
            lat="39.47010514497293",
            lng="-0.3766213363909406",
        ),
        Row(
            id=2,
            country_id=2,
            name="Lisboa",
            lat="38.722252",
            lng="-9.139337",
        ),
        Row(
            id=3,
            country_id=3,
            name="Roma",
            lat="41.9027835",
            lng="12.4963655",
        ),
        Row(
            id=4,
            country_id=4,
            name="Berlín",
            lat="52.5200066",
            lng="13.404954",
        ),
        Row(
            id=5,
            country_id=5,
            name="Atenas",
            lat="37.9838096",
            lng="23.7275388",
        ),
        Row(
            id=6,
            country_id=6,
            name="Ámsterdam",
            lat="52.3675734",
            lng="4.9041389",
        ),
    ]

    lista_paises = [
        Row(
            id=1,
            name="España",
        ),
        Row(
            id=2,
            name="Portugal",
        ),
        Row(
            id=3,
            name="Italia",
        ),
        Row(id=4, name="Alemania"),
        Row(id=5, name="Grecia"),
        Row(
            id=6,
            name="Países Bajos",
        ),
    ]

    df_ciudades = spark.createDataFrame(
        lista_ciudades,
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("country_id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("lat", StringType(), True),
                StructField("lng", StringType(), True),
            ]
        ),
    )

    df_paises = spark.createDataFrame(
        lista_paises,
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        ),
    )

    df_ciudades.toPandas().to_csv("procesado/ciudades.csv", index=False)
    df_paises.toPandas().to_csv("procesado/paises.csv", index=False)
