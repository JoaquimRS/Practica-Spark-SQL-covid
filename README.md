# Proyecto de Spark para Datos de Covid

Hola, soy Joaquim Ribera Soriano y este es mi proyecto de Spark para la recolección y procesamiento de datos de Covid-19. El objetivo de este proyecto es obtener datos diarios de Covid-19, procesarlos utilizando Apache Spark y finalmente mostrarlos en un panel de control interactivo utilizando Power BI.

## Descripción del Proyecto

Este proyecto realiza las siguientes tareas:

1. **Recolección de Datos**: Obtiene datos diarios de Covid-19 desde una API pública.
2. **Procesamiento de Datos**: Utiliza Apache Spark para procesar y transformar los datos.
3. **Almacenamiento de Datos**: Guarda los datos procesados en formato Parquet.
4. **Visualización de Datos**: Los datos procesados se utilizan para crear un panel de control en Power BI.

## Requisitos

Para ejecutar este proyecto, necesitarás tener instalados los siguientes paquetes:

- pandas
- numpy
- pyspark
- pyarrow
- fastparquet
- requests

Puedes instalar estos paquetes utilizando el archivo `requirements.txt` proporcionado.

## Ejecución

Para ejecutar el proyecto, simplemente corre el script `main.py`:
