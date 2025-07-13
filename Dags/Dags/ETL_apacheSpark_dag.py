from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql.functions import split, avg, col, translate
from pyspark.sql import SparkSession
import findspark
import os

def spark_etl():
    # Se define la ruta donde se guardará el archivo CSV
    path = '/home/aitor/airflow/dags/API_AEMET/'

    findspark.init()

    spark = (
        SparkSession.builder
        .appName("AEMET_Spark")
        .master("local[10]")
        .config("spark.driver.memory", "4g")  # Configura la memoria del driver a 7 gigabytes
        .config("spark.driver.cores", "3")  # Configura el número de núcleos utilizados a 5
        .config("spark.executor.instances", "2")  # Configura el número de ejecutores a 5
        .getOrCreate()
    )
    spark

    df = spark.read.csv(path + 'dataset_1991_to_2023.csv', header=True, inferSchema=True)

    def ETL1(df):
        # Eliminamos las columnas y filas que no resultan de interes o nos proporcionan información repetida
        df = df.filter(df["fecha"] != '2024-01-01')
        df = df.drop('nombre_y','provincia_y','altitud_y','hrMedia','horaPresMax','horaPresMin','hrMax','hrMin','horaHRMin','horaHRMax','horaracha','indsinop','dir')
        df = df.withColumnRenamed('nombre_x','nombre').withColumnRenamed('provincia_x','provincia').withColumnRenamed('altitud_x','altitud')
        df = df.dropna(subset=['tmed'])

        # Se definen 3 nuevas columnas
        df = df.withColumn("año", split(df["fecha"], "-")[0])
        df = df.withColumn("año", split(df["fecha"], "-")[0].cast("integer"))
        df = df.filter(col("año") >= 2003)
        #df = df.withColumn("mes", split(df["fecha"], "-")[1])
        #df = df.withColumn("dia", split(df["fecha"], "-")[2])

        # Cambiamos el tipo de datos de algunas columnas a float
        for column in ["tmed", "prec", "tmin", "tmax", "velmedia", "racha", "presMax", "presMin"]:
            df = df.withColumn(column, translate(col(column), ",", "."))
            df = df.withColumn(column, col(column).cast("float"))

        # Ordenamod el dataframe por fecha
        df = df.orderBy('fecha')
        
        return df

    def ETL2(df):
        # Se eliminan más columnas que no resultan de interés
        
        df = df.drop('presMax', 'presMin', 'sol', 'velmedia', 'indicativo', 'horatmin','horatmax','altitud', 'latitud', 'longitud', 'racha')
        ordered_columns = ['fecha', 'comunidad', 'provincia', 'nombre', 'tmed', 'tmin', 'tmax', 'prec']
        df = df.select(*ordered_columns)

        # Calculamos la temperatura y precipitación promedio de cada mes y año a nivel nacional, sin distinción de estaciones
    #     mes_año = ["tmed_mes_año", "prec_mes_año", "tmin_mes_año", "tmax_mes_año"]
    #     tp = ["tmed", "prec", "tmin", "tmax"]
    #     for i in range (0,4):
    #             df_aux = df.groupBy("año", "mes").agg(avg(tp[i]).alias(mes_año[i])).withColumn(mes_año[i], col(mes_año[i]).cast("float"))
    #             df = df.join(df_aux, ["año", "mes"], "inner")
        
        return df

    df_after_ETL1 = ETL1(df)
    df_after_ETL2 = ETL2(df_after_ETL1)

    filename = 'dataset_after_ETL.csv'
    full_path = os.path.join(path, filename)
    df_after_ETL2.toPandas().to_csv(full_path, index=False, header=True)

    spark.stop()

with DAG(
    dag_id='spark_etl_dag',
    schedule=None,
    catchup=False,
    start_date=datetime(2023, 1, 1),
) as dag:
    etl_task = PythonOperator(
        task_id='spark_etl_task',
        python_callable=spark_etl,
    )