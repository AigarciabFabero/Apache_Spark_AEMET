from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sqlite3

def sqlite():
    # Se define la ruta donde se guardará el archivo CSV
    path = '/home/aitor/airflow/dags/API_AEMET/'

    df_SQlite = pd.read_csv(path + 'dataset_after_ETL.csv')

    def consulta_datos_historicos(conn):
        query = """SELECT fecha, comunidad, provincia, tmed, prec, tmin, tmax FROM my_table"""

        df_historico = pd.read_sql_query(query, conn)
        return df_historico
    
    conn = sqlite3.connect('my_database.db')
    df_SQlite.to_sql('my_table', conn, if_exists='replace', index=False)
    df_historico = consulta_datos_historicos(conn)
    conn.close()
    df_historico.to_csv(path + 'historico.csv', index=False)

with DAG(
    dag_id='sqlite_dag',
    schedule=None,
    catchup=False,
    start_date=datetime(2023, 1, 1),
) as dag:
    etl_task = PythonOperator(
        task_id='sqlite_task',
        python_callable=sqlite,
    )