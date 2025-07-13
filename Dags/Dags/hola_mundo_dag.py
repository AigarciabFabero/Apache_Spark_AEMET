from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def hola_mundo():
    print("Hola mundo")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='hola_mundo_dag',
    default_args=default_args,
    schedule='*/30 * * * *',  # Ejecutar cada 30 segundos
    catchup=False
) as dag:
    
    tarea_hola_mundo = PythonOperator(
        task_id='hola_mundo_task',
        python_callable=hola_mundo,
    )


