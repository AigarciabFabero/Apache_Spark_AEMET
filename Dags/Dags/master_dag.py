from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='master_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    trigger_aemet_api = TriggerDagRunOperator(
        task_id='trigger_aemet_api',
        trigger_dag_id='aemet_api_dag',
    )

    trigger_spark_etl = TriggerDagRunOperator(
        task_id='trigger_spark_etl',
        trigger_dag_id='spark_etl_dag',
    )

    trigger_sqlite_etl = TriggerDagRunOperator(
        task_id='trigger_sqlite_etl',
        trigger_dag_id='sqlite_dag',
    )

    trigger_visualization = TriggerDagRunOperator(
        task_id='trigger_visualization',
        trigger_dag_id='visualization_dag',
    )

    trigger_aemet_api >> trigger_spark_etl >> trigger_sqlite_etl >> trigger_visualization
