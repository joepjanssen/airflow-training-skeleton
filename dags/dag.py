import datetime as dt

from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


dag = DAG(
    dag_id="my_second_daggie",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 8, 1),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)


def print_exec_date(**context):
    print(context["execution_date"])


store_some_stuff = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_to_gcs",
    postgres_conn_id="my_database_connection",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket='airflow_training_bucket',
    filename='my_real_estate_result',
    dag=dag
)


my_task = PythonOperator(
    task_id="task_name", python_callable=print_exec_date, provide_context=True, dag=dag
)
