import datetime as dt

from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator)

dag = DAG(
    dag_id="my_second_daggie",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 10, 1),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)


def print_exec_date(**context):
    print(context["execution_date"])


get_data = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_to_gcs",
    postgres_conn_id="my_database_connection",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket='airflow_training_bucket',
    filename='land_registry_price_paid_uk/{{ ds }}/result.json',
    dag=dag
)

my_task = PythonOperator(
    task_id="task_name", python_callable=print_exec_date, provide_context=True, dag=dag
)

create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id='airflowbolcom-20165e4959a78c1d',
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)

comp_aggregate = DataProcPySparkOperator(
    task_id='compute_aggregates',
    main='gs://europe-west1-training-airfl-159310f1-bucket/other/build_statistics_simple.py',
    cluster_name='analyse-pricing-{{ ds }}',
    arguments=["{{ ds }}"],
    dag=dag,
)

del_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id='airflowbolcom-20165e4959a78c1d',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

TABLE_THANG = 'airflowbolcom-20165e4959a78c1d:yolo.piet${{ ds_nodash }}'
into_BQ = GoogleCloudStorageToBigQueryOperator(
    task_id="stuff_to_BQ",
    bucket='airflow_training_bucket',
    source_objects=['average_prices/transfer_date={{ ds }}/*.parquet'],
    destination_project_dataset_table=TABLE_THANG,
    source_format='PARQUET',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

get_data >> create_cluster >> comp_aggregate >> del_cluster >> into_BQ
