from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.ingest import load_all_data
from etl.transform import transform_data
from etl.load import save_all_data
from models.attribution import linear_attribution, time_decay_attribution


def run_ingest():
    return load_all_data()


def run_transform(data):
    return transform_data(data)


def run_load(data):
    save_all_data(data)


def run_attribution(data):
    for file_name, df in data.items():
        if 'purchase_amount' in df.columns:
            df = linear_attribution(df)
            df = time_decay_attribution(df)
    save_all_data(data)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mrip_dag',
    default_args=default_args,
    description='A DAG for the Marketing ROI Intelligence Platform',
    schedule_interval=timedelta(days=1),
)

ingest_task = PythonOperator(
    task_id='ingest_task',
    python_callable=run_ingest,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=run_transform,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="ingest_task") }}'},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=run_load,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="transform_task") }}'},
    dag=dag,
)

attribution_task = PythonOperator(
    task_id='attribution_task',
    python_callable=run_attribution,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="transform_task") }}'},
    dag=dag,
)

ingest_task >> transform_task >> [load_task, attribution_task]
