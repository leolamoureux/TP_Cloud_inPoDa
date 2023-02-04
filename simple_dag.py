from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

def print_hello_world():
    print("Hello World!")

end_task = PythonOperator(
    task_id='end',
    python_callable=print_hello_world,
    dag=dag,
)

start_task >> end_task
