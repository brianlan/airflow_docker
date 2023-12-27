from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


def greet(ti):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    age = ti.xcom_pull(task_ids="get_age", key="age")
    print(f'Catch up! Catch up! '
          f'My name is {first_name} {last_name} and I am {age} years old')


def get_name(ti):
    ti.xcom_push(key="first_name", value="Jerry")
    ti.xcom_push(key="last_name", value="Fridman")

def get_age(ti):
    ti.xcom_push(key="age", value=25)

with DAG(
    dag_id='dag_with_catchup_and_backfill_v2',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2023, 7, 28, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
    )

    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name,
    )

    task3 = PythonOperator(
        task_id="get_age",
        python_callable=get_age,
    )

    [task2, task3] >> task1
