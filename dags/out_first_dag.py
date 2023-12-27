from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='our_first_dag_v3',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2023, 8, 1, 1),
    schedule_interval='@daily',
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello World!"'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "I am the second task, will be running after task1"'
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='echo "I am the third task, will be running almost the same time as task2"'
    )

    task1 >> [task2, task3]
