from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    'owner': 'airflow',
}


with DAG(
    dag_id='dag_run_without_task',
    default_args=default_args,
    description='dag_run_without_task',
    start_date=datetime(2023, 12, 16, 1),
    schedule_interval=None,
    tags=["unit-testing"]
) as dag:
    @task(multiple_outputs=True)
    def prepare_params(ti=None, dag_run=None, **kwargs):
        return dict(**dag_run.conf)

    @task
    def task_xxx():
        print("task_xxx")
        return "task_xxx"

    @task
    def do_nothing():
        print("end of the dag")
    
    prepare_params() >> task_xxx() >> do_nothing()

if __name__ == "__main__":
    dag.test({
        "scene_id": "20231220_1101",
    })
