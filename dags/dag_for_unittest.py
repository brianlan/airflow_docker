from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='dag_for_unittest',
    default_args=default_args,
    description='dag_for_unittest',
    start_date=datetime(2023, 12, 16, 1),
    schedule_interval=None,
    tags=["unit-testing"]
) as dag:
    @task(multiple_outputs=True)
    def prepare_params(ti=None, dag_run=None, **kwargs):
        return dict(**dag_run.conf)

    @task(task_id="starting_task")
    def starting_task(ti=None, dag_run=None, **kwargs):
        print(ti.xcom_pull(key="scene_id"))

    @task(task_id="generate_image")
    def generate_image():
        time.sleep(5)
        print("I am the second task, will be running after task1")
    
    @task_group(group_id='fisheye')
    def fisheye():
        t1 = EmptyOperator(task_id='task_inside_1')
        t2 = EmptyOperator(task_id='task_inside_2')

        t1 >> t2

    @task(task_id="final_task")
    def final_task():
        print("end of the dag")
    
    prepare_params() >> starting_task() >> generate_image() >> fisheye() >> final_task()

if __name__ == "__main__":
    dag.test({
        "scene_id": "20231220_1101",
    })
