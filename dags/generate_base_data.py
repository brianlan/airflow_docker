from datetime import datetime, timedelta
import time
import random

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
    dag_id='generate_base_data',
    default_args=default_args,
    description='generate_base_data',
    start_date=datetime(2023, 12, 16, 1),
    schedule_interval=None,
    tags=['dry_run']
) as dag:
    @task(multiple_outputs=True)
    def prepare_params(ti=None, dag_run=None, **kwargs):
        return dict(**dag_run.conf)

    @task
    def starting_task(ti=None, dag_run=None, **kwargs):
        print(ti.xcom_pull(key="scene_id"))
        time.sleep(5)

    @task
    def generate_image():
        time.sleep(5)
        print("I am the second task, will be running after task1")

    @task_group(group_id='rosbag')
    def rosbag():
        time.sleep(5)
        t1 = EmptyOperator(task_id='ros_1')
        t2 = EmptyOperator(task_id='ros_2')

        t1 >> t2

    @task
    def generate_split_map():
        return [{"S3_SPLIT_MAP_ID": i} for i in range(random.randint(2, 5))]

    @task
    def final_task():
        time.sleep(5)
        print("end of the dag")
    
    prepare_params() >> starting_task() >> generate_image() >> rosbag() >> generate_split_map() >> final_task()

if __name__ == "__main__":
    dag.test({
        "scene_id": "20231220_1101",
    })
