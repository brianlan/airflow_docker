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
    dag_id='dense_map_recon',
    default_args=default_args,
    description='dense_map_recon',
    start_date=datetime(2023, 12, 16, 1),
    schedule_interval=None,
    tags=['dry_run']
) as dag:
    @task(multiple_outputs=True)
    def prepare_params(ti=None, dag_run=None, **kwargs):
        return dict(**dag_run.conf)

    @task
    def do_dense_reconstruction():
        time.sleep(5)
        print("end of the dag")

    @task
    def final_task():
        time.sleep(5)
        print("end of the dag")
    
    prepare_params() >> do_dense_reconstruction() >> final_task()

if __name__ == "__main__":
    dag.test({
        "scene_id": "20231220_1101",
        "split_id": "0"
    })
