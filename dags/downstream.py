from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.decorators import task


default_args = {
    'owner': 'airflow',
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='downstream',
    default_args=default_args,
    description='downstream',
    start_date=datetime(2023, 12, 16, 1),
    schedule_interval=None,
    tags=["unit-testing"]
) as dag:
    @task(multiple_outputs=True)
    def hello(ti=None, dag_run=None, **kwargs):
        return dict(**dag_run.conf)

    @task
    def random_fail():
        number = random.random()
        if number > 0.5:
            raise ValueError("On purpose fail the dag.")
        print(f"random number is {number}")

    @task
    def world():
        print("end of the dag")
    
    hello() >> random_fail() >> world()

if __name__ == "__main__":
    dag.test({
        "scene_id": "20231220_1101",
    })
