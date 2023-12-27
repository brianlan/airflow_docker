from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.decorators import task


default_args = {
    'owner': 'airflow',
}


with DAG(
    dag_id='dag_split_map_generator',
    default_args=default_args,
    description='dag_split_map_generator',
    start_date=datetime(2023, 12, 16, 1),
    schedule_interval=None,
    tags=["unit-testing"]
) as dag:
    @task(multiple_outputs=True)
    def hello(ti=None, dag_run=None, **kwargs):
        return dict(**dag_run.conf)

    @task
    def generate_split_map():
        return [{"S3_SPLIT_MAP_ID": i} for i in range(random.randint(2, 5))]

    @task(multiple_outputs=True)
    def generate_xcom_key_value_pairs():
        return {"p": "past", "f": "future"}

    @task
    def world():
        print("end of the dag")
    
    hello() >> generate_split_map() >> generate_xcom_key_value_pairs() >> world()

if __name__ == "__main__":
    dag.test({
        "scene_id": "20231220_1101",
        "batch_id":"baidu_integration_test",
    })
