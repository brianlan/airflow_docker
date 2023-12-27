from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='dag_for_unittest_another',
    default_args=default_args,
    description='dag_for_unittest_another',
    start_date=datetime(2023, 12, 16, 1),
    schedule_interval=None,
    tags=["unit-testing"]
) as dag:
    @task(multiple_outputs=True)
    def init(ti=None, dag_run=None, **kwargs):
        return dict(**dag_run.conf)

    @task(task_id="begin")
    def begin(ti=None, dag_run=None, **kwargs):
        print(ti.xcom_pull(key="scene_id"))

    @task(task_id="middle")
    def middle():
        time.sleep(5)
        print("I am the second task, will be running after task1")
    
    @task_group(group_id='tg')
    def tg():
        t1 = EmptyOperator(task_id='inner_1')
        t2 = EmptyOperator(task_id='inner_2')

        t1 >> t2

    @task(task_id="end")
    def end():
        print("end of the dag")
    
    init() >> begin() >> middle() >> tg() >> end()

if __name__ == "__main__":
    dag.test({
        "scene_id": "20231220_1101",
    })
