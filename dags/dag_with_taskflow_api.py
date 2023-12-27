from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {"owner": "coder2j", "retries": 5, "retry_delay": timedelta(minutes=2)}


@dag(
    dag_id="dag_with_taskflow_api_v2",
    default_args=default_args,
    description="This is our first dag that write using taskflow api",
    start_date=datetime(2023, 8, 1, 1),
    schedule_interval="@daily",
)
def hello_world_etl():
    @task(multiple_outputs=True)
    def get_name():
        return {"first_name": "Jerry", "last_name": "Fridman"}

    @task()
    def get_age():
        return {"age": 25}

    @task()
    def greet(first_name, last_name, age):
        print(
            f'Hello World! I am a python operator! '
            f'My name is {first_name} {last_name} and I am {age["age"]} years old'
        )

    name_dict = get_name()
    age = get_age()
    greet(name_dict['first_name'], name_dict['last_name'], age)

greet_dag = hello_world_etl()