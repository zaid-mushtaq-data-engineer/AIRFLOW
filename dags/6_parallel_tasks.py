from airflow.decorators import dag, task
from datetime import datetime

@dag(dag_id="parallel_dag")
def my_dag():

    @task
    def task_1(**kwargs):
        ti = kwargs["ti"]

        data = {
            "api_result": [1, 2, 3],
            "db_result": [10, 20, 30],
            "s3_result": [100, 200, 300]
        }

        ti.xcom_push(key="data", value=data)

    @task
    def task_2(**kwargs):
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="task_1", key="data")

        result = [x * 2 for x in data["api_result"]]
        ti.xcom_push(key="api_processed", value=result)

    @task
    def task_3(**kwargs):
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="task_1", key="data")

        result = [x * 3 for x in data["db_result"]]
        ti.xcom_push(key="db_processed", value=result)

    @task
    def task_4(**kwargs):
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="task_1", key="data")

        result = [x * 4 for x in data["s3_result"]]
        ti.xcom_push(key="s3_processed", value=result)

    @task
    def task_5(**kwargs):
        ti = kwargs["ti"]

        api = ti.xcom_pull(task_ids="task_2", key="api_processed")
        db = ti.xcom_pull(task_ids="task_3", key="db_processed")
        s3 = ti.xcom_pull(task_ids="task_4", key="s3_processed")

        print("Final Results:")
        print("API:", api)
        print("DB:", db)
        print("S3:", s3)

    t1 = task_1()
    t2 = task_2()
    t3 = task_3()
    t4 = task_4()
    t5 = task_5()

    t1 >> [t2, t3, t4] >> t5

dag = my_dag()