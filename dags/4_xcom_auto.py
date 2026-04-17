from airflow.sdk import dag, task

@dag(dag_id="xcom_auto")
def xcom_auto():

    @task
    def first_task():
        print("Extracting data")
        return {"data": [1,2,3,4,5]}

    @task
    def second_task(data: dict):
        print("Transforming data")
        transformed = [x*x for x in data["data"]]
        return {"transformed_data": transformed}

    @task
    def third_task(data: dict):
        print("Loading data")
        return data["transformed_data"]

    t1 = first_task()
    t2 = second_task(t1)
    t3 = third_task(t2)

    t1 >> t2 >> t3

dag = xcom_auto()