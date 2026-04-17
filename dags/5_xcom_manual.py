from airflow.sdk import dag, task

@dag(dag_id="xcom_manual")
def xcom_manual():

    @task
    def first_task(**kwargs):
        ti = kwargs["ti"]
        print("Extracting data")

        data = {"data": [1,2,3,4,5]}

        ti.xcom_push(key="my_data", value=data)

    @task
    def second_task(**kwargs):
        ti = kwargs["ti"]

        data = ti.xcom_pull(
            task_ids="first_task",
            key="my_data"
        )

        print("Transforming data")

        transformed = [x*x for x in data["data"]]
        return {"transformed_data": transformed}

    @task
    def third_task(data: dict):
        print("Loading data")
        return data["transformed_data"]

    t1 = first_task()
    t2 = second_task()
    t3 = third_task(t2)

    t1 >> t2 >> t3

dag = xcom_manual()