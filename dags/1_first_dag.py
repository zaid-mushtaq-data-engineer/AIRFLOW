from airflow.sdk import dag, task


# define the dag

@dag(
    dag_id='first_dag'
)
def my_dag():
    @task.python
    def task_1():
        print('This is my first task of first dag')

    @task.python
    def task_2():
        print('This is my second task of first dag')

    @task.python
    def task_3():
        print('This is my third task of first dag')

    # defing task dependencies

    first = task_1()
    second = task_2()
    third = task_3()

    first >> second >> third

# initiating the dag

first_dag = my_dag()
