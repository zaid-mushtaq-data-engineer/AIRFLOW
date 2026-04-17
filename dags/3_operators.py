from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator 
from datetime import datetime

@dag(
    dag_id='operator_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def my_dag():

    # Python tasks
    @task
    def task_1():
        print('This is my first task of first dag')

    @task
    def task_2():
        print('This is my second task of first dag')

    @task
    def task_3():
        print('This is my third task of first dag')

    # Bash using decorator
    @task.bash
    def task_bash_decorator():
        return "echo 'Hello from @task.bash'"

    # Bash using operator
    task_bash_operator = BashOperator(
        task_id="task_bash_operator",
        bash_command="echo 'Hello from BashOperator'"
    )

    # Instantiate tasks
    first = task_1()
    second = task_2()
    third = task_3()
    fourth = task_bash_decorator()

    # Dependencies
    first >> second >> third >> fourth >> task_bash_operator


# Instantiate DAG
first_dag = my_dag()