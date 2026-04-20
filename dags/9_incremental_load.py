from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    dag_id='incremental_load_dag',
    schedule=CronDataIntervalTimetable("@daily", timezone="Asia/Kolkata"),
    start_date=datetime(2026, 4, 15, tz="Asia/Kolkata"),
    end_date=datetime(2026, 4, 20, tz="Asia/Kolkata"),
    catchup=True,
    is_paused_upon_creation=False
)
def incremental_load_dag_python():

    @task
    def interval_data_fetch(**context):
        start = context["data_interval_start"]
        end = context["data_interval_end"]

        print(f"Fetching data for interval: {start} to {end}")

    @task.bash
    def incremental_load_dag_bash():
        return "echo 'Processing incremental data from {{ data_interval_start }} to {{ data_interval_end }}'"

    t1 = interval_data_fetch()
    t2 = incremental_load_dag_bash()

    t1 >> t2


incremental_load_dag_python()
