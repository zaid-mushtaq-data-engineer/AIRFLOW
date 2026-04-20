from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

# 2026 special date
special_date = [
    datetime(year=2026, month=1, day=1),
    datetime(year=2026, month=1, day=15),
    datetime(year=2026, month=2, day=14),
    datetime(year=2026, month=3, day=18),
    datetime(year=2026, month=5, day=1),
    datetime(year=2026, month=6, day=2)
]

@dag(
    dag_id="Special_date_dag",
    start_date=datetime(year=2026, month=4, day=1,tz="Asia/Kolkata"),
    schedule=EventsTimetable(special_date),
    is_paused_upon_creation=False,
    catchup=True
)
def Special_date_dag():

    @task
    def print_date(execution_date=None):
        print(f"Special date: {execution_date}")

    print_date()

Special_date_dag()
