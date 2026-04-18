from airflow.sdk import dag, task
from pendulum import datetime,duration
from airflow.timetables.trigger import CronTriggerTimetable,DeltaTriggerTimetable

# USING PRECIST SCHEDULE 
# @dag(
#     dag_id="Scheduling_dag",
#     start_date=datetime(year=2026, month=4, day=1,tz="Asia/Kolkata"),
#     schedule="@daily",
#     is_paused_upon_creation=False,
#     catchup=True
# )

# using cron syntax logic
# @dag(
#     dag_id="Scheduling_dag",
#     start_date=datetime(year=2026, month=4, day=1,tz="Asia/Kolkata"),
#     schedule=CronTriggerTimetable("0 0 * * MON-FRI",timezone="Asia/Kolkata"),
#     is_paused_upon_creation=False,
#     catchup=True
# )


@dag(
    dag_id="Scheduling_dag",
    start_date=datetime(year=2026, month=4, day=1,tz="Asia/Kolkata"),
    schedule=DeltaTriggerTimetable(duration(days=3)),
    is_paused_upon_creation=False,
    catchup=True
) 
def Scheduling_dag():

    # -----------------------------
    # Step 1: Extract Data
    # -----------------------------
    @task
    def extract(ti=None):
        data = {
            "api_result": [1, 2, 3],
            "db_result": [10, 20, 30],
            "s3_result": [100, 200, 300],
            "weekend_flag": False
        }
        ti.xcom_push(key="data", value=data)

    # -----------------------------
    # Step 2: Parallel Processing
    # -----------------------------
    @task
    def process_api(ti=None):
        data = ti.xcom_pull(task_ids="extract", key="data")
        result = [x * 2 for x in data["api_result"]]
        ti.xcom_push(key="api_processed", value=result)

    @task
    def process_db(ti=None):
        data = ti.xcom_pull(task_ids="extract", key="data")
        result = [x * 3 for x in data["db_result"]]
        ti.xcom_push(key="db_processed", value=result)

    @task
    def process_s3(ti=None):
        data = ti.xcom_pull(task_ids="extract", key="data")
        result = [x * 4 for x in data["s3_result"]]
        ti.xcom_push(key="s3_processed", value=result)

    # -----------------------------
    # Step 3: Decision Logic
    # -----------------------------
    @task.branch
    def decide(ti=None):
        data = ti.xcom_pull(task_ids="extract", key="data")

        if data["weekend_flag"]:
            return "skip_load"
        else:
            return "load_data"

    # -----------------------------
    # Step 4A: Load Path
    # -----------------------------
    @task
    def load_data(ti=None):
        api = ti.xcom_pull(task_ids="process_api", key="api_processed")
        db = ti.xcom_pull(task_ids="process_db", key="db_processed")
        s3 = ti.xcom_pull(task_ids="process_s3", key="s3_processed")

        print("Loading data:")
        print("API:", api)
        print("DB:", db)
        print("S3:", s3)

    # -----------------------------
    # Step 4B: Skip Path
    # -----------------------------
    @task.bash
    def skip_load():
        return "echo 'Skipping load due to weekend'"

    # -----------------------------
    # DAG Execution
    # -----------------------------
    t1 = extract()

    t2 = process_api()
    t3 = process_db()
    t4 = process_s3()

    decision = decide()

    load = load_data()
    skip = skip_load()

    # Correct dependency structure
    t1 >> [t2, t3, t4]
    t1 >> decision
    [t2, t3, t4] >> decision
    decision >> [load, skip]


# Instantiate DAG
dag = Scheduling_dag()

