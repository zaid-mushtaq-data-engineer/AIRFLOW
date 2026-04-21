from airflow.sdk import dag,task,asset
import os

@asset(
    schedule="@daily",
    # This is optional but recomended
    uri="opt/airflow/logs/data/data_extract.txt",
    name="fetch_data"
)
def fetch_data(self):
    os.makedirs(os.path.dirname(self.uri),exist_ok=True)
    with open(self.uri,"w") as f:
        f.write(f"Data extracted and stored at {self.uri}\n")
    
    print(f"Data extracted and stored at {self.uri}")


