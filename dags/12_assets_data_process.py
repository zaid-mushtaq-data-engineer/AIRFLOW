from airflow.sdk import dag,task,asset
import os
from Asset_11_assets_data_fetch import fetch_data

@asset(
    schedule=fetch_data,
    # This is optional but recomended
    uri="opt/airflow/logs/data/data_processed.txt",
    name="fetch_process_data"
)
def fetch_process_data(self):
    os.makedirs(os.path.dirname(self.uri),exist_ok=True)
    with open(self.uri,"w") as f:
        f.write(f"Data processed and stored at {self.uri}\n")
    
    print(f"Data processed and stored at {self.uri}")





