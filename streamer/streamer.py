import pandas as pd
import httpx
import time
import json
import os

API_URL = "http://lakehouse:8000/ingest/context"

def wait_for_api():
    print("Waiting for Lakehouse API to wake up...")
    while True:
        try:
            httpx.get("http://lakehouse:8000/docs")
            break
        except Exception:
            time.sleep(2)

def sync_context_files():
    files_to_sync = {
        "patients": "/app/data/MimicPatient_top50.csv",
        "encounters": "/app/data/MimicEncounterICU_top50.csv",
        "conditions": "/app/data/MimicCondition_top50.csv"
    }

    for table, file_path in files_to_sync.items():
        if not os.path.exists(file_path):
            print(f"Skipping {table}: File not found at {file_path}")
            continue

        print(f"Syncing {table} context...")
        df = pd.read_csv(file_path)

        json_records = df.to_json(orient='records', date_format='iso')
        records = json.loads(json_records)
        
        for record in records:
            response = httpx.post(f"{API_URL}/{table}", json=record)
            if response.status_code != 200:
                print(f"Error sending record: {response.text}")
        
        print(f"Successfully synced {len(records)} rows to bronze_{table}")

if __name__ == "__main__":
    wait_for_api()
    sync_context_files()
    print("Context Sync Complete. System ready for Chronological Stream.")