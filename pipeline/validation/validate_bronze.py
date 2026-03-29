import duckdb
import pandas as pd
import os
import sys

# Define default local DB path if script is run outside container
LOCAL_DB_PATH = os.path.join(os.path.dirname(__file__), "../../lakehouse_db/aki_lakehouse.db")
DB_PATH = os.getenv("DB_PATH", os.path.abspath(LOCAL_DB_PATH))
OUTPUT_FILE = "bronze_validation_report.xlsx"

def validate_bronze_layers():
    if not os.path.exists(DB_PATH):
        print("Error: Database not found at " + DB_PATH + ". Run docker compose up first.")
        return

    print("Connecting to " + DB_PATH + "...")
    conn = duckdb.connect(DB_PATH)

    # Get a list of all bronze tables
    query = "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'bronze_%'"
    tables = conn.execute(query).fetchall()
    
    if not tables:
        print("No bronze tables found in the database.")
        conn.close()
        return

    print("Exporting " + str(len(tables)) + " tables to Excel...")

    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        for (table_name,) in tables:
            print("Processing " + table_name + "...")
            
            df = conn.execute(f"SELECT * FROM {table_name}").df()
            
            # Excel does not support datetimes with timezones
            for col in df.select_dtypes(include=['datetimetz', 'datetime64[ns, UTC]']).columns:
                df[col] = df[col].dt.tz_localize(None)
            
            df.to_excel(writer, sheet_name=table_name, index=False)

    conn.close()
    print("Validation Complete. File saved as: " + OUTPUT_FILE)

if __name__ == "__main__":
    validate_bronze_layers()
