import duckdb
import pandas as pd
import os

DB_PATH = "./lakehouse_db/aki_lakehouse.db"
EXCEL_PATH = "aki_lakehouse_export.xlsx"

def export_db_to_excel():
    print(f"Connecting to {DB_PATH}...\n")
    try:
        conn = duckdb.connect(DB_PATH)
    except Exception as e:
        print(f"Failed to connect: {e}")
        return

    tables = ["bronze_patients", "bronze_encounters", "bronze_conditions"]
    
    print(f"Initializing Excel Export to '{EXCEL_PATH}'...")
    
    # Use pandas ExcelWriter to write multiple DataFrames to multiple sheets
    with pd.ExcelWriter(EXCEL_PATH, engine='openpyxl') as writer:
        for table in tables:
            print(f" --> Exporting table '{table}' to tab...")
            try:
                # Query all data from table directly into a pandas dataframe using duckdb execute
                df = conn.execute(f"SELECT * FROM {table}").df()
                
                # Write logic: Because FHIR data contains deeply nested dictionaries/lists, 
                # Excel cannot handle them as raw python objects. 
                # We need to explicitly cast python objects (like lists/dicts) to string format.
                for col in df.columns:
                    # check if the column holds object types (dicts, lists, missing values usually end up here)
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype(str)
                
                # Write context string representation to the named sheet
                df.to_excel(writer, sheet_name=table, index=False)
                
            except Exception as e:
                print(f"Error querying/exporting {table}: {e}")

    conn.close()
    print(f"\nSuccess! Open '{EXCEL_PATH}' to view your tabs.")

if __name__ == "__main__":
    export_db_to_excel()
