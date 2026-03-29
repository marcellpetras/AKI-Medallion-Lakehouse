import duckdb
import os
from pipeline.config import DB_PATH, DATA_DIR, TABLES_TO_IMPORT

def build_bronze_layer():
    print("Starting Medallion Pipeline: Bronze Phase")
    
    # Ensure the database directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = duckdb.connect(DB_PATH)
    
    for table_name, csv_file in TABLES_TO_IMPORT.items():
        csv_path = os.path.join(DATA_DIR, csv_file)
        
        if not os.path.exists(csv_path):
            print(f"Warning: {csv_file} not found. Skipping {table_name}.")
            continue
            
        print(f"Importing {csv_file} into {table_name}...")
        
        # DuckDB handles CSV headers and type inference automatically
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{csv_path}')
        """)
        
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Created {table_name} with {row_count} rows.")

    conn.close()
    print("Bronze Phase Complete. All raw data is materialized.")
