import duckdb
import os
from pipeline.config import DB_PATH, DATA_DIR, TABLES_TO_IMPORT

def build_bronze_layer():
    print("Starting Medallion Pipeline: Bronze Phase")
    
    # Ensure the database directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = duckdb.connect(DB_PATH)
    
    for table_name, data_file in TABLES_TO_IMPORT.items():
        file_path = os.path.join(DATA_DIR, data_file)
        
        if not os.path.exists(file_path):
            print(f"Warning: {data_file} not found. Skipping {table_name}.")
            continue
            
        print(f"Importing {data_file} into {table_name}...")
        
        # DuckDB handles JSON and compressed files automatically
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_json_auto('{file_path}', sample_size=-1)
        """)
        
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Created {table_name} with {row_count} rows.")

    conn.close()
    print("Bronze Phase Complete. All raw data is materialized.")
