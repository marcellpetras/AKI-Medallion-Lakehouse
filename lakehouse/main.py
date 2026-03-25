from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
import duckdb
import os
import uvicorn

DB_PATH = "/app/database/aki_lakehouse.db"

def init_db():
    print("Initializing Database...")
    conn = duckdb.connect(DB_PATH)
    # Create tables for master data
    conn.execute("CREATE TABLE IF NOT EXISTS bronze_patients AS SELECT * FROM '/app/data/MimicPatient_top50.csv' WHERE 1=0")
    conn.execute("CREATE TABLE IF NOT EXISTS bronze_encounters AS SELECT * FROM '/app/data/MimicEncounterICU_top50.csv' WHERE 1=0")
    conn.execute("CREATE TABLE IF NOT EXISTS bronze_conditions AS SELECT * FROM '/app/data/MimicCondition_top50.csv' WHERE 1=0")
    # Table for high-frequency streaming
    conn.execute("CREATE TABLE IF NOT EXISTS bronze_observations (raw_data JSON, ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    conn.close()
    print("Database Initialized Successfully.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(lifespan=lifespan)

@app.post("/ingest/context/{table_name}")
async def ingest_context(table_name: str, request: Request):
    data = await request.json()
    conn = duckdb.connect(DB_PATH)
    
    # Use parameterized queries to handle strings/nulls safely
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["?" for _ in data])
    sql = f"INSERT INTO bronze_{table_name} ({columns}) VALUES ({placeholders})"
    
    conn.execute(sql, list(data.values()))
    conn.close()
    return {"status": "success"}

# Keep the container alive
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)