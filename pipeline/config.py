import os

DB_PATH = os.getenv("DB_PATH", "/app/lakehouse_db/aki_lakehouse.db")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")

TABLES_TO_IMPORT = {
    "bronze_patients": "MimicPatient.ndjson",
    "bronze_encounters": "MimicEncounterICU.ndjson.gz",
    "bronze_conditions": "MimicCondition.ndjson",
    "bronze_chartevents": "MimicObservationChartevents.ndjson.gz",
    "bronze_labevents": "MimicObservationLabevents.ndjson.gz",
    "bronze_outputevents": "MimicObservationOutputevents.ndjson",
    "bronze_medications": "MimicMedicationAdministrationICU.ndjson.gz"
}
