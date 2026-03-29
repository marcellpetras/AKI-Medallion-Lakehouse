import os

DB_PATH = os.getenv("DB_PATH", "/app/lakehouse_db/aki_lakehouse.db")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")

TABLES_TO_IMPORT = {
    "bronze_patients": "MimicPatient_top50.csv",
    "bronze_encounters": "MimicEncounterICU_top50.csv",
    "bronze_conditions": "MimicCondition_top50.csv",
    "bronze_chartevents": "MimicObservationChartevents_top50.csv",
    "bronze_labevents": "MimicObservationLabevents_top50.csv",
    "bronze_outputevents": "MimicObservationOutputevents_top50.csv",
    "bronze_medications": "MimicMedicationAdministrationICU_top50.csv"
}
