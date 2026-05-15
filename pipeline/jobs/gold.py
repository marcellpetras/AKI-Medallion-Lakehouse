import duckdb
import os
from pipeline.config import DB_PATH

def build_gold_layer():
    print("Starting Medallion Pipeline: Gold Phase")
    
    conn = duckdb.connect(DB_PATH)
    
    # ---------------------------------------------------------
    # 1. gold_hourly_skeleton
    # ---------------------------------------------------------
    # Create a row for every hour of every stay to anchor our features.
    print("Creating hourly skeleton...")
    conn.execute("""
        CREATE OR REPLACE TABLE gold_hourly_skeleton AS 
        SELECT 
            stay_id,
            patient_id,
            generate_series AS hr_timestamp,
            -- Calculate hour index from admission (0, 1, 2...)
            date_diff('hour', intime, generate_series) AS hr_index
        FROM (
            SELECT 
                stay_id, 
                patient_id, 
                date_trunc('hour', intime) as intime, 
                date_trunc('hour', outtime) as outtime 
            FROM silver_stays
        ) AS stays, 
        -- Generate one row per hour
        generate_series(stays.intime, stays.outtime, interval '1 hour');
    """)

    # ---------------------------------------------------------
    # 2. gold_features_step
    # ---------------------------------------------------------
    # Aggregate vitals, sum urine, and prepare labs for forward-filling.
    print("Aggregating features and shifting targets...")
    conn.execute("""
        CREATE OR REPLACE TABLE gold_features_step AS 
        WITH hourly_vitals AS (
            SELECT 
                stay_id,
                date_trunc('hour', charttime) AS hr_timestamp,
                AVG(valuenum) FILTER (WHERE vital_type = 'heart_rate') AS heart_rate,
                AVG(valuenum) FILTER (WHERE vital_type = 'map') AS map,
                AVG(valuenum) FILTER (WHERE vital_type = 'bun') AS bun
            FROM silver_vitals
            GROUP BY 1, 2
        ),
        hourly_uo AS (
            SELECT 
                stay_id,
                date_trunc('hour', charttime) AS hr_timestamp,
                SUM(urine_volume) AS uo_hourly
            FROM silver_urine_output
            GROUP BY 1, 2
        ),
        hourly_labs AS (
            SELECT 
                stay_id,
                date_trunc('hour', charttime) AS hr_timestamp,
                AVG(current_scr) AS creatinine,
                -- Capture the AKI flag at the exact moment it was recorded
                MAX(CAST(is_aki AS INTEGER)) AS is_aki_event
            FROM silver_kdigo_labels
            GROUP BY 1, 2
        )
        SELECT 
            s.*,
            v.heart_rate,
            v.map,
            v.bun,
            u.uo_hourly,
            l.creatinine,
            COALESCE(l.is_aki_event, 0) AS is_aki_now
        FROM gold_hourly_skeleton s
        LEFT JOIN hourly_vitals v ON s.stay_id = v.stay_id AND s.hr_timestamp = v.hr_timestamp
        LEFT JOIN hourly_uo u ON s.stay_id = u.stay_id AND s.hr_timestamp = u.hr_timestamp
        LEFT JOIN hourly_labs l ON s.stay_id = l.stay_id AND s.hr_timestamp = l.hr_timestamp;
    """)

    # ---------------------------------------------------------
    # 3. gold_hourly_clinical
    # ---------------------------------------------------------
    # Forward-fill sparse data and create the 6-hour look-ahead target.
    print("Finalizing Gold Table (Forward-filling and Target Shifting)...")
    conn.execute("""
        CREATE OR REPLACE TABLE gold_hourly_clinical AS 
        WITH filled_data AS (
            SELECT 
                stay_id,
                patient_id,
                hr_index,
                hr_timestamp,
                -- Forward fill labs using Window Functions (Last Value Ignore Nulls)
                LAST_VALUE(creatinine IGNORE NULLS) OVER (PARTITION BY stay_id ORDER BY hr_timestamp) AS creatinine,
                LAST_VALUE(bun IGNORE NULLS) OVER (PARTITION BY stay_id ORDER BY hr_timestamp) AS bun,
                -- Vitals usually frequent enough to use simple COALESCE or carry forward
                LAST_VALUE(heart_rate IGNORE NULLS) OVER (PARTITION BY stay_id ORDER BY hr_timestamp) AS heart_rate,
                LAST_VALUE(map IGNORE NULLS) OVER (PARTITION BY stay_id ORDER BY hr_timestamp) AS map,
                COALESCE(uo_hourly, 0) AS uo_hourly,
                is_aki_now,

                -- NEW: Track the timestamp of the last valid measurement
                LAST_VALUE(CASE WHEN creatinine IS NOT NULL THEN hr_timestamp END IGNORE NULLS) OVER (PARTITION BY stay_id ORDER BY hr_timestamp) AS last_cr_time,
                LAST_VALUE(CASE WHEN bun IS NOT NULL THEN hr_timestamp END IGNORE NULLS) OVER (PARTITION BY stay_id ORDER BY hr_timestamp) AS last_bun_time,
                LAST_VALUE(CASE WHEN heart_rate IS NOT NULL THEN hr_timestamp END IGNORE NULLS) OVER (PARTITION BY stay_id ORDER BY hr_timestamp) AS last_hr_time,
                LAST_VALUE(CASE WHEN map IS NOT NULL THEN hr_timestamp END IGNORE NULLS) OVER (PARTITION BY stay_id ORDER BY hr_timestamp) AS last_map_time
            FROM gold_features_step
        ),
        target_gen AS (
            SELECT 
                f.*,
                -- 6-HOUR LOOK AHEAD TARGET:
                -- Is there an AKI event in any of the next 6 rows?
                MAX(is_aki_now) OVER (
                    PARTITION BY stay_id 
                    ORDER BY hr_index 
                    ROWS BETWEEN 1 FOLLOWING AND 6 FOLLOWING
                ) AS aki_within_6h
            FROM filled_data f
        )
        SELECT 
            t.* EXCLUDE (creatinine, bun, heart_rate, map, last_cr_time, last_bun_time, last_hr_time, last_map_time),
            -- Backfill initial missing creatinine with admission baseline, and other labs/vitals with clinical normals
            COALESCE(t.creatinine, b.baseline_creatinine, 0.8) AS creatinine,
            COALESCE(t.bun, 15.0) AS bun,
            -- Clip HR to 20–300 bpm; values outside this range are data entry errors or unit mismatches
            GREATEST(20.0, LEAST(300.0, COALESCE(t.heart_rate, 80.0))) AS heart_rate,
            -- Clip MAP to 20–200 mmHg; same rationale as HR clipping
            GREATEST(20.0, LEAST(200.0, COALESCE(t.map, 85.0))) AS map,
            
            -- Calculate Staleness (Difference in hours), defaulting to a high dummy value before first measurement
            COALESCE(date_diff('hour', t.last_cr_time, t.hr_timestamp), 48) AS hours_since_last_cr,
            COALESCE(date_diff('hour', t.last_bun_time, t.hr_timestamp), 48) AS hours_since_last_bun,
            COALESCE(date_diff('hour', t.last_hr_time, t.hr_timestamp), 24) AS hours_since_last_hr,
            COALESCE(date_diff('hour', t.last_map_time, t.hr_timestamp), 24) AS hours_since_last_map,
            
            -- Join Static Features
            s.gender,
            -- Clip age at 90 to handle MIMIC-IV HIPAA obfuscation (ages > 89 are shifted to 300)
            LEAST(s.age, 90) AS age,
            s.admission_weight,
            
            -- Calculate weight-normalized urine output (mL/kg/hr)
            CASE WHEN s.admission_weight > 0 THEN t.uo_hourly / s.admission_weight ELSE NULL END AS uo_ml_kg_hr,
            
            -- Assume 0 (False) if patient has no condition history records
            COALESCE(c.history_ckd, 0) AS history_ckd,
            COALESCE(c.history_diabetes, 0) AS history_diabetes,
            COALESCE(c.history_chf, 0) AS history_chf,
            COALESCE(c.history_sepsis, 0) AS history_sepsis
        FROM target_gen t
        LEFT JOIN silver_stays s ON t.stay_id = s.stay_id
        LEFT JOIN silver_comorbidities c ON t.patient_id = c.patient_id
        LEFT JOIN silver_aki_baselines b ON t.stay_id = b.stay_id
        -- Filter out rows where the patient is already in AKI state 
        -- (we only want to predict the first onset)
        WHERE aki_within_6h IS NOT NULL  -- Drop ambiguous tail rows where look-ahead extends past end of stay
        AND (
            (
                SELECT MIN(hr_index) 
                FROM target_gen t2 
                WHERE t2.stay_id = t.stay_id AND t2.is_aki_now = 1
            ) IS NULL 
            OR t.hr_index < (
                SELECT MIN(hr_index) 
                FROM target_gen t2 
                WHERE t2.stay_id = t.stay_id AND t2.is_aki_now = 1
            )
        );
    """)

    print("Gold Phase Complete. Table 'gold_hourly_clinical' is ready.")
    conn.close()