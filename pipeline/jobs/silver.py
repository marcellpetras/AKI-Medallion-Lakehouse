import duckdb
import os
from pipeline.config import DB_PATH

def build_silver_layer():
    print("Starting Medallion Pipeline: Silver Phase")
    
    conn = duckdb.connect(DB_PATH)
    
    # ---------------------------------------------------------
    # 1. silver_stays
    # ---------------------------------------------------------
    print("Building silver_stays...")
    conn.execute("""
        CREATE OR REPLACE TABLE silver_stays AS 
        WITH parsed_patients AS (
            SELECT 
                CAST(id AS VARCHAR) AS patient_id,
                gender,
                CAST(birthDate AS TIMESTAMP) AS birth_date
            FROM bronze_patients
        ),
        parsed_encounters AS (
            SELECT 
                CAST(id AS VARCHAR) AS stay_id,
                SPLIT_PART(subject.reference, '/', 2) AS patient_id,
                CAST(period.start AS TIMESTAMP) AS intime,
                CAST(period."end" AS TIMESTAMP) AS outtime
            FROM bronze_encounters
        ),
        admission_weights AS (
            SELECT 
                SPLIT_PART(encounter.reference, '/', 2) AS stay_id,
                valueQuantity.value AS admission_weight
            FROM bronze_chartevents
            WHERE code.coding[1].code = '226512' -- Admission Weight code (kg)
        )
        
        -- if more than one admission weights exists
        , agg_weights AS (
            SELECT stay_id, AVG(admission_weight) as admission_weight
            FROM admission_weights
            GROUP BY stay_id
        )

        SELECT 
            e.stay_id,
            e.patient_id,
            p.gender,
            DATE_DIFF('year', p.birth_date, e.intime) AS age,
            e.intime,
            e.outtime,
            w.admission_weight
        FROM parsed_encounters e
        LEFT JOIN parsed_patients p ON e.patient_id = p.patient_id
        LEFT JOIN agg_weights w ON e.stay_id = w.stay_id;
    """)

    # ---------------------------------------------------------
    # 2. silver_creatinine
    # ---------------------------------------------------------
    print("Building silver_creatinine...")
    conn.execute("""
        CREATE OR REPLACE TABLE silver_creatinine AS 
        WITH raw_labs AS (
            SELECT 
                SPLIT_PART(subject.reference, '/', 2) AS patient_id,
                CAST(effectiveDateTime AS TIMESTAMP) AS charttime,
                valueQuantity.value AS valuenum
            FROM bronze_labevents
            WHERE code.coding[1].code = '50912' -- Creatinine code
                AND valueQuantity.value IS NOT NULL
        )
        SELECT 
            s.stay_id,
            l.patient_id,
            l.charttime,
            l.valuenum
        FROM raw_labs l
        INNER JOIN silver_stays s 
          ON l.patient_id = s.patient_id 
          AND l.charttime >= s.intime - INTERVAL '7' DAY
          AND l.charttime <= s.outtime;
    """)

    # ---------------------------------------------------------
    # 3. silver_urine_output
    # ---------------------------------------------------------
    print("Building silver_urine_output...")
    conn.execute("""
        CREATE OR REPLACE TABLE silver_urine_output AS 
        SELECT 
            SPLIT_PART(encounter.reference, '/', 2) AS stay_id,
            CAST(effectiveDateTime AS TIMESTAMP) AS charttime,
            valueQuantity.value AS urine_volume
        FROM bronze_outputevents
        WHERE code.coding[1].code IN ('226559', '226560') -- "Urine Out - Foley" and "Urine Out - Other."
            AND stay_id IS NOT NULL
            AND stay_id != ''
            AND urine_volume IS NOT NULL;
    """)

    # ---------------------------------------------------------
    # 4. silver_aki_baselines
    # ---------------------------------------------------------
    print("Building silver_aki_baselines...")
    conn.execute("""
        CREATE OR REPLACE TABLE silver_aki_baselines AS 
        WITH ranked_creatinine AS (
            SELECT 
                c.stay_id,
                c.valuenum,
                c.charttime,
                -- Rank within first 24 hours
                ROW_NUMBER() OVER(
                    PARTITION BY c.stay_id 
                    ORDER BY c.charttime ASC
                ) as rn
            FROM silver_creatinine c
            JOIN silver_stays s ON c.stay_id = s.stay_id
            WHERE c.charttime <= s.intime + INTERVAL '24' HOUR
               AND c.charttime >= s.intime - INTERVAL '7' DAY
        )
        SELECT 
            stay_id, 
            valuenum AS baseline_creatinine 
        FROM ranked_creatinine 
        WHERE rn = 1;
    """)

    # ---------------------------------------------------------
    # 5. silver_kdigo_labels
    # ---------------------------------------------------------
    print("Building silver_kdigo_labels...")
    conn.execute("""
        CREATE OR REPLACE TABLE silver_kdigo_labels AS 
        WITH scr_kdigo AS (
            SELECT 
                c.stay_id,
                c.charttime,
                c.valuenum as current_scr,
                b.baseline_creatinine,
                
                -- 48-Hour Spike
                (c.valuenum - MIN(c.valuenum) OVER (
                    PARTITION BY c.stay_id 
                    ORDER BY c.charttime 
                    RANGE BETWEEN INTERVAL '48' HOUR PRECEDING AND CURRENT ROW
                )) >= 0.3 AS criteria_absolute_spike,
                
                -- 7-Day Rolling Relative Baseline
                (c.valuenum / MIN(c.valuenum) OVER (
                    PARTITION BY c.stay_id 
                    ORDER BY c.charttime 
                    RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW
                )) >= 1.5 AS criteria_relative_rolling

            FROM silver_creatinine c
            LEFT JOIN silver_aki_baselines b ON c.stay_id = b.stay_id
        ),
        uo_kdigo AS (
            SELECT 
                u.stay_id,
                u.charttime,
                -- Urine output needs full 6 hours of elapsed ICU time to avoid false positives
                -- Use NULLIF to prevent division by zero if admission_weight is somehow logged as 0.0
                (SUM(u.urine_volume) OVER (
                    PARTITION BY u.stay_id 
                    ORDER BY u.charttime 
                    RANGE BETWEEN INTERVAL '6' HOUR PRECEDING AND CURRENT ROW
                ) / NULLIF(s.admission_weight, 0)) / 6 < 0.5 
                AND u.charttime >= s.intime + INTERVAL '6' HOUR
                AS criteria_urine_output
            FROM silver_urine_output u
            LEFT JOIN silver_stays s ON u.stay_id = s.stay_id
        )
        SELECT 
            COALESCE(s.stay_id, u.stay_id) AS stay_id,
            COALESCE(s.charttime, u.charttime) AS charttime,
            s.current_scr,
            s.baseline_creatinine,
            s.criteria_absolute_spike,
            s.criteria_relative_rolling,
            u.criteria_urine_output,
            
            -- Status flag
            COALESCE(s.criteria_absolute_spike, FALSE) OR 
            COALESCE(s.criteria_relative_rolling, FALSE) OR 
            COALESCE(u.criteria_urine_output, FALSE) AS is_aki
            
        FROM scr_kdigo s
        FULL OUTER JOIN uo_kdigo u ON s.stay_id = u.stay_id AND s.charttime = u.charttime;
    """)
    
    # ---------------------------------------------------------
    # 6. silver_vitals (Predictive Features)
    # ---------------------------------------------------------
    print("Building silver_vitals...")
    conn.execute("""
        CREATE OR REPLACE TABLE silver_vitals AS 
        WITH combined AS (
            SELECT subject, encounter, effectiveDateTime, valueQuantity, code
            FROM bronze_chartevents
            UNION ALL
            SELECT subject, encounter, effectiveDateTime, valueQuantity, code
            FROM bronze_labevents
        ),
        extracted AS (
            SELECT 
                SPLIT_PART(encounter.reference, '/', 2) AS stay_id_raw,
                SPLIT_PART(subject.reference, '/', 2) AS patient_id,
                CAST(effectiveDateTime AS TIMESTAMP) AS charttime,
                valueQuantity.value AS valuenum,
                CASE 
                    WHEN code.coding[1].code = '220045' THEN 'heart_rate'
                    WHEN code.coding[1].code = '220052' THEN 'map' --shorthand for "Mean Arterial Pressure"
                    WHEN code.coding[1].code = '51006' THEN 'bun' --shorthand for "Blood Urea Nitrogen"
                END AS vital_type
            FROM combined
            WHERE code.coding[1].code IN ('220045', '220052', '51006')
            AND valueQuantity.value IS NOT NULL
        )
        SELECT 
            -- Use the ID provided in the record or link it to a stay based on the timestamp
            COALESCE(e.stay_id_raw, s.stay_id) AS stay_id,
            e.charttime,
            e.valuenum,
            e.vital_type
        FROM extracted e
        LEFT JOIN silver_stays s 
          ON e.patient_id = s.patient_id
          AND e.charttime >= s.intime - INTERVAL '7' DAY
          AND e.charttime <= s.outtime
        WHERE COALESCE(e.stay_id_raw, s.stay_id) != '';
    """)
    
    # ---------------------------------------------------------
    # 7. silver_comorbidities (Expanded Static Risk Factors)
    # ---------------------------------------------------------
    print("Building silver_comorbidities...")
    conn.execute("""
        CREATE OR REPLACE TABLE silver_comorbidities AS 
        WITH extracted_codes AS (
            SELECT 
                SPLIT_PART(subject.reference, '/', 2) AS patient_id,
                -- Extract the system (ICD-9 vs ICD-10)
                code.coding[1].system as icd_system,
                -- Extract the specific code
                code.coding[1].code as icd_code
            FROM bronze_conditions
        ),
        flagged_conditions AS (
            SELECT 
                patient_id,
                -- CKD: ICD-9 (585) or ICD-10 (N18)
                MAX(CASE 
                    WHEN (icd_system LIKE '%icd9' AND icd_code LIKE '585%') 
                      OR (icd_system LIKE '%icd10' AND icd_code LIKE 'N18%') THEN 1 ELSE 0 END) as history_ckd,
                
                -- Diabetes: ICD-9 (250) or ICD-10 (E08, E09, E10, E11, E13)
                MAX(CASE 
                    WHEN (icd_system LIKE '%icd9' AND icd_code LIKE '250%') 
                      OR (icd_system LIKE '%icd10' AND icd_code SIMILAR TO 'E(08|09|10|11|13)%') THEN 1 ELSE 0 END) as history_diabetes,
                
                -- CHF: ICD-9 (428) or ICD-10 (I50)
                MAX(CASE 
                    WHEN (icd_system LIKE '%icd9' AND icd_code LIKE '428%') 
                      OR (icd_system LIKE '%icd10' AND icd_code LIKE 'I50%') THEN 1 ELSE 0 END) as history_chf,

                -- Hypertension: ICD-9 (401-405) or ICD-10 (I10-I15)
                MAX(CASE 
                    WHEN (icd_system LIKE '%icd9' AND icd_code SIMILAR TO '40[1-5]%') 
                      OR (icd_system LIKE '%icd10' AND icd_code SIMILAR TO 'I1[0-5]%') THEN 1 ELSE 0 END) as history_hypertension,

                -- Sepsis: ICD-9 (038, 99591, 99592) or ICD-10 (A40, A41, R652)
                MAX(CASE 
                    WHEN (icd_system LIKE '%icd9' AND icd_code SIMILAR TO '(038|99591|99592)%') 
                      OR (icd_system LIKE '%icd10' AND icd_code SIMILAR TO '(A40|A41|R652)%') THEN 1 ELSE 0 END) as history_sepsis,

                -- Liver Disease: ICD-9 (571) or ICD-10 (K70-K76)
                MAX(CASE 
                    WHEN (icd_system LIKE '%icd9' AND icd_code LIKE '571%') 
                      OR (icd_system LIKE '%icd10' AND icd_code SIMILAR TO 'K7[0-6]%') THEN 1 ELSE 0 END) as history_liver_disease
            FROM extracted_codes
            GROUP BY patient_id
        )
        SELECT * FROM flagged_conditions;
    """)
    
    # ---------------------------------------------------------
    # 8. silver_medications
    # ---------------------------------------------------------
    print("Building silver_medications...")
    conn.execute("""
        CREATE OR REPLACE TABLE silver_medications AS 
        WITH raw_meds AS (
            SELECT 
                SPLIT_PART(context.reference, '/', 2) AS stay_id,
                SPLIT_PART(subject.reference, '/', 2) AS patient_id,
                
                -- Handle both bolus pushes (DateTime) and IV infusions (Period)
                COALESCE(
                    CAST(effectiveDateTime AS TIMESTAMP), 
                    CAST(effectivePeriod.start AS TIMESTAMP)
                ) AS charttime,
                CAST(effectivePeriod."end" AS TIMESTAMP) AS endtime,
                
                -- Extract core medication identifiers
                medicationCodeableConcept.coding[1].code AS medication_code,
                medicationCodeableConcept.coding[1].display AS medication_name,
                
                -- Dosages might be an absolute dose or an infusion rate
                COALESCE(dosage.dose.value, dosage.rateQuantity.value) AS dose,
                COALESCE(dosage.dose.unit, dosage.rateQuantity.unit) AS dose_unit
            FROM bronze_medications
        )
        SELECT 
            m.stay_id,
            m.patient_id,
            m.charttime,
            m.endtime,
            m.medication_code,
            m.medication_name,
            m.dose,
            m.dose_unit
        FROM raw_meds m
        INNER JOIN silver_stays s 
          ON m.stay_id = s.stay_id;
    """)

    print("Silver phase complete.")
    conn.close()
