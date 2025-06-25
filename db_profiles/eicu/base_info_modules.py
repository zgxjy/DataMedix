# --- START OF FILE db_profiles/eicu/base_info_modules.py ---

def col_def(name, type):
    return f"{name} {type}"

def add_demography_and_apache(table_name, db_profile, **kwargs):
    """
    从 eicu_crd.patient 和 eicu_crd.apachepatientresult 表中为队列添加人口学、住院信息和APACHE-IVa评分。
    """
    col_defs = [
        col_def("gender", "character varying"),
        col_def("age", "character varying"),
        col_def("ethnicity", "character varying"),
        col_def("hospitaladmittime24", "character varying"),
        col_def("hospitaldischargetime24", "character varying"),
        col_def("hospitaldischargeyear", "integer"),
        col_def("hospitaldischargeoffset", "integer"),
        col_def("hospitaldischargestatus", "character varying"),
        col_def("unittype", "character varying"),
        col_def("unitadmittime24", "character varying"),
        col_def("unitdischargetime24", "character varying"),
        col_def("uniquepid", "character varying"),
        # APACHE IVa Score
        col_def("apachescore", "integer"),
        col_def("apacheversion", "character varying"),
        col_def("predictedicumortality", "double precision"),
        col_def("predictedhospitalmortality", "double precision"),
    ]
    
    update_sql = f"-- Update Demographics and APACHE IVa scores for {table_name}\n"
    update_sql += f"""
-- First, update patient demographics and hospital stay info
UPDATE {table_name} cohort
SET
    gender = p.gender,
    age = p.age,
    ethnicity = p.ethnicity,
    hospitaladmittime24 = p.hospitaladmittime24,
    hospitaldischargetime24 = p.hospitaldischargetime24,
    hospitaldischargeyear = p.hospitaldischargeyear,
    hospitaldischargeoffset = p.hospitaldischargeoffset,
    hospitaldischargestatus = p.hospitaldischargestatus,
    unittype = p.unittype,
    unitadmittime24 = p.unitadmittime24,
    unitdischargetime24 = p.unitdischargetime24,
    uniquepid = p.uniquepid
FROM eicu_crd.patient p
WHERE cohort.patientunitstayid = p.patientunitstayid;

-- Then, update APACHE IVa scores, taking the first score for the stay
WITH apache_scores AS (
    SELECT
        apr.patientunitstayid,
        apr.apachescore,
        apr.apacheversion,
        apr.predictedicumortality,
        apr.predictedhospitalmortality,
        ROW_NUMBER() OVER(PARTITION BY apr.patientunitstayid ORDER BY apr.apachepatientresultid) as rn
    FROM eicu_crd.apachepatientresult apr
    WHERE apr.apacheversion = 'IVa' AND apr.patientunitstayid IN (SELECT patientunitstayid FROM {table_name})
)
UPDATE {table_name} cohort
SET
    apachescore = a.apachescore,
    apacheversion = a.apacheversion,
    predictedicumortality = a.predictedicumortality,
    predictedhospitalmortality = a.predictedhospitalmortality
FROM apache_scores a
WHERE cohort.patientunitstayid = a.patientunitstayid AND a.rn = 1;
"""
    return col_defs, update_sql