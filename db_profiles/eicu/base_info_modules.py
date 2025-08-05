# --- START OF FILE db_profiles/eicu/base_info_modules.py ---

def col_def(name, type):
    return f"{name} {type}"

# --- 函数1: Demography and APACHE (您已有的函数，保持不变) ---
def add_demography_and_apache(table_name, db_profile, **kwargs):
    """
    从 public.patient 和 public.apachepatientresult 表中为队列添加人口学、住院信息和APACHE-IVa评分。
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
FROM public.patient p
WHERE cohort.patientunitstayid = p.patientunitstayid;

-- Then, update APACHE IVa scores, taking the first score for the stay
WITH apache_scores AS (
    SELECT
        apr.patientunitstayid,
        apr.apachescore,
        apr.apacheversion,
        CAST(apr.predictedicumortality AS double precision) as predictedicumortality,
        CAST(apr.predictedhospitalmortality AS double precision) as predictedhospitalmortality,
        ROW_NUMBER() OVER(PARTITION BY apr.patientunitstayid ORDER BY apr.apachepatientresultsid) as rn
    FROM public.apachepatientresult apr
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

# --- 函数2: Lab Values (新增) ---
def add_lab_values_eicu(table_name, db_profile, **kwargs):
    """
    为队列提取ICU入住24小时内的首次和平均实验室指标。
    """
    # 定义 Lab 名称映射和将要创建的列
    lab_map = {
        'RBC': 'rbc', 'WBC x 1000': 'wbc', 'total cholesterol': 'chol_total',
        'Triglycerides': 'triglycerides', 'HDL': 'chol_hdl', 'LDL': 'chol_ldl',
        'bedside glucose': 'glucose_bedside', 'PT': 'pt', 'chloride': 'chloride',
        'calcium': 'calcium', 'Hgb': 'hgb', 'Hct': 'hct', 'sodium': 'sodium',
        'anion gap': 'aniongap', 'potassium': 'potassium', 'BUN': 'bun',
        'creatinine': 'creatinine', 'PT - INR': 'inr', 'MCH': 'mch', 'RDW': 'rdw',
        'glucose': 'glucose', 'platelets x 1000': 'platelets', 'MCHC': 'mchc',
        'bicarbonate': 'bicarbonate', 'MCV': 'mcv', 'total protein': 'protein_total',
        'albumin': 'albumin', 'Total CO2': 'totalco2', 'AST (SGOT)': 'ast',
        'ALT (SGPT)': 'alt', 'uric acid': 'uricacid', 'lactate': 'lactate',
        'paO2': 'pao2', 'Methemoglobin': 'methemoglobin', 
        'Carboxyhemoglobin': 'carboxyhemoglobin', 'O2 Sat (%)': 'o2sat',
        'pH': 'ph', 'paCO2': 'paco2', 'FiO2': 'fio2'
    }

    col_defs = []
    for base_name in lab_map.values():
        col_defs.append(col_def(f"{base_name}_first", "double precision"))
        col_defs.append(col_def(f"{base_name}_24h_avg", "double precision"))

    # 构建聚合表达式
    agg_expressions = []
    for labname, base_name in lab_map.items():
        # 平均值
        agg_expressions.append(f"AVG(CASE WHEN l.labname = '{labname}' THEN l.labresult END) AS {base_name}_24h_avg")
        # 首次值
        agg_expressions.append(f"MAX(CASE WHEN l.labname = '{labname}' AND l.rn = 1 THEN l.labresult END) AS {base_name}_first")

    update_sql = f"-- Update Lab Values (First & 24h Avg) for {table_name}\n"
    update_sql += f"""
WITH RankedLabs AS (
    SELECT
        lab.patientunitstayid,
        lab.labname,
        lab.labresultoffset,
        lab.labresult,
        ROW_NUMBER() OVER(PARTITION BY lab.patientunitstayid, lab.labname ORDER BY lab.labresultoffset ASC) as rn
    FROM public.lab
    WHERE 
        lab.patientunitstayid IN (SELECT patientunitstayid FROM {table_name})
        AND lab.labname IN {tuple(lab_map.keys())}
        AND lab.labresultoffset BETWEEN 0 AND 1440 -- First 24 hours (in minutes)
        AND lab.labresult IS NOT NULL
),
AggregatedLabs AS (
    SELECT
        l.patientunitstayid,
        {', '.join(agg_expressions)}
    FROM RankedLabs l
    GROUP BY l.patientunitstayid
)
UPDATE {table_name} cohort
SET
    {', '.join([f"{base_name}_24h_avg = agg.{base_name}_24h_avg, {base_name}_first = agg.{base_name}_first" for base_name in lab_map.values()])}
FROM AggregatedLabs agg
WHERE cohort.patientunitstayid = agg.patientunitstayid;
"""
    return col_defs, update_sql


# --- 函数3: Vital Signs (新增) ---
def add_vital_signs_eicu(table_name, db_profile, **kwargs):
    """
    为队列提取ICU入住24小时内的首次和平均生命体征，以及BMI。
    """
    vital_map = {
        'respiration': 'resp_rate',
        'heartrate': 'heart_rate',
        'systemicsystolic': 'sbp',
        'systemicdiastolic': 'dbp',
        'systemicmean': 'mbp'
    }

    col_defs = [col_def("bmi", "double precision")]
    agg_expressions = []
    for db_col, out_col in vital_map.items():
        col_defs.append(col_def(f"{out_col}_first", "double precision"))
        col_defs.append(col_def(f"{out_col}_24h_avg", "double precision"))
        agg_expressions.append(f"AVG(v.{db_col}) AS {out_col}_24h_avg")
        agg_expressions.append(f"MAX(CASE WHEN v.rn = 1 THEN v.{db_col} END) AS {out_col}_first_val")

    # 构建针对不同生命体征的聚合
    vital_agg_expressions = []
    for db_col, out_col in vital_map.items():
        vital_agg_expressions.append(f"AVG(v.{db_col}) AS {out_col}_24h_avg")
        vital_agg_expressions.append(f"MAX(CASE WHEN v.rn = 1 THEN v.{db_col} END) AS {out_col}_first")

    update_sql = f"-- Update Vital Signs (First & 24h Avg) and BMI for {table_name}\n"
    update_sql += f"""
-- First, calculate and update BMI
UPDATE {table_name} cohort
SET
    bmi = CASE 
            WHEN p.admissionheight > 0 AND p.admissionweight > 0 
            THEN p.admissionweight / (p.admissionheight * p.admissionheight / 10000.0)
            ELSE NULL 
          END
FROM public.patient p
WHERE cohort.patientunitstayid = p.patientunitstayid;

-- Then, calculate and update vital signs
WITH RankedVitals AS (
    SELECT
        v.patientunitstayid,
        v.observationoffset,
        v.heartrate,
        v.respiration,
        v.systemicsystolic,
        v.systemicdiastolic,
        v.systemicmean,
        ROW_NUMBER() OVER(PARTITION BY v.patientunitstayid ORDER BY v.observationoffset ASC) as rn
    FROM public.vitalperiodic v
    WHERE
        v.patientunitstayid IN (SELECT patientunitstayid FROM {table_name})
        AND v.observationoffset BETWEEN 0 AND 1440
),
AggregatedVitals AS (
    SELECT 
        v.patientunitstayid,
        {', '.join(vital_agg_expressions)}
    FROM RankedVitals v
    GROUP BY v.patientunitstayid
)
UPDATE {table_name} cohort
SET
    {', '.join([f"{out_col}_24h_avg = agg.{out_col}_24h_avg, {out_col}_first = agg.{out_col}_first" for db_col, out_col in vital_map.items()])}
FROM AggregatedVitals agg
WHERE cohort.patientunitstayid = agg.patientunitstayid;
"""
    return col_defs, update_sql


# --- 函数4: Comorbidities (最终采纳的修正版) ---
def add_comorbidities_eicu(table_name, db_profile, **kwargs):
    """
    为队列添加常见的合并症作为0/1变量。
    数据来源: public.pasthistory 和 public.diagnosis
    """
    comorbidity_map = {
        'comorb_diabetes': ["diabe", "dm"],
        'comorb_hypertension': ["hypertensi", "htn"],
        'comorb_mi': ["myocardial infarction", " mi"],
        'comorb_stroke': ["stroke", "cva", "cerebrovascular accident"],
        'comorb_afib': ["atrial fibrillation", "afib"],
        'comorb_hf': ["heart failure", " hf"],
        'comorb_cvd': ["cerebrovascular", "coronary artery", "cad"],
        'comorb_cancer': ["cancer", "malignan", "tumor", "chemotherapy"]
    }
    
    col_defs = [col_def(col, "integer") for col in comorbidity_map.keys()]
    
    flag_expressions = []
    for col_name, keywords in comorbidity_map.items():
        conditions = [f"LOWER(dx.dx_text) LIKE '%{kw}%'" for kw in keywords]
        flag_expressions.append(f"MAX(CASE WHEN {' OR '.join(conditions)} THEN 1 ELSE 0 END) AS {col_name}")

    # SQL被拆分为两个独立的、按顺序执行的语句。
    # 您的程序可以处理分号分隔的多个SQL语句，这解决了CTE作用域问题。
    update_sql = f"-- Step 1: Update comorbidities for matching patients in {table_name}\n"
    update_sql += f"""
WITH AllDiagnosisText AS (
    SELECT patientunitstayid, pasthistorypath AS dx_text FROM public.pasthistory WHERE pasthistorypath IS NOT NULL
    UNION ALL
    SELECT patientunitstayid, diagnosisstring AS dx_text FROM public.diagnosis WHERE diagnosisstring IS NOT NULL
),
ComorbFlags AS (
    SELECT 
        dx.patientunitstayid,
        {', '.join(flag_expressions)}
    FROM AllDiagnosisText dx
    WHERE dx.patientunitstayid IN (SELECT patientunitstayid FROM {table_name})
    GROUP BY dx.patientunitstayid
)
UPDATE {table_name} AS cohort
SET
    {', '.join([f"{col} = cf.{col}" for col in comorbidity_map.keys()])}
FROM
    ComorbFlags AS cf
WHERE
    cohort.patientunitstayid = cf.patientunitstayid;

-- Step 2: Set non-matching patients' comorbidities to 0
UPDATE {table_name}
SET
    {', '.join([f"{col} = 0" for col in comorbidity_map.keys()])}
WHERE
    { ' AND '.join([f"{col} IS NULL" for col in comorbidity_map.keys()]) };
"""
    return col_defs, update_sql