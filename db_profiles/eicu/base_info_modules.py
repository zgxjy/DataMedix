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
        # patient 表新增指标
        col_def("admissionheight", "double precision"),         # <--- 新增
        col_def("admissionweight", "double precision"),         # <--- 新增
        col_def("dischargeweight", "double precision"),         # <--- 新增
        # APACHE IVa Score
        col_def("apachescore", "integer"),
        col_def("apacheversion", "character varying"),
        col_def("predictedicumortality", "double precision"),
        col_def("predictedhospitalmortality", "double precision"),
        # apachepatientresult 表新增指标
        col_def("acutephysiologyscore", "integer"),             # <--- 新增
        col_def("actualhospitallos", "double precision"),       # <--- 新增
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
    uniquepid = p.uniquepid,
    admissionheight = p.admissionheight, 
    admissionweight = p.admissionweight, 
    dischargeweight = p.dischargeweight
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
        apr.acutephysiologyscore,
        apr.actualhospitallos,
        ROW_NUMBER() OVER(PARTITION BY apr.patientunitstayid ORDER BY apr.apachepatientresultsid) as rn
    FROM public.apachepatientresult apr
    WHERE apr.apacheversion = 'IVa' AND apr.patientunitstayid IN (SELECT patientunitstayid FROM {table_name})
)
UPDATE {table_name} cohort
SET
    apachescore = a.apachescore,
    apacheversion = a.apacheversion,
    predictedicumortality = a.predictedicumortality,
    predictedhospitalmortality = a.predictedhospitalmortality,
    acutephysiologyscore = a.acutephysiologyscore,
    actualhospitallos = a.actualhospitallos 
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

# --- 函数5: Charlson Comorbidity Index (CCI) (新增) ---
def add_charlson_comorbidity_index(table_name, db_profile, **kwargs):
    """
    基于 e-ICU 的 pasthistory 和 diagnosis 表中的文本信息，计算 Charlson 合并症指数。
    """
    # 定义 Charlson 条件、关键词和分数
    # 注意：关键词经过了简化以适应 e-ICU 的文本数据格式
    cci_conditions = {
        'cci_mi': {'score': 1, 'keywords': ['myocardial infarction', ' mi']},
        'cci_hf': {'score': 1, 'keywords': ['heart failure', ' hf', 'chf']},
        'cci_pvd': {'score': 1, 'keywords': ['peripheral vascular', 'pvd']},
        'cci_cvd': {'score': 1, 'keywords': ['cerebrovascular', 'stroke', 'cva', 'tia']},
        'cci_dementia': {'score': 1, 'keywords': ['dementia']},
        'cci_cpd': {'score': 1, 'keywords': ['chronic pulmonary', 'copd', 'emphysema']},
        'cci_rheumatic': {'score': 1, 'keywords': ['rheumatic', 'lupus', 'sle']},
        'cci_pud': {'score': 1, 'keywords': ['peptic ulcer']},
        'cci_mild_liver': {'score': 1, 'keywords': ['chronic hepatitis']},
        'cci_dm_no_cc': {'score': 1, 'keywords': ['diabe', 'dm']},
        'cci_dm_cc': {'score': 2, 'keywords': ['diabetic nephropathy', 'diabetic retinopathy', 'diabetic neuropathy']},
        'cci_paraplegia': {'score': 2, 'keywords': ['paraplegi', 'hemiplegi']},
        'cci_renal': {'score': 2, 'keywords': ['renal disease', 'renal failure', 'dialysis', 'chronic kidney']},
        'cci_cancer': {'score': 2, 'keywords': ['cancer', 'tumor', 'leukemia', 'lymphoma']},
        'cci_severe_liver': {'score': 3, 'keywords': ['cirrhosis', 'portal hypertension']},
        'cci_metastatic': {'score': 6, 'keywords': ['metastatic', 'metastasis']},
        'cci_aids': {'score': 6, 'keywords': ['aids', 'hiv']}
    }
    
    # 动态生成列定义
    col_defs = [col_def(col, "integer") for col in cci_conditions.keys()]
    col_defs.append(col_def("charlson_score", "integer"))

    # --- SQL Generation ---
    
    # 1. 为每个条件生成 CASE WHEN 语句来创建标志位
    flag_expressions = []
    for col_name, data in cci_conditions.items():
        conditions = [f"LOWER(dx.dx_text) LIKE '%{kw}%'" for kw in data['keywords']]
        flag_expressions.append(f"MAX(CASE WHEN {' OR '.join(conditions)} THEN 1 ELSE 0 END) AS {col_name}")

    # 2. 生成计算最终分数的表达式，处理互斥条件
    score_calculation_expressions = [
        # 基础疾病，直接乘以分数
        "cf.cci_mi * 1", "cf.cci_hf * 1", "cf.cci_pvd * 1", "cf.cci_cvd * 1",
        "cf.cci_dementia * 1", "cf.cci_cpd * 1", "cf.cci_rheumatic * 1", "cf.cci_pud * 1",
        "cf.cci_paraplegia * 2", "cf.cci_renal * 2", "cf.cci_aids * 6",
        # 肝病 (轻度 vs 重度，取分高的)
        "GREATEST(cf.cci_mild_liver * 1, cf.cci_severe_liver * 3)",
        # 糖尿病 (无并发症 vs 有并发症，取分高的)
        "GREATEST(cf.cci_dm_no_cc * 1, cf.cci_dm_cc * 2)",
        # 肿瘤 (局限性 vs 转移性，取分高的)
        "GREATEST(cf.cci_cancer * 2, cf.cci_metastatic * 6)"
    ]
    final_score_expression = " + ".join(score_calculation_expressions)

    # 3. 构建完整的 UPDATE SQL
    update_sql = f"-- Step 1: Calculate and update Charlson Comorbidity Index for patients in {table_name}\n"
    update_sql += f"""
WITH CciSourceText AS (
    -- 统一所有诊断和病史文本来源
    SELECT patientunitstayid, pasthistorypath AS dx_text FROM public.pasthistory WHERE pasthistorypath IS NOT NULL
    UNION ALL
    SELECT patientunitstayid, diagnosisstring AS dx_text FROM public.diagnosis WHERE diagnosisstring IS NOT NULL
),
CciFlags AS (
    -- 为每个病人生成所有CCI条件的0/1标志位
    SELECT 
        dx.patientunitstayid,
        {', '.join(flag_expressions)}
    FROM CciSourceText dx
    WHERE dx.patientunitstayid IN (SELECT patientunitstayid FROM {table_name})
    GROUP BY dx.patientunitstayid
)
UPDATE {table_name} AS cohort
SET
    -- 更新所有单独的标志位
    {', '.join([f"{col} = cf.{col}" for col in cci_conditions.keys()])},
    -- 计算并更新最终的CCI总分
    charlson_score = ({final_score_expression})
FROM
    CciFlags AS cf
WHERE
    cohort.patientunitstayid = cf.patientunitstayid;

-- Step 2: Set non-matching patients' comorbidities and score to 0
UPDATE {table_name}
SET
    {', '.join([f"{col} = 0" for col in cci_conditions.keys()])},
    charlson_score = 0
WHERE
    charlson_score IS NULL;
"""
    return col_defs, update_sql