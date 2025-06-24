# --- START OF FILE db_profiles/mimic_iv/base_info_modules.py ---
from psycopg2 import sql
import pandas as pd

def col_def(name, type):
    return f"{name} {type}"

# REFACTOR: All functions now accept a 'db_profile' argument for future-proofing,
# even if it's not used in the MIMIC-IV specific SQL.
# The 'sql_accumulator' argument is also removed as it was not used.

def add_demography(table_name, db_profile, **kwargs):
    # The SQL logic here is identical to your provided `base_info_sql.py`
    # and remains hardcoded for MIMIC-IV derived tables.
    col_defs = [
        col_def("gender", "character"), col_def("dod", "date"),
        col_def("los_hospital", "numeric"), col_def("admission_age", "numeric"),
        col_def("race", "character varying"), col_def("hospital_expire_flag", "smallint"),
        col_def("hospstay_seq", "bigint"), col_def("first_hosp_stay", "boolean"),
        col_def("icustay_seq", "bigint"), col_def("first_icu_stay", "boolean"),
        col_def("los_icu_detail", "numeric"), col_def("marital_status", "CHARACTER(100)"),
        col_def("height", "NUMERIC"), col_def("weight", "NUMERIC"),
        col_def("first_day_admission_weight", "NUMERIC"), col_def("first_day_discharge_weight", "NUMERIC"),
        col_def("first_ever_admission_weight", "NUMERIC"), col_def("last_ever_discharge_weight", "NUMERIC"),
        col_def("bmi", "NUMERIC"), col_def("icu_los_dod_days", "NUMERIC"),
        col_def("hospital_los_dod_days", "NUMERIC"), col_def("time_to_death_days", "NUMERIC")
    ]
    update_sql = f"-- Update Additional Demography and icustay_detail alignment columns for {table_name}\n"
    update_sql += f"""
UPDATE {table_name} af
SET
    gender = i.gender, dod = i.dod, los_hospital = i.los_hospital,
    admission_age = i.admission_age, race = i.race, hospital_expire_flag = i.hospital_expire_flag,
    hospstay_seq = i.hospstay_seq, first_hosp_stay = i.first_hosp_stay,
    icustay_seq = i.icustay_seq, first_icu_stay = i.first_icu_stay,
    los_icu_detail = i.los_icu
FROM mimiciv_derived.icustay_detail i
WHERE af.stay_id = i.stay_id;

UPDATE {table_name} af SET marital_status = ad.marital_status FROM mimiciv_hosp.admissions ad WHERE af.subject_id = ad.subject_id AND af.hadm_id = ad.hadm_id;

UPDATE {table_name} af SET height = ht.height FROM mimiciv_derived.first_day_height ht WHERE af.subject_id = ht.subject_id AND af.stay_id = ht.stay_id;

UPDATE {table_name} af SET weight = (SELECT avg(wt.weight) FROM mimiciv_derived.first_day_weight wt WHERE af.stay_id = wt.stay_id GROUP BY wt.stay_id)
WHERE EXISTS (SELECT 1 FROM mimiciv_derived.first_day_weight wt WHERE af.stay_id = wt.stay_id);

WITH ChartEventsFiltered AS (
    SELECT ce.stay_id, ce.charttime, ce.valuenum, af_ref.icu_intime, af_ref.icu_outtime
    FROM mimiciv_icu.chartevents ce JOIN {table_name} af_ref ON ce.stay_id = af_ref.stay_id
    WHERE ce.itemid IN (226512, 224639) AND ce.valuenum IS NOT NULL AND ce.valuenum > 0 AND ce.valuenum < 500
), RankedWeights AS (
    SELECT stay_id, valuenum, charttime, icu_intime, icu_outtime,
        CASE WHEN charttime >= icu_intime AND charttime <= (icu_intime + interval '24 hours') THEN ROW_NUMBER() OVER(PARTITION BY stay_id ORDER BY charttime ASC, valuenum ASC) ELSE NULL END as rn_adm_24h,
        CASE WHEN icu_outtime IS NOT NULL AND charttime <= icu_outtime AND charttime >= (icu_outtime - interval '24 hours') THEN ROW_NUMBER() OVER(PARTITION BY stay_id ORDER BY charttime DESC, valuenum ASC) ELSE NULL END as rn_dis_24h,
        ROW_NUMBER() OVER(PARTITION BY stay_id ORDER BY charttime ASC, valuenum ASC) as rn_adm_ever,
        ROW_NUMBER() OVER(PARTITION BY stay_id ORDER BY charttime DESC, valuenum ASC) as rn_dis_ever
    FROM ChartEventsFiltered
), WeightValues AS (
    SELECT stay_id,
        MAX(CASE WHEN rn_adm_24h = 1 THEN valuenum ELSE NULL END) as first_day_adm_w,
        MAX(CASE WHEN rn_dis_24h = 1 THEN valuenum ELSE NULL END) as first_day_dis_w,
        MAX(CASE WHEN rn_adm_ever = 1 THEN valuenum ELSE NULL END) as first_ever_adm_w,
        MAX(CASE WHEN rn_dis_ever = 1 THEN valuenum ELSE NULL END) as last_ever_dis_w
    FROM RankedWeights GROUP BY stay_id
)
UPDATE {table_name} af
SET first_day_admission_weight = wv.first_day_adm_w, first_day_discharge_weight = wv.first_day_dis_w, first_ever_admission_weight = wv.first_ever_adm_w, last_ever_discharge_weight = wv.last_ever_dis_w
FROM {table_name} af_join_alias JOIN WeightValues wv ON af_join_alias.stay_id = wv.stay_id WHERE af.stay_id = af_join_alias.stay_id;

UPDATE {table_name} af SET bmi = weight / (height / 100)^2 WHERE height IS NOT NULL AND weight IS NOT NULL AND height > 0;

UPDATE {table_name} af
SET
    icu_los_dod_days = CASE WHEN af.dod IS NOT NULL AND af.icu_outtime IS NOT NULL AND DATE_PART('day', af.dod - af.icu_outtime) < 0 THEN 0 ELSE DATE_PART('day', af.dod - af.icu_outtime) END,
    hospital_los_dod_days = CASE WHEN af.dod IS NOT NULL AND af.dischtime IS NOT NULL AND DATE_PART('day', af.dod - af.dischtime) < 0 THEN 0 ELSE DATE_PART('day', af.dod - af.dischtime) END,
    time_to_death_days = CASE WHEN af.dod IS NOT NULL AND af.admittime IS NOT NULL AND DATE_PART('day', af.dod - af.admittime) < 0 THEN 0 ELSE DATE_PART('day', af.dod - af.admittime) END
WHERE af.dod IS NOT NULL;
    """
    return col_defs, update_sql

def add_antecedent(table_name, db_profile, **kwargs):
    col_defs = [
        col_def("age_score", "integer"), col_def("myocardial_infarct", "integer"),
        col_def("congestive_heart_failure", "integer"), col_def("peripheral_vascular_disease", "integer"),
        col_def("cerebrovascular_disease", "integer"), col_def("dementia", "integer"),
        col_def("chronic_pulmonary_disease", "integer"), col_def("rheumatic_disease", "integer"),
        col_def("peptic_ulcer_disease", "integer"), col_def("mild_liver_disease", "integer"),
        col_def("diabetes_without_cc", "integer"), col_def("diabetes_with_cc", "integer"),
        col_def("paraplegia", "integer"), col_def("renal_disease", "integer"),
        col_def("malignant_cancer", "integer"), col_def("severe_liver_disease", "integer"),
        col_def("metastatic_solid_tumor", "integer"), col_def("aids", "integer"),
        col_def("charlson_comorbidity_index", "integer")
    ]
    update_sql = f"-- Update Charlson Comorbidities for {table_name}\n"
    update_sql += f"""
UPDATE {table_name} af
SET
    age_score = i.age_score, myocardial_infarct = i.myocardial_infarct,
    congestive_heart_failure = i.congestive_heart_failure, peripheral_vascular_disease = i.peripheral_vascular_disease,
    cerebrovascular_disease = i.cerebrovascular_disease, dementia = i.dementia,
    chronic_pulmonary_disease = i.chronic_pulmonary_disease, rheumatic_disease = i.rheumatic_disease,
    peptic_ulcer_disease = i.peptic_ulcer_disease, mild_liver_disease = i.mild_liver_disease,
    diabetes_without_cc = i.diabetes_without_cc, diabetes_with_cc = i.diabetes_with_cc,
    paraplegia = i.paraplegia, renal_disease = i.renal_disease, malignant_cancer = i.malignant_cancer,
    severe_liver_disease = i.severe_liver_disease, metastatic_solid_tumor = i.metastatic_solid_tumor,
    aids = i.aids, charlson_comorbidity_index = i.charlson_comorbidity_index
FROM mimiciv_derived.charlson i
WHERE af.subject_id = i.subject_id and af.hadm_id = i.hadm_id;
"""
    return col_defs, update_sql

def add_vital_sign(table_name, db_profile, **kwargs):
    cols_vitals = [
        "heart_rate_min double precision", "heart_rate_max double precision", "heart_rate_mean double precision",
        "sbp_min double precision", "sbp_max double precision", "sbp_mean double precision",
        "dbp_min double precision", "dbp_max double precision", "dbp_mean double precision",
        "mbp_min double precision", "mbp_max double precision", "mbp_mean double precision",
        "resp_rate_min double precision", "resp_rate_max double precision", "resp_rate_mean double precision",
        "temperature_min numeric", "temperature_max numeric", "temperature_mean numeric",
        "spo2_min double precision", "spo2_max double precision", "spo2_mean double precision",
        "glucose_min double precision", "glucose_max double precision", "glucose_mean double precision"
    ]
    update_vitals = f"""
UPDATE {table_name} af
SET
    heart_rate_min = i.heart_rate_min, heart_rate_max = i.heart_rate_max, heart_rate_mean = i.heart_rate_mean,
    sbp_min = i.sbp_min, sbp_max = i.sbp_max, sbp_mean = i.sbp_mean,
    dbp_min = i.dbp_min, dbp_max = i.dbp_max, dbp_mean = i.dbp_mean,
    mbp_min = i.mbp_min, mbp_max = i.mbp_max, mbp_mean = i.mbp_mean,
    resp_rate_min = i.resp_rate_min, resp_rate_max = i.resp_rate_max, resp_rate_mean = i.resp_rate_mean,
    temperature_min = i.temperature_min, temperature_max = i.temperature_max, temperature_mean = i.temperature_mean,
    spo2_min = i.spo2_min, spo2_max = i.spo2_max, spo2_mean = i.spo2_mean,
    glucose_min = i.glucose_min, glucose_max = i.glucose_max, glucose_mean = i.glucose_mean
FROM mimiciv_derived.first_day_vitalsign i
WHERE af.subject_id = i.subject_id and af.stay_id = i.stay_id;
"""
    cols_bg = [
        "lactate_min double precision", "lactate_max double precision", "ph_min double precision", "ph_max double precision",
        "so2_min double precision", "so2_max double precision", "po2_min double precision", "po2_max double precision",
        "pco2_min double precision", "pco2_max double precision", "aado2_min double precision", "aado2_max double precision",
        "aado2_calc_min double precision", "aado2_calc_max double precision", "pao2fio2ratio_min double precision", "pao2fio2ratio_max double precision",
        "baseexcess_min double precision", "baseexcess_max double precision", "bicarbonate_min double precision", "bicarbonate_max double precision",
        "totalco2_min double precision", "totalco2_max double precision", "hematocrit_min double precision", "hematocrit_max double precision",
        "hemoglobin_min double precision", "hemoglobin_max double precision", "carboxyhemoglobin_min double precision", "carboxyhemoglobin_max double precision",
        "methemoglobin_min double precision", "methemoglobin_max double precision", "chloride_min double precision", "chloride_max double precision",
        "calcium_min double precision", "calcium_max double precision", "potassium_min double precision", "potassium_max double precision",
        "sodium_min double precision", "sodium_max double precision"
    ]
    update_bg = f"""
UPDATE {table_name} af
SET
    lactate_min = i.lactate_min, lactate_max = i.lactate_max, ph_min = i.ph_min, ph_max = i.ph_max,
    so2_min = i.so2_min, so2_max = i.so2_max, po2_min = i.po2_min, po2_max = i.po2_max,
    pco2_min = i.pco2_min, pco2_max = i.pco2_max, aado2_min = i.aado2_min, aado2_max = i.aado2_max,
    aado2_calc_min = i.aado2_calc_min, aado2_calc_max = i.aado2_calc_max,
    pao2fio2ratio_min = i.pao2fio2ratio_min, pao2fio2ratio_max = i.pao2fio2ratio_max,
    baseexcess_min = i.baseexcess_min, baseexcess_max = i.baseexcess_max,
    totalco2_min = i.totalco2_min, totalco2_max = i.totalco2_max,
    carboxyhemoglobin_min = i.carboxyhemoglobin_min, carboxyhemoglobin_max = i.carboxyhemoglobin_max,
    methemoglobin_min = i.methemoglobin_min, methemoglobin_max = i.methemoglobin_max
FROM mimiciv_derived.first_day_bg i
WHERE af.subject_id = i.subject_id and af.stay_id = i.stay_id;
"""
    cols_lab = [
        "hematocrit_min double precision", "hematocrit_max double precision", "hemoglobin_min double precision", "hemoglobin_max double precision",
        "platelets_min double precision", "platelets_max double precision", "wbc_min double precision", "wbc_max double precision",
        "albumin_min double precision", "albumin_max double precision", "globulin_min double precision", "globulin_max double precision",
        "total_protein_min double precision", "total_protein_max double precision", "aniongap_min double precision", "aniongap_max double precision",
        "bicarbonate_min double precision", "bicarbonate_max double precision", "bun_min double precision", "bun_max double precision",
        "calcium_min double precision", "calcium_max double precision", "chloride_min double precision", "chloride_max double precision",
        "creatinine_min double precision", "creatinine_max double precision",
        "abs_basophils_min numeric", "abs_basophils_max numeric", "abs_eosinophils_min numeric", "abs_eosinophils_max numeric",
        "abs_lymphocytes_min numeric", "abs_lymphocytes_max numeric", "abs_monocytes_min numeric", "abs_monocytes_max numeric",
        "abs_neutrophils_min numeric", "abs_neutrophils_max numeric", "atyps_min double precision", "atyps_max double precision",
        "bands_min double precision", "bands_max double precision", "imm_granulocytes_min double precision", "imm_granulocytes_max double precision",
        "metas_min double precision", "metas_max double precision", "nrbc_min double precision", "nrbc_max double precision",
        "d_dimer_min double precision", "d_dimer_max double precision", "fibrinogen_min double precision", "fibrinogen_max double precision",
        "thrombin_min double precision", "thrombin_max double precision", "inr_min double precision", "inr_max double precision",
        "pt_min double precision", "pt_max double precision", "ptt_min double precision", "ptt_max double precision",
        "alt_min double precision", "alt_max double precision", "alp_min double precision", "alp_max double precision",
        "ast_min double precision", "ast_max double precision", "amylase_min double precision", "amylase_max double precision",
        "bilirubin_total_min double precision", "bilirubin_total_max double precision", "bilirubin_direct_min double precision", "bilirubin_direct_max double precision",
        "bilirubin_indirect_min double precision", "bilirubin_indirect_max double precision", "ck_cpk_min double precision", "ck_cpk_max double precision",
        "ck_mb_min double precision", "ck_mb_max double precision", "ggt_min double precision", "ggt_max double precision",
        "ld_ldh_min double precision", "ld_ldh_max double precision"
    ]
    update_lab = f"""
UPDATE {table_name} af
SET
    hematocrit_min = i.hematocrit_min, hematocrit_max = i.hematocrit_max, hemoglobin_min = i.hemoglobin_min, hemoglobin_max = i.hemoglobin_max,
    platelets_min = i.platelets_min, platelets_max = i.platelets_max, wbc_min = i.wbc_min, wbc_max = i.wbc_max,
    albumin_min = i.albumin_min, albumin_max = i.albumin_max, globulin_min = i.globulin_min, globulin_max = i.globulin_max,
    total_protein_min = i.total_protein_min, total_protein_max = i.total_protein_max, aniongap_min = i.aniongap_min, aniongap_max = i.aniongap_max,
    bicarbonate_min = i.bicarbonate_min, bicarbonate_max = i.bicarbonate_max, bun_min = i.bun_min, bun_max = i.bun_max,
    calcium_min = i.calcium_min, calcium_max = i.calcium_max, chloride_min = i.chloride_min, chloride_max = i.chloride_max,
    creatinine_min = i.creatinine_min, creatinine_max = i.creatinine_max,
    abs_basophils_min = i.abs_basophils_min, abs_basophils_max = i.abs_basophils_max, abs_eosinophils_min = i.abs_eosinophils_min, abs_eosinophils_max = i.abs_eosinophils_max,
    abs_lymphocytes_min = i.abs_lymphocytes_min, abs_lymphocytes_max = i.abs_lymphocytes_max, abs_monocytes_min = i.abs_monocytes_min, abs_monocytes_max = i.abs_monocytes_max,
    abs_neutrophils_min = i.abs_neutrophils_min, abs_neutrophils_max = i.abs_neutrophils_max, atyps_min = i.atyps_min, atyps_max = i.atyps_max,
    bands_min = i.bands_min, bands_max = i.bands_max, imm_granulocytes_min = i.imm_granulocytes_min, imm_granulocytes_max = i.imm_granulocytes_max,
    metas_min = i.metas_min, metas_max = i.metas_max, nrbc_min = i.nrbc_min, nrbc_max = i.nrbc_max,
    d_dimer_min = i.d_dimer_min, d_dimer_max = i.d_dimer_max, fibrinogen_min = i.fibrinogen_min, fibrinogen_max = i.fibrinogen_max,
    thrombin_min = i.thrombin_min, thrombin_max = i.thrombin_max, inr_min = i.inr_min, inr_max = i.inr_max,
    pt_min = i.pt_min, pt_max = i.pt_max, ptt_min = i.ptt_min, ptt_max = i.ptt_max,
    alt_min = i.alt_min, alt_max = i.alt_max, alp_min = i.alp_min, alp_max = i.alp_max,
    ast_min = i.ast_min, ast_max = i.ast_max, amylase_min = i.amylase_min, amylase_max = i.amylase_max,
    bilirubin_total_min = i.bilirubin_total_min, bilirubin_total_max = i.bilirubin_total_max, bilirubin_direct_min = i.bilirubin_direct_min, bilirubin_direct_max = i.bilirubin_direct_max,
    bilirubin_indirect_min = i.bilirubin_indirect_min, bilirubin_indirect_max = i.bilirubin_indirect_max, ck_cpk_min = i.ck_cpk_min, ck_cpk_max = i.ck_cpk_max,
    ck_mb_min = i.ck_mb_min, ck_mb_max = i.ck_mb_max, ggt_min = i.ggt_min, ggt_max = i.ggt_max,
    ld_ldh_min = i.ld_ldh_min, ld_ldh_max = i.ld_ldh_max
FROM mimiciv_derived.first_day_lab i
WHERE af.subject_id = i.subject_id and af.stay_id = i.stay_id;
"""
    cols_gcs = [
        "gcs_min double precision", "gcs_motor double precision", "gcs_verbal double precision",
        "gcs_eyes double precision", "gcs_unable integer"
    ]
    update_gcs = f"""
UPDATE {table_name} af
SET
    gcs_min = i.gcs_min, gcs_motor = i.gcs_motor, gcs_verbal = i.gcs_verbal,
    gcs_eyes = i.gcs_eyes, gcs_unable = i.gcs_unable
FROM mimiciv_derived.first_day_gcs i
WHERE af.subject_id = i.subject_id and af.stay_id = i.stay_id;
"""
    update_hr_arv_preparation = f"""
DROP TABLE IF EXISTS heart_rate_arv_temp;
CREATE TEMPORARY TABLE heart_rate_arv_temp AS
SELECT derived.subject_id, derived.stay_id, AVG(derived.abs_diff) AS heart_rate_arv
FROM (
    SELECT ie.subject_id, ie.stay_id, ce.heart_rate,
           LAG(ce.heart_rate) OVER(PARTITION BY ie.stay_id ORDER BY ce.charttime) as prev_heart_rate,
           ABS(ce.heart_rate - LAG(ce.heart_rate) OVER(PARTITION BY ie.stay_id ORDER BY ce.charttime)) as abs_diff
    FROM mimiciv_icu.icustays AS ie
    INNER JOIN mimiciv_derived.vitalsign AS ce ON ie.stay_id = ce.stay_id
    INNER JOIN {table_name} target_af ON ie.stay_id = target_af.stay_id
) AS derived
GROUP BY derived.subject_id, derived.stay_id;
"""
    cols_hr_arv = ["heart_rate_arv NUMERIC"]
    update_hr_arv_application = f"""
UPDATE {table_name} af
SET heart_rate_arv = hr.heart_rate_arv
FROM heart_rate_arv_temp hr
WHERE af.subject_id = hr.subject_id AND af.stay_id = hr.stay_id;
DROP TABLE IF EXISTS heart_rate_arv_temp;
"""
    final_col_defs = []
    processed_names = set()
    for col_list in [cols_vitals, cols_bg, cols_lab, cols_gcs, cols_hr_arv]:
        for col_def_str in col_list:
            name = col_def_str.split()[0]
            if name not in processed_names:
                final_col_defs.append(col_def_str)
                processed_names.add(name)
    all_updates = "\n\n".join([update_vitals, update_bg, update_lab, update_gcs, update_hr_arv_preparation, update_hr_arv_application])
    return final_col_defs, all_updates

def add_scores(table_name, db_profile, **kwargs):
    all_col_defs = []
    all_update_sqls = [f"-- Update Patient Scores for {table_name} --\n"]
    cols_sofa = [
        col_def("sofa", "integer"), col_def("respiration_sofa", "integer"),
        col_def("coagulation_sofa", "integer"), col_def("liver_sofa", "integer"),
        col_def("cardiovascular_sofa", "integer"), col_def("cns_sofa", "integer"), col_def("renal_sofa", "integer")
    ]
    update_sofa = f"""
UPDATE {table_name} af
SET
    sofa = i.sofa, respiration_sofa = i.respiration, coagulation_sofa = i.coagulation, liver_sofa = i.liver,
    cardiovascular_sofa = i.cardiovascular, cns_sofa = i.cns, renal_sofa = i.renal
FROM mimiciv_derived.first_day_sofa i
WHERE af.subject_id = i.subject_id and af.stay_id = i.stay_id;
"""
    all_col_defs.extend(cols_sofa)
    all_update_sqls.append(update_sofa)
    cols_sapsii = [
        col_def("sapsii", "integer"), col_def("sapsii_prob", "double precision"),
        col_def("sapsii_age_score", "integer"), col_def("hr_score_sapsii", "integer"),
        col_def("sysbp_score_sapsii", "integer"), col_def("temp_score_sapsii", "integer"),
        col_def("pao2fio2_score_sapsii", "integer"), col_def("uo_score_sapsii", "integer"),
        col_def("bun_score_sapsii", "integer"), col_def("wbc_score_sapsii", "integer"),
        col_def("potassium_score_sapsii", "integer"), col_def("sodium_score_sapsii", "integer"),
        col_def("bicarbonate_score_sapsii", "integer"), col_def("bilirubin_score_sapsii", "integer"),
        col_def("gcs_score_sapsii", "integer"), col_def("comorbidity_score_sapsii", "integer"),
        col_def("admissiontype_score_sapsii", "integer")
    ]
    update_sapsii = f"""
UPDATE {table_name} af
SET
    sapsii = i.sapsii, sapsii_prob = i.sapsii_prob, sapsii_age_score = i.age_score, hr_score_sapsii = i.hr_score,
    sysbp_score_sapsii = i.sysbp_score, temp_score_sapsii = i.temp_score, pao2fio2_score_sapsii = i.pao2fio2_score,
    uo_score_sapsii = i.uo_score, bun_score_sapsii = i.bun_score, wbc_score_sapsii = i.wbc_score,
    potassium_score_sapsii = i.potassium_score, sodium_score_sapsii = i.sodium_score,
    bicarbonate_score_sapsii = i.bicarbonate_score, bilirubin_score_sapsii = i.bilirubin_score,
    gcs_score_sapsii = i.gcs_score, comorbidity_score_sapsii = i.comorbidity_score,
    admissiontype_score_sapsii = i.admissiontype_score
FROM mimiciv_derived.sapsii i
WHERE af.subject_id = i.subject_id and af.stay_id = i.stay_id;
"""
    all_col_defs.extend(cols_sapsii)
    all_update_sqls.append(update_sapsii)
    cols_apsiii = [col_def("apsiii", "integer"), col_def("apsiii_prob", "double precision")]
    update_apsiii = f"""
UPDATE {table_name} af SET apsiii = i.apsiii, apsiii_prob = i.apsiii_prob FROM mimiciv_derived.apsiii i WHERE af.stay_id = i.stay_id;
"""
    all_col_defs.extend(cols_apsiii)
    all_update_sqls.append(update_apsiii)
    cols_lods = [col_def("lods_score", "integer")]
    update_lods = f"""
UPDATE {table_name} af SET lods_score = i.lods FROM mimiciv_derived.lods i WHERE af.stay_id = i.stay_id;
"""
    all_col_defs.extend(cols_lods)
    all_update_sqls.append(update_lods)
    cols_oasis = [col_def("oasis", "integer"), col_def("oasis_prob", "double precision")]
    update_oasis = f"""
UPDATE {table_name} af SET oasis = i.oasis, oasis_prob = i.oasis_prob FROM mimiciv_derived.oasis i WHERE af.stay_id = i.stay_id;
"""
    all_col_defs.extend(cols_oasis)
    all_update_sqls.append(update_oasis)
    cols_sirs = [col_def("sirs_score", "integer")]
    update_sirs = f"""
UPDATE {table_name} af SET sirs_score = i.sirs FROM mimiciv_derived.sirs i WHERE af.stay_id = i.stay_id;
"""
    all_col_defs.extend(cols_sirs)
    all_update_sqls.append(update_sirs)
    unique_col_defs_dict = {}
    for col_def_str_item in all_col_defs:
        col_name_part = col_def_str_item.split(' ')[0].strip()
        if col_name_part not in unique_col_defs_dict:
            unique_col_defs_dict[col_name_part] = col_def_str_item
    final_unique_col_defs = list(unique_col_defs_dict.values())
    return final_unique_col_defs, "\n\n".join(all_update_sqls)

def add_blood_info(table_name, db_profile, **kwargs):
    cols = [
        "mean_hematocrit double precision", "mean_hemoglobin double precision", "mean_mch double precision",
        "mean_mchc double precision", "mean_mcv double precision", "mean_platelet double precision",
        "mean_rbc double precision", "mean_rdw double precision", "mean_rdwsd double precision", "mean_wbc double precision",
        "first_hematocrit double precision", "first_hemoglobin double precision", "first_mch double precision",
        "first_mchc double precision", "first_mcv double precision", "first_platelet double precision",
        "first_rbc double precision", "first_rdw double precision", "first_rdwsd double precision", "first_wbc double precision"
    ]
    update_sql = f"-- Update Blood Info (Mean and First) for {table_name}\n"
    update_sql += f"""
DROP TABLE IF EXISTS blood_mean_temp;
CREATE TEMPORARY TABLE blood_mean_temp AS
SELECT
  derived.subject_id, derived.hadm_id,
  AVG(derived.hematocrit) AS mean_hematocrit, AVG(derived.hemoglobin) AS mean_hemoglobin,
  AVG(derived.mch) AS mean_mch, AVG(derived.mchc) AS mean_mchc, AVG(derived.mcv) AS mean_mcv,
  AVG(derived.platelet) AS mean_platelet, AVG(derived.rbc) AS mean_rbc, AVG(derived.rdw) AS mean_rdw,
  AVG(derived.rdwsd) AS mean_rdwsd, AVG(derived.wbc) AS mean_wbc
FROM (
    SELECT ce.*
    FROM mimiciv_icu.icustays AS ie
    INNER JOIN mimiciv_derived.complete_blood_count AS ce ON ie.hadm_id = ce.hadm_id
    INNER JOIN {table_name} target_af ON ie.hadm_id = target_af.hadm_id
    WHERE ce.charttime >= ie.intime - INTERVAL '6 HOUR' AND ce.charttime <= ie.intime + INTERVAL '1 DAY'
) AS derived
GROUP BY derived.subject_id, derived.hadm_id;

UPDATE {table_name} af
SET
    mean_hematocrit = i.mean_hematocrit, mean_hemoglobin = i.mean_hemoglobin, mean_mch = i.mean_mch,
    mean_mchc = i.mean_mchc, mean_mcv = i.mean_mcv, mean_platelet = i.mean_platelet,
    mean_rbc = i.mean_rbc, mean_rdw = i.mean_rdw, mean_rdwsd = i.mean_rdwsd, mean_wbc = i.mean_wbc
FROM blood_mean_temp i
WHERE af.hadm_id = i.hadm_id;
DROP TABLE IF EXISTS blood_mean_temp;

DROP TABLE IF EXISTS blood_first_temp;
CREATE TEMPORARY TABLE blood_first_temp AS (
    SELECT cbc.*, ROW_NUMBER() OVER(PARTITION BY cbc.subject_id, cbc.hadm_id ORDER BY cbc.charttime ASC) as lab_rank
    FROM mimiciv_derived.complete_blood_count cbc
    WHERE cbc.hadm_id IN (SELECT hadm_id FROM {table_name})
);

UPDATE {table_name} af
SET
    first_hematocrit = i.hematocrit, first_hemoglobin = i.hemoglobin, first_mch = i.mch,
    first_mchc = i.mchc, first_mcv = i.mcv, first_platelet = i.platelet, first_rbc = i.rbc,
    first_rdw = i.rdw, first_rdwsd = i.rdwsd, first_wbc = i.wbc
FROM blood_first_temp i
WHERE i.lab_rank=1 and af.hadm_id = i.hadm_id;
DROP TABLE IF EXISTS blood_first_temp;
"""
    return cols, update_sql

def add_cardiovascular_lab(table_name, db_profile, **kwargs):
    cols = [
        "first_Triglyceride NUMERIC", "first_LDL NUMERIC", "first_hba1c NUMERIC",
        "first_HDL NUMERIC", "first_Potassium NUMERIC", "first_NTproBNP NUMERIC", "first_glucose NUMERIC"
    ]
    update_sql = f"-- Update First Cardiovascular Labs for {table_name}\n"
    lab_updates = []
    lab_map = {
        'Triglyceride': ['51000'], 'LDL': ['50905', '50906'], 'hba1c': ['50852'],
        'HDL': ['50904'], 'Potassium': ['50822', '50833', '52452', '52610', '50971'],
        'NTproBNP': ['50963'], 'glucose': ['50809', '50931', '51478', '51981', '52027', '52569']
    }
    for lab_name, item_ids in lab_map.items():
        item_ids_str = ", ".join([f"'{item_id}'" for item_id in item_ids])
        lab_updates.append(f"""
WITH tg_{lab_name.lower()} AS (
    SELECT lab.subject_id, lab.hadm_id, lab.charttime, lab.valuenum,
           ROW_NUMBER() OVER(PARTITION BY lab.subject_id, lab.hadm_id ORDER BY lab.charttime ASC) AS lab_rank
    FROM mimiciv_hosp.labevents lab
    WHERE lab.itemid IN ({item_ids_str}) AND lab.valuenum IS NOT NULL
      AND lab.hadm_id IN (SELECT hadm_id FROM {table_name})
)
UPDATE {table_name} af SET first_{lab_name} = hr.valuenum FROM tg_{lab_name.lower()} hr WHERE hr.lab_rank = 1 AND af.hadm_id = hr.hadm_id;
""")
    update_sql += "\n".join(lab_updates)
    return cols, update_sql

def add_medicine(table_name, db_profile, **kwargs):
    cols = [
        "used_aspirin integer", "used_clopidogrel integer", "used_furosemide integer",
        "used_lisinopril integer", "used_metoprolol integer", "used_losartan integer",
        "used_amlodipine integer", "used_diltiazem integer", "used_digoxin integer",
        "used_amiodarone integer", "used_insulin integer", "used_statin integer",
        "used_dabigatran integer", "used_rivaroxaban integer", "used_heparin integer",
        "used_warfarin integer", "used_sacubactril_valsartan integer"
    ]
    update_sql = f"-- Update Medication Usage for {table_name}\n"
    update_sql += f"""
WITH used_drugs_temp as (
    SELECT
        pre.hadm_id,
        MAX(CASE WHEN drug ILIKE '%aspirin%' THEN 1 ELSE 0 END) AS used_aspirin,
        MAX(CASE WHEN drug ILIKE '%clopidogrel%' THEN 1 ELSE 0 END) AS used_clopidogrel,
        MAX(CASE WHEN drug ILIKE '%furosemide%' THEN 1 ELSE 0 END) AS used_furosemide,
        MAX(CASE WHEN drug ILIKE '%lisinopril%' THEN 1 ELSE 0 END) AS used_lisinopril,
        MAX(CASE WHEN drug ILIKE '%metoprolol%' THEN 1 ELSE 0 END) AS used_metoprolol,
        MAX(CASE WHEN drug ILIKE '%losartan%' THEN 1 ELSE 0 END) AS used_losartan,
        MAX(CASE WHEN drug ILIKE '%amlodipine%' THEN 1 ELSE 0 END) AS used_amlodipine,
        MAX(CASE WHEN drug ILIKE '%diltiazem%' THEN 1 ELSE 0 END) AS used_diltiazem,
        MAX(CASE WHEN drug ILIKE '%digoxin%' THEN 1 ELSE 0 END) AS used_digoxin,
        MAX(CASE WHEN drug ILIKE '%amiodarone%' THEN 1 ELSE 0 END) AS used_amiodarone,
        MAX(CASE WHEN drug ILIKE '%insulin%' THEN 1 ELSE 0 END) AS used_insulin,
        MAX(CASE WHEN drug ILIKE '%statin%' THEN 1 ELSE 0 END) AS used_statin,
        MAX(CASE WHEN drug ILIKE '%dabigatran%' THEN 1 ELSE 0 END) AS used_dabigatran,
        MAX(CASE WHEN drug ILIKE '%rivaroxaban%' THEN 1 ELSE 0 END) AS used_rivaroxaban,
        MAX(CASE WHEN drug ILIKE '%heparin%' THEN 1 ELSE 0 END) AS used_heparin,
        MAX(CASE WHEN drug ILIKE '%warfarin%' THEN 1 ELSE 0 END) AS used_warfarin,
        MAX(CASE WHEN LOWER(drug) LIKE '%sacubitril%' AND LOWER(drug) LIKE '%valsartan%' THEN 1 ELSE 0 END) AS used_sacubactril_valsartan
    FROM mimiciv_hosp.prescriptions pre
    WHERE pre.hadm_id IN (SELECT hadm_id FROM {table_name})
    GROUP BY pre.hadm_id
)
UPDATE {table_name} af
SET
    used_aspirin = ud.used_aspirin, used_clopidogrel = ud.used_clopidogrel, used_furosemide = ud.used_furosemide,
    used_lisinopril = ud.used_lisinopril, used_metoprolol = ud.used_metoprolol, used_losartan = ud.used_losartan,
    used_amlodipine = ud.used_amlodipine, used_diltiazem = ud.used_diltiazem, used_digoxin = ud.used_digoxin,
    used_amiodarone = ud.used_amiodarone, used_insulin = ud.used_insulin, used_statin = ud.used_statin,
    used_dabigatran = ud.used_dabigatran, used_rivaroxaban = ud.used_rivaroxaban, used_heparin = ud.used_heparin,
    used_warfarin = ud.used_warfarin, used_sacubactril_valsartan = ud.used_sacubactril_valsartan
FROM used_drugs_temp ud
WHERE af.hadm_id = ud.hadm_id;
"""
    return cols, update_sql

def add_surgeries(table_name, db_profile, **kwargs):
    cols = ["cardiac_surgery_before INT DEFAULT 0"]
    update_sql = f"-- Update Cardiac Surgery History for {table_name}\n"
    update_sql += f"""
WITH temp_heart_sur AS (
    SELECT p.subject_id
    FROM mimiciv_hosp.procedures_icd AS p
    JOIN mimiciv_hosp.d_icd_procedures AS d ON p.icd_code = d.icd_code
    JOIN {table_name} AS a ON p.subject_id = a.subject_id
    WHERE (d.long_title ILIKE '%heart%' OR d.long_title ILIKE '%cardiac%')
      AND EXISTS (
          SELECT 1 FROM mimiciv_hosp.admissions adm_proc
          WHERE adm_proc.hadm_id = p.hadm_id AND adm_proc.admittime < a.icu_intime
      )
    GROUP BY p.subject_id
)
UPDATE {table_name} AS a
SET cardiac_surgery_before = 1
FROM temp_heart_sur AS t
WHERE a.subject_id = t.subject_id;
"""
    return cols, update_sql

def add_past_diagnostic(table_name, db_profile, **kwargs):
    past_diagnoses_data = kwargs.get('past_diagnoses_data', {})
    all_col_defs = []
    all_update_sqls = [f"-- Add Past Diagnostic Information for {table_name} --\n"]

    if not past_diagnoses_data:
        all_update_sqls.append("-- No past diagnoses data provided or ICDs found. --")
        return all_col_defs, "\n".join(all_update_sqls)

    for category_key, icd_codes_list in past_diagnoses_data.items():
        if not icd_codes_list:
            all_update_sqls.append(f"-- No ICD codes found for category '{category_key}', skipping. --")
            continue

        display_name = category_key.replace('_', ' ').capitalize()
        prior_col_name = f"prior_{category_key}"
        icd_col_name = f"{prior_col_name}_icd_codes"
        title_col_name = f"{prior_col_name}_long_titles"

        all_col_defs.extend([
            f"{prior_col_name} INT DEFAULT 0",
            f"{icd_col_name} TEXT DEFAULT NULL",
            f"{title_col_name} TEXT DEFAULT NULL"
        ])

        formatted_icd_codes_str = ", ".join([f"'{str(code).strip()}'" for code in icd_codes_list])
        if not formatted_icd_codes_str:
            all_update_sqls.append(f"-- Formatted ICD codes for '{display_name}' is empty, skipping. --")
            continue

        category_sql_block = f"""
-- ### Processing: Prior Diagnosis for {display_name} (using pre-fetched ICDs) ###
WITH prior_diagnoses_for_{category_key} AS (
    SELECT
        pat.subject_id,
        STRING_AGG(DISTINCT TRIM(d.icd_code), ', ') AS aggregated_icd_codes,
        STRING_AGG(DISTINCT TRIM(diag_desc.long_title), '; ') AS aggregated_long_titles
    FROM
        mimiciv_hosp.patients pat
    JOIN
        mimiciv_hosp.admissions adm ON pat.subject_id = adm.subject_id
    JOIN
        mimiciv_hosp.diagnoses_icd d ON adm.hadm_id = d.hadm_id
    JOIN
        mimiciv_hosp.d_icd_diagnoses diag_desc ON TRIM(d.icd_code) = TRIM(diag_desc.icd_code)
    JOIN
        {table_name} current_event ON pat.subject_id = current_event.subject_id
    WHERE
        TRIM(d.icd_code) IN ({formatted_icd_codes_str})
        AND adm.admittime < current_event.icu_intime
        AND current_event.icu_intime IS NOT NULL
    GROUP BY
        pat.subject_id
)
UPDATE {table_name} AS target_table_alias
SET
    {prior_col_name} = 1,
    {icd_col_name} = p_diag.aggregated_icd_codes,
    {title_col_name} = p_diag.aggregated_long_titles
FROM prior_diagnoses_for_{category_key} AS p_diag
WHERE target_table_alias.subject_id = p_diag.subject_id;
"""
        all_update_sqls.append(category_sql_block)

    return all_col_defs, "\n".join(all_update_sqls)