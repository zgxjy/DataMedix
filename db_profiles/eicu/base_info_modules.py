# --- START OF FILE db_profiles/eicu/base_info_modules.py ---
def col_def(name, type):
    return f"{name} {type}"

def add_demography(table_name, db_profile, **kwargs):
    """
    为 e-ICU 队列添加人口学信息。
    数据源: eicu.patient
    连接键: patientunitstayid
    """
    col_defs = [
        col_def("gender", "VARCHAR(255)"),
        col_def("age", "VARCHAR(255)"),
        col_def("ethnicity", "VARCHAR(255)"),
        col_def("admissionheight", "NUMERIC"),
        col_def("admissionweight", "NUMERIC"),
        col_def("hospitaladmittime24", "VARCHAR(255)"),
        col_def("hospitaldischargetime24", "VARCHAR(255)"),
        col_def("hospitalid", "INTEGER"),
        col_def("unittype", "VARCHAR(255)"),
        col_def("unitstaytype", "VARCHAR(255)"),
        col_def("unitdischargeoffset", "INTEGER"),
        col_def("hospitaldischargeoffset", "INTEGER"),
        col_def("hospitaldischargestatus", "VARCHAR(255)"),
    ]
    
    update_sql = f"-- Update e-ICU Demography for {table_name}\n"
    update_sql += f"""
    UPDATE {table_name} AS cohort_table
    SET
        gender = p.gender,
        age = p.age,
        ethnicity = p.ethnicity,
        admissionheight = p.admissionheight,
        admissionweight = p.admissionweight,
        hospitaladmittime24 = p.hospitaladmittime24,
        hospitaldischargetime24 = p.hospitaldischargetime24,
        hospitalid = p.hospitalid,
        unittype = p.unittype,
        unitstaytype = p.unitstaytype,
        unitdischargeoffset = p.unitdischargeoffset,
        hospitaldischargeoffset = p.hospitaldischargeoffset,
        hospitaldischargestatus = p.hospitaldischargestatus
    FROM
        eicu.patient AS p
    WHERE
        cohort_table.patientunitstayid = p.patientunitstayid;
    """
    
    return col_defs, update_sql