# --- START OF FILE db_profiles/eicu/profile.py ---
from typing import List, Tuple, Callable, Dict, Any

from db_profiles.base_profile import BaseDbProfile

# 导入所有新的Panel和基础信息模块
from .panels.lab_panel import EicuLabPanel
from .panels.vitalperiodic_panel import EicuVitalPeriodicPanel
from .panels.nursecharting_panel import EicuNurseChartingPanel
from .panels.infusiondrug_panel import EicuInfusionDrugPanel
from .panels.diagnosis_panel import EicuDiagnosisPanel
from .panels.treatment_panel import EicuTreatmentPanel
from .panels.diagnosis_panel import EicuDiagnosisPanel
from . import base_info_modules as eicu_base_info

class EICUProfile(BaseDbProfile):
    """e-ICU Collaborative Research Database 的具体配置画像。"""

    def get_display_name(self) -> str:
        return "e-ICU v2.0"

    def get_default_connection_params(self) -> Dict[str, str]:
        return {"dbname": "eicu", "user": "postgres"}
        
    def get_cohort_table_schema(self) -> str:
        return "eicu_data"

    def get_source_panels(self) -> List[Tuple[str, Any]]:
        return [
            ("化验 (lab)", EicuLabPanel),
            ("生命体征-高频 (vitalperiodic)", EicuVitalPeriodicPanel),
            ("护理记录 (nursecharting)", EicuNurseChartingPanel),
            ("输液药物 (infusiondrug)", EicuInfusionDrugPanel),
            ("诊断 (diagnosis)", EicuDiagnosisPanel),
            ("治疗 (treatment)", EicuTreatmentPanel),
            ("既往史 (pasthistory)", EicuDiagnosisPanel),
        ]

    def get_base_info_modules(self) -> List[Tuple[str, str, Callable]]:
        return [
            ("人口学及APACHE-IVa评分", "demography_apache", eicu_base_info.add_demography_and_apache),
        ]

    def get_cohort_creation_configs(self) -> Dict[str, Dict[str, Any]]:
        return {
            "diagnosis": {
                "display_name": "按诊断字符串筛选",
                "event_table": "eicu_crd.diagnosis",
                "dictionary_table": None,
                "event_icd_col": "diagnosisstring",
                "dict_icd_col": None, "dict_title_col": None,
                "event_seq_num_col": "diagnosispriority",
                "event_time_col": "diagnosisoffset",
                "search_fields": [("diagnosisstring", "诊断字符串 (包含)")],
            },
            "treatment": {
                "display_name": "按治疗路径筛选",
                "event_table": "eicu_crd.treatment",
                "dictionary_table": None,
                "event_icd_col": "treatmentstring",
                "dict_icd_col": None, "dict_title_col": None,
                "event_seq_num_col": "treatmentoffset",
                "event_time_col": "treatmentoffset",
                "search_fields": [("treatmentstring", "治疗路径 (包含)")],
            },
        }

    def get_dictionary_tables(self) -> List[Dict[str, Any]]:
        """
        为 e-ICU 提供强大的动态数据字典查询功能。
        """
        return [
            {
                'display_name': "1. 合并症编码 (diagnosis)",
                'table_name': "eicu_crd.diagnosis",
                'columns': [("diagnosisstring", "诊断字符串"), ("icd9code", "ICD9码"), ("diagnosispriority", "优先级")],
                'is_dynamic_view': True,
                'dynamic_sql': "SELECT DISTINCT diagnosisstring, icd9code, diagnosispriority FROM eicu_crd.diagnosis ORDER BY diagnosisstring",
                'search_fields': [("diagnosisstring", "诊断字符串 (包含)"), ("icd9code", "ICD9码 (包含)")],
            },
            {
                'display_name': "2. 手术编码 (admissiondx)",
                'table_name': "eicu_crd.admissiondx",
                'columns': [("admitdxpath", "入院诊断路径"), ("admitdxtext", "入院诊断文本")],
                'is_dynamic_view': True,
                'dynamic_sql': "SELECT DISTINCT admitdxpath, admitdxtext FROM eicu_crd.admissiondx ORDER BY admitdxpath",
                'search_fields': [("admitdxpath", "诊断路径 (包含)"), ("admitdxtext", "诊断文本 (包含)")],
            },
            {
                'display_name': "3. 输液编码 (infusiondrug)",
                'table_name': "eicu_crd.infusiondrug",
                'columns': [("drugname", "药物名称"), ("drugrate", "速率"), ("infusionrate", "输液速率"), ("drugamount", "药量")],
                'is_dynamic_view': True,
                'dynamic_sql': "SELECT DISTINCT drugname, drugrate, infusionrate, drugamount FROM eicu_crd.infusiondrug WHERE drugname IS NOT NULL ORDER BY drugname",
                'search_fields': [("drugname", "药物名称 (包含)")],
            },
            {
                'display_name': "4. 实验室编码 (lab)",
                'table_name': "eicu_crd.lab",
                'columns': [("labname", "化验名称"), ("labmeasurenameinterface", "测量单位"), ("labresulttext", "文本结果")],
                'is_dynamic_view': True,
                'dynamic_sql': "SELECT DISTINCT labname, labmeasurenameinterface, labresulttext FROM eicu_crd.lab ORDER BY labname",
                'search_fields': [("labname", "化验名称 (包含)"), ("labmeasurenameinterface", "测量单位 (包含)"), ("labresulttext", "文本结果 (包含)")],
            },
            {
                'display_name': "5. 药物编码 (medication)",
                'table_name': "eicu_crd.medication",
                'columns': [("drugname", "药物名称"), ("dosage", "剂量"), ("routeadmin", "给药途径"), ("frequency", "频率")],
                'is_dynamic_view': True,
                'dynamic_sql': "SELECT DISTINCT drugname, dosage, routeadmin, frequency FROM eicu_crd.medication WHERE drugname IS NOT NULL ORDER BY drugname",
                'search_fields': [("drugname", "药物名称 (包含)"), ("routeadmin", "给药途径 (包含)")],
            },
            {
                'display_name': "6. 既往史编码 (pasthistory)",
                'table_name': "eicu_crd.pasthistory",
                'columns': [("pasthistorypath", "既往史路径"), ("pasthistoryvalue", "值"), ("pasthistorynotetype", "笔记类型")],
                'is_dynamic_view': True,
                'dynamic_sql': "SELECT DISTINCT pasthistorypath, pasthistoryvalue, pasthistorynotetype FROM eicu_crd.pasthistory ORDER BY pasthistorypath",
                'search_fields': [("pasthistorypath", "既往史路径 (包含)"), ("pasthistoryvalue", "值 (包含)")],
            }
        ]
    
    def get_profile_constants(self) -> Dict[str, Any]:
        return {
            'COHORT_ID_COL': 'patientunitstayid',
            'DEFAULT_VALUE_COLUMN': 'labresult',
            'DEFAULT_TEXT_VALUE_COLUMN': 'labresulttext',
            'DEFAULT_TIME_COLUMN': 'labresultoffset',
        }