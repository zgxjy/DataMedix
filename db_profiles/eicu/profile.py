# --- START OF FILE db_profiles/eicu/profile.py ---
from typing import List, Tuple, Callable, Dict, Any

from db_profiles.base_profile import BaseDbProfile

# 导入所有新的Panel和基础信息模块
from .panels.lab_panel import EicuLabPanel
from .panels.nursecharting_panel import EicuNurseChartingPanel
from .panels.medication_panel import EicuMedicationPanel
from .panels.diagnosis_panel import EicuDiagnosisPanel
from .panels.infusiondrug_panel import EicuInfusionDrugPanel
from .panels.treatment_panel import EicuTreatmentPanel
from .panels.vitalperiodic_panel import EicuVitalPeriodicPanel
from . import base_info_modules as eicu_base_info

class EICUProfile(BaseDbProfile):
    """e-ICU Collaborative Research Database 的具体配置画像。"""

    def get_display_name(self) -> str:
        return "eICU v2.0"

    def get_default_connection_params(self) -> Dict[str, str]:
        return {"dbname": "eicu", "user": "postgres"}
        
    def get_cohort_table_schema(self) -> str:
        return "eicu_data"

    def get_source_panels(self) -> List[Tuple[str, Any]]:
        return [
            ("化验 (lab)", EicuLabPanel),
            ("护理记录 (nursecharting)", EicuNurseChartingPanel),
            ("药物 (medication)", EicuMedicationPanel),
            ("诊断 (diagnosis)", EicuDiagnosisPanel),
            ("输液 (infusiondrug)", EicuInfusionDrugPanel),
            ("治疗 (treatment)", EicuTreatmentPanel),
            ("生命体征 (vitalperiodic)", EicuVitalPeriodicPanel),
        ]

    def get_base_info_modules(self) -> List[Tuple[str, str, Callable]]:
        return [
            ("人口学、体格及APACHE评分", "demography_apache", eicu_base_info.add_demography_and_apache),
            ("实验室指标 (首次/24h平均)", "lab_values", eicu_base_info.add_lab_values_eicu),
            ("生命体征 (首次/24h平均)及BMI", "vital_signs", eicu_base_info.add_vital_signs_eicu),
            ("合并症 (0/1变量)", "comorbidities", eicu_base_info.add_comorbidities_eicu),
        ]

    def get_cohort_creation_configs(self) -> Dict[str, Dict[str, Any]]:
        return {
            "diagnosis": {
                "display_name": "按诊断字符串筛选",
                "event_table": "public.diagnosis",
                "dictionary_table": None,
                "event_icd_col": "diagnosisstring",
                "dict_icd_col": None, "dict_title_col": None,
                "event_seq_num_col": "diagnosispriority",
                "event_time_col": "diagnosisoffset",
                "search_fields": [("diagnosisstring", "诊断字符串 (包含)")],
            },
            "treatment": {
                "display_name": "按治疗路径筛选",
                "event_table": "public.treatment",
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
                'table_name': "public.diagnosis",
                'columns': [("diagnosisstring", "诊断字符串"), ("icd9code", "ICD9码"), ("diagnosispriority", "优先级")],
                'is_dynamic_view': True,
                'dynamic_sql': "SELECT DISTINCT diagnosisstring, icd9code, diagnosispriority FROM public.diagnosis ORDER BY diagnosisstring",
                'search_fields': [("diagnosisstring", "诊断字符串 (包含)"), ("icd9code", "ICD9码 (包含)")],
            },
            {
                'display_name': "2. 手术编码 (admissiondx)",
                'table_name': "public.admissiondx",
                'columns': [("admitdxpath", "入院诊断路径"), ("admitdxtext", "入院诊断文本")],
                'is_dynamic_view': True,
                'dynamic_sql': "SELECT DISTINCT admitdxpath, admitdxtext FROM public.admissiondx ORDER BY admitdxpath",
                'search_fields': [("admitdxpath", "诊断路径 (包含)"), ("admitdxtext", "诊断文本 (包含)")],
            },
            {
                'display_name': "3. 输液编码 (infusiondrug)",
                'table_name': "public.infusiondrug",
                'columns': [("drugname", "药物名称"), ("drugrate", "速率"), ("infusionrate", "输液速率"), ("drugamount", "药量")],
                'is_dynamic_view': True,
                'dynamic_sql': "SELECT DISTINCT drugname, drugrate, infusionrate, drugamount FROM public.infusiondrug WHERE drugname IS NOT NULL ORDER BY drugname",
                'search_fields': [("drugname", "药物名称 (包含)")],
            },
            {
                'display_name': "4. 实验室编码 (lab)",
                'table_name': "public.lab",
                'columns': [("labname", "化验名称"), ("labmeasurenameinterface", "测量单位"), ("labresulttext", "文本结果")],
                'is_dynamic_view': True,
                'dynamic_sql': "SELECT DISTINCT labname, labmeasurenameinterface, labresulttext FROM public.lab ORDER BY labname",
                'search_fields': [("labname", "化验名称 (包含)"), ("labmeasurenameinterface", "测量单位 (包含)"), ("labresulttext", "文本结果 (包含)")],
            },
            {
                'display_name': "5. 药物编码 (medication)",
                'table_name': "public.medication",
                'columns': [("drugname", "药物名称"), ("dosage", "剂量"), ("routeadmin", "给药途径"), ("frequency", "频率")],
                'is_dynamic_view': True,
                'dynamic_sql': "SELECT DISTINCT drugname, dosage, routeadmin, frequency FROM public.medication WHERE drugname IS NOT NULL ORDER BY drugname",
                'search_fields': [("drugname", "药物名称 (包含)"), ("routeadmin", "给药途径 (包含)")],
            },
            {
                'display_name': "6. 既往史编码 (pasthistory)",
                'table_name': "public.pasthistory",
                'columns': [("pasthistorypath", "既往史路径"), ("pasthistoryvalue", "值"), ("pasthistorynotetype", "笔记类型")],
                'is_dynamic_view': True,
                'dynamic_sql': "SELECT DISTINCT pasthistorypath, pasthistoryvalue, pasthistorynotetype FROM public.pasthistory ORDER BY pasthistorypath",
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

    def get_event_table_join_key(self, event_table_name: str) -> str:
        # e-ICU的所有事件表都用 patientunitstayid 连接
        return "patientunitstayid"

    def get_cohort_join_key(self, event_table_name: str) -> str:
        # e-ICU的队列主键总是 patientunitstayid，忽略事件表名
        return "patientunitstayid"