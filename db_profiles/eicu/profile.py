# --- START OF FILE db_profiles/eicu/profile.py ---
from typing import List, Tuple, Callable, Dict, Any

from db_profiles.base_profile import BaseDbProfile

from .panels.lab_panel import EicuLabPanel
from .panels.vitalperiodic_panel import EicuVitalPeriodicPanel
from . import base_info_modules as eicu_base_info

class EICUProfile(BaseDbProfile):
    """e-ICU Collaborative Research Database 的具体配置画像。"""

    def get_display_name(self) -> str:
        return "e-ICU v2.0"

    def get_default_connection_params(self) -> Dict[str, str]:
        return {
            "dbname": "eicu",
            "user": "postgres",
        }
        
    def get_cohort_table_schema(self) -> str:
        return "eicu_data"

    def get_source_panels(self) -> List[Tuple[str, Any]]:
        return [
            ("化验 (lab)", EicuLabPanel),
            ("生命体征 (vitalperiodic)", EicuVitalPeriodicPanel),
        ]

    def get_base_info_modules(self) -> List[Tuple[str, str, Callable]]:
        return [
            ("患者人口学信息 (patient table)", "demography", eicu_base_info.add_demography),
        ]

    def get_cohort_creation_configs(self) -> Dict[str, Dict[str, Any]]:
        """为 e-ICU 定义队列创建逻辑。"""
        return {
            "diagnosis": {
                "display_name": "按诊断字符串筛选",
                "event_table": "eicu.diagnosis",
                "dictionary_table": None,
                "event_icd_col": "diagnosisstring",
                "dict_icd_col": None,
                "dict_title_col": None,
                "event_seq_num_col": "diagnosispriority",
                "event_time_col": "diagnosisoffset",
                "search_fields": [("diagnosisstring", "诊断字符串 (包含)")],
            },
        }

    def get_dictionary_tables(self) -> List[Dict[str, Any]]:
        """e-ICU没有像MIMIC那样标准的字典表，但我们可以为lab表创建一个动态视图。"""
        return [
            {
                'display_name': "化验项 (lab table unique names)",
                'table_name': "eicu.lab", # 基础表
                'columns': [("labname", "Lab Name"), ("labmeasurenameinterface", "Measure Name")],
                'is_dynamic_view': True, # 这是一个动态视图
                'dynamic_sql': "SELECT DISTINCT labname, labmeasurenameinterface FROM eicu.lab ORDER BY labname",
                'search_fields': [("labname", "化验名称 (包含)"), ("labmeasurenameinterface", "测量单位 (包含)")],
            }
        ]
    
    def get_profile_constants(self) -> Dict[str, Any]:
        return {
            'COHORT_ID_COL': 'patientunitstayid',
            'DEFAULT_VALUE_COLUMN': 'labresult',
            'DEFAULT_TEXT_VALUE_COLUMN': 'labresulttext',
            'DEFAULT_TIME_COLUMN': 'labresultoffset',
        }