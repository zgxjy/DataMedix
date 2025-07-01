# --- START OF FILE db_profiles/mimic_iv/profile.py ---
from typing import List, Tuple, Callable, Dict, Any

from db_profiles.base_profile import BaseDbProfile

from .panels.chartevents_panel import CharteventsConfigPanel
from .panels.labevents_panel import LabeventsConfigPanel
from .panels.medication_panel import MedicationConfigPanel
from .panels.procedure_panel import ProcedureConfigPanel
from .panels.diagnosis_panel import DiagnosisConfigPanel
from .panels.note_events_panel import NoteEventsPanel
from . import base_info_modules as mimic_base_info

class MIMICIVProfile(BaseDbProfile):
    """MIMIC-IV 数据库的具体配置画像。"""

    def get_display_name(self) -> str:
        return "MIMIC-IV"

    def get_default_connection_params(self) -> Dict[str, str]:
        return {
            "dbname": "mimiciv",
            "user": "postgres",
        }

    def get_cohort_table_schema(self) -> str:
        return "mimiciv_data"

    def get_source_panels(self) -> List[Tuple[str, Any]]:
        return [
            ("监测指标 (chartevents)", CharteventsConfigPanel),
            ("化验 (labevents)", LabeventsConfigPanel),
            ("用药 (prescriptions)", MedicationConfigPanel),
            ("操作/手术 (procedures_icd)", ProcedureConfigPanel),
            ("诊断 (diagnoses_icd)", DiagnosisConfigPanel),
            ("临床笔记 (note)", NoteEventsPanel),
        ]

    def get_base_info_modules(self) -> List[Tuple[str, str, Callable]]:
        return [
            ("住院及人口学信息", "demography", mimic_base_info.add_demography),
            ("患者既往史 (Charlson)", "antecedent", mimic_base_info.add_antecedent),
            ("患者住院生命体征", "vital_sign", mimic_base_info.add_vital_sign),
            ("患者评分 (SOFA, SAPSII, etc.)", "scores", mimic_base_info.add_scores),
            ("患者住院红细胞相关指标", "blood_info", mimic_base_info.add_blood_info),
            ("患者住院心血管化验指标", "cardiovascular_lab", mimic_base_info.add_cardiovascular_lab),
            ("患者住院用药记录", "medications", mimic_base_info.add_medicine),
            ("患者住院手术记录", "surgery", mimic_base_info.add_surgeries),
            ("患者既往病史 (自定义ICD)", "past_diagnostic", mimic_base_info.add_past_diagnostic),
        ]

    def get_cohort_creation_configs(self) -> Dict[str, Dict[str, Any]]:
        return {
            "disease": {
                "display_name": "按疾病ICD筛选",
                "event_table": "mimiciv_hosp.diagnoses_icd",
                "dictionary_table": "mimiciv_hosp.d_icd_diagnoses",
                "event_icd_col": "icd_code",
                "dict_icd_col": "icd_code",
                "dict_title_col": "long_title",
                "event_seq_num_col": "seq_num",
                "event_time_col": None,
                "search_fields": [
                    ("long_title", "标题/描述"),
                    ("icd_code", "ICD代码 (精确)"),
                    ("icd_version", "ICD版本 (精确)"),
                    ],
            },
            "procedure": {
                "display_name": "按手术/操作ICD筛选",
                "event_table": "mimiciv_hosp.procedures_icd",
                "dictionary_table": "mimiciv_hosp.d_icd_procedures",
                "event_icd_col": "icd_code",
                "dict_icd_col": "icd_code",
                "dict_title_col": "long_title",
                "event_seq_num_col": "seq_num",
                "event_time_col": "chartdate",
                "search_fields": [
                    ("long_title", "标题/描述"),
                    ("icd_code", "ICD代码 (精确)"),
                    ("icd_version", "ICD版本 (精确)"),
                ],
            }
        }

    def get_dictionary_tables(self) -> List[Dict[str, Any]]:
        return [
            {
                'display_name': "监测/输出/操作项 (d_items)",
                'table_name': "mimiciv_icu.d_items",
                'columns': [("itemid", "ItemID"), ("label", "Label"), ("abbreviation", "Abbreviation"),
                            ("category", "Category"), ("param_type", "Param Type"),
                            ("unitname", "Unit Name"), ("linksto", "Links To")],
                'search_fields': [("label", "项目名 (Label)"), ("abbreviation", "缩写 (Abbreviation)"),
                                  ("category", "类别 (Category)"), ("param_type", "参数类型 (Param Type)"),
                                  ("unitname", "单位 (Unit Name)"), ("linksto", "关联表 (Links To)"),
                                  ("itemid", "ItemID (精确)")]
            },
            {
                'display_name': "化验项 (d_labitems)",
                'table_name': "mimiciv_hosp.d_labitems",
                'columns': [("itemid", "ItemID"), ("label", "Label"), ("fluid", "Fluid"), ("category", "Category")],
                'search_fields': [("label", "项目名 (Label)"), ("category", "类别 (Category)"),
                                  ("fluid", "体液类型 (Fluid)"), ("itemid", "ItemID (精确)")]
            },
            {
                'display_name': "诊断代码 (d_icd_diagnoses)",
                'table_name': "mimiciv_hosp.d_icd_diagnoses",
                'columns': [("icd_code", "ICD Code"), ("icd_version", "ICD Version"), ("long_title", "Long Title")],
                'search_fields': [("long_title", "诊断描述 (Long Title)"),
                                  ("icd_code", "诊断代码 (ICD Code 精确)"),
                                   ("icd_version", "ICD 版本 (精确)")]
            },
            {
                'display_name': "操作代码 (d_icd_procedures)",
                'table_name': "mimiciv_hosp.d_icd_procedures",
                'columns': [("icd_code", "ICD Code"), ("icd_version", "ICD Version"), ("long_title", "Long Title")],
                'search_fields': [("long_title", "操作描述 (Long Title)"),
                                  ("icd_code", "操作代码 (ICD Code 精确)"), ("icd_version", "ICD 版本 (精确)")]
            }
        ]
    
    def get_profile_constants(self) -> Dict[str, Any]:
        return {
            'DEFAULT_PAST_DIAGNOSIS_CATEGORIES': [
                "sleep apnea", "insomnia", "depressive", "anxiety", "anxiolytic",
                "diabetes", "hypertension", "myocardial infarction", "stroke", "asthma", "copd"
            ],
            'DEFAULT_VALUE_COLUMN': "valuenum",
            'DEFAULT_TEXT_VALUE_COLUMN': "value",
            'DEFAULT_TIME_COLUMN': "charttime",
        }

    def get_cohort_join_key(self, event_table_name: str) -> str:
        # 根据事件表的级别，决定队列表应该用哪个键去连接
        # 如果事件表是 ICU 级别的 (如 chartevents)，队列表就用 stay_id
        if 'chartevents' in event_table_name:
            return 'stay_id'
        # 如果事件表是住院级别的，队列表就用 hadm_id
        elif 'labevents' in event_table_name or \
             'prescriptions' in event_table_name or \
             'procedures_icd' in event_table_name or \
             'diagnoses_icd' in event_table_name or \
             'note' in event_table_name:
            return 'hadm_id'
        # 默认或未知情况，返回一个通用键，但这应该很少发生
        return 'hadm_id'
        
    def get_event_table_join_key(self, event_table_name: str) -> str:
        """
        根据事件表决定连接键。
        """
        # ICU 级别的事件表
        if 'chartevents' in event_table_name:
            return 'stay_id'
        # 住院级别的事件表
        elif 'labevents' in event_table_name or \
             'prescriptions' in event_table_name or \
             'procedures_icd' in event_table_name or \
             'diagnoses_icd' in event_table_name or \
             'note' in event_table_name:
            return 'hadm_id'
        # 默认返回一个最不可能出错的键，但理想情况下所有支持的表都应被覆盖
        return 'hadm_id'