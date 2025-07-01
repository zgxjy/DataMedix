# --- START OF FILE db_profiles/base_profile.py ---
from abc import ABC, abstractmethod
from typing import List, Tuple, Callable, Dict, Any

class BaseDbProfile(ABC):
    """
    一个抽象基类，定义了支持一个新数据库所需的所有配置和组件。
    """

    @abstractmethod
    def get_display_name(self) -> str:
        """返回数据库的显示名称，例如 'MIMIC-IV'。"""
        pass

    @abstractmethod
    def get_default_connection_params(self) -> Dict[str, str]:
        """返回此数据库的默认连接参数 (dbname, user, host, port)。"""
        pass

    @abstractmethod
    def get_source_panels(self) -> List[Tuple[str, Any]]:
        """
        返回一个列表，包含“专项数据”选项卡可用的所有UI配置面板。
        格式: [("在UI上显示的名称", PanelClass), ...]
        """
        pass

    @abstractmethod
    def get_base_info_modules(self) -> List[Tuple[str, str, Callable]]:
        """
        返回一个列表，包含“基础数据”选项卡可用的所有数据添加模块。
        格式: [("UI显示名", "内部键", a_function_that_returns_sql_parts), ...]
        函数签名: func(table_name, sql_accumulator, db_profile) -> (list_of_col_defs, update_sql_str)
        """
        pass

    @abstractmethod
    def get_cohort_creation_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        返回一个字典，为“查找与创建队列”选项卡提供特定于数据库的配置。
        键是模式 (如 'disease', 'procedure')，值是该模式的配置字典。
        """
        pass

    @abstractmethod
    def get_dictionary_tables(self) -> List[Dict[str, Any]]:
        """
        返回一个列表，包含“数据字典”选项卡可用的所有字典表及其配置。
        格式: [{ 'display_name': str, 'table_name': str, 'columns': [...], 'search_fields': [...] }, ...]
        """
        pass
    
    @abstractmethod
    def get_cohort_table_schema(self) -> str:
        """返回用于存放生成队列的schema名称。"""
        pass

    @abstractmethod
    def get_cohort_join_key(self, event_table_name: str) -> str:
        """
        返回在专项数据提取时，用于连接【目标队列数据表】的主键名称。
        
        Args:
            event_table_name (str): 正在被查询的事件表的名称，以便决定使用哪个键。
        ...
        """
        pass

    @abstractmethod
    def get_event_table_join_key(self, event_table_name: str) -> str:
        """
        返回在专项数据提取时，用于连接【事件表】的主键名称。
        
        Args:
            event_table_name (str): 正在被查询的事件表的名称 (例如, 'mimiciv_icu.chartevents')。
        
        - 对于MIMIC-IV，需要根据表名判断是 'stay_id' 还是 'hadm_id'。
        - 对于e-ICU，所有事件表基本都使用 'patientunitstayid'。
        """
        pass