# --- START OF FILE db_profiles/eicu/panels/vitalperiodic_panel.py ---
from PySide6.QtWidgets import (QVBoxLayout, QHBoxLayout,
                               QApplication, QGroupBox, QLabel, QComboBox)
from PySide6.QtCore import Slot
from typing import Optional, Dict, Any

from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.value_aggregation_widget import ValueAggregationWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class EicuVitalPeriodicPanel(BaseSourceConfigPanel):
    """
    用于配置从 e-ICU 的 `vitalperiodic` 表提取生命体征信息的Panel。
    这是一个针对“宽表”设计的数值聚合类Panel。
    """
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0, 0, 0, 0)
        panel_layout.setSpacing(10)

        # 1. 选择要提取的生命体征
        # 因为每个生命体征都是一个独立的列，所以我们用下拉框而不是筛选器
        selection_group = QGroupBox("1. 选择生命体征")
        selection_layout = QHBoxLayout(selection_group)
        selection_layout.addWidget(QLabel("生命体征项目:"))
        self.vitals_combo = QComboBox()
        self.vitals_combo.currentTextChanged.connect(self.config_changed_signal.emit)
        selection_layout.addWidget(self.vitals_combo, 1)
        panel_layout.addWidget(selection_group)

        # 2. 配置提取逻辑
        logic_group = QGroupBox("2. 配置提取逻辑")
        logic_group_layout = QVBoxLayout(logic_group)
        
        self.value_agg_widget = ValueAggregationWidget()
        self.value_agg_widget.aggregation_changed.connect(self.config_changed_signal.emit)
        logic_group_layout.addWidget(self.value_agg_widget)

        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口 (相对于ICU入院):")
        self.time_window_widget.time_window_changed.connect(lambda: self.config_changed_signal.emit())
        logic_group_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)
        
        # vitalperiodic 的值都是数值，固定为非文本模式
        self.value_agg_widget.set_text_mode(False)

    def populate_panel_if_needed(self):
        """
        填充生命体征下拉框和时间窗口。
        """
        self.vitals_combo.blockSignals(True)
        self.vitals_combo.clear()
        # 提供一个(显示名称, 数据库列名)的元组列表
        vital_columns = [
            ("Heart Rate", "heartrate"),
            ("SaO2", "sao2"),
            ("Respiration", "respiration"),
            ("Temperature", "temperature"),
            ("Systolic BP", "systemicsystolic"),
            ("Diastolic BP", "systemicdiastolic"),
            ("Mean BP", "systemicmean"),
        ]
        for display_name, col_name in vital_columns:
            self.vitals_combo.addItem(display_name, col_name)
        self.vitals_combo.blockSignals(False)

        self.time_window_widget.set_options([
            "ICU入住24小时内",
            "ICU入住48小时内",
            "整个ICU期间",
        ])
        
        # 手动触发一次信号，确保初始状态被捕获
        self.config_changed_signal.emit()
        
    def get_friendly_source_name(self) -> str:
        return "e-ICU 生命体征 (vitalperiodic)"

# --- 替换 get_panel_config ---
    def get_panel_config(self) -> Dict[str, Any]:
        selected_vital_col = self.vitals_combo.currentData()
        aggregation_methods = self.value_agg_widget.get_selected_methods()

        if not selected_vital_col or not any(aggregation_methods.values()):
            return {}

        return {
            "source_event_table": "public.vitalperiodic",
            "item_id_column_in_event_table": None,
            "selected_item_ids": [],
            "value_column_to_extract": selected_vital_col,
            "time_column_in_event_table": "observationoffset",
            "aggregation_methods": aggregation_methods,
            "is_text_extraction": False,
            "event_outputs": {},
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": self.vitals_combo.currentText().split('(')[0].strip(),
            "cte_join_on_cohort_override": None,
            
            # [新增] UI 状态 (保存 Combo 的索引)
            "_ui_state": {
                "vitals_combo_index": self.vitals_combo.currentIndex()
            }
        }

    # --- 新增 set_panel_config ---
    def set_panel_config(self, config: dict):
        ui_state = config.get("_ui_state", {})
        
        # 1. 恢复生命体征选择
        # 注意：populate_panel_if_needed 必须先执行过，下拉框里才有内容
        if "vitals_combo_index" in ui_state:
            self.vitals_combo.setCurrentIndex(ui_state["vitals_combo_index"])
        
        # 2. 恢复聚合和时间
        self.value_agg_widget.set_selected_methods(config.get("aggregation_methods", {}))
        if "time_window_text" in config:
            self.time_window_widget.set_current_time_window_by_text(config["time_window_text"])


    def clear_panel_state(self):
        if self.vitals_combo.count() > 0:
            self.vitals_combo.setCurrentIndex(0)
        self.value_agg_widget.clear_selections()
        self.time_window_widget.clear_selection()
        
    def update_panel_action_buttons_state(self, general_config_ok: bool):
        # 这个Panel没有内部的筛选按钮，所以此方法为空
        pass

# --- END OF FILE db_profiles/eicu/panels/vitalperiodic_panel.py ---