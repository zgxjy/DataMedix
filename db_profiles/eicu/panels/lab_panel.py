# --- START OF FILE db_profiles/eicu/panels/lab_panel.py ---
from PySide6.QtWidgets import QVBoxLayout, QGroupBox, QLineEdit, QHBoxLayout, QLabel
from typing import Optional

from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.value_aggregation_widget import ValueAggregationWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class EicuLabPanel(BaseSourceConfigPanel):
    """用于配置从 eicu.lab 表提取数据的面板。"""

    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0, 0, 0, 0)
        panel_layout.setSpacing(10)

        filter_group = QGroupBox("筛选化验项目 (eicu.lab)")
        filter_layout = QHBoxLayout(filter_group)
        filter_layout.addWidget(QLabel("化验名称 (labname) - 精确匹配, 区分大小写:"))
        self.lab_name_input = QLineEdit()
        self.lab_name_input.setPlaceholderText("例如: creatinine, glucose, potassium")
        self.lab_name_input.textChanged.connect(self.config_changed_signal.emit)
        filter_layout.addWidget(self.lab_name_input)
        panel_layout.addWidget(filter_group)

        logic_group = QGroupBox("提取逻辑")
        logic_layout = QVBoxLayout(logic_group)
        self.value_agg_widget = ValueAggregationWidget()
        self.value_agg_widget.aggregation_changed.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.value_agg_widget)
        
        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口 (相对于ICU入院):")
        self.time_window_widget.time_window_changed.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)
        
        self.value_agg_widget.set_text_mode(False)

    def populate_panel_if_needed(self):
        time_options = [
            "ICU入院后24小时 (0-1440分钟)",
            "ICU入院后48小时 (0-2880分钟)",
            "整个ICU期间",
        ]
        self.time_window_widget.set_options(time_options)

    def get_friendly_source_name(self) -> str:
        return "e-ICU 化验 (lab)"

    def get_panel_config(self) -> dict:
        db_profile = self.get_db_profile()
        if not db_profile: return {}
        
        lab_name = self.lab_name_input.text().strip()
        if not lab_name: return {}
            
        constants = db_profile.get_profile_constants()
        
        return {
            "source_event_table": "eicu.lab",
            "item_id_column_in_event_table": "labname",
            "selected_item_ids": [lab_name],
            "value_column_to_extract": constants.get('DEFAULT_VALUE_COLUMN', 'labresult'),
            "time_column_in_event_table": constants.get('DEFAULT_TIME_COLUMN', 'labresultoffset'),
            "aggregation_methods": self.value_agg_widget.get_selected_methods(),
            "event_outputs": {},
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": lab_name,
            "cte_join_on_cohort_override": None,
        }

    def clear_panel_state(self):
        self.lab_name_input.clear()
        self.value_agg_widget.clear_selections()
        if self.time_window_widget.combo_box.count() > 0:
            self.time_window_widget.combo_box.setCurrentIndex(0)