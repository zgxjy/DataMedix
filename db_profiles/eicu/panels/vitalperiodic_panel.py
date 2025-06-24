# --- START OF FILE db_profiles/eicu/panels/vitalperiodic_panel.py ---
from PySide6.QtWidgets import QVBoxLayout, QGroupBox, QComboBox, QHBoxLayout, QLabel
from typing import Optional

from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.value_aggregation_widget import ValueAggregationWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class EicuVitalPeriodicPanel(BaseSourceConfigPanel):
    """用于配置从 eicu.vitalperiodic 表提取数据的面板。"""

    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0, 0, 0, 0)
        panel_layout.setSpacing(10)

        filter_group = QGroupBox("筛选生命体征 (eicu.vitalperiodic)")
        filter_layout = QHBoxLayout(filter_group)
        filter_layout.addWidget(QLabel("生命体征项:"))
        self.vital_signs_combo = QComboBox()
        items = ["sao2", "heartrate", "respiration", "cvp", "etco2", 
                 "systemicsystolic", "systemicdiastolic", "systemicmean",
                 "pasystolic", "padiastolic", "pamean", "st1", "st2", "st3",
                 "icp", "temperature", "cardiacoutput"]
        self.vital_signs_combo.addItems(sorted(items))
        self.vital_signs_combo.currentTextChanged.connect(self.config_changed_signal.emit)
        filter_layout.addWidget(self.vital_signs_combo)
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
        return "e-ICU 生命体征 (vitalperiodic)"

    def get_panel_config(self) -> dict:
        selected_vital = self.vital_signs_combo.currentText()
        if not selected_vital: return {}
        
        return {
            "source_event_table": "eicu.vitalperiodic",
            "item_id_column_in_event_table": None,
            "selected_item_ids": [],
            "value_column_to_extract": selected_vital,
            "time_column_in_event_table": "observationoffset",
            "aggregation_methods": self.value_agg_widget.get_selected_methods(),
            "event_outputs": {},
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": selected_vital,
            "cte_join_on_cohort_override": None,
        }

    def clear_panel_state(self):
        self.vital_signs_combo.setCurrentIndex(0)
        self.value_agg_widget.clear_selections()
        if self.time_window_widget.combo_box.count() > 0:
            self.time_window_widget.combo_box.setCurrentIndex(0)