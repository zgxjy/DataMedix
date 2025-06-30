# --- START OF FILE db_profiles/eicu/panels/nursecharting_panel.py ---
from PySide6.QtWidgets import (QVBoxLayout, QGroupBox, QComboBox, QHBoxLayout, 
                               QLabel, QRadioButton, QButtonGroup, QStackedWidget)
from typing import Optional

from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.value_aggregation_widget import ValueAggregationWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class EicuNurseChartingPanel(BaseSourceConfigPanel):
    """用于配置从 public.nursecharting 表提取数据的面板。"""

    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0, 0, 0, 0)
        panel_layout.setSpacing(10)

        filter_group = QGroupBox("筛选护理记录项 (public.nursecharting)")
        filter_layout = QHBoxLayout(filter_group)
        filter_layout.addWidget(QLabel("护理记录项:"))
        self.item_combo = QComboBox()
        items = ["GCS Total", "Heart Rate", "MAP", "O2 Saturation", "Respiratory Rate", "Pain Score"]
        self.item_combo.addItems(sorted(items))
        self.item_combo.currentTextChanged.connect(self.config_changed_signal.emit)
        filter_layout.addWidget(self.item_combo)
        panel_layout.addWidget(filter_group)
        
        logic_group = QGroupBox("提取逻辑")
        logic_layout = QVBoxLayout(logic_group)

        self.value_source_group = QButtonGroup(self)
        value_source_layout = QHBoxLayout()
        value_source_layout.addWidget(QLabel("提取值来源:"))
        self.rb_value_numeric = QRadioButton("数值 (nursingchartvalue)")
        self.rb_value_text = QRadioButton("文本 (nursingchartcelltypevalname)")
        self.rb_value_numeric.setChecked(True)
        self.value_source_group.addButton(self.rb_value_numeric)
        self.value_source_group.addButton(self.rb_value_text)
        value_source_layout.addWidget(self.rb_value_numeric)
        value_source_layout.addWidget(self.rb_value_text)
        value_source_layout.addStretch()
        logic_layout.addLayout(value_source_layout)
        self.value_source_group.buttonClicked.connect(self._on_value_source_changed)

        self.value_agg_widget = ValueAggregationWidget()
        self.value_agg_widget.aggregation_changed.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.value_agg_widget)
        
        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口 (相对于ICU入院):")
        self.time_window_widget.time_window_changed.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)
        
        self._on_value_source_changed()

    def _on_value_source_changed(self):
        is_text_mode = self.rb_value_text.isChecked()
        self.value_agg_widget.set_text_mode(is_text_mode)
        self.config_changed_signal.emit()

    def populate_panel_if_needed(self):
        self.time_window_widget.set_options([
            "ICU入院后24小时 (0-1440分钟)",
            "ICU入院后48小时 (0-2880分钟)",
            "整个ICU期间",
        ])

    def get_friendly_source_name(self) -> str:
        return "e-ICU 护理记录 (nursecharting)"

    def get_panel_config(self) -> dict:
        selected_item = self.item_combo.currentText()
        if not selected_item: return {}
        
        is_text_mode = self.rb_value_text.isChecked()
        value_col = "nursingchartcelltypevalname" if is_text_mode else "nursingchartvalue"
        
        return {
            "source_event_table": "public.nursecharting",
            "item_id_column_in_event_table": "nursingchartcelltypecat",
            "selected_item_ids": [selected_item],
            "value_column_to_extract": value_col,
            "time_column_in_event_table": "nursingchartoffset",
            "aggregation_methods": self.value_agg_widget.get_selected_methods(),
            "event_outputs": {},
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": selected_item,
            "is_text_extraction": is_text_mode,
            "cte_join_on_cohort_override": None,
        }

    def clear_panel_state(self):
        self.item_combo.setCurrentIndex(0)
        self.rb_value_numeric.setChecked(True)
        self._on_value_source_changed()
        self.value_agg_widget.clear_selections()
        if self.time_window_widget.combo_box.count() > 0:
            self.time_window_widget.combo_box.setCurrentIndex(0)