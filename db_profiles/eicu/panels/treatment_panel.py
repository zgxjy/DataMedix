# --- START OF FILE db_profiles/eicu/panels/treatment_panel.py ---
from PySide6.QtWidgets import (QVBoxLayout, QGroupBox, QLineEdit, QHBoxLayout, QLabel)
from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.event_output_widget import EventOutputWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class EicuTreatmentPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self); panel_layout.setContentsMargins(0, 0, 0, 0); panel_layout.setSpacing(10)
        filter_group = QGroupBox("筛选治疗 (public.treatment)")
        filter_layout = QHBoxLayout(filter_group)
        filter_layout.addWidget(QLabel("治疗路径 (treatmentstring) 包含 (ILIKE):"))
        self.treat_string_input = QLineEdit()
        self.treat_string_input.setPlaceholderText("例如: invasive ventilation")
        self.treat_string_input.textChanged.connect(self.config_changed_signal.emit)
        filter_layout.addWidget(self.treat_string_input)
        panel_layout.addWidget(filter_group)

        logic_group = QGroupBox("提取逻辑")
        logic_layout = QVBoxLayout(logic_group)
        self.event_output_widget = EventOutputWidget()
        self.event_output_widget.output_type_changed.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.event_output_widget)
        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口 (相对于ICU入院):")
        self.time_window_widget.time_window_changed.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.time_window_widget)
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)

    def populate_panel_if_needed(self):
        self.time_window_widget.set_options(["整个ICU期间"])

    def get_friendly_source_name(self) -> str: return "e-ICU 治疗 (treatment)"

    def get_panel_config(self) -> dict:
        treat_string = self.treat_string_input.text().strip()
        if not treat_string: return {}
        return {
            "source_event_table": "public.treatment",
            "item_id_column_in_event_table": "treatmentstring",
            "selected_item_ids": [f"%{treat_string}%"], # 使用ILIKE
            "value_column_to_extract": None,
            "time_column_in_event_table": "treatmentoffset",
            "aggregation_methods": {}, "event_outputs": self.event_output_widget.get_selected_outputs(),
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": treat_string, "cte_join_on_cohort_override": None,
        }

    def clear_panel_state(self):
        self.treat_string_input.clear()
        self.event_output_widget.clear_selections()
        if self.time_window_widget.combo_box.count() > 0: self.time_window_widget.combo_box.setCurrentIndex(0)