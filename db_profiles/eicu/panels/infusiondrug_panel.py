# --- START OF FILE db_profiles/eicu/panels/infusiondrug_panel.py ---
from PySide6.QtWidgets import (QVBoxLayout, QGroupBox, QLineEdit, QHBoxLayout, QLabel)
from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.event_output_widget import EventOutputWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class EicuInfusionDrugPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self); panel_layout.setContentsMargins(0, 0, 0, 0); panel_layout.setSpacing(10)
        filter_group = QGroupBox("筛选输液药物 (eicu_crd.infusiondrug)")
        filter_layout = QHBoxLayout(filter_group)
        filter_layout.addWidget(QLabel("药物名称 (drugname) 包含 (ILIKE):"))
        self.drug_name_input = QLineEdit()
        self.drug_name_input.setPlaceholderText("例如: norepinephrine, insulin")
        self.drug_name_input.textChanged.connect(self.config_changed_signal.emit)
        filter_layout.addWidget(self.drug_name_input)
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
        self.time_window_widget.set_options([
            "ICU入院后24小时 (0-1440分钟)", "ICU入院后48小时 (0-2880分钟)", "整个ICU期间",
        ])

    def get_friendly_source_name(self) -> str: return "e-ICU 输液药物 (infusiondrug)"

    def get_panel_config(self) -> dict:
        drug_name = self.drug_name_input.text().strip()
        if not drug_name: return {}
        return {
            "source_event_table": "eicu_crd.infusiondrug",
            "item_id_column_in_event_table": "drugname",
            "selected_item_ids": [f"%{drug_name}%"], # 使用ILIKE
            "value_column_to_extract": None,
            "time_column_in_event_table": "infusionoffset",
            "aggregation_methods": {}, "event_outputs": self.event_output_widget.get_selected_outputs(),
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": drug_name, "cte_join_on_cohort_override": None,
        }

    def clear_panel_state(self):
        self.drug_name_input.clear()
        self.event_output_widget.clear_selections()
        if self.time_window_widget.combo_box.count() > 0: self.time_window_widget.combo_box.setCurrentIndex(0)