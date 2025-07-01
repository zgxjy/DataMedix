# --- START OF FULLY CORRECTED AND REFACTORED FILE: db_profiles/mimic_iv/panels/note_events_panel.py ---
from PySide6.QtWidgets import (QVBoxLayout, QGroupBox, QLineEdit, QHBoxLayout, QLabel, 
                               QCheckBox, QWidget, QRadioButton, QButtonGroup,
                               QStackedWidget, QComboBox)
from PySide6.QtCore import Qt, Slot

from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class NoteEventsPanel(BaseSourceConfigPanel):
    """
    用于配置从MIMIC-IV Note模块中提取数据的统一面板。
    已根据用户反馈重构，引入明确的“通用模式”和“快捷模式”选择。
    已修正Schema名称为 'mimiciv_note'。
    """

    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0, 0, 0, 0)
        panel_layout.setSpacing(10)

        # 1. 笔记类型选择 (保持不变)
        type_group = QGroupBox("1. 选择笔记源数据表")
        type_layout = QHBoxLayout(type_group)
        self.note_type_group = QButtonGroup(self)
        self.radio_discharge = QRadioButton("出院小结 (discharge)")
        self.radio_radiology = QRadioButton("放射学报告 (radiology)")
        self.note_type_group.addButton(self.radio_discharge)
        self.note_type_group.addButton(self.radio_radiology)
        type_layout.addWidget(self.radio_discharge)
        type_layout.addWidget(self.radio_radiology)
        type_layout.addStretch()
        self.note_type_group.buttonClicked.connect(self._on_note_type_changed)
        panel_layout.addWidget(type_group)

        # 2. 提取模式选择 (核心修改)
        mode_group = QGroupBox("2. 选择提取模式")
        mode_layout = QHBoxLayout(mode_group)
        self.mode_selection_group = QButtonGroup(self)
        self.radio_general_mode = QRadioButton("通用模式 (筛选并聚合笔记)")
        self.radio_quick_mode = QRadioButton("快捷模式 (提取特定信息)")
        self.mode_selection_group.addButton(self.radio_general_mode)
        self.mode_selection_group.addButton(self.radio_quick_mode)
        mode_layout.addWidget(self.radio_general_mode)
        mode_layout.addWidget(self.radio_quick_mode)
        mode_layout.addStretch()
        self.mode_selection_group.buttonClicked.connect(self._on_extraction_mode_changed)
        panel_layout.addWidget(mode_group)
        
        # 3. 通用模式的配置组
        self.general_mode_group = QGroupBox("3. 通用模式配置")
        general_mode_layout = QVBoxLayout(self.general_mode_group)
        # 3.1 筛选条件
        text_filter_layout = QHBoxLayout()
        text_filter_layout.addWidget(QLabel("笔记主文本 (`text`) 包含 (ILIKE):"))
        self.text_contains_input = QLineEdit()
        self.text_contains_input.setPlaceholderText("可选，例如: history of hypertension")
        self.text_contains_input.textChanged.connect(self.config_changed_signal.emit)
        text_filter_layout.addWidget(self.text_contains_input)
        general_mode_layout.addLayout(text_filter_layout)
        self.detail_filter_stack = QStackedWidget()
        self._create_detail_filter_widgets()
        general_mode_layout.addWidget(self.detail_filter_stack)
        # 3.2 提取逻辑
        self.cb_concat = QCheckBox("拼接所有匹配的笔记文本")
        self.cb_first = QCheckBox("提取第一份匹配的笔记文本")
        self.cb_last = QCheckBox("提取最后一份匹配的笔记文本")
        self.cb_count = QCheckBox("计算匹配的笔记数量")
        self.cb_concat.stateChanged.connect(self.config_changed_signal.emit)
        self.cb_first.stateChanged.connect(self.config_changed_signal.emit)
        self.cb_last.stateChanged.connect(self.config_changed_signal.emit)
        self.cb_count.stateChanged.connect(self.config_changed_signal.emit)
        general_mode_layout.addWidget(self.cb_concat)
        general_mode_layout.addWidget(self.cb_first)
        general_mode_layout.addWidget(self.cb_last)
        general_mode_layout.addWidget(self.cb_count)
        panel_layout.addWidget(self.general_mode_group)

        # 4. 快捷模式的配置组
        self.quick_mode_group = QGroupBox("3. 快捷模式配置")
        quick_mode_layout = QVBoxLayout(self.quick_mode_group)
        quick_mode_layout.addWidget(QLabel("从笔记文本中提取特定值 (使用正则表达式):"))
        self.ef_extractor_cb = QCheckBox("射血分数 (EF)")
        self.ef_extractor_cb.stateChanged.connect(self.config_changed_signal.emit)
        quick_mode_layout.addWidget(self.ef_extractor_cb)
        panel_layout.addWidget(self.quick_mode_group)
        
        # 5. 时间窗口 (两个模式共用)
        time_group = QGroupBox("时间窗口")
        time_layout = QVBoxLayout(time_group)
        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口 (基于 charttime):")
        self.time_window_widget.time_window_changed.connect(self.config_changed_signal.emit)
        time_layout.addWidget(self.time_window_widget)
        panel_layout.addWidget(time_group)

        self.setLayout(panel_layout)
        
        # 初始状态设置
        self.radio_discharge.setChecked(True)
        self.radio_general_mode.setChecked(True)
        self._on_extraction_mode_changed()

    def _create_detail_filter_widgets(self):
        discharge_widget = QWidget()
        layout = QHBoxLayout(discharge_widget)
        layout.setContentsMargins(0, 5, 0, 5)
        layout.addWidget(QLabel("（出院小结无常用详情筛选）"))
        layout.addStretch()
        
        radiology_widget = QWidget()
        radiology_layout = QVBoxLayout(radiology_widget)
        radiology_layout.setContentsMargins(0, 5, 0, 5)
        exam_name_layout = QHBoxLayout()
        exam_name_layout.addWidget(QLabel("检查名称 (`exam_name`) 包含:"))
        self.exam_name_input = QLineEdit()
        self.exam_name_input.textChanged.connect(self.config_changed_signal.emit)
        exam_name_layout.addWidget(self.exam_name_input)
        report_status_layout = QHBoxLayout()
        report_status_layout.addWidget(QLabel("报告状态 (`report_status`):"))
        self.report_status_combo = QComboBox()
        self.report_status_combo.addItems(["", "Final", "Preliminary"])
        self.report_status_combo.currentTextChanged.connect(self.config_changed_signal.emit)
        report_status_layout.addWidget(self.report_status_combo)
        report_status_layout.addStretch()
        radiology_layout.addLayout(exam_name_layout)
        radiology_layout.addLayout(report_status_layout)
        
        self.detail_filter_stack.addWidget(discharge_widget)
        self.detail_filter_stack.addWidget(radiology_widget)

    @Slot()
    def _on_note_type_changed(self):
        is_radiology = self.radio_radiology.isChecked()
        self.detail_filter_stack.setCurrentIndex(1 if is_radiology else 0)
        # 快捷提取中的EF通常只在放射学报告中，所以可以根据笔记类型控制其可用性
        self.ef_extractor_cb.setEnabled(is_radiology)
        if not is_radiology:
            self.ef_extractor_cb.setChecked(False)
        self.config_changed_signal.emit()

    @Slot()
    def _on_extraction_mode_changed(self):
        is_general = self.radio_general_mode.isChecked()
        self.general_mode_group.setEnabled(is_general)
        self.quick_mode_group.setEnabled(not is_general)
        self.config_changed_signal.emit()

    def populate_panel_if_needed(self):
        time_options = ["整个住院期间", "整个ICU期间"]
        self.time_window_widget.set_options(time_options)

    def get_friendly_source_name(self) -> str:
        note_type = "Discharge" if self.radio_discharge.isChecked() else "Radiology"
        mode = "General" if self.radio_general_mode.isChecked() else "Quick"
        return f"笔记 ({note_type} - {mode} Mode)"

    def clear_panel_state(self):
        self.radio_discharge.setChecked(True)
        self.radio_general_mode.setChecked(True)
        self._on_note_type_changed()
        self._on_extraction_mode_changed()
        
        # Clear general mode controls
        self.text_contains_input.clear()
        self.exam_name_input.clear()
        self.report_status_combo.setCurrentIndex(0)
        self.cb_concat.setChecked(False)
        self.cb_first.setChecked(False)
        self.cb_last.setChecked(False)
        self.cb_count.setChecked(False)
        
        # Clear quick mode controls
        self.ef_extractor_cb.setChecked(False)

        if self.time_window_widget.combo_box.count() > 0:
            self.time_window_widget.combo_box.setCurrentIndex(0)
        
        self.config_changed_signal.emit()

    def get_panel_config(self) -> dict:
        config = {
            "is_text_extraction": True,
            "value_column_to_extract": "text",
            "time_column_in_event_table": "charttime",
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "item_id_column_in_event_table": None,
            "selected_item_ids": [],
            "event_outputs": {},
            "aggregation_methods": {},
            "quick_extractors": {},
            "text_filter": None,
            "detail_filters": []
        }

        # 设置正确的表名 (Schema已修正)
        if self.radio_discharge.isChecked():
            config["source_event_table"] = "mimiciv_note.discharge"
            config["detail_table"] = "mimiciv_note.discharge_detail"
            config["primary_item_label_for_naming"] = "discharge"
        else: # Radiology
            config["source_event_table"] = "mimiciv_note.radiology"
            config["detail_table"] = "mimiciv_note.radiology_detail"
            config["primary_item_label_for_naming"] = "radiology"
            exam_name = self.exam_name_input.text().strip()
            if exam_name:
                config["detail_filters"].append(("exam_name", "ILIKE", f"%{exam_name}%"))
            report_status = self.report_status_combo.currentText()
            if report_status:
                config["detail_filters"].append(("report_status", "=", report_status))

        # 根据选择的模式填充配置
        if self.radio_general_mode.isChecked():
            config["text_filter"] = self.text_contains_input.text().strip()
            config["aggregation_methods"] = {
                "NOTE_CONCAT": self.cb_concat.isChecked(),
                "NOTE_FIRST": self.cb_first.isChecked(),
                "NOTE_LAST": self.cb_last.isChecked(),
                "NOTE_COUNT": self.cb_count.isChecked(),
            }
            if not any(config["aggregation_methods"].values()): return {} # 如果没选任何聚合则无效
        else: # Quick Mode
            if self.ef_extractor_cb.isChecked():
                # 这个正则表达式用于安全地捕获第二个分组 (数值)
                config["quick_extractors"]["ef"] = r'\b(?:LVEF|EF|Ejection\s*Fraction)\s*[:=]\s*(\d{1,2})\s*%?'
            
            if not config["quick_extractors"]: return {} # 如果没选任何提取项则无效

        return config

# --- END OF FULLY CORRECTED AND REFACTORED FILE ---