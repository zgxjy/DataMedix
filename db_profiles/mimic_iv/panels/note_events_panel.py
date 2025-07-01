# --- START OF FILE db_profiles/mimic_iv/panels/note_events_panel.py ---
from PySide6.QtWidgets import (QVBoxLayout, QGroupBox, QLineEdit, QHBoxLayout, QLabel, 
                               QCheckBox, QWidget, QRadioButton, QButtonGroup,
                               QStackedWidget, QComboBox)
from PySide6.QtCore import Qt, Slot

from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class NoteEventsPanel(BaseSourceConfigPanel):
    """
    用于配置从MIMIC-IV Note模块中提取数据的统一面板。
    正确处理MIMIC-IV v2.2中 discharge 和 radiology 作为独立表的结构。
    """

    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0, 0, 0, 0)
        panel_layout.setSpacing(10)

        # 1. 笔记类型选择 (核心)
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
        
        # 2. 筛选条件
        filter_group = QGroupBox("2. 筛选条件")
        filter_layout = QVBoxLayout(filter_group)
        
        # 2.1 通用文本筛选
        text_filter_layout = QHBoxLayout()
        text_filter_layout.addWidget(QLabel("笔记主文本 (`text`) 包含 (ILIKE):"))
        self.text_contains_input = QLineEdit()
        self.text_contains_input.setPlaceholderText("可选，例如: history of hypertension")
        self.text_contains_input.textChanged.connect(self.config_changed_signal.emit)
        text_filter_layout.addWidget(self.text_contains_input)
        filter_layout.addLayout(text_filter_layout)

        # 2.2 动态的详细筛选区域
        self.detail_filter_stack = QStackedWidget()
        self._create_detail_filter_widgets()
        filter_layout.addWidget(self.detail_filter_stack)
        panel_layout.addWidget(filter_group)

        # 3. 提取逻辑
        logic_group = QGroupBox("3. 提取逻辑")
        logic_layout = QVBoxLayout(logic_group)
        self.cb_concat = QCheckBox("拼接所有匹配的笔记文本")
        self.cb_concat.setChecked(True)
        self.cb_concat.stateChanged.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.cb_concat)
        self.cb_first = QCheckBox("提取第一份匹配的笔记文本")
        self.cb_first.stateChanged.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.cb_first)
        self.cb_last = QCheckBox("提取最后一份匹配的笔记文本")
        self.cb_last.stateChanged.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.cb_last)
        self.cb_count = QCheckBox("计算匹配的笔记数量")
        self.cb_count.stateChanged.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.cb_count)
        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口 (基于 charttime):")
        self.time_window_widget.time_window_changed.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.time_window_widget)
        panel_layout.addWidget(logic_group)

        # 4. 快捷提取项
        extractor_group = QGroupBox("4. 快捷提取结构化信息 (可选)")
        extractor_layout = QVBoxLayout(extractor_group)
        extractor_layout.addWidget(QLabel("从笔记文本中提取特定值 (使用正则表达式):"))
        self.ef_extractor_cb = QCheckBox("射血分数 (EF)")
        self.ef_extractor_cb.stateChanged.connect(self.config_changed_signal.emit)
        extractor_layout.addWidget(self.ef_extractor_cb)
        panel_layout.addWidget(extractor_group)
        
        self.setLayout(panel_layout)
        # 初始状态设置
        self.radio_discharge.setChecked(True)
        self._on_note_type_changed()

    def _create_detail_filter_widgets(self):
        # 创建出院小结的筛选Widget (现在是空的，因为其detail表字段不常用作筛选)
        discharge_widget = QWidget()
        discharge_layout = QHBoxLayout(discharge_widget)
        discharge_layout.setContentsMargins(0, 5, 0, 5)
        discharge_layout.addWidget(QLabel("（无常用详情筛选）"))
        discharge_layout.addStretch()
        
        # 创建放射学报告的筛选Widget
        radiology_widget = QWidget()
        radiology_layout = QVBoxLayout(radiology_widget)
        radiology_layout.setContentsMargins(0, 5, 0, 5)
        # -- 检查名称
        exam_name_layout = QHBoxLayout()
        exam_name_layout.addWidget(QLabel("检查名称 (`exam_name`) 包含:"))
        self.exam_name_input = QLineEdit()
        self.exam_name_input.textChanged.connect(self.config_changed_signal.emit)
        exam_name_layout.addWidget(self.exam_name_input)
        radiology_layout.addLayout(exam_name_layout)
        # -- 报告状态
        report_status_layout = QHBoxLayout()
        report_status_layout.addWidget(QLabel("报告状态 (`report_status`):"))
        self.report_status_combo = QComboBox()
        self.report_status_combo.addItems(["", "Final", "Preliminary"]) # "" 表示不筛选
        self.report_status_combo.currentTextChanged.connect(self.config_changed_signal.emit)
        report_status_layout.addWidget(self.report_status_combo)
        report_status_layout.addStretch()
        radiology_layout.addLayout(report_status_layout)
        
        # 将widgets添加到stack中
        self.detail_filter_stack.addWidget(discharge_widget)
        self.detail_filter_stack.addWidget(radiology_widget)

    @Slot()
    def _on_note_type_changed(self):
        if self.radio_discharge.isChecked():
            self.detail_filter_stack.setCurrentIndex(0)
        else:
            self.detail_filter_stack.setCurrentIndex(1)
        self.config_changed_signal.emit()

    def populate_panel_if_needed(self):
        time_options = ["整个住院期间", "整个ICU期间"]
        self.time_window_widget.set_options(time_options)

    def get_friendly_source_name(self) -> str:
        if self.radio_discharge.isChecked():
            return "临床笔记 (Discharge)"
        return "临床笔记 (Radiology)"

    def clear_panel_state(self):
        self.radio_discharge.setChecked(True)
        self.text_contains_input.clear()
        self.exam_name_input.clear()
        self.report_status_combo.setCurrentIndex(0)
        
        self.cb_concat.setChecked(True)
        self.cb_first.setChecked(False)
        self.cb_last.setChecked(False)
        self.cb_count.setChecked(False)
        self.ef_extractor_cb.setChecked(False)

        if self.time_window_widget.combo_box.count() > 0:
            self.time_window_widget.combo_box.setCurrentIndex(0)
        
        self.config_changed_signal.emit()

    def get_panel_config(self) -> dict:
        note_aggregation_methods = {
            "NOTE_CONCAT": self.cb_concat.isChecked(),
            "NOTE_FIRST": self.cb_first.isChecked(),
            "NOTE_LAST": self.cb_last.isChecked(),
            "NOTE_COUNT": self.cb_count.isChecked(),
        }

        selected_extractors = {}
        if self.ef_extractor_cb.isChecked():
            selected_extractors["ef"] = r'\b(LVEF|EF|Ejection\s*Fraction)\s*[:=]\s*(\d{1,2})\s*%?'

        if not any(note_aggregation_methods.values()) and not selected_extractors:
            return {}
        
        config = {
            "is_text_extraction": True,
            "event_outputs": {},
            "value_column_to_extract": "text",
            "time_column_in_event_table": "charttime",
            "text_filter": self.text_contains_input.text().strip(),
            "aggregation_methods": note_aggregation_methods,
            "quick_extractors": selected_extractors,
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            # 这两个字段不再需要，因为我们不按ID筛选
            "item_id_column_in_event_table": None,
            "selected_item_ids": [],
        }

        detail_filters = []
        if self.radio_discharge.isChecked():
            config["source_event_table"] = "mimic_note.discharge"
            config["detail_table"] = "mimic_note.discharge_detail"
            config["primary_item_label_for_naming"] = "discharge"
            # discharge_detail 筛选逻辑可以根据需要在这里添加

        elif self.radio_radiology.isChecked():
            config["source_event_table"] = "mimic_note.radiology"
            config["detail_table"] = "mimic_note.radiology_detail"
            config["primary_item_label_for_naming"] = "radiology"
            
            exam_name = self.exam_name_input.text().strip()
            if exam_name:
                detail_filters.append(("exam_name", "ILIKE", f"%{exam_name}%"))
            
            report_status = self.report_status_combo.currentText()
            if report_status:
                detail_filters.append(("report_status", "=", report_status))

        config["detail_filters"] = detail_filters
        
        return config