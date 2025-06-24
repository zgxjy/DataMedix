# --- START OF FILE db_profiles/mimic_iv/panels/note_events_panel.py ---
from PySide6.QtWidgets import (QVBoxLayout, QGroupBox, QLineEdit, QHBoxLayout, QLabel, 
                               QCheckBox, QScrollArea, QWidget)
from PySide6.QtCore import Qt, Slot
from typing import Optional

from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class NoteEventsPanel(BaseSourceConfigPanel):
    """用于配置从 mimic_note.note 表提取数据的面板，并支持快捷提取。"""

    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0, 0, 0, 0)
        panel_layout.setSpacing(10)

        # 1. 筛选条件
        filter_group = QGroupBox("1. 筛选笔记 (mimic_note.note)")
        filter_layout = QVBoxLayout(filter_group)
        category_layout = QHBoxLayout()
        category_layout.addWidget(QLabel("笔记类别 (category) 精确匹配:"))
        self.category_input = QLineEdit()
        self.category_input.setPlaceholderText("例如: Discharge summary, ECHO")
        self.category_input.textChanged.connect(self.config_changed_signal.emit)
        category_layout.addWidget(self.category_input)
        filter_layout.addLayout(category_layout)
        text_filter_layout = QHBoxLayout()
        text_filter_layout.addWidget(QLabel("文本内容 (text) 包含 (ILIKE):"))
        self.text_contains_input = QLineEdit()
        self.text_contains_input.setPlaceholderText("可选，例如: history of hypertension")
        self.text_contains_input.textChanged.connect(self.config_changed_signal.emit)
        text_filter_layout.addWidget(self.text_contains_input)
        filter_layout.addLayout(text_filter_layout)
        panel_layout.addWidget(filter_group)

        # 2. 提取逻辑
        logic_group = QGroupBox("2. 提取逻辑")
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

        # 3. 快捷提取项
        extractor_group = QGroupBox("3. 快捷提取结构化信息 (可选)")
        extractor_layout = QVBoxLayout(extractor_group)
        extractor_layout.addWidget(QLabel("从笔记文本中提取特定值 (使用正则表达式):"))
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setMinimumHeight(100)
        self.extractor_widget = QWidget()
        self.extractor_layout = QVBoxLayout(self.extractor_widget)
        scroll_area.setWidget(self.extractor_widget)
        extractor_layout.addWidget(scroll_area)
        panel_layout.addWidget(extractor_group)

        self.setLayout(panel_layout)
        self._populate_extractors()

    def _populate_extractors(self):
        self.extractors = {}
        available_extractors = {
            "射血分数 (EF)": ("ef", r'\b(LVEF|EF|Ejection\s*Fraction)\s*[:=]\s*(\d{1,2})\s*%?'),
        }
        for display_name, (internal_key, pattern) in available_extractors.items():
            cb = QCheckBox(display_name)
            cb.stateChanged.connect(self.config_changed_signal.emit)
            self.extractor_layout.addWidget(cb)
            self.extractors[internal_key] = {"checkbox": cb, "pattern": pattern}
        self.extractor_layout.addStretch()

    def populate_panel_if_needed(self):
        time_options = ["整个住院期间", "整个ICU期间"]
        self.time_window_widget.set_options(time_options)

    def get_friendly_source_name(self) -> str:
        return "临床笔记 (note_events)"
    
    def clear_panel_state(self):
        self.category_input.clear()
        self.text_contains_input.clear()
        self.cb_concat.setChecked(True)
        self.cb_first.setChecked(False)
        self.cb_last.setChecked(False)
        self.cb_count.setChecked(False)
        for data in self.extractors.values():
            data["checkbox"].setChecked(False)
        if self.time_window_widget.combo_box.count() > 0:
            self.time_window_widget.combo_box.setCurrentIndex(0)

    def get_panel_config(self) -> dict:
        category = self.category_input.text().strip()
        text_contains = self.text_contains_input.text().strip()

        if not category:
            return {}

        note_aggregation_methods = {
            "NOTE_CONCAT": self.cb_concat.isChecked(),
            "NOTE_FIRST": self.cb_first.isChecked(),
            "NOTE_LAST": self.cb_last.isChecked(),
            "NOTE_COUNT": self.cb_count.isChecked(),
        }

        selected_extractors = {}
        for key, data in self.extractors.items():
            if data["checkbox"].isChecked():
                selected_extractors[key] = data["pattern"]

        # 只有在选择了至少一种提取方式时，配置才有效
        if not any(note_aggregation_methods.values()) and not selected_extractors:
            return {}
            
        config = {
            "source_type": "note_event",
            "source_event_table": "mimic_note.note",
            "item_id_column_in_event_table": "category",
            "selected_item_ids": [category],
            "text_filter": text_contains,
            "value_column_to_extract": "text",
            "time_column_in_event_table": "charttime",
            "aggregation_methods": note_aggregation_methods,
            "quick_extractors": selected_extractors, # 新增
            "is_text_extraction": True,
            "event_outputs": {},
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": category.replace(" ", "_"),
        }
        return config