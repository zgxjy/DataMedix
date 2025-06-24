# --- START OF MODIFIED ui_components/value_aggregation_widget.py ---
from PySide6.QtWidgets import QWidget, QGridLayout, QCheckBox, QPushButton, QVBoxLayout, QHBoxLayout # 确保 QHBoxLayout 导入
from PySide6.QtCore import Signal, Qt, Slot # 确保 Slot 导入
from app_config import AGGREGATION_METHODS_DISPLAY # 导入配置

class ValueAggregationWidget(QWidget):
    aggregation_changed = Signal()

    NUMERIC_ONLY_METHODS = [ # "MIN", "MAX" 从这里移除，因为它们对文本也有意义（字典序）
        "MEAN", "MEDIAN", "SUM", "STDDEV_SAMP", "VAR_SAMP",
        "CV", "P25", "P75", "IQR", "RANGE"
    ]
    # FIRST_VALUE, LAST_VALUE, COUNT, TIMESERIES_JSON, MIN, MAX 可以用于文本和数值

    COUNT_METHOD_KEY = "COUNT" # 这个保持不变

    def __init__(self, parent=None):
        super().__init__(parent)
        self.agg_checkboxes = {}
        self._block_aggregation_signal = False
        self.init_ui()

    # init_ui, _emit_aggregation_changed_if_not_blocked, _select_all_methods, _deselect_all_methods,
    # get_selected_methods, set_selected_methods, clear_selections 保持你之前修改后的版本即可。
    # 关键是 set_text_mode 的调整：

    def init_ui(self): # 确保这部分与你之前修改的一致
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(5)

        select_buttons_layout = QHBoxLayout()
        self.select_all_btn = QPushButton("全选")
        self.select_all_btn.clicked.connect(self._select_all_methods)
        select_buttons_layout.addWidget(self.select_all_btn)

        self.deselect_all_btn = QPushButton("全不选")
        self.deselect_all_btn.clicked.connect(self._deselect_all_methods)
        select_buttons_layout.addWidget(self.deselect_all_btn)
        select_buttons_layout.addStretch()
        main_layout.addLayout(select_buttons_layout)

        checkbox_layout = QGridLayout()
        row, col = 0, 0
        for display_name, internal_key in AGGREGATION_METHODS_DISPLAY: # 使用 app_config 中的定义
            cb = QCheckBox(display_name)
            cb.stateChanged.connect(self._emit_aggregation_changed_if_not_blocked)
            checkbox_layout.addWidget(cb, row, col, Qt.AlignmentFlag.AlignLeft)
            self.agg_checkboxes[internal_key] = cb
            
            col += 1
            if col >= 4:
                col = 0
                row += 1
        
        main_layout.addLayout(checkbox_layout)
        self.setLayout(main_layout)

    @Slot()
    def _emit_aggregation_changed_if_not_blocked(self):
        if not self._block_aggregation_signal:
            self.aggregation_changed.emit()

    def _select_all_methods(self):
        self._block_aggregation_signal = True
        any_checkbox_state_actually_changed = False
        try:
            for cb in self.agg_checkboxes.values():
                if cb.isEnabled() and not cb.isChecked():
                    cb.setChecked(True)
                    any_checkbox_state_actually_changed = True
        finally:
            self._block_aggregation_signal = False
        if any_checkbox_state_actually_changed:
            self.aggregation_changed.emit()

    def _deselect_all_methods(self):
        self._block_aggregation_signal = True
        any_checkbox_state_actually_changed = False
        try:
            for cb in self.agg_checkboxes.values():
                if cb.isChecked():
                    cb.setChecked(False)
                    any_checkbox_state_actually_changed = True
        finally:
            self._block_aggregation_signal = False
        if any_checkbox_state_actually_changed:
            self.aggregation_changed.emit()

    def get_selected_methods(self) -> dict:
        return {key: cb.isChecked() for key, cb in self.agg_checkboxes.items()}

    def set_selected_methods(self, methods_state: dict):
        self._block_aggregation_signal = True
        any_checkbox_state_actually_changed = False
        try:
            for key, cb in self.agg_checkboxes.items():
                new_state = methods_state.get(key, False)
                if cb.isChecked() != new_state:
                    cb.setChecked(new_state)
                    any_checkbox_state_actually_changed = True
        finally:
            self._block_aggregation_signal = False
        if any_checkbox_state_actually_changed:
            self.aggregation_changed.emit()

    def set_text_mode(self, is_text_mode: bool):
        self._block_aggregation_signal = True
        any_checkbox_state_actually_changed_due_to_text_mode = False
        try:
            for internal_key, cb in self.agg_checkboxes.items():
                is_strictly_numeric_method = internal_key in self.NUMERIC_ONLY_METHODS
                original_checked_state = cb.isChecked()

                # 对于 "TIMESERIES_JSON", 它对文本和数值都有效，所以不受 is_text_mode 直接禁用
                # MIN, MAX, FIRST_VALUE, LAST_VALUE, COUNT 对文本和数值也都有意义
                if is_text_mode and is_strictly_numeric_method:
                    cb.setEnabled(False)
                    if cb.isChecked():
                        cb.setChecked(False)
                else:
                    cb.setEnabled(True) # 确保其他情况下（包括 TIMESERIES_JSON）是启用的

                if cb.isChecked() != original_checked_state:
                    any_checkbox_state_actually_changed_due_to_text_mode = True
                
                # 更新 "Count" 的标签 (这部分逻辑保持)
                if internal_key == self.COUNT_METHOD_KEY:
                    original_display_name_for_count = "计数 (Count)"
                    for disp, key_in_config in AGGREGATION_METHODS_DISPLAY:
                        if key_in_config == self.COUNT_METHOD_KEY:
                            original_display_name_for_count = disp
                            break
                    cb.setText("文本计数 (Count Text)" if is_text_mode else original_display_name_for_count)
        finally:
            self._block_aggregation_signal = False
        
        if any_checkbox_state_actually_changed_due_to_text_mode:
            self.aggregation_changed.emit()

    def clear_selections(self):
        self._block_aggregation_signal = True
        any_checkbox_state_actually_changed = False
        try:
            for cb in self.agg_checkboxes.values():
                if cb.isChecked():
                    cb.setChecked(False)
                    any_checkbox_state_actually_changed = True
        finally:
            self._block_aggregation_signal = False
        if any_checkbox_state_actually_changed:
            self.aggregation_changed.emit()

# --- END OF MODIFIED ui_components/value_aggregation_widget.py ---