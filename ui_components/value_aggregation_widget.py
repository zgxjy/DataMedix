# --- START OF FILE ui_components/value_aggregation_widget.py ---
from PySide6.QtWidgets import QWidget, QGridLayout, QCheckBox, QPushButton, QVBoxLayout, QHBoxLayout
from PySide6.QtCore import Signal, Qt, Slot
from app_config import AGGREGATION_METHODS_DISPLAY

class ValueAggregationWidget(QWidget):
    aggregation_changed = Signal()

    NUMERIC_ONLY_METHODS = [
        "MEAN", "MEDIAN", "SUM", "STDDEV_SAMP", "VAR_SAMP",
        "CV", "P25", "P75", "IQR", "RANGE"
    ]
    COUNT_METHOD_KEY = "COUNT"

    def __init__(self, parent=None):
        super().__init__(parent)
        self.agg_checkboxes = {}
        self._block_aggregation_signal = False
        self.grid_layout = QGridLayout()
        self.next_row = 0
        self.next_col = 0
        self.init_ui()

    def init_ui(self):
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
        
        # 使用实例变量 self.grid_layout
        for display_name, internal_key in AGGREGATION_METHODS_DISPLAY:
            cb = QCheckBox(display_name)
            cb.stateChanged.connect(self._emit_aggregation_changed_if_not_blocked)
            self.grid_layout.addWidget(cb, self.next_row, self.next_col, Qt.AlignmentFlag.AlignLeft)
            self.agg_checkboxes[internal_key] = cb
            
            self.next_col += 1
            if self.next_col >= 4:
                self.next_col = 0
                self.next_row += 1
        
        main_layout.addLayout(self.grid_layout)
        self.setLayout(main_layout)

    def add_custom_aggregation(self, display_name: str, internal_key: str, is_checked_by_default: bool = True):
        """ <<< NEW METHOD: 动态添一个自定义的聚合选项 """
        if internal_key in self.agg_checkboxes:
            # 如果已存在，则只更新状态和可见性
            self.agg_checkboxes[internal_key].setText(display_name)
            self.agg_checkboxes[internal_key].setVisible(True)
            return

        cb = QCheckBox(display_name)
        cb.stateChanged.connect(self._emit_aggregation_changed_if_not_blocked)
        cb.setChecked(is_checked_by_default)
        
        # 添加到布局的下一个可用位置
        self.grid_layout.addWidget(cb, self.next_row, self.next_col, Qt.AlignmentFlag.AlignLeft)
        self.agg_checkboxes[internal_key] = cb
        
        self.next_col += 1
        if self.next_col >= 4:
            self.next_col = 0
            self.next_row += 1
        
        self.aggregation_changed.emit() # 通知外部已更改

    @Slot()
    def _emit_aggregation_changed_if_not_blocked(self):
        if not self._block_aggregation_signal:
            self.aggregation_changed.emit()

    def _select_all_methods(self):
        self._block_aggregation_signal = True
        any_checkbox_state_actually_changed = False
        try:
            for cb in self.agg_checkboxes.values():
                if cb.isEnabled() and not cb.isChecked() and cb.isVisible(): # 只操作可见的
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
                if cb.isChecked() and cb.isVisible(): # 只操作可见的
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

                if is_text_mode and is_strictly_numeric_method:
                    cb.setEnabled(False)
                    if cb.isChecked():
                        cb.setChecked(False)
                else:
                    cb.setEnabled(True)

                if cb.isChecked() != original_checked_state:
                    any_checkbox_state_actually_changed_due_to_text_mode = True
                
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