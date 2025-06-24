# --- START OF MODIFIED ui_components/event_output_widget.py ---
from PySide6.QtWidgets import QWidget, QGridLayout, QCheckBox, QGroupBox, QHBoxLayout, QPushButton, QVBoxLayout # 新增 QVBoxLayout
from PySide6.QtCore import Signal, Slot, Qt # 新增 Qt

class EventOutputWidget(QWidget):
    output_type_changed = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0,0,0,0)
        main_layout.setSpacing(5)

        select_buttons_layout = QHBoxLayout()
        self.select_all_btn = QPushButton("全选")
        self.select_all_btn.clicked.connect(self._select_all_outputs)
        select_buttons_layout.addWidget(self.select_all_btn)

        self.deselect_all_btn = QPushButton("全不选")
        self.deselect_all_btn.clicked.connect(self._deselect_all_outputs)
        select_buttons_layout.addWidget(self.deselect_all_btn)
        select_buttons_layout.addStretch()
        main_layout.addLayout(select_buttons_layout)

        checkbox_layout = QGridLayout()

        self.cb_exists = QCheckBox("是否存在 (Boolean)")
        self.cb_count_event = QCheckBox("发生次数 (Count Event)")

        self.output_checkboxes = {
            "exists": self.cb_exists,
            "countevt": self.cb_count_event
        }

        checkbox_layout.addWidget(self.cb_exists, 0, 0, Qt.AlignmentFlag.AlignLeft)
        checkbox_layout.addWidget(self.cb_count_event, 0, 1, Qt.AlignmentFlag.AlignLeft)

        for cb in self.output_checkboxes.values():
            cb.stateChanged.connect(lambda state: self.output_type_changed.emit())
        
        main_layout.addLayout(checkbox_layout)
        self.setLayout(main_layout)

    def _select_all_outputs(self):
        for cb in self.output_checkboxes.values(): cb.setChecked(True)
    def _deselect_all_outputs(self):
        for cb in self.output_checkboxes.values(): cb.setChecked(False)
    def get_selected_outputs(self) -> dict:
        return {key: cb.isChecked() for key, cb in self.output_checkboxes.items()}
    def set_selected_outputs(self, outputs_state: dict):
        any_changed = False
        for key, cb in self.output_checkboxes.items():
            new_state = outputs_state.get(key, False)
            if cb.isChecked() != new_state:
                cb.blockSignals(True); cb.setChecked(new_state); cb.blockSignals(False)
                any_changed = True
        if any_changed: self.output_type_changed.emit()
    def clear_selections(self):
        any_changed = False
        for cb in self.output_checkboxes.values():
            if cb.isChecked():
                cb.blockSignals(True); cb.setChecked(False); cb.blockSignals(False)
                any_changed = True
        if any_changed: self.output_type_changed.emit()

# --- END OF MODIFIED ui_components/event_output_widget.py ---