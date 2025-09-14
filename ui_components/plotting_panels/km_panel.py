# ui_components/plotting_panels/km_panel.py
from PySide6.QtWidgets import QWidget, QVBoxLayout, QFormLayout, QComboBox, QLineEdit, QCheckBox, QLabel, QCompleter
from PySide6.QtCore import Qt

class KM_Panel(QWidget):
    """一个专门用于配置Kaplan-Meier曲线参数的UI面板。"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        form_layout = QFormLayout()

        self.time_col_combo = QComboBox()
        self.event_col_combo = QComboBox()
        self.group_col_combo = QComboBox()
        self.title_input = QLineEdit("Kaplan-Meier Survival Curve")
        self.show_ci_check = QCheckBox("显示95%置信区间")
        self.show_ci_check.setChecked(True)
        self.show_pvalue_check = QCheckBox("显示Log-rank检验p值 (需分组)")
        self.show_pvalue_check.setChecked(True)

        # 为所有列选择框添加搜索功能
        for combo_box in [self.time_col_combo, self.event_col_combo, self.group_col_combo]:
            combo_box.setEditable(True); combo_box.setInsertPolicy(QComboBox.NoInsert)
            combo_box.completer().setCompletionMode(QCompleter.PopupCompletion)
            combo_box.completer().setFilterMode(Qt.MatchContains)

        time_label = QLabel("时间列 (Duration):"); time_label.setToolTip("选择表示生存时间的数值列（如天数、月数）。")
        event_label = QLabel("事件列 (Event Observed):"); event_label.setToolTip("选择表示事件是否发生的列（1=事件发生, 0=删失）。")
        group_label = QLabel("分组列 (Group, 可选):"); group_label.setToolTip("选择用于比较不同曲线的分类列（如治疗方案、性别）。")

        form_layout.addRow(time_label, self.time_col_combo)
        form_layout.addRow(event_label, self.event_col_combo)
        form_layout.addRow(group_label, self.group_col_combo)
        form_layout.addRow("图表标题:", self.title_input)
        
        layout.addLayout(form_layout)
        layout.addWidget(self.show_ci_check); layout.addWidget(self.show_pvalue_check)
        layout.addStretch()

    def update_columns(self, df):
        if df is None or df.empty:
            for combo in [self.time_col_combo, self.event_col_combo, self.group_col_combo]: combo.clear()
            return

        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        all_cols = df.columns.tolist()
        
        for combo, cols, is_optional in [(self.time_col_combo, numeric_cols, False), 
                                          (self.event_col_combo, all_cols, False), 
                                          (self.group_col_combo, all_cols, True)]:
            current_text = combo.currentText()
            combo.clear()
            if is_optional: combo.addItem("")
            combo.addItems(cols)
            if current_text in cols: combo.setCurrentText(current_text)

    def get_config(self):
        group_col = self.group_col_combo.currentText()
        return {
            "time_col": self.time_col_combo.currentText(),
            "event_col": self.event_col_combo.currentText(),
            "group_col": group_col if group_col else None,
            "title": self.title_input.text(),
            "show_ci": self.show_ci_check.isChecked(),
            "show_pvalue": self.show_pvalue_check.isChecked() and bool(group_col)
        }