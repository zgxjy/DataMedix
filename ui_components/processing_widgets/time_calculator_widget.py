# ui_components/processing_widgets/time_calculator_widget.py
from PySide6.QtWidgets import QWidget, QVBoxLayout, QFormLayout, QComboBox, QLineEdit, QPushButton, QMessageBox, QCompleter
from PySide6.QtCore import Slot, Signal, Qt
import psycopg2.sql as psql
from utils import validate_column_name

class TimeCalculatorWidget(QWidget):
    execute_sql_signal = Signal(list, str)
    def __init__(self, parent=None):
        super().__init__(parent)
        self._columns = []
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        form = QFormLayout()
        self.start_col_combo = QComboBox()
        self.end_col_combo = QComboBox()
        self.censor_col_combo = QComboBox()
        self.new_col_input = QLineEdit("duration_days")
        self.unit_combo = QComboBox()
        self.unit_combo.addItems(["天 (Days)", "小时 (Hours)", "月 (Months)", "年 (Years)"])
        self.censor_col_combo.addItem("")

        # --- 新增：为所有列选择框添加搜索功能 ---
        for combo_box in [self.start_col_combo, self.end_col_combo, self.censor_col_combo]:
            combo_box.setEditable(True)
            combo_box.setInsertPolicy(QComboBox.NoInsert)
            combo_box.completer().setCompletionMode(QCompleter.PopupCompletion)
            combo_box.completer().setFilterMode(Qt.MatchContains)

        form.addRow("开始时间列:", self.start_col_combo)
        form.addRow("结束时间列:", self.end_col_combo)
        form.addRow("截尾时间列 (可选):", self.censor_col_combo)
        form.addRow("新持续时间列名:", self.new_col_input)
        form.addRow("时间单位:", self.unit_combo)
        layout.addLayout(form)
        self.execute_btn = QPushButton("计算持续时间")
        self.execute_btn.clicked.connect(self.prepare_sql)
        layout.addWidget(self.execute_btn)
        layout.addStretch()

    def update_columns(self, columns: list):
        self._columns = columns
        for combo in [self.start_col_combo, self.end_col_combo, self.censor_col_combo]:
            current_text = combo.currentText()
            combo.clear()
            if combo == self.censor_col_combo: combo.addItem("")
            combo.addItems(self._columns)
            if current_text in self._columns: combo.setCurrentText(current_text)

    @Slot()
    def prepare_sql(self):
        start_col, end_col, censor_col, new_col, unit = self.start_col_combo.currentText(), self.end_col_combo.currentText(), self.censor_col_combo.currentText(), self.new_col_input.text().strip(), self.unit_combo.currentText()
        if not all([start_col, end_col, new_col]): QMessageBox.warning(self, "信息不完整", "请选择开始/结束列并提供新列名。"); return
        is_valid, err_msg = validate_column_name(new_col)
        if not is_valid: QMessageBox.warning(self, "列名无效", err_msg); return
        unit_map = {"天 (Days)": ("INTEGER", "EXTRACT(EPOCH FROM ({end} - {start})) / (24*3600)"), "小时 (Hours)": ("INTEGER", "EXTRACT(EPOCH FROM ({end} - {start})) / 3600"), "月 (Months)": ("NUMERIC(10, 2)", "EXTRACT(EPOCH FROM ({end} - {start})) / (24*3600*30.44)"), "年 (Years)": ("NUMERIC(10, 2)", "EXTRACT(EPOCH FROM ({end} - {start})) / (24*3600*365.25)")}
        col_type, calculation_template = unit_map.get(unit)
        final_end_col = psql.SQL("COALESCE({}, {})").format(psql.Identifier(end_col), psql.Identifier(censor_col)) if censor_col else psql.Identifier(end_col)
        calculation_sql = psql.SQL(calculation_template).format(end=final_end_col, start=psql.Identifier(start_col))
        alter_sql = psql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(psql.Identifier(new_col), psql.SQL(col_type))
        update_sql = psql.SQL("SET {} = {}").format(psql.Identifier(new_col), calculation_sql)
        self.execute_sql_signal.emit([(alter_sql, None), (update_sql, None)], f"计算 {start_col} 和 {end_col} 之间的时间差，存入 {new_col}")