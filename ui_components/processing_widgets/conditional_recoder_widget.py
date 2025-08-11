# ui_components/processing_widgets/conditional_recoder_widget.py
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QFormLayout, QComboBox, QLineEdit, QPushButton, 
    QHBoxLayout, QLabel, QScrollArea, QMessageBox, QGroupBox, QCompleter
)
from PySide6.QtCore import Qt, Slot, Signal
import psycopg2.sql as psql
from utils import validate_column_name

class _ConditionLine(QWidget):
    """代表一条 '(column op value)' 的子条件行。"""
    delete_requested = Signal(QWidget)

    def __init__(self, columns: list, parent=None):
        super().__init__(parent)
        self.columns = columns
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        self.source_col_combo = QComboBox()
        self.operator_combo = QComboBox()
        self.operator_combo.addItems(["等于", "不等于", "大于", "小于", "大于等于", "小于等于", "包含 (ILIKE)", "为空 (IS NULL)", "不为空 (IS NOT NULL)"])
        self.value_input = QLineEdit()
        self.delete_btn = QPushButton("DEL")
        self.delete_btn.setToolTip("删除此条件行")

        for combo_box in [self.source_col_combo]:
            combo_box.setEditable(True); combo_box.setInsertPolicy(QComboBox.NoInsert)
            combo_box.completer().setCompletionMode(QCompleter.PopupCompletion)
            combo_box.completer().setFilterMode(Qt.MatchContains)

        layout.addWidget(self.source_col_combo, 2); layout.addWidget(self.operator_combo, 1)
        layout.addWidget(self.value_input, 2); layout.addWidget(self.delete_btn)

        self.update_columns(self.columns)
        self.operator_combo.currentTextChanged.connect(self._on_operator_changed)
        self.delete_btn.clicked.connect(lambda: self.delete_requested.emit(self))

    def update_columns(self, columns: list):
        self.columns = columns
        current_selection = self.source_col_combo.currentText()
        self.source_col_combo.clear(); self.source_col_combo.addItems(self.columns)
        if current_selection in self.columns: self.source_col_combo.setCurrentText(current_selection)

    @Slot(str)
    def _on_operator_changed(self, text):
        is_null_op = "为空" in text
        self.value_input.setEnabled(not is_null_op)
        if is_null_op: self.value_input.clear()

    def get_condition(self):
        op_map = {"等于": "=", "不等于": "!=", "大于": ">", "小于": "<", "大于等于": ">=", "小于等于": "<=", "包含 (ILIKE)": "ILIKE", "为空 (IS NULL)": "IS NULL", "不为空 (IS NOT NULL)": "IS NOT NULL"}
        return {"source_col": self.source_col_combo.currentText(), "operator": op_map.get(self.operator_combo.currentText()), "value": self.value_input.text()}

    def set_condition(self, config):
        self.source_col_combo.setCurrentText(config.get("source_col", ""))
        op_map_inv = {v: k for k, v in {"等于": "=", "不等于": "!=", "大于": ">", "小于": "<", "大于等于": ">=", "小于等于": "<=", "包含 (ILIKE)": "ILIKE", "为空 (IS NULL)": "IS NULL", "不为空 (IS NOT NULL)": "IS NOT NULL"}.items()}
        self.operator_combo.setCurrentText(op_map_inv.get(config.get("operator", "="), "等于"))
        self.value_input.setText(config.get("value", ""))

class _SingleRuleWidget(QGroupBox):
    """一个自包含的规则卡片，直接继承自QGroupBox。"""
    delete_requested = Signal(QWidget)
    copy_requested = Signal(QWidget)

    def __init__(self, columns: list, rule_number: int, parent=None):
        super().__init__(parent)
        self.columns = columns
        self.condition_lines = []
        self.rule_number = rule_number
        self.init_ui()
        self.add_condition_line()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(10, 5, 10, 10); main_layout.setSpacing(5)
        
        logic_layout = QHBoxLayout()
        logic_layout.addWidget(QLabel("IF")); logic_layout.addStretch()
        logic_layout.addWidget(QLabel("条件间逻辑:")); self.logic_combo = QComboBox()
        self.logic_combo.addItems(["AND", "OR"]); logic_layout.addWidget(self.logic_combo)
        main_layout.addLayout(logic_layout)

        self.conditions_layout = QVBoxLayout(); main_layout.addLayout(self.conditions_layout)

        assign_layout = QHBoxLayout()
        assign_layout.addWidget(QLabel("THEN ASSIGN")); self.assign_input = QLineEdit()
        assign_layout.addWidget(self.assign_input)
        main_layout.addLayout(assign_layout)
        
        control_layout = QHBoxLayout()
        self.add_cond_btn = QPushButton("+ 添加条件"); control_layout.addWidget(self.add_cond_btn)
        control_layout.addStretch()
        self.copy_btn = QPushButton("复制规则"); control_layout.addWidget(self.copy_btn)
        self.delete_rule_btn = QPushButton("删除规则"); control_layout.addWidget(self.delete_rule_btn)
        main_layout.addLayout(control_layout)

        self.add_cond_btn.clicked.connect(self.add_condition_line)
        self.copy_btn.clicked.connect(lambda: self.copy_requested.emit(self))
        self.delete_rule_btn.clicked.connect(lambda: self.delete_requested.emit(self))
        self.update_rule_number(self.rule_number)

    def update_rule_number(self, number):
        self.rule_number = number; self.setTitle(f"规则 #{self.rule_number}")

    @Slot()
    def add_condition_line(self, config=None):
        cond_line = _ConditionLine(self.columns)
        cond_line.delete_requested.connect(self.remove_condition_line)
        if config: cond_line.set_condition(config)
        self.conditions_layout.addWidget(cond_line); self.condition_lines.append(cond_line)

    @Slot(QWidget)
    def remove_condition_line(self, line_widget):
        if line_widget in self.condition_lines:
            self.condition_lines.remove(line_widget); line_widget.deleteLater()
            if not self.condition_lines: self.add_condition_line()

    def update_columns(self, columns: list):
        self.columns = columns
        for line in self.condition_lines: line.update_columns(columns)

    def get_config(self):
        conditions = [line.get_condition() for line in self.condition_lines if line.get_condition()["source_col"] and line.get_condition()["operator"]]
        return {"conditions": conditions, "logic": self.logic_combo.currentText(), "assignment": self.assign_input.text()}

    def set_config(self, config):
        for line in reversed(self.condition_lines): line.deleteLater()
        self.condition_lines.clear()
        for cond_config in config.get("conditions", []): self.add_condition_line(cond_config)
        self.logic_combo.setCurrentText(config.get("logic", "AND"))
        self.assign_input.setText(config.get("assignment", ""))

class ConditionalRecoderWidget(QWidget):
    execute_sql_signal = Signal(list, str)
    def __init__(self, parent=None):
        super().__init__(parent)
        self._columns = []; self.rules = []
        self.init_ui()
    
    def init_ui(self):
        main_layout = QVBoxLayout(self)
        form = QFormLayout()
        self.new_col_input = QLineEdit(); self.default_value_input = QLineEdit()
        form.addRow("新编码列名:", self.new_col_input); form.addRow("默认值 (ELSE):", self.default_value_input)
        main_layout.addLayout(form)
        
        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        rules_container = QWidget(); self.rules_layout = QVBoxLayout(rules_container)
        self.rules_layout.setContentsMargins(2, 5, 2, 5); self.rules_layout.setSpacing(10)
        scroll.setWidget(rules_container)
        main_layout.addWidget(scroll, 1)
        
        bottom_bar = QHBoxLayout()
        self.add_rule_btn = QPushButton("添加新规则"); bottom_bar.addWidget(self.add_rule_btn)
        bottom_bar.addStretch()
        self.execute_btn = QPushButton("执行编码"); bottom_bar.addWidget(self.execute_btn)
        main_layout.addLayout(bottom_bar)
        
        self.add_rule_btn.clicked.connect(lambda: self.add_rule())
        self.execute_btn.clicked.connect(self.prepare_sql)

    def _update_rule_numbers(self):
        for i, rule_widget in enumerate(self.rules, 1): rule_widget.update_rule_number(i)

    def update_columns(self, columns: list):
        self._columns = columns
        for rule_widget in self.rules: rule_widget.update_columns(self._columns)

    @Slot()
    def add_rule(self, config=None):
        rule_number = len(self.rules) + 1
        rule_widget = _SingleRuleWidget(self._columns, rule_number)
        if config: rule_widget.set_config(config)
        rule_widget.delete_requested.connect(self.remove_rule)
        rule_widget.copy_requested.connect(self.copy_rule)
        self.rules.append(rule_widget)
        self.rules_layout.addWidget(rule_widget)

    @Slot(QWidget)
    def remove_rule(self, rule_widget):
        if rule_widget in self.rules:
            self.rules.remove(rule_widget); rule_widget.deleteLater()
            self._update_rule_numbers()
            
    @Slot(QWidget)
    def copy_rule(self, rule_widget_to_copy):
        config_to_copy = rule_widget_to_copy.get_config()
        self.add_rule(config=config_to_copy)

    @Slot()
    def prepare_sql(self):
        new_col, default_val = self.new_col_input.text().strip(), self.default_value_input.text().strip()
        is_valid, err_msg = validate_column_name(new_col)
        if not is_valid: QMessageBox.warning(self, "列名无效", err_msg); return
        if not self.rules: QMessageBox.warning(self, "无规则", "请至少添加一条编码规则。"); return

        case_parts, params = [], []
        for rule_widget in self.rules:
            rule_data = rule_widget.get_config()
            if not rule_data["conditions"] or not rule_data["assignment"]: continue
            
            single_rule_conditions_sql = []
            for cond in rule_data["conditions"]:
                if "为空" in cond["operator"]:
                    sql_part = psql.SQL("CAST({col} AS TEXT) {op}").format(col=psql.Identifier(cond["source_col"]), op=psql.SQL(cond["operator"]))
                    single_rule_conditions_sql.append(sql_part)
                else:
                    is_numeric_op, val, can_be_numeric = cond["operator"] in ['>', '<', '>=', '<='], cond["value"], False
                    if is_numeric_op:
                        try: float(val); can_be_numeric = True
                        except (ValueError, TypeError): pass
                    
                    sql_part_str = f"CAST({{col}} AS {'NUMERIC' if can_be_numeric else 'TEXT'}) {cond['operator']} %s"
                    params.append(float(val) if can_be_numeric else (f"%{val}%" if cond["operator"] == "ILIKE" else val))
                    single_rule_conditions_sql.append(psql.SQL(sql_part_str).format(col=psql.Identifier(cond["source_col"])))
            
            if not single_rule_conditions_sql: continue
            
            full_condition = psql.SQL(f" {rule_data['logic']} ").join(single_rule_conditions_sql)
            case_parts.append(psql.SQL("WHEN ({cond}) THEN %s").format(cond=full_condition)); params.append(rule_data["assignment"])
        
        if not case_parts: QMessageBox.warning(self, "规则不完整", "请确保所有规则都已正确填写。"); return
        
        final_case_parts = [psql.SQL("CASE")] + case_parts
        if default_val: final_case_parts.extend([psql.SQL("ELSE %s")]); params.append(default_val)
        final_case_parts.append(psql.SQL("END")); case_sql = psql.SQL(' ').join(final_case_parts)
        
        alter_sql = psql.SQL("ADD COLUMN IF NOT EXISTS {} TEXT").format(psql.Identifier(new_col))
        update_sql = psql.SQL("SET {} = {}").format(psql.Identifier(new_col), case_sql)
        self.execute_sql_signal.emit([(alter_sql, None), (update_sql, params)], f"对列进行条件编码，存入 {new_col}")