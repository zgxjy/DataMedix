# --- START OF FILE tabs/tab_combine_base_info.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QComboBox, QGroupBox, QCheckBox,
                          QScrollArea, QProgressBar, QApplication)
from PySide6.QtCore import Qt, Signal, QThread, QObject, Slot
import psycopg2
import time
import pandas as pd
from typing import Optional, Dict, Callable

from db_profiles.base_profile import BaseDbProfile

class SQLWorker(QObject):
    finished = Signal(list, list)
    error = Signal(str)
    progress = Signal(int, int)
    log = Signal(str)

    def __init__(self, sql_to_execute, db_params, table_name):
        super().__init__()
        self.sql_to_execute = sql_to_execute
        self.db_params = db_params
        self.table_name = table_name
        self.is_cancelled = False

    def cancel(self):
        self.log.emit("SQL 执行被请求取消...")
        self.is_cancelled = True

    def run(self):
        conn_extract = None
        try:
            cohort_schema, table_name_only = self.table_name.split('.')
            self.log.emit(f"准备为表 '{self.table_name}' 执行SQL批处理...")
            self.log.emit("连接数据库...")
            conn_extract = psycopg2.connect(**self.db_params)
            conn_extract.autocommit = False
            cur = conn_extract.cursor()

            self.log.emit("开始解析和执行SQL语句...")
            sql_statements = self._parse_sql(self.sql_to_execute)
            total_statements = len(sql_statements)

            if total_statements == 0:
                self.log.emit("没有可执行的SQL语句。")
                self.progress.emit(0, 0)
                self.finished.emit([], [])
                return

            self.progress.emit(0, total_statements)
            executed_count = 0

            for i, stmt in enumerate(sql_statements):
                if self.is_cancelled:
                    self.log.emit("SQL 执行已取消。正在回滚...")
                    if conn_extract: conn_extract.rollback()
                    self.error.emit("操作已取消")
                    return

                stmt_trimmed = stmt.strip()
                if not stmt_trimmed or stmt_trimmed.startswith('--'):
                    self.log.emit(f"跳过空语句或注释: 第 {i+1}/{total_statements} 条")
                    self.progress.emit(i + 1, total_statements)
                    continue

                self.log.emit(f"--- [执行SQL {i+1}/{total_statements}] ---")
                self.log.emit(stmt_trimmed)

                try:
                    start_time = time.time()
                    cur.execute(stmt_trimmed)
                    end_time = time.time()
                    self.log.emit(f"语句执行成功 (耗时: {end_time - start_time:.2f} 秒)")
                    executed_count +=1
                except psycopg2.Error as db_err:
                    self.log.emit(f"数据库语句执行出错: {db_err}")
                    self.log.emit(f"出错的SQL语句 (完整):\n{stmt_trimmed}")
                    if conn_extract: conn_extract.rollback()
                    self.error.emit(f"数据库错误: {db_err}\n问题语句: {stmt_trimmed[:200]}...")
                    return
                
                self.progress.emit(i + 1, total_statements)

            if self.is_cancelled:
                self.log.emit("SQL 执行在提交前已取消。正在回滚...")
                if conn_extract: conn_extract.rollback()
                self.error.emit("操作已取消")
                return

            if executed_count > 0:
                self.log.emit("所有语句执行完毕。正在提交事务...")
                conn_extract.commit()
                self.log.emit("事务已成功提交。")
            else:
                self.log.emit("没有实际执行的修改语句，无需提交。")

            self.log.emit("准备获取更新后的表结构和预览数据...")
            cur.execute(f"""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position
            """, (cohort_schema, table_name_only))
            columns = cur.fetchall()
            cur.execute(f"SELECT * FROM {self.table_name} LIMIT 100")
            rows = cur.fetchall()
            self.log.emit("数据提取和预览准备完成。")
            self.finished.emit(columns, rows)
        except Exception as e:
            if conn_extract and not conn_extract.closed:
                try: 
                    conn_extract.rollback()
                except Exception as rb_err: 
                    self.log.emit(f"尝试回滚失败: {rb_err}")
            self.error.emit(f"SQLWorker 意外错误: {str(e)}")
        finally:
            if conn_extract:
                conn_extract.close()

    def _parse_sql(self, sql_script):
        statements = []
        current_statement = []
        for line in sql_script.splitlines():
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith('--'):
                if current_statement:
                     current_statement.append(line)
                continue
            
            current_statement.append(line)
            if stripped_line.endswith(';'):
                full_stmt = "\n".join(current_statement).strip()
                is_only_comment_or_empty = all(s_line.strip().startswith('--') or not s_line.strip() for s_line in full_stmt.split('\n'))
                if not is_only_comment_or_empty:
                    statements.append(full_stmt)
                current_statement = []

        if current_statement:
            full_stmt = "\n".join(current_statement).strip()
            is_only_comment_or_empty = all(s_line.strip().startswith('--') or not s_line.strip() for s_line in full_stmt.split('\n'))
            if not is_only_comment_or_empty:
                 statements.append(full_stmt)
                 
        self.log.emit(f"解析得到 {len(statements)} 条有效SQL语句。")
        return statements

# ... BaseInfoDataExtractionTab 类的剩余部分保持不变 ...
class BaseInfoDataExtractionTab(QWidget):
    def __init__(self, get_db_params_func, get_db_profile_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.get_db_profile = get_db_profile_func
        self.db_profile: Optional[BaseDbProfile] = None

        self.selected_table = None
        self.sql_confirmed = False
        self.worker = None
        self.worker_thread = None
        
        self.option_checkboxes: Dict[str, QCheckBox] = {}
        self.base_info_modules: Dict[str, Callable] = {}
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Orientation.Vertical)
        
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        
        instruction_label = QLabel("从数据库中选择队列表，选择要添加的基础数据选项，然后点击“SQL确认预览”生成SQL并确认，最后点击“提取基础数据”。")
        instruction_label.setWordWrap(True)
        top_layout.addWidget(instruction_label)

        table_select_layout = QHBoxLayout()
        table_select_layout.addWidget(QLabel("选择队列表:"))
        self.table_combo = QComboBox()
        self.table_combo.setMinimumWidth(300)
        self.table_combo.currentIndexChanged.connect(self.on_table_selected)
        table_select_layout.addWidget(self.table_combo)
        self.refresh_btn = QPushButton("刷新表列表")
        self.refresh_btn.clicked.connect(self.refresh_tables)
        self.refresh_btn.setEnabled(False)
        table_select_layout.addWidget(self.refresh_btn)
        top_layout.addLayout(table_select_layout)

        options_group = QGroupBox("数据提取选项")
        options_layout = QVBoxLayout(options_group)

        select_buttons_layout = QHBoxLayout()
        self.select_all_btn = QPushButton("全选")
        self.select_all_btn.clicked.connect(self.select_all_options)
        select_buttons_layout.addWidget(self.select_all_btn)
        self.deselect_all_btn = QPushButton("全不选")
        self.deselect_all_btn.clicked.connect(self.deselect_all_options)
        select_buttons_layout.addWidget(self.deselect_all_btn)
        select_buttons_layout.addStretch()
        options_layout.addLayout(select_buttons_layout)
        
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_content = QWidget()
        self.scroll_layout = QVBoxLayout(self.scroll_content)
        self.scroll_area.setWidget(self.scroll_content)
        options_layout.addWidget(self.scroll_area)
        
        top_layout.addWidget(options_group)
        
        self.execution_status_group = QGroupBox("SQL执行状态")
        execution_status_layout = QVBoxLayout(self.execution_status_group)
        self.execution_progress = QProgressBar(); self.execution_progress.setRange(0,100); self.execution_progress.setValue(0)
        execution_status_layout.addWidget(self.execution_progress)
        self.execution_log = QTextEdit(); self.execution_log.setReadOnly(True); self.execution_log.setMaximumHeight(150)
        execution_status_layout.addWidget(self.execution_log)
        self.execution_status_group.setVisible(False)
        top_layout.addWidget(self.execution_status_group)
        
        top_layout.addWidget(QLabel("SQL预览:"))
        self.sql_preview = QTextEdit(); self.sql_preview.setReadOnly(True); self.sql_preview.setMinimumHeight(150)
        top_layout.addWidget(self.sql_preview)
        
        buttons_layout = QHBoxLayout()
        self.confirm_sql_btn = QPushButton("SQL确认预览"); self.confirm_sql_btn.clicked.connect(self.handle_confirm_sql_preview); self.confirm_sql_btn.setEnabled(False)
        buttons_layout.addWidget(self.confirm_sql_btn)
        self.extract_btn = QPushButton("提取基础数据"); self.extract_btn.clicked.connect(self.extract_data); self.extract_btn.setEnabled(False)
        buttons_layout.addWidget(self.extract_btn)
        self.cancel_extraction_btn = QPushButton("取消操作"); self.cancel_extraction_btn.clicked.connect(self.cancel_extraction); self.cancel_extraction_btn.setEnabled(False)
        buttons_layout.addWidget(self.cancel_extraction_btn)
        top_layout.addLayout(buttons_layout)
        
        splitter.addWidget(top_widget)
        self.result_table = QTableWidget(); self.result_table.setAlternatingRowColors(True)
        splitter.addWidget(self.result_table)
        splitter.setSizes([700, 200])
        main_layout.addWidget(splitter)
        self.setLayout(main_layout)

    def on_profile_changed(self):
        self.db_profile = self.get_db_profile()
        self.refresh_tables()
        
        for i in reversed(range(self.scroll_layout.count())): 
            self.scroll_layout.itemAt(i).widget().setParent(None)
        self.option_checkboxes.clear()
        self.base_info_modules.clear()

        if not self.db_profile:
            return

        modules = self.db_profile.get_base_info_modules()
        for display_name, internal_key, sql_func in modules:
            cb = QCheckBox(display_name)
            cb.setChecked(True)
            cb.stateChanged.connect(self._reset_sql_confirmation)
            self.scroll_layout.addWidget(cb)
            self.option_checkboxes[internal_key] = cb
            self.base_info_modules[internal_key] = sql_func

    def on_db_connected(self):
        self.refresh_btn.setEnabled(True)
        self.refresh_tables()

    def refresh_tables(self):
        self.selected_table = None
        self.sql_confirmed = False
        self.sql_preview.clear()
        self.confirm_sql_btn.setEnabled(False)
        self.extract_btn.setEnabled(False)
        self.table_combo.clear()
        
        db_params = self.get_db_params()
        if not self.db_profile:
            self.table_combo.addItem("请先选择数据库类型")
            return
        cohort_schema = self.db_profile.get_cohort_table_schema()
        
        if not db_params:
            self.table_combo.addItem("数据库未连接")
            return

        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s
                ORDER BY table_name
            """, (cohort_schema,))
            tables = cur.fetchall()
            if tables:
                for table in tables: self.table_combo.addItem(f"{cohort_schema}.{table[0]}")
                if self.table_combo.count() > 0:
                     self.table_combo.setCurrentIndex(0)
                     self.on_table_selected(0)
            else: 
                self.table_combo.addItem(f"在 '{cohort_schema}' 中未找到队列表")
        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取 '{cohort_schema}' 中的表列表: {str(e)}")
            self.table_combo.addItem("查询表失败")
        finally:
            if conn: conn.close()

    def on_table_selected(self, index):
        self._reset_sql_confirmation()
        current_item_text = self.table_combo.itemText(index) if index >= 0 else None
        is_valid_table = current_item_text and "未找到" not in current_item_text and "失败" not in current_item_text
        if is_valid_table:
            self.selected_table = current_item_text
            self.confirm_sql_btn.setEnabled(True)
            self.sql_preview.setText("-- 请点击 'SQL确认预览' 生成SQL --")
        else:
            self.selected_table = None
            self.confirm_sql_btn.setEnabled(False)
            self.sql_preview.clear()

    def _reset_sql_confirmation(self):
        self.sql_confirmed = False
        self.extract_btn.setEnabled(False)

    def select_all_options(self):
        for cb in self.option_checkboxes.values(): 
            cb.setChecked(True)

    def deselect_all_options(self):
        for cb in self.option_checkboxes.values(): 
            cb.setChecked(False)

    def generate_sql_parts(self, conn_for_icd_lookup):
        if not self.selected_table or not self.db_profile: 
            return "", ""

        all_col_defs = []
        all_update_sqls = [f"-- SQL for table {self.selected_table} --\n"]
        past_diag_data_for_sql = {}
        
        constants = self.db_profile.get_profile_constants()

        if 'past_diagnostic' in self.option_checkboxes and self.option_checkboxes['past_diagnostic'].isChecked():
            if conn_for_icd_lookup:
                try:
                    keywords = constants.get('DEFAULT_PAST_DIAGNOSIS_CATEGORIES', [])
                    for keyword in keywords:
                        if not keyword or not isinstance(keyword, str): continue
                        category_key = keyword.strip().lower().replace(' ', '_')
                        icd_query_template = "SELECT DISTINCT TRIM(icd_code) AS icd_code FROM mimiciv_hosp.d_icd_diagnoses WHERE LOWER(long_title) LIKE %s;"
                        icd_df = pd.read_sql_query(icd_query_template, conn_for_icd_lookup, params=(f'%{keyword.strip().lower()}%',))
                        icd_codes_list = [code for code in icd_df['icd_code'].tolist() if code and str(code).strip()]
                        if icd_codes_list:
                            past_diag_data_for_sql[category_key] = icd_codes_list
                except Exception as db_err:
                    all_update_sqls.append(f"-- [错误] 查询自定义既往病史ICD码时出错: {db_err} --\n")
            
        for key, checkbox in self.option_checkboxes.items():
            if checkbox.isChecked():
                sql_func = self.base_info_modules.get(key)
                if sql_func:
                    kwargs = {}
                    if key == 'past_diagnostic':
                        kwargs['past_diagnoses_data'] = past_diag_data_for_sql
                    
                    defs, updates = sql_func(self.selected_table, self.db_profile, **kwargs)
                    all_col_defs.extend(defs)
                    all_update_sqls.append(updates)

        alter_table_sql = ""
        if all_col_defs:
            unique_col_defs_dict = {}
            for col_def_str in all_col_defs:
                col_name = col_def_str.split(' ')[0].strip()
                if col_name not in unique_col_defs_dict:
                    unique_col_defs_dict[col_name] = col_def_str
            if unique_col_defs_dict:
                add_clauses = [f"ADD COLUMN IF NOT EXISTS {col_def}" for col_def in unique_col_defs_dict.values()]
                alter_table_sql = f"ALTER TABLE {self.selected_table}\n    " + ",\n    ".join(add_clauses) + ";\n"
        
        update_statements_sql = "\n\n".join(all_update_sqls)
        return alter_table_sql, update_statements_sql

    def preview_sql(self):
        if not self.selected_table:
            self.sql_preview.clear()
            return
        db_params = self.get_db_params()
        conn_preview = None
        generated_sql = ""
        try:
            needs_db_for_icd = 'past_diagnostic' in self.option_checkboxes and self.option_checkboxes['past_diagnostic'].isChecked()
            if needs_db_for_icd:
                if db_params:
                    conn_preview = psycopg2.connect(**db_params)
                else:
                    alter_sql, update_sql = self.generate_sql_parts(None)
                    generated_sql = (alter_sql + "\n\n" + update_sql).strip()
                    generated_sql += "\n-- [预览警告] 未连接数据库，无法生成'患者既往病史 (自定义ICD)'部分的SQL。--"
                    self.sql_preview.setText(generated_sql)
                    return
            final_alter_sql, final_update_sql = self.generate_sql_parts(conn_preview)
            generated_sql = (final_alter_sql + "\n\n" + final_update_sql).strip()
            if len(generated_sql) < 100:
                generated_sql = "-- 没有选择任何数据提取选项，SQL为空。 --"
            self.sql_preview.setText(generated_sql)
        except (Exception, psycopg2.Error) as e:
            self.sql_preview.setText(f"-- 生成SQL预览时出错: {str(e)} --")
        finally:
            if conn_preview: conn_preview.close()

    def handle_confirm_sql_preview(self):
        if not self.selected_table:
            QMessageBox.warning(self, "无操作", "请先选择一个队列表。")
            return
        self.preview_sql()
        current_sql_text = self.sql_preview.toPlainText().strip()
        problematic_phrases = ["-- 请先连接数据库", "-- 生成SQL预览时出错", "-- 没有选择任何数据提取选项", "-- [预览警告]", "-- [错误]", "-- SQL为空。"]
        is_problematic = any(phrase in current_sql_text for phrase in problematic_phrases) or not current_sql_text
        if is_problematic:
            QMessageBox.warning(self, "SQL预览问题", "SQL预览为空、包含错误或警告。\n请检查后重试。")
            self.sql_confirmed = False
            self.extract_btn.setEnabled(False)
        else:
            self.sql_confirmed = True
            self.extract_btn.setEnabled(True)
            QMessageBox.information(self, "SQL已确认", "SQL预览已生成并确认。")

    def extract_data(self):
        if not self.selected_table:
            QMessageBox.warning(self, "未选择表", "请先选择一个队列表")
            return
        if not self.sql_confirmed:
            QMessageBox.warning(self, "SQL未确认", "请先点击SQL确认预览按钮。")
            return
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库。")
            return
            
        sql_to_execute = self.sql_preview.toPlainText()
        if not sql_to_execute or "-- SQL为空" in sql_to_execute:
             QMessageBox.information(self, "无操作", "没有可执行的SQL。")
             return

        self.prepare_for_long_operation(True)
        self.worker = SQLWorker(sql_to_execute, db_params, self.selected_table)
        self.worker_thread = QThread()
        self.worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_sql_execution_finished)
        self.worker.error.connect(self.on_sql_execution_error)
        self.worker.progress.connect(self.update_execution_progress)
        self.worker.log.connect(self.update_execution_log)
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.error.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        self.worker.finished.connect(lambda: setattr(self, 'worker', None))
        self.worker_thread.start()

    def prepare_for_long_operation(self, starting=True):
        if starting:
            self.execution_status_group.setVisible(True)
            self.execution_progress.setValue(0)
            self.execution_log.clear()
            self.update_execution_log("开始执行SQL操作...")
        self.extract_btn.setEnabled(not starting and self.sql_confirmed)
        self.confirm_sql_btn.setEnabled(not starting)
        self.refresh_btn.setEnabled(not starting)
        self.table_combo.setEnabled(not starting)
        self.cancel_extraction_btn.setEnabled(starting)
        for cb in self.option_checkboxes.values(): 
            cb.setEnabled(not starting)
        self.select_all_btn.setEnabled(not starting)
        self.deselect_all_btn.setEnabled(not starting)

    def update_execution_progress(self, value, max_value=None):
        if max_value is not None and self.execution_progress.maximum() != max_value:
             self.execution_progress.setMaximum(max_value)
        self.execution_progress.setValue(value)

    def update_execution_log(self, message):
        self.execution_log.append(message)
        QApplication.processEvents()

    def cancel_extraction(self):
        if self.worker:
            self.update_execution_log("正在请求取消SQL执行...")
            self.worker.cancel()
            self.cancel_extraction_btn.setEnabled(False)

    @Slot(list, list)
    def on_sql_execution_finished(self, columns, rows):
        self.result_table.setRowCount(len(rows))
        self.result_table.setColumnCount(len(columns))
        column_names = [col[0] for col in columns]
        self.result_table.setHorizontalHeaderLabels(column_names)
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                self.result_table.setItem(i, j, QTableWidgetItem(str(value) if value is not None else ""))
        self.result_table.resizeColumnsToContents()
        self.update_execution_log("SQL执行完成！")
        QMessageBox.information(self, "提取成功", f"已成功为表 {self.selected_table} 添加基础数据")
        self.prepare_for_long_operation(False)
        self.worker = None
        self.worker_thread = None

    @Slot(str)
    def on_sql_execution_error(self, error_message):
        self.update_execution_log(f"错误: {error_message}")
        if "操作已取消" not in error_message:
            QMessageBox.critical(self, "提取失败", f"无法提取基础数据: {error_message}")
        else:
            QMessageBox.information(self, "操作取消", "数据提取操作已取消。")
        self.sql_confirmed = False
        self.extract_btn.setEnabled(False)
        self.prepare_for_long_operation(False)
        self.worker = None
        self.worker_thread = None