# --- START OF FILE tabs/tab_data_dictionary.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                               QComboBox, QTableWidget, QTableWidgetItem, QLabel,
                               QMessageBox, QApplication, QHeaderView, QAbstractItemView,
                               QScrollArea,QGroupBox, QTextEdit, QProgressBar) 
from PySide6.QtCore import Qt, Slot
import psycopg2
import psycopg2.sql as psql 
import traceback
from typing import Optional

from ui_components.conditiongroup import ConditionGroupWidget
from db_profiles.base_profile import BaseDbProfile

class DataDictionaryTab(QWidget):
    # ... (init is the same) ...
    def __init__(self, get_db_params_func, get_db_profile_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.get_db_profile = get_db_profile_func
        self.db_profile: Optional[BaseDbProfile] = None
        self.init_ui()

    def init_ui(self):
        # ... (init_ui is the same) ...
        main_layout = QVBoxLayout(self)
        dict_select_layout = QHBoxLayout()
        dict_select_layout.addWidget(QLabel("搜索字典表:"))
        self.dict_table_combo = QComboBox()
        self.dict_table_combo.currentIndexChanged.connect(self._on_dict_table_changed)
        dict_select_layout.addWidget(self.dict_table_combo, 1)
        dict_select_layout.addStretch()
        main_layout.addLayout(dict_select_layout)

        condition_group_box = QGroupBox("构建搜索条件")
        condition_layout = QVBoxLayout(condition_group_box)
        self.condition_group_widget = ConditionGroupWidget(is_root=True)
        self.condition_group_widget.condition_changed.connect(self._on_condition_changed_update_preview)
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True); scroll_area.setWidget(self.condition_group_widget)
        scroll_area.setMinimumHeight(150); scroll_area.setMaximumHeight(300) 
        condition_layout.addWidget(scroll_area)
        main_layout.addWidget(condition_group_box)

        sql_preview_group = QGroupBox("SQL 预览 (只读)")
        sql_preview_layout = QVBoxLayout(sql_preview_group)
        self.sql_preview_textedit = QTextEdit()
        self.sql_preview_textedit.setReadOnly(True)
        self.sql_preview_textedit.setMaximumHeight(80) 
        sql_preview_layout.addWidget(self.sql_preview_textedit)
        main_layout.addWidget(sql_preview_group)

        search_button_layout = QHBoxLayout()
        search_button_layout.addStretch()
        self.search_button = QPushButton("执行搜索")
        self.search_button.clicked.connect(self.perform_search)
        self.search_button.setEnabled(False)
        search_button_layout.addWidget(self.search_button)
        search_button_layout.addStretch()
        main_layout.addLayout(search_button_layout)

        self.execution_status_group = QGroupBox("搜索执行状态")
        execution_status_layout = QVBoxLayout(self.execution_status_group)
        self.execution_progress = QProgressBar()
        self.execution_progress.setRange(0, 100) 
        self.execution_progress.setValue(0)
        execution_status_layout.addWidget(self.execution_progress)
        self.execution_log = QTextEdit()
        self.execution_log.setReadOnly(True)
        self.execution_log.setMaximumHeight(100) 
        execution_status_layout.addWidget(self.execution_log)
        self.execution_status_group.setVisible(False) 
        main_layout.addWidget(self.execution_status_group)
        
        self.result_table = QTableWidget()
        self.result_table.setAlternatingRowColors(True)
        self.result_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.result_table.horizontalHeader().setStretchLastSection(True)
        self.result_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        main_layout.addWidget(self.result_table, 1) 
        
        self.setLayout(main_layout)

    def on_profile_changed(self):
        # ... (unchanged) ...
        self.db_profile = self.get_db_profile()
        self.dict_table_combo.blockSignals(True)
        self.dict_table_combo.clear()

        if not self.db_profile:
            self.dict_table_combo.addItem("请先选择数据库类型")
            self.dict_table_combo.blockSignals(False)
            self._on_dict_table_changed()
            return

        dict_tables_config = self.db_profile.get_dictionary_tables()
        if not dict_tables_config:
            self.dict_table_combo.addItem("当前数据库无可用字典")
        else:
            for config in dict_tables_config:
                self.dict_table_combo.addItem(config['display_name'], config)
        
        self.dict_table_combo.blockSignals(False)
        self._on_dict_table_changed()
        self._update_search_button_state()

    def on_db_connected(self):
        # ... (unchanged) ...
        self._update_search_button_state()
        self._update_execution_log("数据库已连接。请选择字典表并构建搜索条件。")
        self._on_condition_changed_update_preview() 

    def _on_dict_table_changed(self):
        # ... (unchanged) ...
        config = self.dict_table_combo.currentData()
        
        self.result_table.clearContents()
        self.result_table.setRowCount(0)

        if not config:
            self.result_table.setColumnCount(0)
            self.condition_group_widget.set_available_search_fields([])
            self.condition_group_widget.clear_all()
            self._update_execution_log("请选择一个可用的字典表。")
        else:
            column_config = config.get('columns', [])
            self.result_table.setColumnCount(len(column_config))
            self.result_table.setHorizontalHeaderLabels([c[1] for c in column_config])
            
            available_fields = config.get('search_fields', [])
            self.condition_group_widget.set_available_search_fields(available_fields)
            self.condition_group_widget.clear_all()
        
            self.execution_log.clear()
            self.execution_progress.setValue(0)
            self._update_execution_log(f"当前字典表: {self.dict_table_combo.currentText()}。请构建搜索条件。")
        
        self._on_condition_changed_update_preview()
        self._update_search_button_state()

    @Slot() 
    def _on_condition_changed_update_preview(self):
        # ... (unchanged) ...
        self._update_sql_preview()
        self._update_search_button_state()
        if self.execution_status_group.isVisible():
            self._update_execution_log("条件已更改，请重新执行搜索以查看结果。")

    def _update_sql_preview(self):
        # ... (unchanged) ...
        if not self.condition_group_widget.has_valid_input():
            self.sql_preview_textedit.setText("-- 请构建有效的搜索条件以生成SQL预览 --")
            return
            
        config = self.dict_table_combo.currentData()
        if not config:
            self.sql_preview_textedit.setText("-- 请先选择一个字典表 --")
            return

        condition_sql_template_str, query_params = self.condition_group_widget.get_condition()
        
        db_cols_to_select_idents = [psql.Identifier(col_info[0]) for col_info in config.get('columns', [])]
        if not db_cols_to_select_idents:
             self.sql_preview_textedit.setText("-- 字典表列配置错误 --")
             return

        if config.get('is_dynamic_view', False):
            table_source = psql.SQL("({}) AS dynamic_view").format(psql.SQL(config.get('dynamic_sql', 'SELECT NULL')))
        else:
            table_source = psql.SQL(config.get('table_name', 'information_schema.tables'))

        query_base_obj = psql.SQL("SELECT {cols} FROM {table}").format(
            cols=psql.SQL(', ').join(db_cols_to_select_idents),
            table=table_source
        )
        
        full_query_obj = query_base_obj
        if condition_sql_template_str:
            full_query_obj = psql.Composed([query_base_obj, psql.SQL(" WHERE "), psql.SQL(condition_sql_template_str)])
        
        order_by_col_ident = db_cols_to_select_idents[0]
        full_query_obj += psql.SQL(" ORDER BY {} LIMIT 500").format(order_by_col_ident)

        try:
            db_params = self.get_db_params()
            if db_params:
                with psycopg2.connect(**db_params) as conn:
                    with conn.cursor() as cur:
                        readable_sql = cur.mogrify(full_query_obj, query_params).decode(conn.encoding or 'utf-8')
                        self.sql_preview_textedit.setText(readable_sql)
            else:
                self.sql_preview_textedit.setText("-- 无法连接数据库生成完整预览 --\n" + str(full_query_obj))
        except Exception as e:
            self.sql_preview_textedit.setText(f"-- 生成SQL预览失败: {e} --")

    def _update_search_button_state(self):
        # REPAIR: Explicitly convert to boolean
        db_ok = bool(self.get_db_params())
        conditions_ok = self.condition_group_widget.has_valid_input()
        self.search_button.setEnabled(db_ok and conditions_ok)

    def _prepare_for_search(self, starting=True):
        self.execution_status_group.setVisible(True) 
        if starting:
            self.execution_progress.setValue(0)
            self.execution_log.clear()
            self._update_execution_log("准备开始搜索...")
        
        self.dict_table_combo.setEnabled(not starting)
        self.condition_group_widget.setEnabled(not starting)
        
        # REPAIR: Explicitly convert to boolean
        is_ready = not starting and self.condition_group_widget.has_valid_input() and bool(self.get_db_params())
        self.search_button.setEnabled(is_ready)

    def _update_execution_log(self, message):
        # ... (unchanged) ...
        self.execution_log.append(message)
        QApplication.processEvents()

    def _update_execution_progress(self, value):
        # ... (unchanged) ...
        self.execution_progress.setValue(value)
        QApplication.processEvents()

    @Slot()
    def perform_search(self):
        # ... (unchanged) ...
        db_params = self.get_db_params()
        config = self.dict_table_combo.currentData()
        if not db_params or not config or not self.condition_group_widget.has_valid_input():
            QMessageBox.warning(self, "信息不完整", "请确保已连接数据库、选择字典表并输入了有效的搜索条件。")
            return

        self._prepare_for_search(True)
        self._update_execution_log(f"开始从 {config.get('display_name')} 中搜索...")
        self._update_execution_progress(10)
        
        self._update_sql_preview()
        
        self.result_table.setRowCount(0)
        conn = None
        try:
            self._update_execution_log("正在连接数据库...")
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            self._update_execution_progress(25)
            
            condition_sql_template, query_params = self.condition_group_widget.get_condition()
            column_config = config.get('columns', [])
            db_cols_to_select = [psql.Identifier(col_info[0]) for col_info in column_config]
            
            if config.get('is_dynamic_view', False):
                table_source = psql.SQL("({}) AS dynamic_view").format(psql.SQL(config.get('dynamic_sql', 'SELECT NULL')))
            else:
                table_source = psql.SQL(config.get('table_name'))

            query = psql.SQL("SELECT {cols} FROM {table} WHERE {cond} ORDER BY {order_col} LIMIT 500").format(
                cols=psql.SQL(', ').join(db_cols_to_select),
                table=table_source,
                cond=psql.SQL(condition_sql_template),
                order_col=db_cols_to_select[0]
            )

            self._update_execution_log(f"正在执行SQL查询...") 
            cur.execute(query, query_params)
            self._update_execution_progress(60)
            rows = cur.fetchall()
            self._update_execution_log(f"查询完成，获取到 {len(rows)} 条记录。正在填充表格...")
            
            if rows:
                self.result_table.setRowCount(len(rows))
                for i, row_data in enumerate(rows):
                    for j, value in enumerate(row_data): 
                        self.result_table.setItem(i, j, QTableWidgetItem(str(value) if value is not None else ""))
                self.result_table.resizeColumnsToContents()
                self._update_execution_log(f"找到 {len(rows)} 条符合条件的记录 (最多显示500条)。")
            else: 
                self._update_execution_log("未找到符合条件的记录。")
            self._update_execution_progress(100)

        except Exception as e:
            log_msg = f"查询错误: {e}\n{traceback.format_exc()}"
            self._update_execution_log(log_msg)
            QMessageBox.critical(self, "查询错误", log_msg)
            self._update_execution_progress(0)
        finally:
            if conn: conn.close()
            self._update_execution_log("数据库连接已关闭。搜索操作完成。")
            self._prepare_for_search(False)