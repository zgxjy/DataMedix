# tabs/tab_data_processing.py
import pandas as pd
import psycopg2
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QComboBox, QLabel,
    QGroupBox, QTableView, QSplitter, QTabWidget, QMessageBox, QApplication, QHeaderView
)
from PySide6.QtCore import Qt, Slot, QThread, Signal, QObject
from PySide6.QtGui import QStandardItemModel, QStandardItem
from ui_components.processing_widgets.time_calculator_widget import TimeCalculatorWidget
from ui_components.processing_widgets.conditional_recoder_widget import ConditionalRecoderWidget

class PandasTableModel(QStandardItemModel):
    def __init__(self, data):
        super().__init__()
        if data.empty: return
        self.setHorizontalHeaderLabels(data.columns.tolist())
        for i in range(data.shape[0]):
            items = [QStandardItem(str(val)) for val in data.iloc[i]]
            self.appendRow(items)
            
class SqlProcessingWorker(QObject):
    finished, error = Signal(str), Signal(str)
    def __init__(self, db_params, table_name, sql_and_params_list, description):
        super().__init__()
        self.db_params, self.table_name, self.sql_list, self.description = db_params, table_name, sql_and_params_list, description
    def run(self):
        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    alter_sql, _ = self.sql_list[0]; update_sql, params = self.sql_list[1]
                    full_sql = psycopg2.sql.SQL("ALTER TABLE {table} {alter}; UPDATE {table} {update};").format(table=psycopg2.sql.Identifier(*self.table_name.split('.')), alter=alter_sql, update=update_sql)
                    cur.execute(full_sql, params)
                conn.commit()
            self.finished.emit(f"成功完成操作: {self.description}")
        except Exception as e: self.error.emit(str(e))

class DataProcessingTab(QWidget):
    # ... (__init__ 和 on_* 方法不变) ...
    def __init__(self, get_db_params_func, get_db_profile_func, parent=None):
        super().__init__(parent)
        self.get_db_params, self.get_db_profile = get_db_params_func, get_db_profile_func
        self.df, self.worker_thread = pd.DataFrame(), None
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        
        # --- 核心修改：使用垂直 QSplitter ---
        v_splitter = QSplitter(Qt.Vertical, self)
        main_layout.addWidget(v_splitter)

        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        data_source_group = QGroupBox("1. 选择并加载队列表")
        data_source_layout = QHBoxLayout(data_source_group)
        data_source_layout.addWidget(QLabel("Schema:"))
        self.schema_combo = QComboBox()
        data_source_layout.addWidget(self.schema_combo)
        data_source_layout.addWidget(QLabel("队列表:"))
        self.table_combo = QComboBox()
        data_source_layout.addWidget(self.table_combo, 1)
        self.load_data_btn = QPushButton("加载/刷新数据")
        data_source_layout.addWidget(self.load_data_btn)
        top_layout.addWidget(data_source_group)
        self.preview_table = QTableView()
        self.preview_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        top_layout.addWidget(self.preview_table, 1)
        v_splitter.addWidget(top_widget)
        
        bottom_widget = QWidget()
        bottom_layout = QVBoxLayout(bottom_widget)
        self.tools_tabs = QTabWidget()
        bottom_layout.addWidget(self.tools_tabs)
        v_splitter.addWidget(bottom_widget)
        
        # --- 关键：设置初始尺寸比例 ---
        v_splitter.setSizes([450, 350])

        self.setup_tool_panels()
        self.schema_combo.currentIndexChanged.connect(self.refresh_tables)
        self.load_data_btn.clicked.connect(self.load_data_from_db)

    # ... (其他方法基本不变，只需确保信号连接正确) ...
    def setup_tool_panels(self):
        self.time_calculator = TimeCalculatorWidget()
        self.conditional_recoder = ConditionalRecoderWidget()
        self.tools_tabs.addTab(self.time_calculator, "时间计算")
        self.tools_tabs.addTab(self.conditional_recoder, "条件编码")
        self.time_calculator.execute_sql_signal.connect(self.execute_processing_sql)
        self.conditional_recoder.execute_sql_signal.connect(self.execute_processing_sql)
    
    # ... (refresh_schemas, refresh_tables, load_data_from_db, execute_processing_sql 等方法保持不变) ...
    @Slot()
    def on_db_connected(self): self.refresh_schemas()
    @Slot()
    def on_profile_changed(self):
        self.schema_combo.clear(); self.table_combo.clear(); self.df = pd.DataFrame(); self.preview_table.setModel(None)
    def refresh_schemas(self):
        db_params = self.get_db_params()
        if not db_params: return
        try:
            with psycopg2.connect(**db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute("""SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND schema_name NOT LIKE 'pg_temp%' ORDER BY schema_name;""")
                    schemas = [s[0] for s in cur.fetchall()]
                    current_schema = self.schema_combo.currentText(); self.schema_combo.blockSignals(True)
                    self.schema_combo.clear(); self.schema_combo.addItems(schemas)
                    if current_schema in schemas: self.schema_combo.setCurrentText(current_schema)
                    self.schema_combo.blockSignals(False)
                    if schemas: self.refresh_tables()
        except Exception as e: QMessageBox.critical(self, "错误", f"无法获取Schemas: {e}")
    def refresh_tables(self):
        schema = self.schema_combo.currentText()
        if not schema: return
        try:
            with psycopg2.connect(**self.get_db_params()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type='BASE TABLE' ORDER BY table_name", (schema,))
                    tables = [t[0] for t in cur.fetchall()]
                    current_table = self.table_combo.currentText(); self.table_combo.clear(); self.table_combo.addItems(tables)
                    if current_table in tables: self.table_combo.setCurrentText(current_table)
        except Exception as e: QMessageBox.critical(self, "错误", f"无法获取数据表: {e}")
    @Slot()
    def load_data_from_db(self):
        schema, table = self.schema_combo.currentText(), self.table_combo.currentText()
        if not schema or not table: QMessageBox.warning(self, "信息不全", "请选择Schema和队列表。"); return
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            with psycopg2.connect(**self.get_db_params()) as conn: self.df = pd.read_sql(f"SELECT * FROM {schema}.{table} LIMIT 500", conn)
            self.preview_table.setModel(PandasTableModel(self.df))
            columns = self.df.columns.tolist()
            self.time_calculator.update_columns(columns); self.conditional_recoder.update_columns(columns)
            QMessageBox.information(self, "加载成功", f"已加载 {schema}.{table} 前500行预览。")
        except Exception as e: QMessageBox.critical(self, "加载失败", str(e))
        finally: QApplication.restoreOverrideCursor()
    @Slot(list, str)
    def execute_processing_sql(self, sql_and_params_list, description):
        full_table_name = f"{self.schema_combo.currentText()}.{self.table_combo.currentText()}"
        if QMessageBox.question(self, "确认操作", f"确定要对表 '{full_table_name}' 执行以下操作吗？\n\n{description}\n\n此操作将直接修改数据库表。", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No) == QMessageBox.StandardButton.No: return
        self.worker = SqlProcessingWorker(self.get_db_params(), full_table_name, sql_and_params_list, description)
        self.worker_thread = QThread(); self.worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_processing_finished); self.worker.error.connect(self.on_processing_error)
        self.worker_thread.start()
    @Slot(str)
    def on_processing_finished(self, message):
        QMessageBox.information(self, "操作成功", message); self.load_data_from_db(); self.worker_thread.quit()
    @Slot(str)
    def on_processing_error(self, error_message):
        QMessageBox.critical(self, "操作失败", f"执行SQL时发生错误:\n{error_message}"); self.worker_thread.quit()