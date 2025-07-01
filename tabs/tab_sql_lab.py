# --- START OF FILE tabs/tab_sql_lab.py ---
import time
import psycopg2
import pandas as pd
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTextEdit, QTableView,
    QMessageBox, QSplitter, QLabel, QHeaderView, QSpinBox, QApplication,
    QDialog, QFormLayout, QLineEdit, QDialogButtonBox
)
from PySide6.QtCore import QObject, Signal, Slot, QThread, Qt
from PySide6.QtGui import QStandardItemModel, QStandardItem, QFont, QColor, QSyntaxHighlighter, QTextCharFormat

# --- 可选但强烈推荐的语法高亮 ---
try:
    from pygments import highlight
    from pygments.lexers.sql import PostgresLexer
    from pygments.formatters import Formatter
    PYGMENTS_AVAILABLE = True
except ImportError:
    PYGMENTS_AVAILABLE = False


if PYGMENTS_AVAILABLE:
    class PygmentsFormatter(Formatter):
        def __init__(self):
            super().__init__()
            self.data = []
        def format(self, tokensource, outfile):
            self.data = []
            for ttype, value in tokensource:
                self.data.append((ttype, value))

    class SqlHighlighter(QSyntaxHighlighter):
        def __init__(self, parent):
            super().__init__(parent)
            self.formatter = PygmentsFormatter()
            self.lexer = PostgresLexer()
            
            self.formats = {
                'Token.Keyword': self.create_format(QColor("#0000ff"), bold=True),
                'Token.Comment': self.create_format(QColor("#00aa00")),
                'Token.Name.Builtin': self.create_format(QColor("#9c27b0")),
                'Token.Operator': self.create_format(QColor("#e91e63")),
                'Token.String': self.create_format(QColor("#4caf50")),
                'Token.Literal.Number': self.create_format(QColor("#ff9800")),
                'Token.Punctuation': self.create_format(QColor("#888888")),
            }
            self.default_format = self.create_format(QColor("#000000"))

        def create_format(self, color, bold=False):
            fmt = QTextCharFormat()
            fmt.setForeground(color)
            if bold:
                fmt.setFontWeight(QFont.Bold)
            return fmt

        def highlightBlock(self, text):
            highlight(text, self.lexer, self.formatter)
            start = 0
            for ttype, value in self.formatter.data:
                length = len(value)
                ttype_str = str(ttype)
                
                # 匹配最具体的类型
                format_to_apply = self.default_format
                for key in self.formats:
                    if ttype_str.startswith(key):
                        format_to_apply = self.formats[key]
                        break
                
                self.setFormat(start, length, format_to_apply)
                start += length
# --- 语法高亮结束 ---


class SqlWorker(QObject):
    """在后台执行SQL查询的Worker"""
    finished = Signal(object, float)  # 发送 (DataFrame, 执行秒数)
    error = Signal(str)      # 发送错误信息
    log = Signal(str)        # 发送日志/状态信息

    def __init__(self, db_params, sql_query):
        super().__init__()
        self.db_params = db_params
        self.sql_query = sql_query
        self._is_cancelled = False

    def cancel(self):
        self._is_cancelled = True
        self.log.emit("正在请求取消查询...")

    def run(self):
        conn = None
        start_time = time.time()
        try:
            self.log.emit(f"正在连接数据库...")
            conn = psycopg2.connect(**self.db_params)
            
            if self._is_cancelled:
                self.error.emit("操作已在连接后取消")
                return

            self.log.emit("正在执行查询...")
            df = pd.read_sql_query(self.sql_query, conn)
            
            if self._is_cancelled:
                self.error.emit("操作已在查询后取消")
                return
            
            duration = time.time() - start_time
            self.log.emit(f"查询完成，获取到 {len(df)} 条记录。")
            self.finished.emit(df, duration)

        except Exception as e:
            self.log.emit(f"查询失败: {e}")
            self.error.emit(str(e))
        finally:
            if conn:
                conn.close()


class PandasModel(QStandardItemModel):
    def __init__(self, df: pd.DataFrame, parent=None):
        super().__init__(parent)
        self._df = df
        
        self.setColumnCount(len(df.columns))
        self.setRowCount(len(df.index))
        self.setHorizontalHeaderLabels(df.columns)

        for i in range(len(df.index)):
            for j in range(len(df.columns)):
                value = df.iloc[i, j]
                item = QStandardItem(str(value) if pd.notna(value) else "NULL")
                self.setItem(i, j, item)


class SqlLabTab(QWidget):
    def __init__(self, get_db_params_func, main_window, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.main_window = main_window # 用于调用 get_active_db_profile
        self.worker = None
        self.worker_thread = None
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Vertical)
        main_layout.addWidget(splitter)

        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)

        controls_layout = QHBoxLayout()
        self.execute_btn = QPushButton("▶️ 执行SQL")
        self.execute_btn.setStyleSheet("font-weight: bold; color: green;")
        self.execute_btn.clicked.connect(self.execute_sql)

        self.cancel_btn = QPushButton("⏹️ 取消")
        self.cancel_btn.setEnabled(False)
        self.cancel_btn.clicked.connect(self.cancel_execution)

        self.save_as_cohort_btn = QPushButton("💾 另存为队列表...")
        self.save_as_cohort_btn.setEnabled(False)
        self.save_as_cohort_btn.clicked.connect(self.save_as_cohort)

        self.limit_spinbox = QSpinBox()
        self.limit_spinbox.setRange(0, 100000)
        self.limit_spinbox.setValue(100)
        self.limit_spinbox.setSpecialValueText("无限制")
        
        controls_layout.addWidget(self.execute_btn)
        controls_layout.addWidget(self.cancel_btn)
        controls_layout.addWidget(self.save_as_cohort_btn)
        controls_layout.addStretch()
        controls_layout.addWidget(QLabel("结果行数限制:"))
        controls_layout.addWidget(self.limit_spinbox)
        top_layout.addLayout(controls_layout)

        self.sql_editor = QTextEdit()
        self.sql_editor.setPlaceholderText("在这里输入您的SQL查询...\n可以执行选中的文本，否则执行全部。")
        self.sql_editor.setFont(QFont("Consolas", 11))
        
        if PYGMENTS_AVAILABLE:
            self.highlighter = SqlHighlighter(self.sql_editor.document())

        top_layout.addWidget(self.sql_editor)
        splitter.addWidget(top_widget)

        bottom_widget = QWidget()
        bottom_layout = QVBoxLayout(bottom_widget)

        self.status_label = QLabel("状态: 未连接。请在“数据库连接”页面连接数据库。")
        bottom_layout.addWidget(self.status_label)
        
        self.result_table = QTableView()
        self.result_table.setAlternatingRowColors(True)
        self.result_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        bottom_layout.addWidget(self.result_table)

        splitter.addWidget(bottom_widget)
        splitter.setSizes([300, 400])

    def on_db_connected(self):
        self.status_label.setText("状态: 已连接，待执行查询。")
        self.execute_btn.setEnabled(True)

    def on_profile_changed(self):
        self.sql_editor.clear()
        self.result_table.setModel(None)
        db_connected = bool(self.get_db_params())
        if db_connected:
            self.on_db_connected()
        else:
            self.status_label.setText("状态: 未连接。请在“数据库连接”页面连接数据库。")
            self.execute_btn.setEnabled(False)

    def execute_sql(self):
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库。")
            return

        cursor = self.sql_editor.textCursor()
        query = cursor.selectedText().strip() if cursor.hasSelection() else self.sql_editor.toPlainText().strip()

        if not query:
            QMessageBox.warning(self, "无内容", "SQL编辑器中没有可执行的查询。")
            return
            
        limit = self.limit_spinbox.value()
        is_create_or_drop = any(keyword in query.upper().split() for keyword in ["CREATE", "DROP", "ALTER", "UPDATE", "INSERT"])
        if limit > 0 and 'limit' not in query.lower() and not is_create_or_drop:
             if query.endswith(';'):
                 query = query[:-1] + f" LIMIT {limit};"
             else:
                 query += f" LIMIT {limit}"

        self.prepare_for_long_operation(True)
        self.worker = SqlWorker(db_params, query)
        self.worker_thread = QThread()
        self.worker.moveToThread(self.worker_thread)

        self.worker_thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_query_finished)
        self.worker.error.connect(self.on_query_error)
        self.worker.log.connect(self.update_status_label)
        
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.error.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        self.worker_thread.finished.connect(self.worker.deleteLater)

        self.worker_thread.start()

    def cancel_execution(self):
        if self.worker:
            self.worker.cancel()

    def prepare_for_long_operation(self, starting: bool):
        self.execute_btn.setEnabled(not starting)
        self.cancel_btn.setEnabled(starting)
        # 仅在非空查询后才启用保存按钮
        self.save_as_cohort_btn.setEnabled(False)
        if starting:
            QApplication.setOverrideCursor(Qt.WaitCursor)
            self.result_table.setModel(None)
        else:
            QApplication.restoreOverrideCursor()
            # self.worker = None
            # self.worker_thread = None

    @Slot(object, float)
    def on_query_finished(self, df, duration):
        self.update_status_label(f"查询成功 (耗时: {duration:.2f}秒)，返回 {len(df)} 行。")
        self.result_table.setModel(PandasModel(df))
        self.prepare_for_long_operation(False)
        
        db_profile = self.main_window.get_active_db_profile()
        if not db_profile:
            return

        id_cols_mimic = ['subject_id', 'hadm_id']
        id_cols_eicu = ['patientunitstayid']
        is_mimic = all(col in df.columns for col in id_cols_mimic)
        is_eicu = all(col in df.columns for col in id_cols_eicu)
        
        if not df.empty and (is_mimic or is_eicu):
             self.save_as_cohort_btn.setEnabled(True)

    @Slot(str)
    def on_query_error(self, error_msg):
        self.update_status_label(f"查询失败!")
        QMessageBox.critical(self, "查询失败", f"执行SQL时出错:\n{error_msg}")
        self.prepare_for_long_operation(False)
    
    @Slot(str)
    def update_status_label(self, message):
        self.status_label.setText(f"状态: {message}")

    def save_as_cohort(self):
        db_profile = self.main_window.get_active_db_profile()
        if not db_profile:
             QMessageBox.critical(self, "错误", "无法获取当前数据库画像，无法确定存储位置。")
             return

        dialog = QDialog(self)
        dialog.setWindowTitle("另存为队列表")
        layout = QFormLayout(dialog)
        
        schema_name = db_profile.get_cohort_table_schema()
        
        schema_label = QLabel(f"将在 Schema <b>'{schema_name}'</b> 中创建新表:")
        name_input = QLineEdit()
        name_input.setPlaceholderText("请输入新表名 (例如: my_special_cohort)")
        layout.addRow(schema_label)
        layout.addRow("新表名:", name_input)
        
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addRow(button_box)
        
        if dialog.exec() == QDialog.Accepted:
            table_name = name_input.text().strip()
            # 简单的验证
            if not table_name or not table_name.replace('_', '').isalnum():
                QMessageBox.warning(self, "名称无效", "表名只能包含字母、数字和下划线。")
                return

            original_query = self.sql_editor.toPlainText().strip()
            if original_query.endswith(';'):
                original_query = original_query[:-1]

            # 移除可能存在的 LIMIT 子句
            if 'limit' in original_query.lower().split()[-2:]:
                 parts = original_query.lower().split()
                 limit_index = -1
                 try:
                     limit_index = parts.index('limit')
                     original_query = " ".join(original_query.split()[:limit_index])
                 except ValueError:
                     pass

            create_sql = f"CREATE TABLE {schema_name}.{table_name} AS ({original_query});"
            
            QApplication.setOverrideCursor(Qt.WaitCursor)
            db_params = self.get_db_params()
            try:
                with psycopg2.connect(**db_params) as conn:
                    conn.autocommit = True 
                    with conn.cursor() as cur:
                        cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")
                        cur.execute(create_sql)
                QMessageBox.information(self, "成功", f"队列表 '{schema_name}.{table_name}' 创建成功！\n请到“数据库结构查看”页面刷新列表查看。")
            except Exception as e:
                QMessageBox.critical(self, "创建失败", f"创建表失败:\n{e}")
            finally:
                QApplication.restoreOverrideCursor()
# --- END OF FILE tabs/tab_sql_lab.py ---