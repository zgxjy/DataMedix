# --- START OF FILE tabs/tab_data_export.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QComboBox, QGroupBox,
                          QFileDialog, QLineEdit, QSpinBox, QGridLayout, QAbstractItemView, QApplication)
from PySide6.QtCore import Qt, Slot
import psycopg2
import psycopg2.sql as pgsql
import os
import pandas as pd
import traceback
import numpy as np

class DataExportTab(QWidget):
    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.selected_table_schema = None
        self.selected_table_name = None
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Orientation.Vertical)
        main_layout.addWidget(splitter)

        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        splitter.addWidget(top_widget)

        schema_table_group = QGroupBox("选择要导出的表")
        schema_table_layout = QGridLayout(schema_table_group)
        schema_table_layout.addWidget(QLabel("Schema:"), 0, 0)
        self.schema_combo = QComboBox()
        self.schema_combo.currentIndexChanged.connect(lambda: self.refresh_tables(schema_changed=True))
        schema_table_layout.addWidget(self.schema_combo, 0, 1)
        schema_table_layout.addWidget(QLabel("数据表:"), 1, 0)
        self.table_combo = QComboBox()
        self.table_combo.setMinimumWidth(300)
        self.table_combo.currentIndexChanged.connect(self.on_table_selected)
        schema_table_layout.addWidget(self.table_combo, 1, 1)
        self.refresh_btn = QPushButton("刷新列表")
        self.refresh_btn.clicked.connect(self.refresh_schemas_and_tables)
        self.refresh_btn.setEnabled(False)
        schema_table_layout.addWidget(self.refresh_btn, 0, 2, 2, 1)
        top_layout.addWidget(schema_table_group)

        export_options_group = QGroupBox("导出选项")
        export_options_layout = QVBoxLayout(export_options_group)
        format_layout = QHBoxLayout()
        format_layout.addWidget(QLabel("导出格式:"))
        self.format_combo = QComboBox()
        self.format_combo.addItems(["CSV (.csv)", "Parquet (.parquet)", "Excel (.xlsx)"])
        self.format_combo.currentTextChanged.connect(self._update_export_path_suggestion)
        format_layout.addWidget(self.format_combo)
        format_layout.addStretch()
        export_options_layout.addLayout(format_layout)

        path_layout = QHBoxLayout()
        self.export_path_input = QLineEdit()
        self.export_path_input.setPlaceholderText("选择导出文件路径...")
        self.browse_btn = QPushButton("浏览...")
        self.browse_btn.clicked.connect(self.browse_export_path)
        path_layout.addWidget(QLabel("导出文件:"))
        path_layout.addWidget(self.export_path_input, 1)
        path_layout.addWidget(self.browse_btn)
        export_options_layout.addLayout(path_layout)
        
        limit_layout = QHBoxLayout()
        limit_layout.addWidget(QLabel("导出最大行数 (0=全部):"))
        self.limit_spinbox = QSpinBox()
        self.limit_spinbox.setRange(0, 2000000000)
        self.limit_spinbox.setValue(0)
        self.limit_spinbox.setSpecialValueText("全部")
        limit_layout.addWidget(self.limit_spinbox)
        limit_layout.addStretch()
        export_options_layout.addLayout(limit_layout)
        top_layout.addWidget(export_options_group)

        action_layout = QHBoxLayout()
        self.preview_btn = QPushButton("预览数据")
        self.preview_btn.clicked.connect(self.preview_data)
        self.preview_btn.setEnabled(False)
        action_layout.addWidget(self.preview_btn)
        self.export_btn = QPushButton("导出数据")
        self.export_btn.clicked.connect(self.export_data)
        self.export_btn.setEnabled(False)
        action_layout.addWidget(self.export_btn)
        top_layout.addLayout(action_layout)

        result_widget = QWidget()
        result_layout = QVBoxLayout(result_widget)
        splitter.addWidget(result_widget)
        
        result_layout.addWidget(QLabel("SQL预览 (数据导出/预览):"))
        self.sql_preview_display = QTextEdit()
        self.sql_preview_display.setReadOnly(True)
        self.sql_preview_display.setMaximumHeight(80)
        result_layout.addWidget(self.sql_preview_display)

        preview_options_layout = QHBoxLayout()
        preview_options_layout.addWidget(QLabel("预览行数:"))
        self.preview_spinbox = QSpinBox()
        self.preview_spinbox.setRange(10, 1000)
        self.preview_spinbox.setValue(100)
        preview_options_layout.addWidget(self.preview_spinbox)
        preview_options_layout.addStretch()
        result_layout.addLayout(preview_options_layout)
        
        self.result_table = QTableWidget()
        self.result_table.setAlternatingRowColors(True)
        self.result_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        result_layout.addWidget(self.result_table)
        splitter.setSizes([400, 450])

    def _connect_db(self):
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先在“数据库连接”页面连接数据库")
            return None
        try:
            return psycopg2.connect(**db_params)
        except Exception as e:
            QMessageBox.critical(self, "数据库连接失败", f"无法连接到数据库: {str(e)}")
            return None

    @Slot()
    def on_db_connected(self):
        self.refresh_btn.setEnabled(True)
        self.refresh_schemas_and_tables()

    @Slot()
    def on_profile_changed(self):
        # When profile changes, the connection is reset. We should clear the lists.
        self.schema_combo.clear()
        self.table_combo.clear()
        self.result_table.clear()
        self.result_table.setRowCount(0)
        self.result_table.setColumnCount(0)
        self.sql_preview_display.clear()
        self.export_path_input.clear()
        self.preview_btn.setEnabled(False)
        self.export_btn.setEnabled(False)
        self.refresh_btn.setEnabled(False) # Will be re-enabled on DB connection

    def refresh_schemas_and_tables(self):
        conn = self._connect_db()
        if not conn: return
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT schema_name FROM information_schema.schemata
                    WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
                      AND schema_name NOT LIKE 'pg_toast%' AND schema_name NOT LIKE 'pg_temp%'
                    ORDER BY schema_name;
                """)
                schemas = [s[0] for s in cur.fetchall()]
                current_schema = self.schema_combo.currentText()
                self.schema_combo.blockSignals(True)
                self.schema_combo.clear()
                self.schema_combo.addItems(schemas)
                if current_schema in schemas:
                    self.schema_combo.setCurrentText(current_schema)
                elif schemas:
                    self.schema_combo.setCurrentIndex(0)
                self.schema_combo.blockSignals(False)
                # Manually trigger the refresh for the newly set schema
                self.refresh_tables(schema_changed=True)
        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取 Schemas: {str(e)}")
        finally:
            if conn: conn.close()

    def refresh_tables(self, schema_changed=False):
        selected_schema = self.schema_combo.currentText()
        if not selected_schema:
            self.table_combo.clear()
            self.table_combo.addItem("请先选择 Schema")
            self.preview_btn.setEnabled(False)
            self.export_btn.setEnabled(False)
            return

        conn = self._connect_db()
        if not conn: return
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name", (selected_schema,))
                table_names = [table[0] for table in cur.fetchall()]
                
                current_table_text = self.table_combo.currentText() if not schema_changed and self.table_combo.count() > 0 else None

                self.table_combo.blockSignals(True)
                self.table_combo.clear()
                if table_names:
                    self.table_combo.addItems(table_names)
                    if current_table_text and current_table_text in table_names:
                        self.table_combo.setCurrentText(current_table_text)
                    else:
                        self.table_combo.setCurrentIndex(0)
                else:
                    self.table_combo.addItem("未找到表")
                self.table_combo.blockSignals(False)
                
                # Explicitly call on_table_selected to update state
                self.on_table_selected(self.table_combo.currentIndex())
        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取 '{selected_schema}' 中的表列表: {str(e)}")
        finally:
            if conn: conn.close()

    def on_table_selected(self, index):
        if index >= 0 and self.table_combo.count() > 0 and "未找到" not in self.table_combo.itemText(index) and self.schema_combo.currentText():
            self.selected_table_schema = self.schema_combo.currentText()
            self.selected_table_name = self.table_combo.itemText(index)
            self.preview_btn.setEnabled(True)
            self.export_btn.setEnabled(True)
            self._update_export_path_suggestion()
        else:
            self.selected_table_schema = None
            self.selected_table_name = None
            self.preview_btn.setEnabled(False)
            self.export_btn.setEnabled(False)
            self.export_path_input.clear()
            self.sql_preview_display.clear()
            self.result_table.clear()
            self.result_table.setRowCount(0); self.result_table.setColumnCount(0)

    def _update_export_path_suggestion(self):
        if self.selected_table_name and self.selected_table_schema:
             current_dir = os.path.dirname(self.export_path_input.text()) if self.export_path_input.text() and os.path.isabs(self.export_path_input.text()) else os.getcwd()
             file_ext_map = {"CSV": ".csv", "Parquet": ".parquet", "Excel": ".xlsx"}
             fmt_key = self.format_combo.currentText().split(" ")[0]
             file_ext = file_ext_map.get(fmt_key, ".csv")
             suggested_path = os.path.join(current_dir, f"{self.selected_table_schema}_{self.selected_table_name}{file_ext}")
             self.export_path_input.setText(suggested_path.replace("\\", "/"))

    def browse_export_path(self):
        if not self.selected_table_name:
            QMessageBox.warning(self, "未选定表", "请先选择要导出的表。")
            return
        fmt_map = {"CSV": ("CSV 文件 (*.csv)", ".csv"), "Parquet": ("Parquet 文件 (*.parquet)", ".parquet"), "Excel": ("Excel 文件 (*.xlsx)", ".xlsx")}
        fmt_key = self.format_combo.currentText().split(" ")[0]
        file_filter, default_ext = fmt_map.get(fmt_key, ("所有文件 (*)", ""))
        default_filename = f"{self.selected_table_schema}_{self.selected_table_name}{default_ext}"
        
        start_dir = os.path.dirname(self.export_path_input.text()) or os.getcwd()

        filePath, _ = QFileDialog.getSaveFileName(self, "选择导出文件", os.path.join(start_dir, default_filename), file_filter)
        if filePath:
            if default_ext and not filePath.lower().endswith(default_ext.lower()):
                filePath += default_ext
            self.export_path_input.setText(filePath.replace("\\", "/"))

    @Slot(str, str)
    def preview_specific_table(self, schema_name, table_name):
        if not self.refresh_btn.isEnabled():
            self.on_db_connected() 
            QApplication.processEvents()
            if not self.refresh_btn.isEnabled():
                QMessageBox.warning(self, "预览联动失败", "数据库连接似乎有问题，无法刷新列表。")
                return

        self.schema_combo.blockSignals(True)
        schema_idx = self.schema_combo.findText(schema_name, Qt.MatchFlag.MatchFixedString)
        if schema_idx >= 0:
            if self.schema_combo.currentIndex() != schema_idx:
                self.schema_combo.setCurrentIndex(schema_idx)
                self.refresh_tables(schema_changed=True)
        else:
            self.schema_combo.blockSignals(False)
            QMessageBox.warning(self, "预览联动失败", f"未找到 Schema '{schema_name}'。")
            return
        self.schema_combo.blockSignals(False)
        QApplication.processEvents()

        self.table_combo.blockSignals(True)
        table_idx = self.table_combo.findText(table_name, Qt.MatchFlag.MatchFixedString)
        if table_idx >= 0:
            self.table_combo.setCurrentIndex(table_idx) 
            self.on_table_selected(table_idx)
        else:
            self.table_combo.blockSignals(False)
            QMessageBox.warning(self, "预览联动失败", f"在 Schema '{schema_name}' 中未动态找到表 '{table_name}'。")
            return
        self.table_combo.blockSignals(False)
        QApplication.processEvents()

        if self.selected_table_schema == schema_name and self.selected_table_name == table_name:
            self.preview_data()
        else:
            QMessageBox.warning(self, "预览联动失败", f"无法自动选中表 '{schema_name}.{table_name}' 进行预览。")

    @Slot()
    def preview_data(self):
        if not self.selected_table_name or not self.selected_table_schema:
            QMessageBox.warning(self, "未选择表", "请先选择 Schema 和数据表。")
            return

        conn = self._connect_db()
        if not conn: return

        try:
            preview_limit = self.preview_spinbox.value()
            table_identifier = pgsql.Identifier(self.selected_table_schema, self.selected_table_name)
            query = pgsql.SQL("SELECT * FROM {table} ORDER BY RANDOM() LIMIT {limit}").format(
                table=table_identifier, limit=pgsql.Literal(preview_limit))
            
            with conn.cursor() as cur:
                final_sql_string = cur.mogrify(query).decode(conn.encoding or 'utf-8')

            self.sql_preview_display.setText(f"-- Preview Query:\n{final_sql_string}")

            df = pd.read_sql_query(final_sql_string, conn)

            self.result_table.clearContents()
            self.result_table.setRowCount(df.shape[0])
            self.result_table.setColumnCount(df.shape[1])
            self.result_table.setHorizontalHeaderLabels(df.columns)
            
            # REPAIR: New logic to handle array-like data in cells
            for i in range(df.shape[0]):
                for j in range(df.shape[1]):
                    value = df.iloc[i, j]
                    
                    # Check if the value is a list, tuple, or numpy array
                    if isinstance(value, (list, tuple, np.ndarray)):
                        # If it's an array-like object, convert it directly to a string for display
                        display_text = str(value)
                    else:
                        # If it's a scalar value, use pd.notna for checking
                        display_text = str(value) if pd.notna(value) else ""
                        
                    self.result_table.setItem(i, j, QTableWidgetItem(display_text))

            self.result_table.resizeColumnsToContents()

            with conn.cursor() as cur:
                cur.execute(pgsql.SQL("SELECT COUNT(*) FROM {table}").format(table=table_identifier))
                total_rows = cur.fetchone()[0]
            QMessageBox.information(self, "预览成功", f"表 {self.selected_table_schema}.{self.selected_table_name} (共 {total_rows} 行) 加载预览 {df.shape[0]} 行。")
        except Exception as e:
            QMessageBox.critical(self, "预览失败", f"无法预览数据: {str(e)}\n{traceback.format_exc()}")
            self.sql_preview_display.append(f"\n-- ERROR: {str(e)}")
        finally:
            if conn: conn.close()
            
    # The export_data method does not need changes, as it writes the whole DataFrame
    # and doesn't iterate cell by cell. The pandas `to_csv`/`to_excel` methods
    # handle array-like objects correctly by default (usually by writing their string representation).

    def export_data(self):
        if not self.selected_table_name or not self.selected_table_schema:
            QMessageBox.warning(self, "未选择表", "请选择要导出的 Schema 和数据表。"); return
        export_file_path = self.export_path_input.text()
        if not export_file_path:
            QMessageBox.warning(self, "未指定文件", "请指定导出路径。")
            return
        
        export_dir = os.path.dirname(export_file_path)
        if not os.path.exists(export_dir):
            try:
                os.makedirs(export_dir)
            except Exception as e:
                QMessageBox.critical(self, "创建目录失败", f"无法创建 '{export_dir}': {e}")
                return

        conn = self._connect_db()
        if not conn: return

        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:
            table_identifier = pgsql.Identifier(self.selected_table_schema, self.selected_table_name)
            query_sql_obj = psql.SQL("SELECT * FROM {table}").format(table=table_identifier)
            limit_value = self.limit_spinbox.value()
            if limit_value > 0:
                query_sql_obj += psql.SQL(" LIMIT {limit}").format(limit=psql.Literal(limit_value))
            
            # REPAIR: Use mogrify to get a clean SQL string.
            with conn.cursor() as cur:
                final_sql_for_export = cur.mogrify(query_sql_obj).decode(conn.encoding or 'utf-8')
            
            self.sql_preview_display.setText(f"-- Export Query:\n{final_sql_for_export}")
            
            # REPAIR: Call read_sql_query with only the SQL string.
            df = pd.read_sql_query(final_sql_for_export, conn)

            export_format = self.format_combo.currentText()
            if "CSV" in export_format:
                df.to_csv(export_file_path, index=False, encoding='utf-8-sig')
            elif "Parquet" in export_format:
                try:
                    df.to_parquet(export_file_path, index=False)
                except ImportError:
                    raise ImportError("导出 Parquet 需 'pyarrow' 库: pip install pyarrow")
            elif "Excel" in export_format:
                if len(df) > 100000:
                    if QMessageBox.question(self, "内存警告", f"导出整个表 ({len(df)} 行) 为 Excel 可能消耗大量内存并耗时较久。\n是否继续？",
                                             QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No) == QMessageBox.StandardButton.No:
                        raise InterruptedError("用户取消了大型Excel导出。")
                try:
                    df.to_excel(export_file_path, index=False, engine='openpyxl')
                except ImportError:
                    raise ImportError("导出 Excel 需 'openpyxl' 库: pip install openpyxl")
            
            QMessageBox.information(self, "导出成功", f"已成功导出 {len(df)} 条记录到:\n{export_file_path}")
        
        except (Exception, ImportError, InterruptedError) as e:
            QMessageBox.critical(self, "导出失败", f"无法导出数据: {str(e)}\n{traceback.format_exc()}")
        finally:
            if conn: conn.close()
            QApplication.restoreOverrideCursor()