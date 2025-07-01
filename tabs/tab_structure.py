# --- START OF FILE tabs/tab_structure.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QPushButton, QTreeWidget,
                               QTreeWidgetItem, QMessageBox, QMenu, QApplication)
from PySide6.QtCore import Qt, Slot, Signal
import psycopg2

class StructureTab(QWidget):
    request_table_preview_signal = Signal(str, str) # schema_name, table_name
    request_send_to_sql_lab_signal = Signal(str) # <<< 新增信号

    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        self.view_btn = QPushButton("刷新数据库结构")
        self.view_btn.setEnabled(False)
        self.view_btn.clicked.connect(self.view_db_structure)
        layout.addWidget(self.view_btn)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Schema / Table", "Type"])
        self.tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.show_context_menu)
        self.tree.itemDoubleClicked.connect(self.handle_item_double_clicked)
        layout.addWidget(self.tree)

    def on_db_connected(self):
        """Called when the database connection is established."""
        self.view_btn.setEnabled(True)
        self.view_db_structure()
    
    def on_profile_changed(self):
        """Called when the database profile changes."""
        self.tree.clear()
        self.view_btn.setEnabled(False)

    @Slot()
    def view_db_structure(self):
        self.tree.clear()
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库")
            return

        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            cur.execute("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                  AND schema_name NOT LIKE 'pg_temp%'
                ORDER BY schema_name;
            """)
            schemas = cur.fetchall()
            
            for schema_row in schemas:
                schema_name = schema_row[0]
                schema_item = QTreeWidgetItem([schema_name, "Schema"])
                self.tree.addTopLevelItem(schema_item)

                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """, (schema_name,))
                tables = cur.fetchall()
                for table_row in tables:
                    table_name = table_row[0]
                    table_item = QTreeWidgetItem([table_name, "Table"])
                    # Store schema and table name in the item's data role
                    table_item.setData(0, Qt.ItemDataRole.UserRole, (schema_name, table_name))
                    schema_item.addChild(table_item)
            
            self.tree.expandToDepth(0)
            self.tree.resizeColumnToContents(0)
            self.tree.resizeColumnToContents(1)
        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取数据库结构: {str(e)}")
        finally:
            if conn:
                conn.close()
            QApplication.restoreOverrideCursor()

    @Slot(QTreeWidgetItem, int)
    def handle_item_double_clicked(self, item, column):
        """Handles double-click on a table item to trigger preview."""
        item_data = item.data(0, Qt.ItemDataRole.UserRole)
        if item_data and isinstance(item_data, tuple) and len(item_data) == 2:
            schema_name, table_name = item_data
            self.request_table_preview_signal.emit(schema_name, table_name)

    @Slot(object)
    def show_context_menu(self, position):
        item = self.tree.itemAt(position)
        if not item: return

        item_data = item.data(0, Qt.ItemDataRole.UserRole)
        if not (item_data and isinstance(item_data, tuple) and len(item_data) == 2):
            return

        schema_name, table_name = item_data
        full_table_name = f"{schema_name}.{table_name}"
        
        menu = QMenu()
        preview_action = menu.addAction(f"在导出页预览: {full_table_name}")
        
        # 新增的联动菜单项
        sql_lab_preview_action = menu.addAction(f"在SQL实验室中预览")
        sql_lab_count_action = menu.addAction(f"在SQL实验室中统计行数")
        
        action = menu.exec(self.tree.viewport().mapToGlobal(position))

        if action == preview_action:
            self.request_table_preview_signal.emit(schema_name, table_name)
        elif action == sql_lab_preview_action:
            sql = f"SELECT * FROM {full_table_name} LIMIT 100;"
            self.request_send_to_sql_lab_signal.emit(sql) # <<< 发出信号
        elif action == sql_lab_count_action:
            sql = f"SELECT COUNT(*) FROM {full_table_name};"
            self.request_send_to_sql_lab_signal.emit(sql) # <<< 发出信号

    def confirm_delete_table(self, schema_name, table_name):
        reply = QMessageBox.question(self, '确认删除',
                                     f"您确定要永久删除表 '{schema_name}.{table_name}' 吗？\n"
                                     "这个操作无法撤销！",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            self.delete_table(schema_name, table_name)

    def delete_table(self, schema_name, table_name):
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库。")
            return

        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            table_identifier = psycopg2.sql.Identifier(schema_name, table_name)
            drop_sql = psycopg2.sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(table_identifier)
            cur.execute(drop_sql)
            conn.commit()
            QMessageBox.information(self, "删除成功", f"表 '{schema_name}.{table_name}' 已成功删除。")
            self.view_db_structure()
        except Exception as e:
            if conn: conn.rollback()
            QMessageBox.critical(self, "删除失败", f"无法删除表 '{schema_name}.{table_name}':\n{str(e)}")
        finally:
            if conn: conn.close()
            QApplication.restoreOverrideCursor()