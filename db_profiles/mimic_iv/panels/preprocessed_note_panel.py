# --- START OF FILE db_profiles/mimic_iv/panels/preprocessed_note_panel.py ---
from PySide6.QtWidgets import (QVBoxLayout, QGroupBox, QComboBox, QHBoxLayout, 
                               QLabel, QListWidget, QAbstractItemView, QPushButton,
                               QApplication, QMessageBox, QListWidgetItem)
from PySide6.QtCore import Qt, Slot
import psycopg2
import traceback

from ui_components.base_panel import BaseSourceConfigPanel

class PreprocessedNotePanel(BaseSourceConfigPanel):
    """
    一个新的Panel，用于从mimiciv_note schema下选择预处理好的表，并将其列合并到队列表。
    """
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0, 0, 0, 0)
        panel_layout.setSpacing(10)

        # 1. 选择预处理好的表
        table_group = QGroupBox("1. 选择要合并的预处理笔记表")
        table_layout = QHBoxLayout(table_group)
        table_layout.addWidget(QLabel("可用表 (来自 mimiciv_note):"))
        self.table_combo = QComboBox()
        self.table_combo.currentIndexChanged.connect(self._on_table_selected)
        table_layout.addWidget(self.table_combo, 1)
        self.refresh_btn = QPushButton("刷新列表")
        self.refresh_btn.clicked.connect(self.populate_panel_if_needed)
        table_layout.addWidget(self.refresh_btn)
        panel_layout.addWidget(table_group)

        # 2. 选择要保留的列
        column_group = QGroupBox("2. 选择要添加到队列表的列")
        column_layout = QVBoxLayout(column_group)
        self.column_list = QListWidget()
        self.column_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.column_list.itemSelectionChanged.connect(self.config_changed_signal.emit)
        
        # 方便操作的按钮
        btn_layout = QHBoxLayout()
        self.select_all_btn = QPushButton("全选")
        self.select_all_btn.clicked.connect(lambda: self._set_all_columns_selected(True))
        self.deselect_all_btn = QPushButton("全不选")
        self.deselect_all_btn.clicked.connect(lambda: self._set_all_columns_selected(False))
        btn_layout.addWidget(self.select_all_btn)
        btn_layout.addWidget(self.deselect_all_btn)
        btn_layout.addStretch()
        
        column_layout.addLayout(btn_layout)
        column_layout.addWidget(self.column_list)
        panel_layout.addWidget(column_group)

        # 3. 信息提示
        info_label = QLabel("<b>提示:</b> 数据将通过 `hadm_id` 与队列表进行左连接 (LEFT JOIN)。")
        info_label.setWordWrap(True)
        panel_layout.addWidget(info_label)
        
        panel_layout.addStretch()
        self.setLayout(panel_layout)

    def populate_panel_if_needed(self):
        """动态查找 mimiciv_note schema 下的所有表来填充下拉框"""
        if not self._connect_panel_db():
            QMessageBox.warning(self, "数据库连接失败", "无法连接数据库以获取预处理表列表。")
            return

        self.table_combo.blockSignals(True)
        self.table_combo.clear()
        self.column_list.clear()
        
        try:
            # 查询 mimiciv_note schema 下的所有用户表
            self._db_cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'mimiciv_note'
                ORDER BY table_name;
            """)
            tables = self._db_cursor.fetchall()
            if tables:
                self.table_combo.addItems([table[0] for table in tables])
            else:
                self.table_combo.addItem("未找到预处理表")
        except Exception as e:
            self.table_combo.addItem("获取列表失败")
            QMessageBox.critical(self, "查询失败", f"无法获取 `mimiciv_note` schema下的表: {e}")
        finally:
            self.table_combo.blockSignals(False)
            self._close_panel_db()
            # 手动触发一次，加载第一个表的列
            if self.table_combo.count() > 0:
                self._on_table_selected()

    @Slot()
    def _on_table_selected(self):
        """当用户选择一个新表时，查询并显示它的列"""
        table_name = self.table_combo.currentText()
        if not table_name or "未找到" in table_name or "失败" in table_name:
            self.column_list.clear()
            return
            
        if not self._connect_panel_db():
            QMessageBox.warning(self, "数据库连接失败", "无法连接数据库以获取表的列信息。")
            return

        self.column_list.clear()
        try:
            self._db_cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'mimiciv_note' AND table_name = %s
                AND column_name NOT IN ('subject_id', 'hadm_id') -- 自动排除连接键
                ORDER BY ordinal_position;
            """, (table_name,))
            columns = self._db_cursor.fetchall()
            for col in columns:
                self.column_list.addItem(QListWidgetItem(col[0]))
            self._set_all_columns_selected(True) # 默认全选
        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取表 '{table_name}' 的列: {e}")
        finally:
            self._close_panel_db()
            self.config_changed_signal.emit()

    def _set_all_columns_selected(self, select: bool):
        for i in range(self.column_list.count()):
            self.column_list.item(i).setSelected(select)

    def get_friendly_source_name(self) -> str:
        return f"笔记预处理表 ({self.table_combo.currentText()})"

    def clear_panel_state(self):
        # 刷新列表即可，因为没有太多状态
        self.populate_panel_if_needed()

    def get_panel_config(self) -> dict:
        selected_table = self.table_combo.currentText()
        if not selected_table or "未找到" in selected_table or "失败" in selected_table:
            return {}

        selected_cols = [item.text() for item in self.column_list.selectedItems()]
        if not selected_cols:
            return {}

        # 返回一个特殊结构的配置，让SQL构建器知道这是新模式
        return {
            "panel_type": "merge_preprocessed",  # <--- 关键标识符
            "source_event_table": f"mimiciv_note.{selected_table}",
            "selected_columns": selected_cols,
            "join_key": "hadm_id", # 固定连接键
            "primary_item_label_for_naming": selected_table
        }