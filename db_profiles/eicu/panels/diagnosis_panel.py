# --- START OF FILE db_profiles/eicu/panels/diagnosis_panel.py ---
from PySide6.QtWidgets import (QVBoxLayout, QHBoxLayout, QPushButton,
                               QListWidget, QListWidgetItem, QAbstractItemView, QTextEdit,
                               QApplication, QGroupBox, QLabel, QMessageBox, QScrollArea, QFrame)
from PySide6.QtCore import Qt, Slot
import psycopg2.sql as pgsql
import traceback
from typing import Optional

from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.conditiongroup import ConditionGroupWidget
from ui_components.event_output_widget import EventOutputWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class EicuDiagnosisPanel(BaseSourceConfigPanel):
    """
    用于配置从 e-ICU 的 `diagnosis` 表提取诊断事件的Panel。
    """
    def init_panel_ui(self):
        # --- UI 布局和组件与您其他事件类Panel保持一致 ---
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0, 0, 0, 0)

        filter_group = QGroupBox("筛选诊断 (来自 public.diagnosis)")
        filter_group_layout = QVBoxLayout(filter_group)
        filter_group_layout.setSpacing(8)

        self.condition_widget = ConditionGroupWidget(is_root=True)
        self.condition_widget.condition_changed.connect(self.config_changed_signal.emit)
        cg_scroll_area_panel = QScrollArea()
        cg_scroll_area_panel.setWidgetResizable(True)
        cg_scroll_area_panel.setWidget(self.condition_widget)
        cg_scroll_area_panel.setMinimumHeight(200)
        filter_group_layout.addWidget(cg_scroll_area_panel, 2)

        filter_action_layout = QHBoxLayout()
        filter_action_layout.addStretch()
        self.filter_items_btn = QPushButton("筛选诊断项目")
        self.filter_items_btn.clicked.connect(self._filter_items_action)
        filter_action_layout.addWidget(self.filter_items_btn)
        filter_group_layout.addLayout(filter_action_layout)

        separator1 = QFrame()
        separator1.setFrameShape(QFrame.Shape.HLine)
        separator1.setFrameShadow(QFrame.Shadow.Sunken)
        filter_group_layout.addWidget(separator1)

        filter_group_layout.addWidget(QLabel("最近筛选SQL预览:"))
        self.filter_sql_preview_textedit = QTextEdit()
        self.filter_sql_preview_textedit.setReadOnly(True)
        self.filter_sql_preview_textedit.setFixedHeight(60)
        self.filter_sql_preview_textedit.setPlaceholderText("执行“筛选诊断项目”后将在此显示SQL...")
        filter_group_layout.addWidget(self.filter_sql_preview_textedit)
        
        separator2 = QFrame()
        separator2.setFrameShape(QFrame.Shape.HLine)
        separator2.setFrameShadow(QFrame.Shadow.Sunken)
        filter_group_layout.addWidget(separator2)

        self.item_list = QListWidget()
        self.item_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.item_list.itemSelectionChanged.connect(self._on_item_selection_changed)
        item_list_scroll_area = QScrollArea()
        item_list_scroll_area.setWidgetResizable(True)
        item_list_scroll_area.setWidget(self.item_list)
        item_list_scroll_area.setMinimumHeight(100)
        filter_group_layout.addWidget(item_list_scroll_area, 1)

        self.selected_items_label = QLabel("已选项目: 0")
        self.selected_items_label.setAlignment(Qt.AlignmentFlag.AlignRight)
        filter_group_layout.addWidget(self.selected_items_label)
        panel_layout.addWidget(filter_group)

        logic_group = QGroupBox("提取逻辑")
        logic_group_layout = QVBoxLayout(logic_group)
        self.event_output_widget = EventOutputWidget()
        self.event_output_widget.output_type_changed.connect(self.config_changed_signal.emit)
        logic_group_layout.addWidget(self.event_output_widget)

        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口 (相对于ICU入院):")
        self.time_window_widget.time_window_changed.connect(lambda: self.config_changed_signal.emit())
        logic_group_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)

    def populate_panel_if_needed(self):
        # e-ICU诊断表没有单独的字典表，因此直接使用其列进行筛选
        available_fields = [
            ("diagnosisstring", "诊断字符串 (包含)"),
            ("icd9code", "ICD-9编码 (包含)")
        ]
        self.condition_widget.set_available_search_fields(available_fields)
        
        # 为e-ICU设置合适的时间窗口选项
        self.time_window_widget.set_options([
            "ICU入住24小时内",
            "ICU入住48小时内",
            "整个ICU期间",
        ])
        
    def get_friendly_source_name(self) -> str:
        return "e-ICU 诊断 (diagnosis)"
    
    def get_panel_config(self) -> dict:
        selected_ids = self.get_selected_item_ids()
        current_event_outputs = self.event_output_widget.get_selected_outputs()

        # 如果没有选择任何诊断项或任何输出方式，则配置无效
        if not selected_ids or not any(current_event_outputs.values()):
            return {}

        return {
            "source_event_table": "public.diagnosis",
            "item_id_column_in_event_table": "diagnosisstring",  # 使用诊断字符串作为筛选依据
            "selected_item_ids": selected_ids,
            "value_column_to_extract": None,  # 事件类Panel没有数值列
            "time_column_in_event_table": "diagnosisoffset",
            "aggregation_methods": {},  # 事件类Panel没有数值聚合
            "event_outputs": current_event_outputs,
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": self._get_primary_item_label_for_naming(),
            "cte_join_on_cohort_override": None, # 使用标准时间窗口逻辑
        }

    def _get_primary_item_label_for_naming(self) -> Optional[str]:
        if self.item_list.selectedItems():
            # 诊断字符串本身就是最好的标签
            return self.item_list.selectedItems()[0].text()
        return None

    def clear_panel_state(self):
        self.condition_widget.clear_all()
        self.item_list.clear()
        self.selected_items_label.setText("已选项目: 0")
        self.filter_sql_preview_textedit.clear()
        self.event_output_widget.clear_selections()
        self.time_window_widget.clear_selection()

    def _on_item_selection_changed(self):
        count = len(self.item_list.selectedItems())
        self.selected_items_label.setText(f"已选项目: {count}")
        self.config_changed_signal.emit()

    @Slot()
    def _filter_items_action(self):
        if not self._connect_panel_db():
            QMessageBox.warning(self, "数据库连接失败", "无法连接到数据库以筛选项目。")
            return

        # 直接从 public.diagnosis 表中筛选不重复的诊断字符串
        event_table, name_col = "public.diagnosis", "diagnosisstring"
        condition_sql_template, condition_params = self.condition_widget.get_condition()

        self.item_list.clear()
        self.item_list.addItem("正在查询...")
        self.filter_items_btn.setEnabled(False)
        QApplication.processEvents()

        if not condition_sql_template:
            self.item_list.clear()
            self.item_list.addItem("请输入筛选条件。")
            self.filter_items_btn.setEnabled(True)
            self._close_panel_db()
            return
            
        try:
            query_template_obj = pgsql.SQL("SELECT DISTINCT {name} FROM {table} WHERE {cond} ORDER BY {name} LIMIT 500").format(
                name=pgsql.Identifier(name_col), 
                table=pgsql.SQL(event_table), 
                cond=pgsql.SQL(condition_sql_template)
            )
            
            if self._db_conn and not self._db_conn.closed:
                mogrified_sql = self._db_cursor.mogrify(query_template_obj, condition_params).decode(self._db_conn.encoding or 'utf-8')
                self.filter_sql_preview_textedit.setText(mogrified_sql)

            self._db_cursor.execute(query_template_obj, condition_params)
            items = self._db_cursor.fetchall()
            self.item_list.clear()
            
            if items:
                for item_tuple in items:
                    diagnosis_string = str(item_tuple[0]) if item_tuple[0] is not None else "Unknown Diagnosis"
                    list_item = QListWidgetItem(diagnosis_string)
                    # UserRole数据存储 (ID, DisplayName)，这里ID和DisplayName都是诊断字符串
                    list_item.setData(Qt.ItemDataRole.UserRole, (diagnosis_string, diagnosis_string))
                    self.item_list.addItem(list_item)
            else: 
                self.item_list.addItem("未找到符合条件的诊断")
        except Exception as e:
            self.item_list.clear()
            self.item_list.addItem("查询项目出错!")
            QMessageBox.critical(self, "筛选项目失败", f"查询项目时出错: {str(e)}\n{traceback.format_exc()}")
        finally:
            self.filter_items_btn.setEnabled(True)
            self._close_panel_db()
            self.config_changed_signal.emit()
            
    def update_panel_action_buttons_state(self, general_config_ok: bool):
        # 筛选按钮的可用状态取决于“主配置是否OK”和“本面板的条件是否有效”
        has_valid_conditions_in_panel = self.condition_widget.has_valid_input()
        can_filter = general_config_ok and has_valid_conditions_in_panel
        self.filter_items_btn.setEnabled(can_filter)

# --- END OF FILE db_profiles/eicu/panels/diagnosis_panel.py ---