# --- START OF FULLY CORRECTED FILE: db_profiles/mimic_iv/panels/procedure_panel.py ---
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

class ProcedureConfigPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self); panel_layout.setContentsMargins(0,0,0,0)
        filter_group = QGroupBox("筛选操作/手术 (来自 mimc_hosp.d_icd_procedures)"); filter_group_layout = QVBoxLayout(filter_group); filter_group_layout.setSpacing(8)
        self.condition_widget = ConditionGroupWidget(is_root=True); self.condition_widget.condition_changed.connect(self.config_changed_signal.emit)
        cg_scroll_area_panel = QScrollArea(); cg_scroll_area_panel.setWidgetResizable(True); cg_scroll_area_panel.setWidget(self.condition_widget); cg_scroll_area_panel.setMinimumHeight(200); filter_group_layout.addWidget(cg_scroll_area_panel, 2)
        filter_action_layout = QHBoxLayout(); filter_action_layout.addStretch()
        self.filter_items_btn = QPushButton("筛选操作项目"); self.filter_items_btn.clicked.connect(self._filter_items_action); filter_action_layout.addWidget(self.filter_items_btn); filter_group_layout.addLayout(filter_action_layout)
        separator1 = QFrame(); separator1.setFrameShape(QFrame.Shape.HLine); separator1.setFrameShadow(QFrame.Shadow.Sunken); filter_group_layout.addWidget(separator1)
        filter_group_layout.addWidget(QLabel("最近筛选SQL预览:"))
        self.filter_sql_preview_textedit = QTextEdit(); self.filter_sql_preview_textedit.setReadOnly(True); self.filter_sql_preview_textedit.setFixedHeight(60); self.filter_sql_preview_textedit.setPlaceholderText("执行“筛选操作项目”后将在此显示SQL..."); filter_group_layout.addWidget(self.filter_sql_preview_textedit)
        separator2 = QFrame(); separator2.setFrameShape(QFrame.Shape.HLine); separator2.setFrameShadow(QFrame.Shadow.Sunken); filter_group_layout.addWidget(separator2)
        self.item_list = QListWidget(); self.item_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection); self.item_list.itemSelectionChanged.connect(self._on_item_selection_changed)
        item_list_scroll_area = QScrollArea(); item_list_scroll_area.setWidgetResizable(True); item_list_scroll_area.setWidget(self.item_list); item_list_scroll_area.setMinimumHeight(100); filter_group_layout.addWidget(item_list_scroll_area, 1)
        self.selected_items_label = QLabel("已选项目: 0"); self.selected_items_label.setAlignment(Qt.AlignmentFlag.AlignRight); filter_group_layout.addWidget(self.selected_items_label)
        panel_layout.addWidget(filter_group)
        logic_group = QGroupBox("提取逻辑"); logic_group_layout = QVBoxLayout(logic_group)
        self.event_output_widget = EventOutputWidget(); self.event_output_widget.output_type_changed.connect(self.config_changed_signal.emit); logic_group_layout.addWidget(self.event_output_widget)
        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口:"); self.time_window_widget.time_window_changed.connect(lambda: self.config_changed_signal.emit()); logic_group_layout.addWidget(self.time_window_widget)
        panel_layout.addWidget(logic_group); self.setLayout(panel_layout)

    def populate_panel_if_needed(self):
        available_fields = [("long_title", "操作描述 (Long Title)"), ("icd_code", "操作代码 (ICD Code 精确)"), ("icd_version", "ICD 版本 (精确)")]
        self.condition_widget.set_available_search_fields(available_fields)
        self.time_window_widget.set_options(["整个住院期间", "整个ICU期间", "住院以前 (既往史)"])
        
    def get_friendly_source_name(self): return "操作/手术 (Procedures - d_icd_procedures)"
    def get_panel_config(self) -> dict:
        condition_sql, condition_params = self.condition_widget.get_condition(); selected_ids = self.get_selected_item_ids(); current_time_window = self.time_window_widget.get_current_time_window_text()
        if not any(self.event_output_widget.get_selected_outputs().values()): return {}
        join_override_sql = None
        if current_time_window == "住院以前 (既往史)": join_override_sql = pgsql.SQL("FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.subject_id = {coh_alias}.subject_id JOIN mimiciv_hosp.admissions {adm_evt} ON {evt_alias}.hadm_id = {adm_evt}.hadm_id")
        return {"source_event_table": "mimiciv_hosp.procedures_icd", "item_id_column_in_event_table": "icd_code", "selected_item_ids": selected_ids, "value_column_to_extract": None, "time_column_in_event_table": "chartdate", "time_column_is_date_only": True, "aggregation_methods": {}, "event_outputs": self.event_output_widget.get_selected_outputs(), "time_window_text": current_time_window, "primary_item_label_for_naming": self._get_primary_item_label_for_naming(), "cte_join_on_cohort_override": join_override_sql, "item_filter_conditions": (condition_sql, condition_params)}
    def _get_primary_item_label_for_naming(self) -> Optional[str]:
        if self.item_list.selectedItems(): return self.item_list.selectedItems()[0].text().split(' (ICD:')[0].strip()
        return None
    def clear_panel_state(self):
        self.condition_widget.clear_all(); self.item_list.clear(); self.selected_items_label.setText("已选项目: 0"); self.filter_sql_preview_textedit.clear()
        self.event_output_widget.clear_selections(); self.time_window_widget.clear_selection()
    def _on_item_selection_changed(self): self.selected_items_label.setText(f"已选项目: {len(self.item_list.selectedItems())}"); self.config_changed_signal.emit()
    @Slot()
    def _filter_items_action(self):
        if not self._connect_panel_db(): QMessageBox.warning(self, "数据库连接失败", "无法连接到数据库以筛选项目。"); return
        dict_table, name_col, id_col, ver_col = "mimiciv_hosp.d_icd_procedures", "long_title", "icd_code", "icd_version"
        condition_sql_template, condition_params = self.condition_widget.get_condition()
        self.item_list.clear(); self.item_list.addItem("正在查询..."); self.filter_items_btn.setEnabled(False); QApplication.processEvents()
        if not condition_sql_template: self.item_list.clear(); self.item_list.addItem("请输入筛选条件。"); self.filter_items_btn.setEnabled(True); self._close_panel_db(); return
        try:
            query_template_obj = pgsql.SQL("SELECT {id}, {name}, {ver} FROM {table} WHERE {cond} ORDER BY {name} LIMIT 500").format(id=pgsql.Identifier(id_col), name=pgsql.Identifier(name_col), ver=pgsql.Identifier(ver_col), table=pgsql.SQL(dict_table), cond=pgsql.SQL(condition_sql_template))
            if self._db_conn and not self._db_conn.closed: self.filter_sql_preview_textedit.setText(self._db_cursor.mogrify(query_template_obj, condition_params).decode(self._db_conn.encoding or 'utf-8'))
            self._db_cursor.execute(query_template_obj, condition_params); items = self._db_cursor.fetchall(); self.item_list.clear()
            if items:
                for item_id_val, item_name_disp_val, item_ver_val in items:
                    display_name = str(item_name_disp_val) if item_name_disp_val is not None else f"ID_{item_id_val}"
                    list_item = QListWidgetItem(f"{display_name} (ICD: {item_id_val}, v{item_ver_val})")
                    list_item.setData(Qt.ItemDataRole.UserRole, (str(item_id_val), display_name))
                    self.item_list.addItem(list_item)
            else: self.item_list.addItem("未找到符合条件的项目")
        except Exception as e: self.item_list.clear(); self.item_list.addItem("查询项目出错!"); QMessageBox.critical(self, "筛选项目失败", f"查询项目时出错: {str(e)}\n{traceback.format_exc()}")
        finally: self.filter_items_btn.setEnabled(True); self._close_panel_db(); self.config_changed_signal.emit()
    def update_panel_action_buttons_state(self, general_config_ok: bool): self.filter_items_btn.setEnabled(general_config_ok and self.condition_widget.has_valid_input())
# --- END OF FULLY CORRECTED FILE ---