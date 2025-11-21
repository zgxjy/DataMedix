# --- START OF FILE db_profiles/eicu/panels/infusiondrug_panel.py ---
from PySide6.QtWidgets import (QVBoxLayout, QHBoxLayout, QPushButton,
                               QListWidget, QListWidgetItem, QAbstractItemView, QTextEdit,
                               QApplication, QGroupBox, QLabel, QMessageBox, QScrollArea, QFrame)
from PySide6.QtCore import Qt, Slot
import psycopg2.sql as pgsql
import traceback
from typing import Optional

from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.conditiongroup import ConditionGroupWidget
from ui_components.value_aggregation_widget import ValueAggregationWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class EicuInfusionDrugPanel(BaseSourceConfigPanel):
    """
    用于配置从 e-ICU 的 `infusiondrug` 表提取持续输注药物信息的Panel。
    这是一个数值聚合类Panel。
    """
    def init_panel_ui(self):
        # --- UI 布局和组件与您其他数值聚合类Panel保持一致 ---
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0, 0, 0, 0)
        panel_layout.setSpacing(10)

        filter_group = QGroupBox("筛选输液药物 (来自 public.infusiondrug)")
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
        self.filter_items_btn = QPushButton("筛选药物项目")
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
        self.filter_sql_preview_textedit.setPlaceholderText("执行“筛选药物项目”后将在此显示SQL...")
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
        self.value_agg_widget = ValueAggregationWidget()
        self.value_agg_widget.aggregation_changed.connect(self.config_changed_signal.emit)
        logic_group_layout.addWidget(self.value_agg_widget)

        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口 (相对于ICU入院):")
        self.time_window_widget.time_window_changed.connect(lambda: self.config_changed_signal.emit())
        logic_group_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)
        
        # infusiondrug 主要是数值聚合，固定为非文本模式
        self.value_agg_widget.set_text_mode(False)

    def populate_panel_if_needed(self):
        # infusiondrug 表没有字典表，直接使用其列进行筛选
        available_fields = [
            ("drugname", "药物名称 (包含)")
        ]
        self.condition_widget.set_available_search_fields(available_fields)
        
        # 设置合适的时间窗口选项
        self.time_window_widget.set_options([
            "ICU入住24小时内",
            "ICU入住48小时内",
            "整个ICU期间",
        ])
        
    def get_friendly_source_name(self) -> str:
        return "e-ICU 输液药物 (infusiondrug)"
    
# --- 替换 get_panel_config ---
    def get_panel_config(self) -> dict:
        selected_ids = self.get_selected_item_ids()
        aggregation_methods = self.value_agg_widget.get_selected_methods()

        if not selected_ids or not any(aggregation_methods.values()):
            return {}

        return {
            "source_event_table": "public.infusiondrug",
            "item_id_column_in_event_table": "drugname",
            "selected_item_ids": selected_ids,
            "value_column_to_extract": "infusionrate",
            "time_column_in_event_table": "infusionoffset",
            "aggregation_methods": aggregation_methods,
            "is_text_extraction": False,
            "event_outputs": {},
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": self._get_primary_item_label_for_naming(),
            "cte_join_on_cohort_override": None,
            
            # [新增] UI 状态
            "_ui_state": {
                "condition_widget": self.condition_widget.get_state(),
                "selected_items_display": [item.text() for item in self.item_list.selectedItems()]
            }
        }

    # --- 新增 set_panel_config ---
    def set_panel_config(self, config: dict):
        ui_state = config.get("_ui_state", {})
        
        if "condition_widget" in ui_state:
            available_fields = [("drugname", "药物名称 (包含)")]
            self.condition_widget.set_state(ui_state["condition_widget"], available_fields)

        selected_ids = config.get("selected_item_ids", [])
        selected_display = ui_state.get("selected_items_display", [])
        self.item_list.clear()
        for i, item_id in enumerate(selected_ids):
            display_text = selected_display[i] if i < len(selected_display) else str(item_id)
            list_item = QListWidgetItem(display_text)
            list_item.setData(Qt.ItemDataRole.UserRole, (str(item_id), display_text))
            self.item_list.addItem(list_item)
            list_item.setSelected(True)
        self._on_item_selection_changed()

        self.value_agg_widget.set_selected_methods(config.get("aggregation_methods", {}))
        
        if "time_window_text" in config:
            self.time_window_widget.set_current_time_window_by_text(config["time_window_text"])

    def _get_primary_item_label_for_naming(self) -> Optional[str]:
        if self.item_list.selectedItems():
            return self.item_list.selectedItems()[0].text()
        return None

    def clear_panel_state(self):
        self.condition_widget.clear_all()
        self.item_list.clear()
        self.selected_items_label.setText("已选项目: 0")
        self.filter_sql_preview_textedit.clear()
        self.value_agg_widget.clear_selections()
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

        event_table, name_col = "public.infusiondrug", "drugname"
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
            query_template_obj = pgsql.SQL("SELECT DISTINCT {name} FROM {table} WHERE {cond} AND {name} IS NOT NULL ORDER BY {name} LIMIT 500").format(
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
                    drug_name = str(item_tuple[0])
                    list_item = QListWidgetItem(drug_name)
                    list_item.setData(Qt.ItemDataRole.UserRole, (drug_name, drug_name))
                    self.item_list.addItem(list_item)
            else: 
                self.item_list.addItem("未找到符合条件的药物")
        except Exception as e:
            self.item_list.clear()
            self.item_list.addItem("查询项目出错!")
            QMessageBox.critical(self, "筛选项目失败", f"查询项目时出错: {str(e)}\n{traceback.format_exc()}")
        finally:
            self.filter_items_btn.setEnabled(True)
            self._close_panel_db()
            self.config_changed_signal.emit()
            
    def update_panel_action_buttons_state(self, general_config_ok: bool):
        has_valid_conditions_in_panel = self.condition_widget.has_valid_input()
        can_filter = general_config_ok and has_valid_conditions_in_panel
        self.filter_items_btn.setEnabled(can_filter)

# --- END OF FILE db_profiles/eicu/panels/infusiondrug_panel.py ---