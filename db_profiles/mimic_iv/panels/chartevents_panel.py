# --- START OF FILE db_profiles/mimic_iv/panels/chartevents_panel.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                               QListWidget, QListWidgetItem, QAbstractItemView,
                               QApplication, QGroupBox, QLabel, QMessageBox, QTextEdit,
                               QComboBox, QScrollArea,QFrame)
from PySide6.QtCore import Qt, Slot
import psycopg2.sql as pgsql
import traceback
from typing import Optional

from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.conditiongroup import ConditionGroupWidget
from ui_components.value_aggregation_widget import ValueAggregationWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class CharteventsConfigPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0,0,0,0)
        panel_layout.setSpacing(10)

        filter_group = QGroupBox("筛选监测指标 (来自 mimiciv_icu.d_items)")
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
        
        self.filter_items_btn = QPushButton("筛选指标项目")
        self.filter_items_btn.clicked.connect(self._filter_items_action)
        filter_action_layout.addWidget(self.filter_items_btn)
        filter_group_layout.addLayout(filter_action_layout)

        separator1 = QFrame()
        separator1.setFrameShape(QFrame.Shape.HLine); separator1.setFrameShadow(QFrame.Shadow.Sunken)
        filter_group_layout.addWidget(separator1)

        self.filter_sql_preview_label = QLabel("最近筛选SQL预览:")
        filter_group_layout.addWidget(self.filter_sql_preview_label)
        self.filter_sql_preview_textedit = QTextEdit()
        self.filter_sql_preview_textedit.setReadOnly(True)
        self.filter_sql_preview_textedit.setFixedHeight(60)
        self.filter_sql_preview_textedit.setPlaceholderText("执行“筛选指标项目”后将在此显示SQL...")
        filter_group_layout.addWidget(self.filter_sql_preview_textedit)

        separator2 = QFrame()
        separator2.setFrameShape(QFrame.Shape.HLine); separator2.setFrameShadow(QFrame.Shadow.Sunken)
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
        logic_group_layout.setSpacing(8)

        # REFACTOR: Get column names from the profile
        db_profile = self.get_db_profile()
        numeric_col, text_col = "valuenum", "value"
        if db_profile:
            constants = db_profile.get_profile_constants()
            numeric_col = constants.get('DEFAULT_VALUE_COLUMN', 'valuenum')
            text_col = constants.get('DEFAULT_TEXT_VALUE_COLUMN', 'value')

        value_type_layout = QHBoxLayout()
        value_type_layout.addWidget(QLabel("提取值列:"))
        self.value_type_combo = QComboBox()
        self.value_type_combo.addItem(f"数值 ({numeric_col})", numeric_col)
        self.value_type_combo.addItem(f"文本 ({text_col})", text_col)
        self.value_type_combo.currentIndexChanged.connect(self._on_value_type_combo_changed)
        value_type_layout.addWidget(self.value_type_combo)
        value_type_layout.addStretch()
        logic_group_layout.addLayout(value_type_layout)

        self.value_agg_widget = ValueAggregationWidget()
        self.value_agg_widget.aggregation_changed.connect(self.config_changed_signal.emit)
        logic_group_layout.addWidget(self.value_agg_widget)

        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口:")
        self.time_window_widget.time_window_changed.connect(lambda: self.config_changed_signal.emit())
        logic_group_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)
        self._on_value_type_combo_changed(self.value_type_combo.currentIndex())

    @Slot(int)
    def _on_value_type_combo_changed(self, index):
        db_profile = self.get_db_profile()
        text_val_col = "value"
        if db_profile:
            text_val_col = db_profile.get_profile_constants().get('DEFAULT_TEXT_VALUE_COLUMN', 'value')
        
        is_text_mode = (self.value_type_combo.currentData() == text_val_col)
        self.value_agg_widget.set_text_mode(is_text_mode)
        self.config_changed_signal.emit()

    def populate_panel_if_needed(self):
        available_fields = [
            ("label", "项目名 (Label)"), ("abbreviation", "缩写 (Abbreviation)"),
            ("category", "类别 (Category)"), ("param_type", "参数类型 (Param Type)"),
            ("unitname", "单位 (Unit Name)"), ("linksto", "关联表 (Links To)"),("itemid", "ItemID (精确)")
        ]
        self.condition_widget.set_available_search_fields(available_fields)
        
        value_agg_time_window_options = [
            "ICU24小时内", "ICU48小时内", "整个ICU期间", 
            "住院24小时内", "住院48小时内","整个住院期间"
        ]
        self.time_window_widget.set_options(value_agg_time_window_options)

    def get_friendly_source_name(self) -> str:
        return "监测指标 (Chartevents - d_items)"
        
    def get_panel_config(self) -> dict:
        db_profile = self.get_db_profile()
        if not db_profile: return {}
        
        constants = db_profile.get_profile_constants()
        time_col = constants.get('DEFAULT_TIME_COLUMN', 'charttime')
        text_val_col = constants.get('DEFAULT_TEXT_VALUE_COLUMN', 'value')

        condition_sql, condition_params = self.condition_widget.get_condition()
        selected_ids = self.get_selected_item_ids()
        aggregation_methods_from_widget = self.value_agg_widget.get_selected_methods()
        
        if not any(aggregation_methods_from_widget.values()):
            return {}
        
        condition_ui_state = self.condition_widget.get_state()
        config = {
            "source_event_table": "mimiciv_icu.chartevents",
            "item_id_column_in_event_table": "itemid",
            "selected_item_ids": selected_ids,
            "value_column_to_extract": self.value_type_combo.currentData(),
            "time_column_in_event_table": time_col,
            "aggregation_methods": aggregation_methods_from_widget,
            "is_text_extraction": self.value_type_combo.currentData() == text_val_col,
            "event_outputs": {},
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": self._get_primary_item_label_for_naming(),
            "cte_join_on_cohort_override": None,
            # [新增] 保存额外的 UI 状态，用于恢复
            "_ui_state": {
                "condition_widget": condition_ui_state,
                "value_type_index": self.value_type_combo.currentIndex(),
                # 还需要保存选中项的显示名称，因为加载时 ItemList 是空的
                "selected_items_display": [item.text() for item in self.item_list.selectedItems()]
            }

        }
        return config

    def set_panel_config(self, config: dict):
            """恢复 Chartevents 面板的配置"""
            
            # 1. 恢复筛选条件 UI
            ui_state = config.get("_ui_state", {})
            if "condition_widget" in ui_state:
                # 注意：需要传递当前可用的字段列表，否则下拉框可能为空
                available_fields = [
                    ("label", "项目名 (Label)"), ("abbreviation", "缩写 (Abbreviation)"),
                    ("category", "类别 (Category)"), ("param_type", "参数类型 (Param Type)"),
                    ("unitname", "单位 (Unit Name)"), ("linksto", "关联表 (Links To)"),("itemid", "ItemID (精确)")
                ]
                self.condition_widget.set_state(ui_state["condition_widget"], available_fields)

            # 2. 恢复选中项目列表
            # 这是一个技巧：我们不需要重新去数据库筛选，直接把保存的 ID 和名称加回列表并选中即可
            selected_ids = config.get("selected_item_ids", [])
            selected_display_names = ui_state.get("selected_items_display", [])
            
            self.item_list.clear()
            if selected_ids:
                for i, item_id in enumerate(selected_ids):
                    # 尝试匹配显示名称，如果没有则用 ID 代替
                    display_text = selected_display_names[i] if i < len(selected_display_names) else str(item_id)
                    
                    list_item = QListWidgetItem(display_text)
                    list_item.setData(Qt.ItemDataRole.UserRole, (str(item_id), display_text))
                    self.item_list.addItem(list_item)
                    list_item.setSelected(True) # 设置为选中状态
            
            self._on_item_selection_changed() # 触发标签更新

            # 3. 恢复提取值类型 (数值/文本)
            if "value_type_index" in ui_state:
                self.value_type_combo.setCurrentIndex(ui_state["value_type_index"])
            elif config.get("is_text_extraction"):
                 # 兼容性处理
                 self.value_type_combo.setCurrentIndex(1) # 假设 1 是文本
            
            # 4. 恢复时间窗口
            time_window = config.get("time_window_text")
            if time_window:
                self.time_window_widget.set_current_time_window_by_text(time_window)

            # 5. 恢复聚合方法
            aggs = config.get("aggregation_methods", {})
            self.value_agg_widget.set_selected_methods(aggs)

    def _get_primary_item_label_for_naming(self) -> Optional[str]:
        if self.item_list.selectedItems():
            first_selected_item_text = self.item_list.selectedItems()[0].text()
            return first_selected_item_text.split(' (ID:')[0].strip()
        return None

    def clear_panel_state(self):
        self.condition_widget.clear_all()
        self.item_list.clear()
        self.selected_items_label.setText("已选项目: 0")
        self.filter_sql_preview_textedit.clear()
        self.value_type_combo.setCurrentIndex(0)
        self._on_value_type_combo_changed(0)
        self.value_agg_widget.clear_selections()
        if self.time_window_widget.combo_box.count() > 0:
            self.time_window_widget.combo_box.setCurrentIndex(0)

    def _on_item_selection_changed(self):
        count = len(self.item_list.selectedItems())
        self.selected_items_label.setText(f"已选项目: {count}")
        self.config_changed_signal.emit()

    @Slot()
    def _filter_items_action(self):
        if not self._connect_panel_db():
            QMessageBox.warning(self, "数据库连接失败", "无法连接到数据库以筛选项目。")
            return
        
        dict_table, name_col, id_col = "mimiciv_icu.d_items", "label", "itemid"
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
            query_template_obj = pgsql.SQL("SELECT {id}, {name} FROM {table} WHERE {cond} ORDER BY {name} LIMIT 500").format(
                id=pgsql.Identifier(id_col),
                name=pgsql.Identifier(name_col),
                table=pgsql.SQL(dict_table),
                cond=pgsql.SQL(condition_sql_template)
            )
            
            if self._db_conn and not self._db_conn.closed:
                mogrified_sql = self._db_cursor.mogrify(query_template_obj, condition_params).decode(self._db_conn.encoding or 'utf-8')
                self.filter_sql_preview_textedit.setText(mogrified_sql)

            self._db_cursor.execute(query_template_obj, condition_params)
            items = self._db_cursor.fetchall()
            self.item_list.clear()
            if items:
                for item_id_val, item_name_disp_val in items:
                    display_name = str(item_name_disp_val) if item_name_disp_val is not None else f"ID_{item_id_val}"
                    list_item = QListWidgetItem(f"{display_name} (ID: {item_id_val})")
                    list_item.setData(Qt.ItemDataRole.UserRole, (str(item_id_val), display_name)) 
                    self.item_list.addItem(list_item)
            else:
                self.item_list.addItem("未找到符合条件的项目")
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