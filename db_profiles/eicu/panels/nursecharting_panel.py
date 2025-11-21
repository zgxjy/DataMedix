# --- START OF NEW FILE: db_profiles/eicu/panels/nursecharting_panel.py ---
from PySide6.QtWidgets import (QVBoxLayout, QHBoxLayout, QPushButton,
                               QListWidget, QListWidgetItem, QAbstractItemView, QTextEdit,
                               QApplication, QGroupBox, QLabel, QMessageBox, QScrollArea, QFrame,
                               QRadioButton, QButtonGroup, QComboBox)
from PySide6.QtCore import Qt, Slot
import psycopg2.sql as pgsql
import traceback
from typing import Optional

from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.conditiongroup import ConditionGroupWidget
from ui_components.value_aggregation_widget import ValueAggregationWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class EicuNurseChartingPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0, 0, 0, 0)
        panel_layout.setSpacing(10)

        # 1. 筛选项目
        filter_group = QGroupBox("1. 筛选护理项目 (来自 public.nursecharting)")
        filter_group_layout = QVBoxLayout(filter_group)
        
        self.condition_widget = ConditionGroupWidget(is_root=True)
        self.condition_widget.condition_changed.connect(self.config_changed_signal.emit)
        cg_scroll = QScrollArea()
        cg_scroll.setWidgetResizable(True)
        cg_scroll.setWidget(self.condition_widget)
        cg_scroll.setMinimumHeight(150)
        filter_group_layout.addWidget(cg_scroll)

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self.filter_items_btn = QPushButton("筛选项目")
        self.filter_items_btn.clicked.connect(self._filter_items_action)
        btn_layout.addWidget(self.filter_items_btn)
        filter_group_layout.addLayout(btn_layout)

        filter_group_layout.addWidget(QFrame(frameShape=QFrame.Shape.HLine, frameShadow=QFrame.Shadow.Sunken))

        self.item_list = QListWidget()
        self.item_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.item_list.itemSelectionChanged.connect(self._on_item_selection_changed)
        item_list_scroll = QScrollArea()
        item_list_scroll.setWidgetResizable(True)
        item_list_scroll.setWidget(self.item_list)
        item_list_scroll.setMinimumHeight(150)
        filter_group_layout.addWidget(item_list_scroll)
        
        self.selected_items_label = QLabel("已选项目: 0")
        filter_group_layout.addWidget(self.selected_items_label, alignment=Qt.AlignmentFlag.AlignRight)
        panel_layout.addWidget(filter_group)

        # 2. 提取逻辑
        logic_group = QGroupBox("2. 配置提取逻辑")
        logic_layout = QVBoxLayout(logic_group)

        # 2.1 选择值来源
        value_source_layout = QHBoxLayout()
        value_source_layout.addWidget(QLabel("提取值来源:"))
        self.value_source_group = QButtonGroup(self)
        self.rb_value_numeric = QRadioButton("数值 (nursingchartvalue)")
        self.rb_value_text = QRadioButton("文本 (nursingchartcelltypevalname)")
        self.rb_value_numeric.setChecked(True)
        self.value_source_group.addButton(self.rb_value_numeric, 1)
        self.value_source_group.addButton(self.rb_value_text, 2)
        value_source_layout.addWidget(self.rb_value_numeric)
        value_source_layout.addWidget(self.rb_value_text)
        value_source_layout.addStretch()
        self.value_source_group.buttonClicked.connect(self._on_value_source_changed)
        logic_layout.addLayout(value_source_layout)

        # 2.2 聚合方法
        self.value_agg_widget = ValueAggregationWidget()
        self.value_agg_widget.aggregation_changed.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.value_agg_widget)
        
        # 2.3 时间窗口
        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口 (相对于ICU入院):")
        self.time_window_widget.time_window_changed.connect(lambda: self.config_changed_signal.emit())
        logic_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)
        
        # 初始化UI状态
        self._on_value_source_changed()

    def populate_panel_if_needed(self):
        # eICU nursecharting 没有字典表，所以筛选字段就是表自身的列
        self.condition_widget.set_available_search_fields([
            ("nursingchartcelltypevallabel", "标签 (Label)"),
            ("nursingchartcelltypecat", "类别 (Category)"),
            ("nursingchartcelltypevalname", "值名称 (Value Name)")
        ])
        self.time_window_widget.set_options([
            "ICU入住24小时内",
            "ICU入住48小时内",
            "整个ICU期间",
        ])

    def get_friendly_source_name(self) -> str:
        return "e-ICU 护理记录 (nursecharting)"

    def clear_panel_state(self):
        self.condition_widget.clear_all()
        self.item_list.clear()
        self.selected_items_label.setText("已选项目: 0")
        self.rb_value_numeric.setChecked(True)
        self._on_value_source_changed()
        self.value_agg_widget.clear_selections()
        if self.time_window_widget.combo_box.count() > 0:
            self.time_window_widget.combo_box.setCurrentIndex(0)

# --- 替换 get_panel_config ---
    def get_panel_config(self) -> dict:
        selected_ids = self.get_selected_item_ids()
        is_text_mode = self.rb_value_text.isChecked()
        value_col = "nursingchartcelltypevalname" if is_text_mode else "nursingchartvalue"
        
        aggregation_methods = self.value_agg_widget.get_selected_methods()
        if not selected_ids or not any(aggregation_methods.values()):
            return {}

        return {
            "source_event_table": "public.nursecharting",
            "item_id_column_in_event_table": "nursingchartcelltypevallabel",
            "selected_item_ids": selected_ids,
            "value_column_to_extract": value_col,
            "time_column_in_event_table": "nursingchartoffset",
            "aggregation_methods": aggregation_methods,
            "is_text_extraction": is_text_mode,
            "event_outputs": {},
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": self._get_primary_item_label_for_naming(),
            "cte_join_on_cohort_override": None,
            
            # [新增] UI 状态
            "_ui_state": {
                "condition_widget": self.condition_widget.get_state(),
                "selected_items_display": [item.text() for item in self.item_list.selectedItems()],
                "is_text_mode": is_text_mode # 保存单选状态
            }
        }

    # --- 新增 set_panel_config ---
    def set_panel_config(self, config: dict):
        ui_state = config.get("_ui_state", {})
        
        # 1. 恢复筛选
        if "condition_widget" in ui_state:
            available_fields = [("nursingchartcelltypevallabel", "标签 (Label)"), ("nursingchartcelltypecat", "类别 (Category)"), ("nursingchartcelltypevalname", "值名称 (Value Name)")]
            self.condition_widget.set_state(ui_state["condition_widget"], available_fields)

        # 2. 恢复列表
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

        # 3. 恢复单选按钮状态
        is_text = ui_state.get("is_text_mode", False)
        if is_text: self.rb_value_text.setChecked(True)
        else: self.rb_value_numeric.setChecked(True)
        self._on_value_source_changed() # 触发联动

        # 4. 恢复聚合和时间
        self.value_agg_widget.set_selected_methods(config.get("aggregation_methods", {}))
        if "time_window_text" in config:
            self.time_window_widget.set_current_time_window_by_text(config["time_window_text"])

    def _get_primary_item_label_for_naming(self) -> Optional[str]:
        if self.item_list.selectedItems():
            return self.item_list.selectedItems()[0].text()
        return None

    @Slot()
    def _on_value_source_changed(self):
        is_text_mode = self.rb_value_text.isChecked()
        self.value_agg_widget.set_text_mode(is_text_mode)
        self.config_changed_signal.emit()

    def _on_item_selection_changed(self):
        count = len(self.item_list.selectedItems())
        self.selected_items_label.setText(f"已选项目: {count}")
        self.config_changed_signal.emit()

    @Slot()
    def _filter_items_action(self):
        if not self._connect_panel_db():
            QMessageBox.warning(self, "数据库连接失败", "无法连接数据库以筛选项目。")
            return

        self.item_list.clear()
        self.item_list.addItem("正在查询...")
        self.filter_items_btn.setEnabled(False)
        QApplication.processEvents()
        
        condition_sql_template, condition_params = self.condition_widget.get_condition()
        if not condition_sql_template:
            self.item_list.clear()
            self.item_list.addItem("请输入筛选条件。")
            self.filter_items_btn.setEnabled(True)
            self._close_panel_db()
            return

        try:
            # 查询 nursecharting 表中所有不重复的标签 (label)
            query = pgsql.SQL("SELECT DISTINCT nursingchartcelltypevallabel FROM public.nursecharting WHERE {cond} ORDER BY nursingchartcelltypevallabel LIMIT 500").format(
                cond=pgsql.SQL(condition_sql_template)
            )
            self._db_cursor.execute(query, condition_params)
            items = self._db_cursor.fetchall()
            
            self.item_list.clear()
            if items:
                for row in items:
                    item_name = row[0]
                    if item_name: # 确保不添加空的标签
                        list_item = QListWidgetItem(item_name)
                        # 用户角色数据存储 (ID, DisplayName)，这里ID和DisplayName相同
                        list_item.setData(Qt.ItemDataRole.UserRole, (item_name, item_name))
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
        # 按钮的可用状态取决于“常规配置是否OK”和“本面板的条件是否有效”
        has_valid_conditions = self.condition_widget.has_valid_input()
        self.filter_items_btn.setEnabled(general_config_ok and has_valid_conditions)

# --- END OF NEW FILE ---