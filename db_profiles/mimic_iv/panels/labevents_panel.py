# --- START OF FILE db_profiles/mimic_iv/panels/labevents_panel.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
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

class LabeventsConfigPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0,0,0,0)
        panel_layout.setSpacing(10)

        filter_group = QGroupBox("筛选化验项目 (来自 mimiciv_hosp.d_labitems)")
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

        # Labevents 通常只处理数值，所以不需要选择值类型的下拉框
        self.value_agg_widget = ValueAggregationWidget()
        self.value_agg_widget.aggregation_changed.connect(self.config_changed_signal.emit)
        logic_group_layout.addWidget(self.value_agg_widget)

        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口:")
        self.time_window_widget.time_window_changed.connect(lambda: self.config_changed_signal.emit())
        logic_group_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)
        
        self.value_agg_widget.set_text_mode(False) # 固定为非文本模式

    def populate_panel_if_needed(self):
        available_fields = [
            ("label", "项目名 (Label)"),
            ("category", "类别 (Category)"),
            ("fluid", "体液类型 (Fluid)"),
            ("itemid", "ItemID (精确)")
        ]
        self.condition_widget.set_available_search_fields(available_fields)
        
        value_agg_time_window_options = [
            "ICU入住后24小时", "ICU入住后48小时", "整个ICU期间", "整个住院期间"
        ]
        self.time_window_widget.set_options(value_agg_time_window_options)

    def get_friendly_source_name(self) -> str:
        return "化验 (Labevents - d_labitems)"
      
    def get_panel_config(self) -> dict:
        db_profile = self.get_db_profile()
        if not db_profile: return {}
        
        constants = db_profile.get_profile_constants()
        numeric_col = constants.get('DEFAULT_VALUE_COLUMN', 'valuenum')
        time_col = constants.get('DEFAULT_TIME_COLUMN', 'charttime')

        condition_sql, condition_params = self.condition_widget.get_condition()
        selected_ids = self.get_selected_item_ids()
        aggregation_methods_from_widget = self.value_agg_widget.get_selected_methods()
            
        if not any(aggregation_methods_from_widget.values()):
            return {}

        config = {
            "source_event_table": "mimiciv_hosp.labevents",
            "item_id_column_in_event_table": "itemid",
            "selected_item_ids": selected_ids,
            "value_column_to_extract": numeric_col, # 固定为数值列
            "time_column_in_event_table": time_col,
            "aggregation_methods": aggregation_methods_from_widget,
            "is_text_extraction": False, # 固定为非文本提取
            "event_outputs": {},
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": self._get_primary_item_label_for_naming(),
            "cte_join_on_cohort_override": None
        }
        return config

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
        self.value_agg_widget.set_text_mode(False)
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
        
        dict_table, name_col, id_col = "mimiciv_hosp.d_labitems", "label", "itemid"
        condition_sql_template, condition_params = self.condition_widget.get_condition()
        
        self.item_list.clear(); self.item_list.addItem("正在查询...")
        self.filter_items_btn.setEnabled(False); QApplication.processEvents()

        if not condition_sql_template:
            self.item_list.clear(); self.item_list.addItem("请输入筛选条件。")
            self.filter_items_btn.setEnabled(True); self._close_panel_db(); return
            
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
            self.item_list.clear(); self.item_list.addItem("查询项目出错!")
            QMessageBox.critical(self, "筛选项目失败", f"查询项目时出错: {str(e)}\n{traceback.format_exc()}")
        finally:
            self.filter_items_btn.setEnabled(True)
            self._close_panel_db()
            self.config_changed_signal.emit()
            
    def update_panel_action_buttons_state(self, general_config_ok: bool):
        has_valid_conditions_in_panel = self.condition_widget.has_valid_input()
        can_filter = general_config_ok and has_valid_conditions_in_panel
        self.filter_items_btn.setEnabled(can_filter)