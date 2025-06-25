# --- START OF FILE db_profiles/eicu/panels/lab_panel.py ---
from PySide6.QtWidgets import (QVBoxLayout, QGroupBox, QLineEdit, QHBoxLayout, 
                               QLabel, QListWidget, QAbstractItemView, QPushButton,
                               QApplication, QMessageBox, QScrollArea, QFrame)
from PySide6.QtCore import Qt, Slot
import psycopg2.sql as pgsql
import traceback
from typing import Optional

from ui_components.base_panel import BaseSourceConfigPanel
from ui_components.conditiongroup import ConditionGroupWidget
from ui_components.value_aggregation_widget import ValueAggregationWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

class EicuLabPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0,0,0,0)
        panel_layout.setSpacing(10)

        filter_group = QGroupBox("筛选化验项目 (来自 eicu_crd.lab)")
        filter_layout = QVBoxLayout(filter_group)
        self.condition_widget = ConditionGroupWidget(is_root=True)
        self.condition_widget.condition_changed.connect(self.config_changed_signal.emit)
        cg_scroll = QScrollArea()
        cg_scroll.setWidgetResizable(True)
        cg_scroll.setWidget(self.condition_widget)
        filter_layout.addWidget(cg_scroll)

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self.filter_items_btn = QPushButton("筛选项目")
        self.filter_items_btn.clicked.connect(self._filter_items_action)
        btn_layout.addWidget(self.filter_items_btn)
        filter_layout.addLayout(btn_layout)

        self.item_list = QListWidget()
        self.item_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.item_list.itemSelectionChanged.connect(self._on_item_selection_changed)
        filter_layout.addWidget(self.item_list)
        self.selected_items_label = QLabel("已选项目: 0")
        filter_layout.addWidget(self.selected_items_label, alignment=Qt.AlignRight)
        panel_layout.addWidget(filter_group)

        logic_group = QGroupBox("提取逻辑")
        logic_layout = QVBoxLayout(logic_group)
        self.value_agg_widget = ValueAggregationWidget()
        self.value_agg_widget.aggregation_changed.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.value_agg_widget)
        
        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口 (相对于ICU入院):")
        self.time_window_widget.time_window_changed.connect(self.config_changed_signal.emit)
        logic_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)
        
        self.value_agg_widget.set_text_mode(False)

    def populate_panel_if_needed(self):
        self.condition_widget.set_available_search_fields([
            ("labname", "化验名称 (Lab Name)"),
            ("labmeasurenameinterface", "测量单位 (Measure Name)")
        ])
        self.time_window_widget.set_options([
            "ICU入院后24小时 (0-1440分钟)",
            "ICU入院后48小时 (0-2880分钟)",
            "整个ICU期间",
        ])

    def get_friendly_source_name(self) -> str:
        return "e-ICU 化验 (lab)"

    def get_panel_config(self) -> dict:
        db_profile = self.get_db_profile()
        if not db_profile: return {}
        
        selected_ids = self.get_selected_item_ids()
        if not selected_ids: return {}
            
        constants = db_profile.get_profile_constants()
        
        return {
            "source_event_table": "eicu_crd.lab",
            "item_id_column_in_event_table": "labname",
            "selected_item_ids": selected_ids,
            "value_column_to_extract": constants.get('DEFAULT_VALUE_COLUMN', 'labresult'),
            "time_column_in_event_table": constants.get('DEFAULT_TIME_COLUMN', 'labresultoffset'),
            "aggregation_methods": self.value_agg_widget.get_selected_methods(),
            "event_outputs": {},
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": self._get_primary_item_label_for_naming(),
            "cte_join_on_cohort_override": None,
        }

    def _get_primary_item_label_for_naming(self) -> Optional[str]:
        if self.item_list.selectedItems():
            return self.item_list.selectedItems()[0].text()
        return None

    def clear_panel_state(self):
        self.condition_widget.clear_all()
        self.item_list.clear()
        self.selected_items_label.setText("已选项目: 0")
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
            query = pgsql.SQL("SELECT DISTINCT labname FROM eicu_crd.lab WHERE {cond} ORDER BY labname LIMIT 500").format(
                cond=pgsql.SQL(condition_sql_template)
            )
            self._db_cursor.execute(query, condition_params)
            items = self._db_cursor.fetchall()
            self.item_list.clear()
            if items:
                for row in items:
                    item_name = row[0]
                    list_item = QListWidgetItem(item_name)
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