# --- START OF FILE tabs/tab_special_data_master.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QTextEdit, QComboBox, QGroupBox,
                          QRadioButton, QButtonGroup, QStackedWidget,
                          QLineEdit, QProgressBar, QAbstractItemView, QApplication,
                          QScrollArea,QSizePolicy)
from PySide6.QtCore import Qt, Signal, Slot, QObject, QThread, QTimer
from typing import Optional, Dict, Any

import psycopg2
import psycopg2.sql as pgsql
import pandas as pd
import time
import traceback
import numpy as np

from ui_components.base_panel import BaseSourceConfigPanel
from sql_logic.sql_builder_special import build_special_data_sql
from utils import sanitize_name_part, validate_column_name
from app_config import SQL_BUILDER_DUMMY_DB_FOR_AS_STRING
from db_profiles.base_profile import BaseDbProfile

class MergeSQLWorker(QObject):
    finished = Signal()
    error = Signal(str)
    progress = Signal(int, int)
    log = Signal(str)

    def __init__(self, db_params, execution_steps, target_table_name, new_cols_description_str):
        super().__init__()
        self.db_params = db_params
        self.execution_steps = execution_steps
        self.target_table_name = target_table_name
        self.new_cols_description_str = new_cols_description_str
        self.is_cancelled = False
        self.current_sql_for_debug = ""

    def cancel(self): 
        self.log.emit("合并操作被请求取消...")
        self.is_cancelled = True

    def run(self):
        conn_merge = None
        total_actual_steps = len(self.execution_steps)
        current_step_num = 0
        self.log.emit(f"开始为表 '{self.target_table_name}' 添加/更新列 (基于: {self.new_cols_description_str})，共 {total_actual_steps} 个数据库步骤...")
        self.progress.emit(current_step_num, total_actual_steps)
        try:
            self.log.emit("连接数据库...")
            conn_merge = psycopg2.connect(**self.db_params)
            conn_merge.autocommit = False
            cur = conn_merge.cursor()
            self.log.emit("数据库已连接。")
            for i, (sql_obj_or_str, params_for_step) in enumerate(self.execution_steps):
                current_step_num += 1
                step_description_short = ""
                
                try: 
                    self.current_sql_for_debug = cur.mogrify(sql_obj_or_str, params_for_step if params_for_step else None).decode(conn_merge.encoding or 'utf-8', 'replace')
                except Exception as e_mogrify:
                    self.log.emit(f"DEBUG: Error mogrifying SQL: {e_mogrify}")
                    self.current_sql_for_debug = f"-- Mogrify failed --\nTemplate: {sql_obj_or_str}\nParams: {params_for_step}"

                step_description_peek = self.current_sql_for_debug[:200].upper()
                if "ALTER TABLE" in step_description_peek: step_description_short = " (ALTER)"
                elif "CREATE TEMPORARY TABLE" in step_description_peek: step_description_short = " (CREATE TEMP)"
                elif "UPDATE" in step_description_peek: step_description_short = " (UPDATE)"
                elif "DROP TABLE" in step_description_peek: step_description_short = " (DROP TEMP)"
                
                step_title = f"--- [执行SQL {current_step_num}/{total_actual_steps}]{step_description_short} ---"
                self.log.emit(step_title)
                self.log.emit(self.current_sql_for_debug)

                if self.is_cancelled: raise InterruptedError("操作在执行步骤前被取消。")
                
                start_time = time.time()
                cur.execute(sql_obj_or_str, params_for_step if params_for_step else None)
                end_time = time.time()
                
                self.log.emit(f"步骤 {current_step_num} 执行成功 (耗时: {end_time - start_time:.2f} 秒)。")
                self.progress.emit(current_step_num, total_actual_steps)

            if self.is_cancelled: raise InterruptedError("操作在提交前被取消，正在回滚...")
            self.log.emit("所有数据库步骤完成，正在提交事务...")
            start_commit_time = time.time()
            conn_merge.commit()
            end_commit_time = time.time()
            self.log.emit(f"事务提交成功 (耗时: {end_commit_time - start_commit_time:.2f} 秒)。")
            self.finished.emit()
        except InterruptedError as ie:
            if conn_merge and not conn_merge.closed: conn_merge.rollback()
            self.log.emit(f"操作已取消: {str(ie)}")
            self.error.emit("操作已取消")
        except psycopg2.Error as db_err:
            if conn_merge and not conn_merge.closed: conn_merge.rollback()
            err_msg = f"数据库错误: {db_err}\n相关SQL (完整): {self.current_sql_for_debug}"
            self.log.emit(err_msg)
            self.log.emit(f"Traceback: {traceback.format_exc()}")
            self.error.emit(err_msg)
        except Exception as e:
            if conn_merge and not conn_merge.closed: conn_merge.rollback()
            err_msg = f"发生意外错误: {e}\n相关SQL (完整): {self.current_sql_for_debug}"
            self.log.emit(err_msg)
            self.log.emit(f"Traceback: {traceback.format_exc()}")
            self.error.emit(err_msg)
        finally:
            if conn_merge and not conn_merge.closed: 
                self.log.emit("关闭数据库连接。")
                conn_merge.close()

# ... SpecialDataMasterTab 类的剩余部分保持不变 ...
class SpecialDataMasterTab(QWidget):
    request_preview_signal = Signal(str, str)

    def __init__(self, get_db_params_func, get_db_profile_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.get_db_profile = get_db_profile_func
        self.db_profile: Optional[BaseDbProfile] = None

        self.selected_cohort_table = None
        self.worker_thread = None
        self.merge_worker = None
        self.config_panels: Dict[int, BaseSourceConfigPanel] = {}
        self.user_manually_edited_col_name = False
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        main_layout.addWidget(scroll_area)
        
        content_widget = QWidget()
        scroll_area.setWidget(content_widget)
        content_layout = QVBoxLayout(content_widget)
        content_layout.setContentsMargins(10,10,10,10)
        content_layout.setSpacing(10)
        
        cohort_group = QGroupBox("1. 选择目标队列数据表")
        cohort_layout = QHBoxLayout(cohort_group)
        cohort_layout.addWidget(QLabel("队列表:"))
        self.table_combo = QComboBox(); self.table_combo.setMinimumWidth(250)
        self.table_combo.currentIndexChanged.connect(self.on_cohort_table_selected)
        cohort_layout.addWidget(self.table_combo)
        self.refresh_btn = QPushButton("刷新列表"); self.refresh_btn.clicked.connect(self.refresh_cohort_tables); self.refresh_btn.setEnabled(False)
        cohort_layout.addWidget(self.refresh_btn); cohort_layout.addStretch()
        content_layout.addWidget(cohort_group)
        
        source_and_panel_group = QGroupBox("2. 选择数据来源并配置提取项")
        source_main_layout = QVBoxLayout(source_and_panel_group)
        
        source_select_layout = QHBoxLayout()
        source_select_layout.addWidget(QLabel("数据来源:"))
        self.source_selection_group = QButtonGroup(self)
        self.source_radio_buttons_container = QWidget()
        self.source_radio_buttons_layout = QHBoxLayout(self.source_radio_buttons_container)
        self.source_radio_buttons_layout.setContentsMargins(0,0,0,0)
        source_select_layout.addWidget(self.source_radio_buttons_container)
        source_select_layout.addStretch()
        source_main_layout.addLayout(source_select_layout)
        
        self.config_panel_stack = QStackedWidget()
        self.config_panel_stack.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        source_main_layout.addWidget(self.config_panel_stack)
        content_layout.addWidget(source_and_panel_group)
        
        column_name_group = QGroupBox("3. 定义新列基础名")
        column_name_layout = QHBoxLayout(column_name_group)
        column_name_layout.addWidget(QLabel("新列基础名 (可修改):"))
        self.new_column_name_input = QLineEdit()
        self.new_column_name_input.setPlaceholderText("根据选择自动生成或手动输入...")
        self.new_column_name_input.textEdited.connect(self._on_new_column_name_manually_edited)
        self.new_column_name_input.editingFinished.connect(self.update_master_action_buttons_state)
        column_name_layout.addWidget(self.new_column_name_input, 1)
        content_layout.addWidget(column_name_group)
        
        self.execution_status_group = QGroupBox("合并执行状态")
        execution_status_layout = QVBoxLayout(self.execution_status_group)
        self.execution_progress = QProgressBar(); self.execution_progress.setRange(0,4); self.execution_progress.setValue(0)
        execution_status_layout.addWidget(self.execution_progress)
        self.execution_log = QTextEdit(); self.execution_log.setReadOnly(True); self.execution_log.setMaximumHeight(100)
        execution_status_layout.addWidget(self.execution_log)
        self.execution_status_group.setVisible(False)
        content_layout.addWidget(self.execution_status_group)
        
        action_layout = QHBoxLayout()
        self.preview_merge_btn = QPushButton("预览待合并数据"); self.preview_merge_btn.clicked.connect(self.preview_merge_data); self.preview_merge_btn.setEnabled(False)
        action_layout.addWidget(self.preview_merge_btn)
        self.execute_merge_btn = QPushButton("执行合并到表"); self.execute_merge_btn.clicked.connect(self.execute_merge); self.execute_merge_btn.setEnabled(False)
        action_layout.addWidget(self.execute_merge_btn)
        self.cancel_merge_btn = QPushButton("取消合并"); self.cancel_merge_btn.clicked.connect(self.cancel_merge); self.cancel_merge_btn.setEnabled(False)
        action_layout.addWidget(self.cancel_merge_btn)
        content_layout.addLayout(action_layout)
        
        content_layout.addWidget(QLabel("SQL预览 (仅供参考):"))
        self.sql_preview = QTextEdit(); self.sql_preview.setReadOnly(True)
        self.sql_preview.setMinimumHeight(100); self.sql_preview.setMaximumHeight(200)
        content_layout.addWidget(self.sql_preview)
        
        content_layout.addWidget(QLabel("数据预览 (最多100条):"))
        self.preview_table = QTableWidget(); self.preview_table.setAlternatingRowColors(True); self.preview_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.preview_table.setMinimumHeight(200)
        content_layout.addWidget(self.preview_table)
        
        self.source_selection_group.buttonToggled.connect(self._on_source_type_changed)
        self.setLayout(main_layout)

    def on_profile_changed(self):
        self.db_profile = self.get_db_profile()
        self.refresh_cohort_tables()

        for i in reversed(range(self.source_radio_buttons_layout.count())): 
            widget = self.source_radio_buttons_layout.itemAt(i).widget()
            if widget:
                self.source_selection_group.removeButton(widget)
                widget.setParent(None)
        
        while self.config_panel_stack.count() > 0:
            self.config_panel_stack.widget(0).setParent(None)

        self.config_panels.clear()
        
        if not self.db_profile:
            self._update_active_panel(force_col_name_update=True)
            return

        panels_from_profile = self.db_profile.get_source_panels()
        for idx, (display_name, PanelClass) in enumerate(panels_from_profile):
            rb = QRadioButton(display_name)
            self.source_selection_group.addButton(rb, idx)
            self.source_radio_buttons_layout.addWidget(rb)
            
            panel = PanelClass(self.get_db_params, self.get_db_profile, self)
            panel.config_changed_signal.connect(self._on_panel_config_changed)
            self.config_panel_stack.addWidget(panel)
            self.config_panels[idx] = panel
        
        if self.source_selection_group.buttons():
            self.source_selection_group.buttons()[0].setChecked(True)
        else:
            self._update_active_panel(force_col_name_update=True)

    def _update_active_panel(self, force_col_name_update=False):
        current_id = self.source_selection_group.checkedId()
        if current_id == -1: # No button selected
            self.config_panel_stack.setCurrentIndex(-1)
            self.update_master_action_buttons_state()
            return
            
        active_panel = self.config_panels.get(current_id)
        if active_panel:
            self.config_panel_stack.setCurrentWidget(active_panel)
            if hasattr(active_panel, 'populate_panel_if_needed'):
                active_panel.populate_panel_if_needed()
            QTimer.singleShot(0, lambda: self._generate_and_set_default_col_name(force_update=force_col_name_update))
        self.update_master_action_buttons_state()

    @Slot(int, bool)
    def _on_source_type_changed(self, id, checked):
        if checked:
            self._update_active_panel(force_col_name_update=True)

    @Slot()
    def _on_panel_config_changed(self):
        self.user_manually_edited_col_name = False
        QTimer.singleShot(0, lambda: self._generate_and_set_default_col_name(force_update=False))
        QTimer.singleShot(0, self.update_master_action_buttons_state)

    @Slot()
    def _on_new_column_name_manually_edited(self):
        self.user_manually_edited_col_name = True

    @Slot()
    def _on_new_column_name_editing_finished(self):
        self.update_master_action_buttons_state()

    def _generate_and_set_default_col_name(self, force_update=False):
        parts = []
        active_panel: Optional[BaseSourceConfigPanel] = self.config_panels.get(self.source_selection_group.checkedId())
        panel_config = active_panel.get_panel_config() if active_panel else {}
        
        logic_code = "data"
        agg_methods = panel_config.get("aggregation_methods")
        evt_outputs = panel_config.get("event_outputs")
        if agg_methods and any(agg_methods.values()):
            logic_code = next((k for k, v in agg_methods.items() if v), "data")
        elif evt_outputs and any(evt_outputs.values()):
            logic_code = next((k for k, v in evt_outputs.items() if v), "data")
        parts.append(sanitize_name_part(logic_code))

        item_name_part = panel_config.get("primary_item_label_for_naming")
        parts.append(sanitize_name_part(item_name_part or "item"))
        
        time_code = ""
        time_window_text = panel_config.get("time_window_text", "")
        if time_window_text:
            time_map = {
                "ICU入住后24小时": "icu24h", "ICU入住后48小时": "icu48h",
                "整个ICU期间": "icuall", "整个住院期间": "hospall",
                "整个住院期间 (当前入院)": "hosp", "整个ICU期间 (当前入院)": "icu",
                "住院以前 (既往史)": "prior"
            }
            time_code = time_map.get(time_window_text, sanitize_name_part(time_window_text.split(" ")[0]))
        if time_code: parts.append(time_code)
        
        default_name = "_".join(filter(None, parts))[:50]
        if default_name and default_name[0].isdigit():
            default_name = "_" + default_name
        
        if force_update or not self.user_manually_edited_col_name:
            if self.new_column_name_input.text() != default_name:
                self.new_column_name_input.blockSignals(True)
                self.new_column_name_input.setText(default_name or "new_col")
                self.new_column_name_input.blockSignals(False)
            if force_update:
                self.user_manually_edited_col_name = False

    def _are_configs_valid_for_action(self) -> bool:
        if not self.selected_cohort_table: return False
        is_valid_col_name, _ = validate_column_name(self.new_column_name_input.text().strip())
        if not is_valid_col_name: return False
        
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if not active_panel: return False
        
        panel_config = active_panel.get_panel_config()
        return bool(panel_config)

    def _build_merge_query(self, preview_limit=100, for_execution=False):
        if not self.selected_cohort_table:
            return None, "未选择目标队列数据表.", [], []
        base_new_col_name = self.new_column_name_input.text().strip()
        is_valid_base_name, name_error = validate_column_name(base_new_col_name)
        if not is_valid_base_name:
            return None, f"基础列名 '{base_new_col_name}' 无效: {name_error}", [], []
        
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if not active_panel:
            return None, "未选择有效的数据来源面板。", [], []
            
        panel_config_dict = active_panel.get_panel_config()
        if not panel_config_dict:
            return None, f"来自 {active_panel.get_friendly_source_name()} 的配置不完整或无效。", [], []
        
        try:
            return build_special_data_sql(
                target_cohort_table_name=f"{self.db_profile.get_cohort_table_schema()}.{self.selected_cohort_table}",
                base_new_column_name=base_new_col_name,
                panel_specific_config=panel_config_dict,
                for_execution=for_execution,
                preview_limit=preview_limit
            )
        except Exception as e:
            return None, f"构建SQL时发生内部错误: {e}\n{traceback.format_exc()}", [], []

    def prepare_for_long_operation(self, starting=True):
        is_enabled = not starting
        if starting:
            self.execution_status_group.setVisible(True)
            self.execution_progress.setValue(0)
            self.execution_log.clear()
            self.update_execution_log("开始执行合并操作...")
        
        self.table_combo.setEnabled(is_enabled)
        self.refresh_btn.setEnabled(is_enabled and bool(self.get_db_params()))
        self.source_radio_buttons_container.setEnabled(is_enabled)
        
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if active_panel: active_panel.setEnabled(is_enabled)
        
        self.new_column_name_input.setEnabled(is_enabled)
        self.cancel_merge_btn.setEnabled(starting)
        
        if not starting:
            self.update_master_action_buttons_state()
        else:
            self.preview_merge_btn.setEnabled(False)
            self.execute_merge_btn.setEnabled(False)

    def update_execution_progress(self, value, max_value=None):
        if max_value is not None and self.execution_progress.maximum() != max_value:
            self.execution_progress.setMaximum(max_value)
        self.execution_progress.setValue(value)

    def update_execution_log(self, message):
        self.execution_log.append(message)
        QApplication.processEvents()

    @Slot()
    def on_db_connected(self):
        self.refresh_btn.setEnabled(True)
        self.refresh_cohort_tables()

    def refresh_cohort_tables(self):
        self.table_combo.blockSignals(True)
        current_sel_text = self.table_combo.currentText()
        self.table_combo.clear()
        
        db_params = self.get_db_params()
        if not self.db_profile:
            self.table_combo.addItem("请先选择数据库类型")
        elif not db_params:
            self.table_combo.addItem("数据库未连接")
        else:
            conn = None
            try:
                cohort_schema = self.db_profile.get_cohort_table_schema()
                conn = psycopg2.connect(**db_params)
                cur = conn.cursor()
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name", (cohort_schema,))
                tables = [r[0] for r in cur.fetchall()]
                if tables:
                    self.table_combo.addItems(tables)
                    idx = self.table_combo.findText(current_sel_text)
                    self.table_combo.setCurrentIndex(idx if idx != -1 else 0)
                else:
                    self.table_combo.addItem(f"在 '{cohort_schema}' 中未找到队列表")
            except Exception as e:
                self.table_combo.addItem("获取列表失败")
            finally:
                if conn: conn.close()

        self.table_combo.blockSignals(False)
        self.on_cohort_table_selected(self.table_combo.currentIndex())

    def on_cohort_table_selected(self, index):
        current_text = self.table_combo.itemText(index)
        valid_texts = ["未找到", "失败", "未连接", "请先"]
        if index >= 0 and not any(vt in current_text for vt in valid_texts):
            self.selected_cohort_table = current_text
        else:
            self.selected_cohort_table = None
        self.update_master_action_buttons_state()

    @Slot()
    def update_master_action_buttons_state(self):
        is_valid_for_action = self._are_configs_valid_for_action()
        self.preview_merge_btn.setEnabled(is_valid_for_action)
        self.execute_merge_btn.setEnabled(is_valid_for_action)
        
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if active_panel:
            db_connected = bool(self.get_db_params())
            cohort_table_selected = bool(self.selected_cohort_table)
            active_panel.update_panel_action_buttons_state(db_connected and cohort_table_selected)

    def execute_merge(self):
        if not self._are_configs_valid_for_action():
            QMessageBox.warning(self, "配置不完整", "请确保所有必要的选项已选择或填写，并且基础列名有效。")
            return
            
        build_result = self._build_merge_query(for_execution=True)
        if build_result is None or len(build_result) < 4:
            QMessageBox.critical(self, "内部错误", "构建合并查询时未能返回预期结果结构。")
            return
            
        execution_steps, signal_type, new_cols_desc, col_details = build_result
        if signal_type != "execution_list":
            QMessageBox.critical(self, "合并准备失败", f"无法构建SQL: {signal_type if isinstance(signal_type, str) else '未知构建错误'}")
            return
            
        col_preview_msg = f"确定要向表 '{self.selected_cohort_table}' 中添加/更新以下列吗？\n" + \
                           "\n".join([f" - {name} (类型: {type_str})" for name, type_str in col_details]) + \
                           "\n\n此操作将直接修改数据库表。"
        if QMessageBox.question(self, '确认操作', col_preview_msg, QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No) == QMessageBox.StandardButton.No:
            return
            
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.critical(self, "合并失败", "无法获取数据库连接参数。")
            return
            
        self.prepare_for_long_operation(True)
        self.merge_worker = MergeSQLWorker(db_params, execution_steps, self.selected_cohort_table, new_cols_desc)
        self.worker_thread = QThread()
        self.merge_worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.merge_worker.run)
        self.merge_worker.finished.connect(self.on_merge_worker_finished_actions)
        self.merge_worker.error.connect(self.on_merge_error_actions)
        self.merge_worker.progress.connect(self.update_execution_progress)
        self.merge_worker.log.connect(self.update_execution_log)
        self.merge_worker.finished.connect(self.worker_thread.quit)
        self.merge_worker.error.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self.trigger_preview_after_thread_finish)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        self.worker_thread.finished.connect(lambda: setattr(self, 'merge_worker', None))
        self.worker_thread.start()

    def preview_merge_data(self):
        if not self._are_configs_valid_for_action():
            QMessageBox.warning(self, "配置不完整", "请确保所有必要的选项已选择或填写以进行预览。")
            return
            
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库。")
            return
            
        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            preview_sql_obj, error_msg, params_for_cte, _ = self._build_merge_query(preview_limit=100, for_execution=False)

            if error_msg:
                QMessageBox.warning(self, "无法预览", error_msg)
                self.sql_preview.setText(f"-- BUILD ERROR: {error_msg}")
                return
            if not preview_sql_obj:
                QMessageBox.warning(self, "无法预览", "未能生成预览SQL。")
                return

            with conn.cursor() as cur:
                final_sql_for_preview = cur.mogrify(preview_sql_obj, params_for_cte).decode(conn.encoding or 'utf-8')
            
            self.sql_preview.setText(f"-- Preview Query (parameters embedded):\n{final_sql_for_preview}")
            QApplication.processEvents()

            df = pd.read_sql_query(final_sql_for_preview, conn)
            
            self.preview_table.clearContents()
            self.preview_table.setRowCount(df.shape[0])
            self.preview_table.setColumnCount(df.shape[1])
            self.preview_table.setHorizontalHeaderLabels(df.columns)
            
            for i in range(df.shape[0]):
                for j in range(df.shape[1]):
                    value = df.iloc[i, j]
                    
                    if isinstance(value, (list, tuple, np.ndarray)):
                        display_text = str(value)
                    else:
                        display_text = str(value) if pd.notna(value) else ""
                        
                    self.preview_table.setItem(i, j, QTableWidgetItem(display_text))

            self.preview_table.resizeColumnsToContents()
            QMessageBox.information(self, "预览成功", f"已生成预览数据 ({df.shape[0]} 条)。")
        except Exception as e:
            QMessageBox.critical(self, "预览失败", f"执行预览查询失败: {e}\n{traceback.format_exc()}")
            self.sql_preview.append(f"\n-- ERROR DURING PREVIEW: {str(e)}")
        finally:
            if conn: conn.close()
            
    def _get_readable_sql_with_conn(self, sql_obj, params, conn):
        if conn and not conn.closed:
            try:
                return conn.cursor().mogrify(sql_obj, params).decode(conn.encoding or 'utf-8')
            except Exception:
                pass
        
        dummy_conn = None
        try:
            dummy_conn = psycopg2.connect(SQL_BUILDER_DUMMY_DB_FOR_AS_STRING)
            return dummy_conn.cursor().mogrify(sql_obj.as_string(dummy_conn), params).decode()
        except Exception:
            return f"{str(sql_obj)}\n-- Params: {params}"
        finally:
            if dummy_conn: dummy_conn.close()

    def cancel_merge(self):
        if self.merge_worker:
            self.update_execution_log("正在请求取消合并操作...")
            self.merge_worker.cancel()
            self.cancel_merge_btn.setEnabled(False)

    @Slot()
    def on_merge_worker_finished_actions(self):
        desc = self.merge_worker.new_cols_description_str if self.merge_worker else ""
        self.update_execution_log(f"成功向表 {self.selected_cohort_table} 添加/更新与 '{desc}' 相关的列。")
        QMessageBox.information(self, "合并成功", f"已成功向表 {self.selected_cohort_table} 添加/更新列。")
        self.prepare_for_long_operation(False)

    @Slot()
    def trigger_preview_after_thread_finish(self):
        if self.selected_cohort_table and self.db_profile:
            self.request_preview_signal.emit(self.db_profile.get_cohort_table_schema(), self.selected_cohort_table)

    @Slot(str)
    def on_merge_error_actions(self, error_message):
        self.update_execution_log(f"合并失败: {error_message}")
        if "操作已取消" not in error_message:
            QMessageBox.critical(self, "合并失败", f"执行合并SQL失败: {error_message}")
        else:
            QMessageBox.information(self, "操作取消", "数据合并操作已取消。")
        self.prepare_for_long_operation(False)