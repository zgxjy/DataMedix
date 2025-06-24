# --- START OF FILE tabs/tab_query_cohort.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QDialog, QLineEdit, QFormLayout,
                          QApplication, QProgressBar, QGroupBox, QComboBox,
                          QRadioButton, QButtonGroup, QScrollArea, QAbstractButton)
from PySide6.QtCore import Qt, Signal, QObject, QThread, Slot
import psycopg2
from psycopg2 import sql as psql
import re
import time
import traceback
from typing import Optional, Dict, Any

from ui_components.conditiongroup import ConditionGroupWidget 
from db_profiles.base_profile import BaseDbProfile

COHORT_TYPE_FIRST_EVENT_KEY = "first_event_admission"
COHORT_TYPE_ALL_EVENTS_KEY = "all_event_admissions"
COHORT_TYPE_FIRST_EVENT_STR = "首次事件入院"
COHORT_TYPE_ALL_EVENTS_STR = "所有事件入院"

class CohortCreationWorker(QObject):
    # ... Worker代码本身没有问题，保持原样 ...
    finished = Signal(str, int)
    error = Signal(str)
    progress = Signal(int, int)
    log = Signal(str)

    def __init__(self, db_params, target_table_name_str,
                 condition_sql_template, condition_params,
                 admission_cohort_type, source_mode_details, cohort_schema):
        super().__init__()
        self.db_params = db_params
        self.target_table_name_str = target_table_name_str
        self.condition_sql_template = condition_sql_template
        self.condition_params = condition_params
        self.admission_cohort_type = admission_cohort_type
        self.source_mode_details = source_mode_details
        self.cohort_schema = cohort_schema
        self.is_cancelled = False

    def cancel(self):
        self.log.emit("队列创建操作被请求取消...")
        self.is_cancelled = True

    def run(self):
        conn = None
        total_steps = 5
        current_step = 0

        try:
            event_source_type_str = self.source_mode_details.get("display_name", "Unknown Source")
            self.log.emit(f"开始创建队列: {self.target_table_name_str} (类型: {self.admission_cohort_type}, 来源: {event_source_type_str})...")
            self.progress.emit(current_step, total_steps)
            self.log.emit("连接数据库...")
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            conn.autocommit = False
            self.log.emit("数据库已连接。")

            current_step += 1 
            self.log.emit(f"步骤 {current_step}/{total_steps}: 确保 '{self.cohort_schema}' schema 存在...")
            cur.execute(psql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(psql.Identifier(self.cohort_schema)))
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            target_table_ident = psql.Identifier(self.cohort_schema, self.target_table_name_str)
            
            base_event_select_sql, base_event_params = self._build_base_event_query()
            if base_event_select_sql is None:
                raise ValueError("无法构建基础事件查询SQL。")

            if self.admission_cohort_type == COHORT_TYPE_FIRST_EVENT_KEY:
                order_by_parts = self._get_ranking_order_by()
                final_event_select_sql = psql.SQL("""
                    SELECT * FROM (
                        SELECT base.*, ROW_NUMBER() OVER(PARTITION BY base.subject_id ORDER BY {order}) AS rn
                        FROM ({base}) AS base
                    ) ranked WHERE ranked.rn = 1
                """).format(order=psql.SQL(', ').join(order_by_parts), base=base_event_select_sql)
            else:
                final_event_select_sql = base_event_select_sql
            
            current_step += 1 
            self.log.emit(f"步骤 {current_step}/{total_steps}: 创建临时表 (符合事件条件的入院记录)...")
            temp_event_ad_table = psql.Identifier(f"temp_event_ad_{int(time.time())}")
            cur.execute(psql.SQL("CREATE TEMPORARY TABLE {temp_table} AS ({query})").format(
                temp_table=temp_event_ad_table, query=final_event_select_sql), base_event_params)
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")
            
            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 创建目标队列数据表 {self.target_table_name_str}...")
            cur.execute(psql.SQL("""
                DROP TABLE IF EXISTS {target_table};
                CREATE TABLE {target_table} AS
                SELECT 
                    evt.subject_id, evt.hadm_id, evt.admittime, adm.dischtime,
                    icu.stay_id, icu.intime AS icu_intime, icu.outtime AS icu_outtime, 
                    EXTRACT(EPOCH FROM (icu.outtime - icu.intime)) / 3600.0 AS los_icu_hours,
                    evt.qualifying_event_code, evt.qualifying_event_icd_version,
                    evt.qualifying_event_title, evt.qualifying_event_seq_num
                FROM {temp_event} evt
                JOIN mimiciv_hosp.admissions adm ON evt.hadm_id = adm.hadm_id
                LEFT JOIN (
                    SELECT i.*, ROW_NUMBER() OVER(PARTITION BY i.hadm_id ORDER BY i.intime) as rn 
                    FROM mimiciv_icu.icustays i
                ) icu ON evt.hadm_id = icu.hadm_id AND icu.rn = 1;
            """).format(target_table=target_table_ident, temp_event=temp_event_ad_table))
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 为表创建索引...")
            cur.execute(psql.SQL("CREATE INDEX ON {target_table} (subject_id);").format(target_table=target_table_ident))
            cur.execute(psql.SQL("CREATE INDEX ON {target_table} (hadm_id);").format(target_table=target_table_ident))
            if self.is_cancelled: raise InterruptedError("操作已取消")
            self.progress.emit(current_step, total_steps)
            
            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 提交更改并获取行数...")
            conn.commit()
            cur.execute(psql.SQL("SELECT COUNT(*) FROM {}").format(target_table_ident))
            count = cur.fetchone()[0]
            self.progress.emit(current_step, total_steps)
            self.finished.emit(self.target_table_name_str, count)

        except InterruptedError:
            if conn: conn.rollback()
            self.error.emit("操作已取消")
        except (Exception, psycopg2.Error) as error:
            if conn: conn.rollback()
            self.error.emit(f"创建队列时出错: {error}\n{traceback.format_exc()}")
        finally:
            if conn: conn.close()
    
    def _get_ranking_order_by(self):
        order_by_parts = [psql.SQL("base.admittime ASC"), psql.SQL("base.hadm_id ASC")]
        event_time_col = self.source_mode_details.get("event_time_col")
        if event_time_col:
            order_by_parts.append(psql.SQL("base.qualifying_event_time ASC NULLS LAST"))
        seq_num_col = self.source_mode_details.get("event_seq_num_col")
        if seq_num_col:
            order_by_parts.append(psql.SQL("base.qualifying_event_seq_num ASC"))
        return order_by_parts

    def _build_base_event_query(self):
        details = self.source_mode_details
        event_table_sql = psql.SQL(details['event_table'])
        
        select_list = [
            psql.SQL("e.subject_id"),
            psql.SQL("e.hadm_id"),
            psql.SQL("adm.admittime"),
            psql.SQL("e.{} AS qualifying_event_code").format(psql.Identifier(details['event_icd_col'])),
        ]
        
        dict_table_sql = psql.SQL(details['dictionary_table']) if details.get('dictionary_table') else None
        if dict_table_sql:
            select_list.append(psql.SQL("dd.{} AS qualifying_event_title").format(psql.Identifier(details['dict_title_col'])))
        else:
             select_list.append(psql.SQL("e.{} AS qualifying_event_title").format(psql.Identifier(details['event_icd_col'])))

        if details.get("event_seq_num_col"):
            select_list.append(psql.SQL("e.{} AS qualifying_event_seq_num").format(psql.Identifier(details['event_seq_num_col'])))
        else:
            select_list.append(psql.SQL("NULL AS qualifying_event_seq_num"))

        if details.get("event_time_col"):
             select_list.append(psql.SQL("e.{} AS qualifying_event_time").format(psql.Identifier(details['event_time_col'])))
        
        if "diagnoses_icd" in details['event_table'] or "procedures_icd" in details['event_table']:
            select_list.append(psql.SQL("e.icd_version AS qualifying_event_icd_version"))
        else:
            select_list.append(psql.SQL("NULL AS qualifying_event_icd_version"))
        
        from_clause = psql.SQL("FROM {event_table} e JOIN mimiciv_hosp.admissions adm ON e.hadm_id = adm.hadm_id").format(event_table=event_table_sql)
        
        if dict_table_sql:
            join_on_parts = [psql.SQL("e.{} = dd.{}").format(psql.Identifier(details['event_icd_col']), psql.Identifier(details['dict_icd_col']))]
            if "diagnoses_icd" in details['event_table'] or "procedures_icd" in details['event_table']:
                 join_on_parts.append(psql.SQL("e.icd_version = dd.icd_version"))
            from_clause += psql.SQL(" JOIN {dict_table} dd ON {join_on}").format(
                dict_table=dict_table_sql, join_on=psql.SQL(" AND ").join(join_on_parts))

        query = psql.SQL("SELECT {selects} {froms} WHERE ({condition})").format(
            selects=psql.SQL(', ').join(select_list),
            froms=from_clause,
            condition=psql.SQL(self.condition_sql_template)
        )
        return query, self.condition_params


class QueryCohortTab(QWidget):
    # ... (init is the same) ...
    def __init__(self, get_db_params_func, get_db_profile_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.get_db_profile = get_db_profile_func
        self.db_profile: Optional[BaseDbProfile] = None

        self.last_query_condition_template = None
        self.last_query_params = None
        self.cohort_worker_thread = None
        self.cohort_worker = None
        self.cohort_configs: Dict[str, Dict[str, Any]] = {}

        self.init_ui()

    def init_ui(self):
        # ... (init_ui is the same) ...
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Orientation.Vertical)
        main_layout.addWidget(splitter)

        controls_and_preview_widget = QWidget()
        controls_and_preview_layout = QVBoxLayout(controls_and_preview_widget)
        splitter.addWidget(controls_and_preview_widget)

        mode_selection_groupbox = QGroupBox("队列筛选模式")
        mode_layout = QHBoxLayout()
        self.mode_selection_group = QButtonGroup(self)
        self.mode_radio_button_container = QWidget() # Dynamic container
        self.mode_radio_button_layout = QHBoxLayout(self.mode_radio_button_container)
        mode_layout.addWidget(self.mode_radio_button_container)
        mode_selection_groupbox.setLayout(mode_layout)
        controls_and_preview_layout.addWidget(mode_selection_groupbox)

        self.mode_selection_group.buttonToggled.connect(self.on_mode_changed)

        instruction_label = QLabel("使用下方条件组构建筛选条件:")
        controls_and_preview_layout.addWidget(instruction_label)
        
        self.condition_group = ConditionGroupWidget(is_root=True) 
        self.condition_group.condition_changed.connect(self.update_button_states)
        
        cg_scroll_area = QScrollArea()
        cg_scroll_area.setWidgetResizable(True)
        cg_scroll_area.setWidget(self.condition_group)
        cg_scroll_area.setMinimumHeight(200) 
        controls_and_preview_layout.addWidget(cg_scroll_area)

        cohort_type_layout = QHBoxLayout()
        self.admission_type_label = QLabel("选择入院类型:")
        cohort_type_layout.addWidget(self.admission_type_label)
        self.admission_type_combo = QComboBox()
        cohort_type_layout.addWidget(self.admission_type_combo); cohort_type_layout.addStretch()
        controls_and_preview_layout.addLayout(cohort_type_layout)

        btn_layout = QHBoxLayout()
        self.query_btn = QPushButton("查询代码")
        self.query_btn.clicked.connect(self.execute_query); self.query_btn.setEnabled(False)
        btn_layout.addWidget(self.query_btn)
        self.preview_btn = QPushButton("预览查询SQL")
        self.preview_btn.clicked.connect(self.preview_sql_action); self.preview_btn.setEnabled(False)
        btn_layout.addWidget(self.preview_btn)
        self.create_table_btn = QPushButton("创建目标队列数据表")
        self.create_table_btn.clicked.connect(self.create_cohort_table_with_preview); self.create_table_btn.setEnabled(False)
        btn_layout.addWidget(self.create_table_btn)
        controls_and_preview_layout.addLayout(btn_layout)

        self.cohort_creation_status_group = QGroupBox("队列创建状态")
        cohort_status_layout = QVBoxLayout(self.cohort_creation_status_group)
        self.cohort_creation_progress = QProgressBar(); self.cohort_creation_progress.setRange(0, 5); self.cohort_creation_progress.setValue(0)
        cohort_status_layout.addWidget(self.cohort_creation_progress)
        self.cohort_creation_log = QTextEdit(); self.cohort_creation_log.setReadOnly(True); self.cohort_creation_log.setMaximumHeight(100)
        cohort_status_layout.addWidget(self.cohort_creation_log)
        self.cohort_creation_status_group.setVisible(False)
        controls_and_preview_layout.addWidget(self.cohort_creation_status_group)

        self.sql_preview = QTextEdit(); self.sql_preview.setReadOnly(True); self.sql_preview.setMaximumHeight(150)
        controls_and_preview_layout.addWidget(self.sql_preview)

        result_display_widget = QWidget()
        result_display_layout = QVBoxLayout(result_display_widget)
        self.table_content_label = QLabel("当前表格内容: 查询结果")
        result_display_layout.addWidget(self.table_content_label)
        self.result_table = QTableWidget(); self.result_table.setAlternatingRowColors(True)
        result_display_layout.addWidget(self.result_table)
        splitter.addWidget(result_display_widget)
        
        splitter.setSizes([controls_and_preview_widget.sizeHint().height() + 50, 250])

    def on_profile_changed(self):
        self.db_profile = self.get_db_profile()

        for i in reversed(range(self.mode_radio_button_layout.count())): 
            widget = self.mode_radio_button_layout.itemAt(i).widget()
            if widget:
                self.mode_selection_group.removeButton(widget)
                widget.setParent(None)
        self.cohort_configs.clear()
        
        if not self.db_profile:
            self.on_mode_changed(None, False)
            return

        self.cohort_configs = self.db_profile.get_cohort_creation_configs()
        for i, (key, config) in enumerate(self.cohort_configs.items()):
            rb = QRadioButton(config['display_name'])
            self.mode_selection_group.addButton(rb, i)
            rb.setProperty("mode_key", key)
            self.mode_radio_button_layout.addWidget(rb)
        
        if self.mode_selection_group.buttons():
            self.mode_selection_group.buttons()[0].setChecked(True)
        else:
            self.on_mode_changed(None, False)

    def on_db_connected(self):
        self.update_button_states()

    @Slot(QAbstractButton, bool)
    def on_mode_changed(self, button: Optional[QAbstractButton], checked: bool):
        # REPAIR: added `and button is not None` to prevent crash on clear
        if not checked and button is not None: 
            return

        self.result_table.setRowCount(0)
        self.sql_preview.clear()
        self.last_query_condition_template = None
        self.last_query_params = None
        self.table_content_label.setText("当前表格内容: 查询结果")
        self.admission_type_combo.clear()

        active_config = None
        if button:
            mode_key = button.property("mode_key")
            active_config = self.cohort_configs.get(mode_key)
        
        if active_config:
            self.condition_group.set_available_search_fields(active_config.get("search_fields", []))
            self.admission_type_combo.addItem(COHORT_TYPE_FIRST_EVENT_STR, COHORT_TYPE_FIRST_EVENT_KEY)
            self.admission_type_combo.addItem(COHORT_TYPE_ALL_EVENTS_STR, COHORT_TYPE_ALL_EVENTS_KEY)
        else:
            self.condition_group.set_available_search_fields([])
        
        self.condition_group.clear_all()
        self.update_button_states()

    def get_active_mode_config(self) -> Optional[Dict[str, Any]]:
        checked_button = self.mode_selection_group.checkedButton()
        if not checked_button: return None
        mode_key = checked_button.property("mode_key")
        return self.cohort_configs.get(mode_key)

    def update_button_states(self):
        # REPAIR: Explicitly convert results to boolean for setEnabled
        db_connected = bool(self.get_db_params())
        has_valid_conditions = self.condition_group.has_valid_input()
        
        # REPAIR: Check if thread object exists before calling isRunning()
        is_worker_running = self.cohort_worker_thread is not None and self.cohort_worker_thread.isRunning()
        
        can_create = db_connected and bool(self.last_query_condition_template) and not is_worker_running

        self.query_btn.setEnabled(db_connected and has_valid_conditions and not is_worker_running)
        self.preview_btn.setEnabled(db_connected and has_valid_conditions and not is_worker_running)
        self.create_table_btn.setEnabled(can_create)

    def execute_query(self):
        # ... (unchanged) ...
        db_params = self.get_db_params()
        active_config = self.get_active_mode_config()
        if not db_params or not active_config:
            QMessageBox.warning(self, "错误", "请连接数据库并选择一种筛选模式。")
            return
        
        query_obj, params = self._build_query_parts()
        if query_obj is None:
            QMessageBox.warning(self, "错误", "无法为当前模式构建查询。")
            return

        self.last_query_condition_template, self.last_query_params = self.condition_group.get_condition()
        self.preview_sql_action()
        self.table_content_label.setText(f"当前表格内容: {active_config['display_name']} 查询结果")

        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            cur.execute(query_obj, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            
            self.result_table.setRowCount(0)
            self.result_table.setColumnCount(len(columns))
            self.result_table.setHorizontalHeaderLabels(columns)
            for i, row in enumerate(rows):
                self.result_table.insertRow(i)
                for j, val in enumerate(row):
                    self.result_table.setItem(i, j, QTableWidgetItem(str(val) if val is not None else ""))
            self.result_table.resizeColumnsToContents()
            QMessageBox.information(self, "查询完成", f"共找到 {len(rows)} 条记录。")
        except Exception as error:
            QMessageBox.critical(self, "查询失败", f"无法执行查询: {error}\n{traceback.format_exc()}")
            self.last_query_condition_template = None
        finally:
            if conn: conn.close()
            self.update_button_states()

    def _build_query_parts(self):
        # ... (unchanged) ...
        active_config = self.get_active_mode_config()
        if not active_config: return None, None
        
        condition_template, params = self.condition_group.get_condition()
        
        if not active_config.get("dictionary_table"):
            base_query = psql.SQL("SELECT DISTINCT {code_col} FROM {dict_table}").format(
                code_col=psql.Identifier(active_config["event_icd_col"]),
                dict_table=psql.SQL(active_config["event_table"])
            )
        else:
             base_query = psql.SQL("SELECT {code_col}, {title_col} FROM {dict_table}").format(
                code_col=psql.Identifier(active_config["dict_icd_col"]),
                title_col=psql.Identifier(active_config["dict_title_col"]),
                dict_table=psql.SQL(active_config["dictionary_table"])
            )

        if condition_template:
            return psql.SQL("{base} WHERE {cond}").format(base=base_query, cond=psql.SQL(condition_template)), params
        return base_query, params

    def create_cohort_table_with_preview(self):
        if not self.last_query_condition_template:
            QMessageBox.warning(self, "缺少条件", "请先执行一次查询来确定筛选条件。")
            return
        
        raw_cohort_identifier, ok = self.get_cohort_identifier_name()
        if not ok or not raw_cohort_identifier: 
            return
        
        # 规范化用户输入的基础标识符
        cleaned_identifier = re.sub(r'[^a-z0-9_]+', '_', raw_cohort_identifier.lower()).strip('_')
        if not cleaned_identifier:
            QMessageBox.warning(self, "名称无效", "基础标识符清理后为空，请输入有效的名称。")
            return

        # 根据入院类型确定表名前缀
        admission_type_key = self.admission_type_combo.currentData()
        table_prefix = "first_" if admission_type_key == COHORT_TYPE_FIRST_EVENT_KEY else "all_"
        
        active_config = self.get_active_mode_config()
        if not active_config: 
            return
        
        # REPAIR: 使用更健壮的方式生成来源前缀
        # 我们直接使用在 cohort_configs 中定义的 key ('disease', 'procedure'等)
        # 这是一个稳定且不含中文的标识符。
        mode_key = ""
        checked_button = self.mode_selection_group.checkedButton()
        if checked_button:
            mode_key = checked_button.property("mode_key")
        
        # 如果key是'disease'，前缀就是'dis'；'procedure'就是'proc'。
        # 这样可以避免从中文显示名称中提取字符。
        source_prefix = (mode_key[:3] + '_') if mode_key else 'src_'

        # 拼接成最终的表名
        target_table_name = f"{table_prefix}{source_prefix}{cleaned_identifier}_cohort"
        
        if len(target_table_name) > 63:
            QMessageBox.warning(self, "名称过长", f"生成的表名 '{target_table_name}' 过长 (超过63个字符)。请缩短队列标识符。")
            return
            
        reply = QMessageBox.question(self, '确认创建队列', f"将创建队列数据表 '{target_table_name}'.\n确定要继续吗?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.No: 
            return
        
        db_params = self.get_db_params()
        if not db_params or not self.db_profile: 
            return

        self.prepare_for_cohort_creation(True)
        self.cohort_worker = CohortCreationWorker(
            db_params, target_table_name, self.last_query_condition_template, self.last_query_params,
            admission_type_key, active_config, self.db_profile.get_cohort_table_schema())
        
        self.cohort_worker_thread = QThread()
        self.cohort_worker.moveToThread(self.cohort_worker_thread)
        self.cohort_worker_thread.started.connect(self.cohort_worker.run)
        self.cohort_worker.finished.connect(self.on_cohort_creation_finished)
        self.cohort_worker.error.connect(self.on_cohort_creation_error)
        self.cohort_worker.progress.connect(self.update_cohort_creation_progress)
        self.cohort_worker.log.connect(self.update_cohort_creation_log)
        
        self.cohort_worker_thread.finished.connect(self.worker_cleanup)

        self.cohort_worker_thread.start()

    # REPAIR: New cleanup slot
    @Slot()
    def worker_cleanup(self):
        """Safely cleans up worker and thread objects."""
        self.cohort_worker.deleteLater()
        self.cohort_worker_thread.deleteLater()
        self.cohort_worker = None
        self.cohort_worker_thread = None

    def get_cohort_identifier_name(self):
        dialog = QDialog(self); dialog.setWindowTitle("输入队列基础标识符")
        layout = QVBoxLayout(dialog); form_layout = QFormLayout(); name_input = QLineEdit()
        form_layout.addRow("队列基础标识符 (英文,数字,下划线):", name_input); layout.addLayout(form_layout)
        info_label = QLabel("注意: 此标识符将用于构成数据库表名...\n只能包含英文字母、数字和下划线，且必须以字母或下划线开头。")
        info_label.setWordWrap(True); layout.addWidget(info_label)
        btn_layout = QHBoxLayout(); ok_btn = QPushButton("确定"); cancel_btn = QPushButton("取消")
        btn_layout.addWidget(ok_btn); btn_layout.addWidget(cancel_btn); layout.addLayout(btn_layout)
        ok_btn.clicked.connect(dialog.accept); cancel_btn.clicked.connect(dialog.reject)
        result = dialog.exec_()
        return name_input.text().strip(), result == QDialog.DialogCode.Accepted

    def prepare_for_cohort_creation(self, starting=True):
        self.cohort_creation_status_group.setVisible(starting)
        if starting:
            self.cohort_creation_progress.setValue(0)
            self.cohort_creation_log.clear()
            self.update_cohort_creation_log("开始创建队列...")
        
        is_enabled = not starting
        self.condition_group.setEnabled(is_enabled)
        self.admission_type_combo.setEnabled(is_enabled)
        self.mode_radio_button_container.setEnabled(is_enabled)
        
        self.update_button_states()

    def update_cohort_creation_progress(self, value, max_value):
        if self.cohort_creation_progress.maximum() != max_value:
            self.cohort_creation_progress.setMaximum(max_value)
        self.cohort_creation_progress.setValue(value)

    def update_cohort_creation_log(self, message):
        self.cohort_creation_log.append(message)
        QApplication.processEvents()

    @Slot(str, int)
    def on_cohort_creation_finished(self, table_name, count):
        self.update_cohort_creation_log(f"队列数据表 {table_name} 创建成功，包含 {count} 条记录。")
        QMessageBox.information(self, "创建成功", f"队列数据表 {table_name} 创建成功，包含 {count} 条记录。")
        self.prepare_for_cohort_creation(False)
        if self.db_profile:
            self.preview_created_cohort_table(self.db_profile.get_cohort_table_schema(), table_name)
    
    @Slot(str)
    def on_cohort_creation_error(self, error_message):
        self.update_cohort_creation_log(f"队列创建失败: {error_message}")
        if "操作已取消" not in error_message:
            QMessageBox.critical(self, "创建失败", f"无法创建队列数据表: {error_message}")
        else: 
            QMessageBox.information(self, "操作取消", "队列创建操作已取消。")
        self.prepare_for_cohort_creation(False)
        
    def preview_created_cohort_table(self, schema_name, table_name):
        db_params = self.get_db_params()
        if not db_params: return
        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            table_identifier = psql.Identifier(schema_name, table_name)
            preview_query = psql.SQL("SELECT * FROM {} ORDER BY subject_id, hadm_id LIMIT 100;").format(table_identifier)
            self.sql_preview.append(f"\n-- 队列表预览SQL:\n{preview_query.as_string(conn)}")
            cur.execute(preview_query)
            columns = [desc[0] for desc in cur.description]; rows = cur.fetchall()
            self.result_table.setRowCount(0); self.result_table.setColumnCount(len(columns))
            self.result_table.setHorizontalHeaderLabels(columns)
            for i, row in enumerate(rows):
                self.result_table.insertRow(i)
                for j, val in enumerate(row):
                    self.result_table.setItem(i, j, QTableWidgetItem(str(val) if val is not None else ""))
            self.result_table.resizeColumnsToContents()
            self.table_content_label.setText(f"当前表格内容: 队列表 '{schema_name}.{table_name}' 预览 (前100行)")
        except Exception as error:
            QMessageBox.critical(self, "队列表预览失败", f"无法预览队列表 '{table_name}': {error}")
        finally:
            if conn: conn.close()

    def preview_sql_action(self):
        query_obj, params = self._build_query_parts()
        if query_obj is None: return
        
        active_config = self.get_active_mode_config()
        query_type_str = active_config.get('display_name', '查询') if active_config else '查询'
        
        db_params = self.get_db_params()
        if not db_params:
            self.sql_preview.setText(f"SQL Template ({query_type_str}):\n{str(query_obj)}\n\nParameters:\n{params}\n\n(无法连接数据库以生成完整预览)")
            return

        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            preview_sql_filled = conn.cursor().mogrify(query_obj, params).decode(conn.encoding or 'utf-8')
            self.sql_preview.setText(f"-- SQL Preview ({query_type_str}):\n{preview_sql_filled}")
        except Exception as e:
            self.sql_preview.setText(f"SQL Template ({query_type_str}):\n{str(query_obj)}\n\nParameters:\n{params}\n\n(生成预览时出错: {e})")
        finally:
            if conn: conn.close()