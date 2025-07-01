# --- START OF FINAL CORRECTED FILE: tabs/tab_query_cohort.py ---
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
from typing import Optional, Dict, Any, Tuple

from ui_components.conditiongroup import ConditionGroupWidget 
from db_profiles.base_profile import BaseDbProfile

# --- Constants (no change) ---
COHORT_TYPE_FIRST_EVENT_KEY = "first_event_admission"
COHORT_TYPE_ALL_EVENTS_KEY = "all_event_admissions"
COHORT_TYPE_FIRST_EVENT_STR = "首次事件入院"
COHORT_TYPE_ALL_EVENTS_STR = "所有事件入院"

# --- CohortCreationWorker (no change) ---
class CohortCreationWorker(QObject):
    # ... (The entire CohortCreationWorker class remains unchanged) ...
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
                partition_field = "base.patientunitstayid" if 'eicu' in self.cohort_schema else "base.subject_id"
                final_event_select_sql = psql.SQL("""
                    SELECT * FROM (
                        SELECT base.*, ROW_NUMBER() OVER(PARTITION BY {partition_field} ORDER BY {order}) AS rn
                        FROM ({base}) AS base
                    ) ranked WHERE ranked.rn = 1
                """).format(
                    partition_field=psql.SQL(partition_field),
                    order=psql.SQL(', ').join(order_by_parts), 
                    base=base_event_select_sql
                )
            else:
                final_event_select_sql = base_event_select_sql
            
            current_step += 1 
            self.log.emit(f"步骤 {current_step}/{total_steps}: 创建临时表 (符合事件条件的入院记录)...")
            temp_event_ad_table = psql.Identifier(f"temp_event_ad_{int(time.time())}")
            
            temp_table_creation_sql = psql.SQL("CREATE TEMPORARY TABLE {temp_table} AS ({query})").format(
                temp_table=temp_event_ad_table, query=final_event_select_sql)
                
            self.log.emit("--- [将执行SQL]: 创建临时事件表 ---")
            self.log.emit(cur.mogrify(temp_table_creation_sql, base_event_params).decode(conn.encoding or 'utf-8', 'replace'))

            cur.execute(temp_table_creation_sql, base_event_params)
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")
            
            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 创建目标队列数据表 {self.target_table_name_str}...")
            
            if 'eicu' in self.cohort_schema:
                final_table_creation_sql = psql.SQL("""
                    DROP TABLE IF EXISTS {target_table};
                    CREATE TABLE {target_table} AS
                    SELECT 
                        evt.patientunitstayid, pat.uniquepid, evt.admittime,
                        pat.unitdischargeoffset AS los_icu_minutes, pat.unitadmittime24 AS icu_intime,
                        evt.qualifying_event_title, evt.qualifying_event_time AS diagnosis_offset_min,
                        pat.age, pat.gender, pat.hospitaldischargestatus
                    FROM {temp_event} evt
                    JOIN public.patient pat ON evt.patientunitstayid = pat.patientunitstayid;
                """).format(target_table=target_table_ident, temp_event=temp_event_ad_table)
            else:
                final_table_creation_sql = psql.SQL("""
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
                """).format(target_table=target_table_ident, temp_event=temp_event_ad_table)
            
            self.log.emit("--- [将执行SQL]: 创建最终队列数据表 ---")
            self.log.emit(cur.mogrify(final_table_creation_sql).decode(conn.encoding or 'utf-8', 'replace'))

            cur.execute(final_table_creation_sql)
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 为表创建索引...")
            if 'eicu' in self.cohort_schema:
                cur.execute(psql.SQL("CREATE INDEX ON {target_table} (patientunitstayid);").format(target_table=target_table_ident))
                cur.execute(psql.SQL("CREATE INDEX ON {target_table} (uniquepid);").format(target_table=target_table_ident))
            else:
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
        if 'eicu' in self.cohort_schema:
            order_by_parts = []
            if self.source_mode_details.get("event_time_col"): order_by_parts.append(psql.SQL("COALESCE(base.qualifying_event_time, 0) ASC"))
            else: order_by_parts.append(psql.SQL("base.admittime ASC"))
        else:
            order_by_parts = [psql.SQL("base.admittime ASC"), psql.SQL("base.hadm_id ASC")]
            if self.source_mode_details.get("event_time_col"): order_by_parts.append(psql.SQL("base.qualifying_event_time ASC NULLS LAST"))
            if self.source_mode_details.get("event_seq_num_col"): order_by_parts.append(psql.SQL("base.qualifying_event_seq_num ASC"))
        return order_by_parts

    def _build_base_event_query(self):
        details = self.source_mode_details
        event_table = psql.SQL(details['event_table'])
        
        select_list = []
        if 'eicu' in self.cohort_schema:
            select_list = [
                psql.SQL("e.patientunitstayid AS patientunitstayid"), 
                psql.SQL("pat.unitadmittime24 AS admittime"),
                psql.SQL("e.{} AS qualifying_event_title").format(psql.Identifier(details['event_icd_col'])),
                psql.SQL("e.{} AS qualifying_event_seq_num").format(psql.Identifier(details.get('event_seq_num_col', 'diagnosispriority'))),
                psql.SQL("e.{} AS qualifying_event_time").format(psql.Identifier(details.get('event_time_col', 'diagnosisoffset'))),
                psql.SQL("NULL AS qualifying_event_icd_version")
            ]
            from_clause = psql.SQL("FROM {event_table} e JOIN public.patient pat ON e.patientunitstayid = pat.patientunitstayid").format(event_table=event_table)
        else:
            select_list = [
                psql.SQL("e.subject_id"), psql.SQL("e.hadm_id"), psql.SQL("adm.admittime"),
                psql.SQL("e.{} AS qualifying_event_code").format(psql.Identifier(details['event_icd_col'])),
            ]
            from_clause = psql.SQL("FROM {event_table} e JOIN mimiciv_hosp.admissions adm ON e.hadm_id = adm.hadm_id").format(event_table=event_table)
            dict_table = psql.SQL(details['dictionary_table']) if details.get('dictionary_table') else None
            if dict_table:
                join_on_parts = [psql.SQL("e.{event_icd_col} = dd.{dict_icd_col}").format(
                    event_icd_col=psql.Identifier(details['event_icd_col']),
                    dict_icd_col=psql.Identifier(details['dict_icd_col'])
                )]
                if "diagnoses_icd" in details['event_table'] or "procedures_icd" in details['event_table']:
                     join_on_parts.append(psql.SQL("e.icd_version = dd.icd_version"))
                from_clause += psql.SQL(" JOIN {dict_table} dd ON {join_on}").format(dict_table=dict_table, join_on=psql.SQL(" AND ").join(join_on_parts))
                select_list.append(psql.SQL("dd.{} AS qualifying_event_title").format(psql.Identifier(details['dict_title_col'])))
            else:
                 select_list.append(psql.SQL("e.{} AS qualifying_event_title").format(psql.Identifier(details['event_icd_col'])))
            select_list.append(psql.SQL("e.{} AS qualifying_event_seq_num").format(psql.Identifier(details['event_seq_num_col'])) if details.get("event_seq_num_col") else psql.SQL("NULL AS qualifying_event_seq_num"))
            if details.get("event_time_col"): select_list.append(psql.SQL("e.{} AS qualifying_event_time").format(psql.Identifier(details['event_time_col'])))
            select_list.append(psql.SQL("e.icd_version AS qualifying_event_icd_version") if "diagnoses_icd" in details['event_table'] or "procedures_icd" in details['event_table'] else psql.SQL("NULL AS qualifying_event_icd_version"))

        where_clause = self.condition_sql_template
        query = psql.SQL("SELECT {selects} {froms} WHERE {where}").format(
            selects=psql.SQL(', ').join(select_list), froms=from_clause, where=where_clause
        )
        return query, self.condition_params

# --- QueryCohortTab (Corrected Final Version) ---
class QueryCohortTab(QWidget):
    def __init__(self, get_db_params_func, get_db_profile_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.get_db_profile = get_db_profile_func
        self.db_profile: Optional[BaseDbProfile] = None
        self.cohort_configs: Dict[str, Dict[str, Any]] = {}
        
        self.last_filter_conditions: Optional[Tuple[str, list]] = None
        self.cohort_worker_thread: Optional[QThread] = None
        self.cohort_worker: Optional[CohortCreationWorker] = None

        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Orientation.Vertical)
        main_layout.addWidget(splitter)

        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        splitter.addWidget(top_widget)

        # Step 1: Mode
        mode_group = QGroupBox("1. 选择筛选模式")
        mode_layout = QHBoxLayout(mode_group)
        self.mode_selection_group = QButtonGroup(self)
        self.mode_radio_button_container = QWidget()
        self.mode_radio_button_layout = QHBoxLayout(self.mode_radio_button_container)
        self.mode_radio_button_layout.setContentsMargins(0,0,0,0)
        mode_layout.addWidget(self.mode_radio_button_container)
        self.mode_selection_group.buttonToggled.connect(self.on_mode_changed)
        top_layout.addWidget(mode_group)

        # Step 2: Conditions & Filter Action
        condition_group = QGroupBox("2. 构建筛选条件并预览项目")
        condition_layout = QVBoxLayout(condition_group)
        self.condition_group = ConditionGroupWidget(is_root=True)
        self.condition_group.condition_changed.connect(self.update_button_states)
        cg_scroll = QScrollArea()
        cg_scroll.setWidgetResizable(True)
        cg_scroll.setWidget(self.condition_group)
        cg_scroll.setMinimumHeight(150)
        condition_layout.addWidget(cg_scroll)
        
        filter_btn_layout = QHBoxLayout()
        filter_btn_layout.addStretch()
        self.filter_btn = QPushButton("筛选并预览项目")
        self.filter_btn.clicked.connect(self.filter_items_action)
        filter_btn_layout.addWidget(self.filter_btn)
        condition_layout.addLayout(filter_btn_layout)
        top_layout.addWidget(condition_group)

        # Step 3: Create Cohort
        create_group = QGroupBox("3. 设置队列选项并创建")
        create_layout = QVBoxLayout(create_group)
        
        cohort_type_layout = QHBoxLayout()
        cohort_type_layout.addWidget(QLabel("入院类型:"))
        self.admission_type_combo = QComboBox()
        cohort_type_layout.addWidget(self.admission_type_combo)
        cohort_type_layout.addStretch()
        create_layout.addLayout(cohort_type_layout)
        
        create_btn_layout = QHBoxLayout()
        create_btn_layout.addStretch()
        self.create_cohort_btn = QPushButton("创建队列")
        self.create_cohort_btn.setStyleSheet("font-weight: bold; color: green;")
        self.create_cohort_btn.clicked.connect(self.create_cohort_action)
        create_btn_layout.addWidget(self.create_cohort_btn)
        self.cancel_btn = QPushButton("取消操作")
        self.cancel_btn.clicked.connect(self.cancel_action)
        create_btn_layout.addWidget(self.cancel_btn)
        create_layout.addLayout(create_btn_layout)
        top_layout.addWidget(create_group)

        # Status Group
        self.status_group = QGroupBox("执行状态")
        status_layout = QVBoxLayout(self.status_group)
        self.progress_bar = QProgressBar()
        status_layout.addWidget(self.progress_bar)
        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        self.log_display.setMaximumHeight(100)
        status_layout.addWidget(self.log_display)
        top_layout.addWidget(self.status_group)

        # Result Area
        bottom_widget = QWidget()
        bottom_layout = QVBoxLayout(bottom_widget)
        splitter.addWidget(bottom_widget)
        self.result_label = QLabel("筛选/队列预览:")
        bottom_layout.addWidget(self.result_label)
        self.result_table = QTableWidget()
        self.result_table.setAlternatingRowColors(True)
        bottom_layout.addWidget(self.result_table)

        splitter.setSizes([600, 250])
        self.update_button_states()

    def on_profile_changed(self):
        self.db_profile = self.get_db_profile()
        while self.mode_radio_button_layout.count() > 0:
            item = self.mode_radio_button_layout.takeAt(0)
            widget = item.widget()
            if widget:
                self.mode_selection_group.removeButton(widget)
                widget.deleteLater()
        
        self.cohort_configs.clear()
        
        if self.db_profile:
            self.cohort_configs = self.db_profile.get_cohort_creation_configs()
            for i, (key, config) in enumerate(self.cohort_configs.items()):
                rb = QRadioButton(config['display_name'])
                self.mode_selection_group.addButton(rb, i)
                rb.setProperty("mode_key", key)
                self.mode_radio_button_layout.addWidget(rb)
        
            if self.mode_selection_group.buttons():
                self.mode_selection_group.buttons()[0].setChecked(True)
        self.on_mode_changed()

    def on_db_connected(self):
        self.update_button_states()

    @Slot()
    def on_mode_changed(self):
        self.clear_all_states()
        
    def clear_all_states(self):
        self.condition_group.clear_all()
        self.result_table.setRowCount(0)
        self.result_table.setColumnCount(0)
        self.result_label.setText("筛选/队列预览:")
        self.log_display.clear()
        self.last_filter_conditions = None

        active_config = self.get_active_mode_config()
        if active_config:
            self.condition_group.set_available_search_fields(active_config.get("search_fields", []))
            self.admission_type_combo.clear()
            self.admission_type_combo.addItem(COHORT_TYPE_FIRST_EVENT_STR, COHORT_TYPE_FIRST_EVENT_KEY)
            self.admission_type_combo.addItem(COHORT_TYPE_ALL_EVENTS_STR, COHORT_TYPE_ALL_EVENTS_KEY)
        else:
            self.condition_group.set_available_search_fields([])
            self.admission_type_combo.clear()

        self.update_button_states()
        
    def get_active_mode_config(self):
        btn = self.mode_selection_group.checkedButton()
        return self.cohort_configs.get(btn.property("mode_key")) if btn else None

    @Slot()
    def update_button_states(self):
        is_busy = bool(self.cohort_worker_thread and self.cohort_worker_thread.isRunning())
        db_connected = bool(self.get_db_params())
        has_valid_conditions = self.condition_group.has_valid_input()
        
        self.filter_btn.setEnabled(db_connected and has_valid_conditions and not is_busy)
        self.create_cohort_btn.setEnabled(self.last_filter_conditions is not None and not is_busy)
        self.cancel_btn.setEnabled(is_busy)
        
        for w in [self.mode_radio_button_container, self.condition_group, self.admission_type_combo]:
            w.setEnabled(not is_busy)
        self.status_group.setVisible(is_busy)

    def filter_items_action(self):
        config = self.get_active_mode_config()
        db_params = self.get_db_params()
        if not config or not db_params:
            QMessageBox.warning(self, "错误", "请确保已连接数据库并选择筛选模式。")
            return
            
        condition_sql, params = self.condition_group.get_condition()
        
        dict_table_name = config.get("dictionary_table")
        if not dict_table_name:
            query = psql.SQL("SELECT DISTINCT {code_col} FROM {event_table} WHERE {cond} LIMIT 500").format(
                code_col=psql.Identifier(config["event_icd_col"]), event_table=psql.SQL(config["event_table"]), cond=psql.SQL(condition_sql)
            )
        else:
            query = psql.SQL("SELECT {code_col}, {title_col} FROM {dict_table} WHERE {cond} LIMIT 500").format(
                code_col=psql.Identifier(config["dict_icd_col"]), title_col=psql.Identifier(config["dict_title_col"]),
                dict_table=psql.SQL(dict_table_name), cond=psql.SQL(condition_sql)
            )
        
        try:
            with psycopg2.connect(**db_params) as conn, conn.cursor() as cur:
                cur.execute(query, params)
                cols = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                self.result_label.setText(f"筛选项目预览 ({len(rows)} 条):")
                self.result_table.setRowCount(len(rows))
                self.result_table.setColumnCount(len(cols))
                self.result_table.setHorizontalHeaderLabels(cols)
                for i, row in enumerate(rows):
                    for j, val in enumerate(row):
                        self.result_table.setItem(i, j, QTableWidgetItem(str(val) if val else ""))
                self.result_table.resizeColumnsToContents()
            
            self.last_filter_conditions = (condition_sql, params)
            QMessageBox.information(self, "筛选成功", f"找到 {len(rows)} 个匹配项（最多显示500条）。\n您现在可以创建队列了。")
        except Exception as e:
            QMessageBox.critical(self, "筛选失败", f"执行筛选查询时出错: {e}")
            self.last_filter_conditions = None
        finally:
            self.update_button_states()

    def create_cohort_action(self):
        if self.cohort_worker_thread and self.cohort_worker_thread.isRunning():
            QMessageBox.warning(self, "任务进行中", "一个队列创建任务正在运行，请等待其完成。")
            return
            
        if not self.last_filter_conditions:
            QMessageBox.warning(self, "缺少条件", "请先成功执行一次“筛选并预览项目”。")
            return

        raw_name, ok = self.get_cohort_identifier_name()
        if not ok or not raw_name: return
        cleaned_name = re.sub(r'[^a-z0-9_]+', '_', raw_name.lower()).strip('_')
        if not cleaned_name:
            QMessageBox.warning(self, "名称无效", "请输入有效的队列标识符。")
            return
            
        config = self.get_active_mode_config()
        db_params = self.get_db_params()
        if not config or not db_params or not self.db_profile: return
        
        admission_type = self.admission_type_combo.currentData()
        table_prefix = "first_" if admission_type == COHORT_TYPE_FIRST_EVENT_KEY else "all_"
        mode_key = self.mode_selection_group.checkedButton().property("mode_key")
        source_prefix = f"{mode_key[:3]}_" if mode_key else "src_"
        target_table_name = f"{table_prefix}{source_prefix}{cleaned_name}_cohort"

        if len(target_table_name) > 63:
            QMessageBox.warning(self, "名称过长", f"生成的表名 '{target_table_name}' 超过63字符。")
            return

        reply = QMessageBox.question(self, '确认创建', f"将使用之前的筛选条件创建表:\n{target_table_name}\n确定吗?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.No: return

        condition_sql, params = self.last_filter_conditions
        
        self.progress_bar.setRange(0, 5)
        self.progress_bar.setValue(0)
        self.log_display.clear()

        self.cohort_worker = CohortCreationWorker(db_params, target_table_name, psql.SQL(condition_sql), params, admission_type, config, self.db_profile.get_cohort_table_schema())
        self.cohort_worker_thread = QThread()
        self.cohort_worker.moveToThread(self.cohort_worker_thread)
        self.cohort_worker_thread.started.connect(self.cohort_worker.run)
        self.cohort_worker.finished.connect(self.on_worker_finished)
        self.cohort_worker.error.connect(self.on_worker_error)
        self.cohort_worker.progress.connect(self.progress_bar.setValue)
        self.cohort_worker.log.connect(self.log_display.append)
        
        self.update_button_states() # Lock UI
        self.cohort_worker_thread.start()

    def cancel_action(self):
        if self.cohort_worker: self.cohort_worker.cancel()

    @Slot(str, int)
    def on_worker_finished(self, table_name, count):
        QMessageBox.information(self, "创建成功", f"队列 '{table_name}' 创建成功，共 {count} 条记录。")
        self.preview_created_cohort_table(self.db_profile.get_cohort_table_schema(), table_name)
        self.reset_worker_state()

    @Slot(str)
    def on_worker_error(self, error_message):
        if "操作已取消" not in error_message: QMessageBox.critical(self, "创建失败", f"创建队列失败: {error_message}")
        else: QMessageBox.information(self, "操作取消", "队列创建操作已取消。")
        self.reset_worker_state()
        
    def reset_worker_state(self):
        if self.cohort_worker:
            try:
                self.cohort_worker.finished.disconnect()
                self.cohort_worker.error.disconnect()
                self.cohort_worker.progress.disconnect()
                self.cohort_worker.log.disconnect()
            except (TypeError, RuntimeError): pass

        if self.cohort_worker_thread:
            self.cohort_worker_thread.quit()
            self.cohort_worker_thread.wait(1000)

        self.cohort_worker = None
        self.cohort_worker_thread = None
        self.update_button_states()

    def preview_created_cohort_table(self, schema_name, table_name):
        self.result_label.setText(f"队列预览: {schema_name}.{table_name} (前100行)")
        db_params = self.get_db_params()
        if not db_params: return
        try:
            with psycopg2.connect(**db_params) as conn, conn.cursor() as cur:
                query = psql.SQL("SELECT * FROM {}.{} LIMIT 100").format(psql.Identifier(schema_name), psql.Identifier(table_name))
                cur.execute(query)
                cols = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                self.result_table.setRowCount(len(rows))
                self.result_table.setColumnCount(len(cols))
                self.result_table.setHorizontalHeaderLabels(cols)
                for i, row in enumerate(rows):
                    for j, val in enumerate(row):
                        self.result_table.setItem(i, j, QTableWidgetItem(str(val) if val else ""))
                self.result_table.resizeColumnsToContents()
        except Exception as e:
            QMessageBox.critical(self, "预览失败", f"无法预览创建的队列表: {e}")

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