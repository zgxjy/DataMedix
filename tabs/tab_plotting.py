# tabs/tab_plotting.py
import pandas as pd
import psycopg2
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QComboBox, QLabel,
    QGroupBox, QSplitter, QStackedWidget, QMessageBox, QApplication
)
from PySide6.QtCore import Qt, Slot

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar

import matplotlib.pyplot as plt
from lifelines import KaplanMeierFitter
from lifelines.statistics import logrank_test

from ui_components.plotting_panels.km_panel import KM_Panel

class PlottingTab(QWidget):
    def __init__(self, get_db_params_func, get_db_profile_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.get_db_profile = get_db_profile_func
        self.df = None
        self.init_ui()
        self.setup_plot_panels()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        
        data_source_group = QGroupBox("1. 选择数据源")
        ds_layout = QHBoxLayout(data_source_group)
        ds_layout.addWidget(QLabel("Schema:"))
        self.schema_combo = QComboBox()
        ds_layout.addWidget(self.schema_combo)
        ds_layout.addWidget(QLabel("队列表:"))
        self.table_combo = QComboBox()
        ds_layout.addWidget(self.table_combo, 1)
        self.load_data_btn = QPushButton("加载数据")
        ds_layout.addWidget(self.load_data_btn)
        main_layout.addWidget(data_source_group)

        splitter = QSplitter(Qt.Horizontal); main_layout.addWidget(splitter, 1)

        config_widget = QWidget()
        config_layout = QVBoxLayout(config_widget)
        plot_type_group = QGroupBox("2. 选择图表类型并配置参数")
        pt_layout = QVBoxLayout(plot_type_group)
        self.plot_type_combo = QComboBox(); pt_layout.addWidget(self.plot_type_combo)
        self.config_stack = QStackedWidget(); pt_layout.addWidget(self.config_stack, 1)
        config_layout.addWidget(plot_type_group)
        self.generate_plot_btn = QPushButton("生成图表")
        self.generate_plot_btn.setEnabled(False)
        self.generate_plot_btn.setStyleSheet("font-weight: bold; color: green;")
        config_layout.addWidget(self.generate_plot_btn)
        splitter.addWidget(config_widget)

        plot_display_widget = QWidget()
        plot_layout = QVBoxLayout(plot_display_widget)
        self.figure = Figure(figsize=(8, 6), dpi=100)
        self.canvas = FigureCanvas(self.figure)
        self.toolbar = NavigationToolbar(self.canvas, self)
        plot_layout.addWidget(self.toolbar); plot_layout.addWidget(self.canvas)
        splitter.addWidget(plot_display_widget)
        splitter.setSizes([350, 650])

        self.schema_combo.currentIndexChanged.connect(self.refresh_tables)
        self.load_data_btn.clicked.connect(self.load_data_from_db)
        self.plot_type_combo.currentIndexChanged.connect(self.on_plot_type_changed)
        self.generate_plot_btn.clicked.connect(self.generate_plot)

    def setup_plot_panels(self):
        self.km_panel = KM_Panel()
        self.config_stack.addWidget(self.km_panel)
        self.plot_type_combo.addItem("Kaplan-Meier 生存曲线", self.km_panel)
        self.on_plot_type_changed(0)

    @Slot()
    def on_db_connected(self): self.refresh_schemas()
    @Slot()
    def on_profile_changed(self):
        self.schema_combo.clear(); self.table_combo.clear(); self.df = None
        self.generate_plot_btn.setEnabled(False)
        self.km_panel.update_columns(None)
        self.figure.clear(); self.canvas.draw()
    
    def refresh_schemas(self):
        db_params = self.get_db_params();
        if not db_params: return
        try:
            with psycopg2.connect(**db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute("""SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND schema_name NOT LIKE 'pg_temp%' ORDER BY schema_name;""")
                    schemas = [s[0] for s in cur.fetchall()]
                    current = self.schema_combo.currentText()
                    self.schema_combo.blockSignals(True); self.schema_combo.clear(); self.schema_combo.addItems(schemas)
                    if current in schemas: self.schema_combo.setCurrentText(current)
                    self.schema_combo.blockSignals(False)
                    if schemas: self.refresh_tables()
        except Exception as e: QMessageBox.critical(self, "错误", f"无法获取Schemas: {e}")

    def refresh_tables(self):
        schema = self.schema_combo.currentText()
        if not schema: return
        try:
            with psycopg2.connect(**self.get_db_params()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type='BASE TABLE' ORDER BY table_name", (schema,))
                    tables = [t[0] for t in cur.fetchall()]
                    current = self.table_combo.currentText()
                    self.table_combo.clear(); self.table_combo.addItems(tables)
                    if current in tables: self.table_combo.setCurrentText(current)
        except Exception as e: QMessageBox.critical(self, "错误", f"无法获取数据表: {e}")

    @Slot()
    def load_data_from_db(self):
        schema, table = self.schema_combo.currentText(), self.table_combo.currentText()
        if not schema or not table: QMessageBox.warning(self, "信息不全", "请选择Schema和队列表。"); return
        
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            with psycopg2.connect(**self.get_db_params()) as conn:
                self.df = pd.read_sql(f"SELECT * FROM \"{schema}\".\"{table}\"", conn)
            
            self.on_plot_type_changed(self.plot_type_combo.currentIndex())
            self.generate_plot_btn.setEnabled(True)
            QMessageBox.information(self, "加载成功", f"成功加载 {len(self.df)} 条记录。")
        except Exception as e:
            self.df = None; self.generate_plot_btn.setEnabled(False)
            QMessageBox.critical(self, "加载失败", str(e))
        finally:
            QApplication.restoreOverrideCursor()

    @Slot(int)
    def on_plot_type_changed(self, index):
        panel = self.plot_type_combo.itemData(index)
        if panel:
            self.config_stack.setCurrentWidget(panel)
            if hasattr(panel, 'update_columns'): panel.update_columns(self.df)

    @Slot()
    def generate_plot(self):
        if self.df is None or self.df.empty: QMessageBox.warning(self, "无数据", "请先加载数据。"); return
        panel = self.config_stack.currentWidget(); plot_type = self.plot_type_combo.currentText()
        self.figure.clear(); plt.style.use('seaborn-v0_8-whitegrid')
        try:
            if "Kaplan-Meier" in plot_type: self.plot_kaplan_meier(panel.get_config())
            else: QMessageBox.warning(self, "未实现", "该图表类型的绘图逻辑尚未实现。"); return
            self.canvas.draw()
        except Exception as e:
            self.figure.clear(); self.canvas.draw()
            QMessageBox.critical(self, "绘图失败", f"生成图表时发生错误:\n{e}")

    def plot_kaplan_meier(self, config):
        time_col, event_col = config['time_col'], config['event_col']
        if not time_col or not event_col: raise ValueError("必须选择时间和事件列。")
        
        T = pd.to_numeric(self.df[time_col], errors='coerce')
        E = pd.to_numeric(self.df[event_col], errors='coerce')
        valid_idx = T.notna() & E.notna()

        if config['group_col']:
            valid_idx = valid_idx & self.df[config['group_col']].notna()
        
        if not valid_idx.any(): raise ValueError("选择的列中没有有效的数值数据，或过滤后数据为空。")
        
        df_valid = self.df[valid_idx].copy()
        ax = self.figure.add_subplot(111)
        
        if config['group_col']:
            group_col = config['group_col']
            groups = sorted(df_valid[group_col].unique())
            
            for group in groups:
                df_group = df_valid[df_valid[group_col] == group]
                durations_group = pd.to_numeric(df_group[time_col])
                events_group = pd.to_numeric(df_group[event_col])
                kmf = KaplanMeierFitter().fit(durations_group, events_group, label=str(group))
                kmf.plot_survival_function(ax=ax, ci_show=config['show_ci'])
            
            if len(groups) >= 2 and config['show_pvalue']:
                args_for_test = []
                for group in groups:
                    df_group = df_valid[df_valid[group_col] == group]
                    args_for_test.append(pd.to_numeric(df_group[time_col]))
                    args_for_test.append(pd.to_numeric(df_group[event_col]))
                result = logrank_test(*args_for_test)
                
                ax.text(0.95, 0.05, f'Log-rank p-value: {result.p_value:.4f}',
                        transform=ax.transAxes, ha='right', va='bottom',
                        bbox=dict(boxstyle='round,pad=0.5', fc='wheat', alpha=0.5))
        else:
            durations = pd.to_numeric(df_valid[time_col])
            events = pd.to_numeric(df_valid[event_col])
            kmf = KaplanMeierFitter().fit(durations, events, label='Overall Survival')
            kmf.plot_survival_function(ax=ax, ci_show=config['show_ci'])
        
        ax.set_title(config['title']); ax.set_xlabel("Time (Duration)"); ax.set_ylabel("Survival Probability")
        ax.legend(title=config.get('group_col', ''))
        self.figure.tight_layout()