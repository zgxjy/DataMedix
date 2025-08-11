# --- START OF FILE medical_data_extractor.py ---

import sys
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QTabWidget, QMessageBox, QVBoxLayout, QWidget, 
    QHBoxLayout, QComboBox, QLabel, QDockWidget, QToolBar
)
from PySide6.QtCore import Qt, Slot, QThread, Signal
from PySide6.QtGui import QIcon, QAction

# 导入所有 Profile
from db_profiles.mimic_iv.profile import MIMICIVProfile
from db_profiles.eicu.profile import EICUProfile

# 导入所有 Tab 页面
from tabs.tab_connection import ConnectionTab
from tabs.tab_structure import StructureTab
from tabs.tab_query_cohort import QueryCohortTab
from tabs.tab_combine_base_info import BaseInfoDataExtractionTab
from tabs.tab_special_data_master import SpecialDataMasterTab
from tabs.tab_data_dictionary import DataDictionaryTab
from tabs.tab_data_export import DataExportTab
from tabs.tab_data_merge import DataMergeTab
from tabs.tab_sql_lab import SqlLabTab
from tabs.tab_data_processing import DataProcessingTab
from app_config import APP_NAME, APP_VERSION

class MedicalDataExtractor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} - v{APP_VERSION}")
        self.setGeometry(100, 100, 1100, 900) # 稍微调大默认尺寸
        self.setMinimumSize(950, 750)
        
        icon_path = "assets/icons/icon.ico"
        try:
            self.setWindowIcon(QIcon(icon_path))
        except Exception as e:
            print(f"Could not load window icon: {e}")

        self.db_profiles = {
            "MIMIC-IV": MIMICIVProfile,
            "e-ICU": EICUProfile,
        }
        self.active_db_profile = None

        # --- 实例化所有 Tab 页面 ---
        self.connection_tab = ConnectionTab(self.get_active_db_profile)
        self.query_cohort_tab = QueryCohortTab(self.get_db_params, self.get_active_db_profile)
        self.data_extraction_tab = BaseInfoDataExtractionTab(self.get_db_params, self.get_active_db_profile)
        self.special_data_master_tab = SpecialDataMasterTab(self.get_db_params, self.get_active_db_profile)
        self.data_export_tab = DataExportTab(self.get_db_params)
        self.data_processing_tab = DataProcessingTab(self.get_db_params, self.get_active_db_profile)
        
        # 实例化辅助工具页面
        self.data_merge_tab = DataMergeTab()
        self.structure_tab = StructureTab(self.get_db_params, self.get_current_db_profile)
        self.data_dictionary_tab = DataDictionaryTab(self.get_db_params, self.get_active_db_profile)
        self.sql_lab_tab = SqlLabTab(self.get_db_params, self)

        # 存储所有页面，方便后续遍历
        self.all_pages = [
            self.connection_tab, self.query_cohort_tab, self.data_extraction_tab,
            self.special_data_master_tab, self.data_export_tab, self.data_merge_tab,
            self.structure_tab, self.data_dictionary_tab, self.sql_lab_tab, self.data_processing_tab
        ]

        # --- 设置主布局和中心控件 ---
        self.setup_main_layout_and_tabs()
        
        # --- 设置辅助工具的停靠窗口和工具栏 ---
        self.setup_docks_and_toolbar()
        
        # --- 信号连接 ---
        self.profile_combo.currentTextChanged.connect(self.on_profile_changed)
        self.connection_tab.connected_signal.connect(self.on_db_connected)
        self.special_data_master_tab.request_preview_signal.connect(self.handle_special_data_preview)
        self.structure_tab.request_table_preview_signal.connect(self.handle_structure_table_preview)
        self.structure_tab.request_send_to_sql_lab_signal.connect(self.handle_send_to_sql_lab)

        # 初始 profile 设置
        self.on_profile_changed(self.profile_combo.currentText())

    def setup_main_layout_and_tabs(self):
        """设置主窗口的核心布局和主流程标签页"""
        # 主流程标签页作为中心控件
        self.main_tabs = QTabWidget()
        self.setCentralWidget(self.main_tabs)
        self.main_tabs.setDocumentMode(True) # 样式更紧凑

        # --- 添加主流程 Tab ---
        self.main_tabs.addTab(self.connection_tab, "1. 数据库连接")
        self.main_tabs.addTab(self.query_cohort_tab, "2. 查找与创建队列")
        self.main_tabs.addTab(self.data_extraction_tab, "3. 添加基础数据")
        self.main_tabs.addTab(self.special_data_master_tab, "4. 添加专项数据")
        self.main_tabs.addTab(self.data_export_tab, "5. 数据预览与导出")
        self.main_tabs.addTab(self.data_processing_tab, "6. 数据处理")
        
        # --- 数据库 Profile 选择器 (放在一个工具栏里) ---
        profile_toolbar = QToolBar("数据库选择")
        profile_toolbar.setObjectName("ProfileToolbar")
        profile_toolbar.setMovable(False)
        self.addToolBar(Qt.TopToolBarArea, profile_toolbar)
        
        profile_toolbar.addWidget(QLabel("<b>选择数据库类型:</b>"))
        self.profile_combo = QComboBox()
        self.profile_combo.addItems(self.db_profiles.keys())
        profile_toolbar.addWidget(self.profile_combo)

    def setup_docks_and_toolbar(self):
        """创建辅助工具的停靠窗口 (QDockWidget) 和启动它们的工具栏"""
        self.tools_toolbar = QToolBar("辅助工具")
        self.tools_toolbar.setObjectName("ToolsToolbar")
        self.addToolBar(Qt.TopToolBarArea, self.tools_toolbar)
        self.tools_toolbar.setIconSize(self.tools_toolbar.iconSize() * 0.8)

        # --- 1. 数据合并工具 ---
        self.merge_dock = self.create_dock("数据合并", self.data_merge_tab, Qt.BottomDockWidgetArea)
        merge_action = self.create_tool_action("数据合并", "assets/icons/merge.png", self.merge_dock)
        self.tools_toolbar.addAction(merge_action)

        # --- 2. 数据库结构查看 ---
        self.structure_dock = self.create_dock("数据库结构", self.structure_tab, Qt.LeftDockWidgetArea)
        structure_action = self.create_tool_action("结构查看", "assets/icons/database.png", self.structure_dock)
        self.tools_toolbar.addAction(structure_action)
        
        # --- 3. 数据字典 ---
        self.dictionary_dock = self.create_dock("数据字典", self.data_dictionary_tab, Qt.LeftDockWidgetArea)
        dictionary_action = self.create_tool_action("数据字典", "assets/icons/book.png", self.dictionary_dock)
        self.tools_toolbar.addAction(dictionary_action)
        
        # --- 4. SQL实验室 ---
        self.sql_lab_dock = self.create_dock("SQL实验室", self.sql_lab_tab, Qt.BottomDockWidgetArea)
        sql_lab_action = self.create_tool_action("SQL实验室", "assets/icons/code.png", self.sql_lab_dock)
        self.tools_toolbar.addAction(sql_lab_action)
        
        # 允许DockWidget堆叠和嵌套
        self.setDockOptions(QMainWindow.AnimatedDocks | QMainWindow.AllowTabbedDocks | QMainWindow.AllowNestedDocks)
        # 将结构和字典默认组合在一个标签页里
        self.tabifyDockWidget(self.structure_dock, self.dictionary_dock)

    def create_dock(self, title, widget, area):
        """辅助函数，用于创建和配置QDockWidget"""
        dock = QDockWidget(title, self)
        dock.setWidget(widget)
        dock.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea | Qt.BottomDockWidgetArea | Qt.TopDockWidgetArea)
        dock.hide() # 默认隐藏
        self.addDockWidget(area, dock)
        return dock

    def create_tool_action(self, text, icon_path, dock_widget):
        """辅助函数，用于创建工具栏的QAction来切换Dock"""
        action = QAction(QIcon(icon_path), text, self)
        action.setCheckable(True)
        action.toggled.connect(dock_widget.setVisible)
        dock_widget.visibilityChanged.connect(action.setChecked)
        return action

    def get_active_db_profile(self):
        return self.active_db_profile

    def get_db_params(self):
        return self.connection_tab.db_params if self.connection_tab.connected else None

    def get_current_db_profile(self):
        return self.active_db_profile

    @Slot(str)
    def on_profile_changed(self, profile_name: str):
        profile_class = self.db_profiles.get(profile_name)
        if profile_class:
            self.active_db_profile = profile_class()
            self.setWindowTitle(f"{APP_NAME}: {self.active_db_profile.get_display_name()} - v{APP_VERSION}")
            self.connection_tab.set_default_params(self.active_db_profile.get_default_connection_params())
            
            if self.connection_tab.connected:
                self.connection_tab.reset_connection()
                QMessageBox.information(self, "数据库已切换", "数据库类型已更改。请重新连接。")

            for page in self.all_pages:
                if hasattr(page, 'on_profile_changed'):
                    page.on_profile_changed()
        else:
            self.active_db_profile = None

    @Slot()
    def on_db_connected(self):
        for page in self.all_pages:
            if hasattr(page, 'on_db_connected'):
                page.on_db_connected()

    @Slot(str, str)
    def handle_special_data_preview(self, schema_name, table_name):
        self.main_tabs.setCurrentWidget(self.data_export_tab)
        self.data_export_tab.preview_specific_table(schema_name, table_name)

    @Slot(str, str)
    def handle_structure_table_preview(self, schema_name, table_name):
        self.main_tabs.setCurrentWidget(self.data_export_tab)
        if self.connection_tab.connected and not self.data_export_tab.refresh_btn.isEnabled():
            self.data_export_tab.on_db_connected()
            QApplication.processEvents()
        self.data_export_tab.preview_specific_table(schema_name, table_name)
    
    @Slot(str)
    def handle_send_to_sql_lab(self, sql_query: str):
        if not self.sql_lab_dock.isVisible():
            self.sql_lab_dock.show()
        self.sql_lab_dock.raise_()
        self.sql_lab_tab.sql_editor.setText(sql_query)
        self.sql_lab_tab.execute_sql()
        
    def closeEvent(self, event):
        # 查找所有包含正在运行的QThread的页面
        active_workers_pages = [
            p for p in self.all_pages 
            if any(isinstance(getattr(p, attr, None), QThread) and getattr(p, attr).isRunning() for attr in dir(p))
        ]
        
        for tab_instance in active_workers_pages:
            print(f"Stopping worker in {tab_instance.__class__.__name__}...")
            # 找到worker对象 (通常与线程名相关)
            worker_obj = None
            if hasattr(tab_instance, 'worker'): worker_obj = tab_instance.worker
            elif hasattr(tab_instance, 'cohort_worker'): worker_obj = tab_instance.cohort_worker
            elif hasattr(tab_instance, 'merge_worker'): worker_obj = tab_instance.merge_worker
            
            # 找到正在运行的线程并停止它
            for attr_name in dir(tab_instance):
                thread = getattr(tab_instance, attr_name)
                if isinstance(thread, QThread) and thread.isRunning():
                    if worker_obj and hasattr(worker_obj, 'cancel'):
                        worker_obj.cancel()
                    thread.quit()
                    if not thread.wait(1000): # 等待1秒
                        print(f"Warning: Worker thread '{attr_name}' in {tab_instance.__class__.__name__} did not quit gracefully.")
                        
        super().closeEvent(event)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MedicalDataExtractor()
    window.show()
    sys.exit(app.exec())