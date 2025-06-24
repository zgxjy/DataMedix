# --- START OF FILE medical_data_extractor.py ---
import sys
from PySide6.QtWidgets import (QApplication, QMainWindow, QTabWidget, QMessageBox,
                               QVBoxLayout, QWidget, QHBoxLayout, QComboBox, QLabel)
from PySide6.QtCore import Slot, Qt
from PySide6.QtGui import QIcon

from db_profiles.mimic_iv.profile import MIMICIVProfile
from db_profiles.eicu.profile import EICUProfile 

from tabs.tab_connection import ConnectionTab
from tabs.tab_structure import StructureTab
from tabs.tab_query_cohort import QueryCohortTab
from tabs.tab_combine_base_info import BaseInfoDataExtractionTab
from tabs.tab_special_data_master import SpecialDataMasterTab
from tabs.tab_data_dictionary import DataDictionaryTab
from tabs.tab_data_export import DataExportTab
from tabs.tab_data_merge import DataMergeTab
from app_config import APP_NAME, APP_VERSION

class MedicalDataExtractor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} - v{APP_VERSION}")
        self.setGeometry(100, 100, 950, 880)
        self.setMinimumSize(900, 700)
        
        icon_path = "assets/icons/icon.ico"
        try:
            self.setWindowIcon(QIcon(icon_path))
        except Exception as e:
            print(f"Could not load window icon: {e}")

        # REFACTOR: Profile management
        self.db_profiles = {
            "MIMIC-IV": MIMICIVProfile,
            "e-ICU": EICUProfile,
        }
        self.active_db_profile = None

        # --- Main Layout Setup ---
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # --- Profile Selector UI ---
        profile_selector_layout = QHBoxLayout()
        profile_selector_layout.addWidget(QLabel("<b>选择数据库类型:</b>"))
        self.profile_combo = QComboBox()
        self.profile_combo.addItems(self.db_profiles.keys())
        profile_selector_layout.addWidget(self.profile_combo)
        profile_selector_layout.addStretch()
        main_layout.addLayout(profile_selector_layout)

        # --- Tab Widget ---
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # REFACTOR: Instantiate tabs, passing profile getter
        self.connection_tab = ConnectionTab(self.get_active_db_profile)
        self.structure_tab = StructureTab(self.get_db_params)
        self.data_dictionary_tab = DataDictionaryTab(self.get_db_params, self.get_active_db_profile)
        self.query_cohort_tab = QueryCohortTab(self.get_db_params, self.get_active_db_profile)
        self.data_extraction_tab = BaseInfoDataExtractionTab(self.get_db_params, self.get_active_db_profile)
        self.special_data_master_tab = SpecialDataMasterTab(self.get_db_params, self.get_active_db_profile)
        self.data_export_tab = DataExportTab(self.get_db_params)
        self.data_merge_tab = DataMergeTab()

        # Add tabs
        self.tabs.addTab(self.connection_tab, "1. 数据库连接")
        self.tabs.addTab(self.structure_tab, "数据库结构查看")
        self.tabs.addTab(self.data_dictionary_tab, "数据字典查看")
        self.tabs.addTab(self.query_cohort_tab, "2. 查找与创建队列")
        self.tabs.addTab(self.data_extraction_tab, "3. 添加基础数据")
        self.tabs.addTab(self.special_data_master_tab, "4. 添加专项数据")
        self.tabs.addTab(self.data_export_tab, "5. 数据预览与导出")
        self.tabs.addTab(self.data_merge_tab, "6. 数据合并")

        # --- Signal Connections ---
        self.profile_combo.currentTextChanged.connect(self.on_profile_changed)
        self.connection_tab.connected_signal.connect(self.on_db_connected)
        self.special_data_master_tab.request_preview_signal.connect(self.data_export_tab.preview_specific_table)
        self.structure_tab.request_table_preview_signal.connect(self.handle_structure_table_preview)

        # Initial profile setup
        self.on_profile_changed(self.profile_combo.currentText())

    def get_active_db_profile(self):
        """Provides the currently active database profile instance to other components."""
        return self.active_db_profile

    def get_db_params(self):
        """Provides the current database connection parameters."""
        return self.connection_tab.db_params if self.connection_tab.connected else None

    @Slot(str)
    def on_profile_changed(self, profile_name: str):
        """Handles switching the active database profile."""
        profile_class = self.db_profiles.get(profile_name)
        if profile_class:
            self.active_db_profile = profile_class()
            self.setWindowTitle(f"{APP_NAME}: {self.active_db_profile.get_display_name()} - v{APP_VERSION}")
            print(f"Active database profile changed to: {self.active_db_profile.get_display_name()}")

            # Update connection tab with profile defaults
            self.connection_tab.set_default_params(self.active_db_profile.get_default_connection_params())
            
            # Lock UI and prompt user to reconnect
            if self.connection_tab.connected:
                self.connection_tab.reset_connection()
                QMessageBox.information(self, "数据库已切换", 
                                          "数据库类型已更改。请使用新的默认参数重新连接数据库。")

            # Notify all tabs that the profile has changed, so they can reconfigure themselves
            for i in range(self.tabs.count()):
                tab_widget = self.tabs.widget(i)
                if hasattr(tab_widget, 'on_profile_changed'):
                    tab_widget.on_profile_changed()
        else:
            self.active_db_profile = None

    @Slot()
    def on_db_connected(self):
        """Propagates the connected signal to all relevant tabs."""
        print("Database connected signal received by main window.")
        for i in range(self.tabs.count()):
            tab_widget = self.tabs.widget(i)
            if hasattr(tab_widget, 'on_db_connected'):
                print(f"Calling on_db_connected for tab: {tab_widget.__class__.__name__}")
                tab_widget.on_db_connected()

    @Slot(str, str)
    def handle_structure_table_preview(self, schema_name, table_name):
        export_tab_index = -1
        for i in range(self.tabs.count()):
            if isinstance(self.tabs.widget(i), DataExportTab):
                export_tab_index = i
                break
        
        if export_tab_index != -1:
            self.tabs.setCurrentIndex(export_tab_index)
            if self.connection_tab.connected and not self.data_export_tab.refresh_btn.isEnabled():
                self.data_export_tab.on_db_connected()
                QApplication.processEvents()
            self.data_export_tab.preview_specific_table(schema_name, table_name)
        else:
            QMessageBox.warning(self, "错误", "无法找到数据导出标签页。")

    def closeEvent(self, event):
        tabs_with_workers = [
            self.query_cohort_tab,
            self.data_extraction_tab,
            self.special_data_master_tab
        ]

        for tab_instance in tabs_with_workers:
            # This logic assumes worker thread attributes are consistently named.
            # A more robust solution might be a common interface for tabs with workers.
            worker_thread_attr_name = None
            worker_obj_attr_name = None

            if hasattr(tab_instance, 'cohort_worker_thread'):
                worker_thread_attr_name = 'cohort_worker_thread'
                worker_obj_attr_name = 'cohort_worker'
            elif hasattr(tab_instance, 'worker_thread'):
                worker_thread_attr_name = 'worker_thread'
                if hasattr(tab_instance, 'worker'):
                     worker_obj_attr_name = 'worker'
                elif hasattr(tab_instance, 'merge_worker'):
                     worker_obj_attr_name = 'merge_worker'
            
            if worker_thread_attr_name and worker_obj_attr_name:
                thread = getattr(tab_instance, worker_thread_attr_name, None)
                worker = getattr(tab_instance, worker_obj_attr_name, None)
                
                if thread and thread.isRunning():
                    print(f"Attempting to stop worker in {tab_instance.__class__.__name__} on close...")
                    if worker and hasattr(worker, 'cancel'):
                        worker.cancel()
                    thread.quit()
                    if not thread.wait(1500):
                        print(f"Warning: Worker thread in {tab_instance.__class__.__name__} did not quit in time.")
                        
        super().closeEvent(event)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MedicalDataExtractor()
    window.show()
    sys.exit(app.exec())