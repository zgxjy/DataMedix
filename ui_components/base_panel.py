# --- START OF FILE ui_components/base_panel.py ---
from PySide6.QtWidgets import QWidget, QMessageBox
from PySide6.QtCore import Signal, Qt, Slot
import psycopg2
from typing import Optional, Dict, Any, Callable

class BaseSourceConfigPanel(QWidget):
    """
    配置专项数据提取的UI面板的抽象基类。
    每个特定的数据源（如MIMIC的chartevents，eICU的lab）都应该有一个继承自此类的面板。
    """
    config_changed_signal = Signal()

    def __init__(self, db_params_getter: Callable[[], Optional[Dict]], db_profile_getter: Callable[[], Optional[Any]], parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.get_db_params = db_params_getter
        self.get_db_profile = db_profile_getter
        self._db_conn = None
        self._db_cursor = None
        self.init_panel_ui()

    def init_panel_ui(self):
        """[子类实现] 在这里构建面板特定的UI。"""
        pass

    def _connect_panel_db(self) -> bool:
        """为面板建立一个独立的数据库连接。"""
        if self._db_conn and self._db_conn.closed == 0:
            try:
                if not self._db_cursor or self._db_cursor.closed:
                    self._db_cursor = self._db_conn.cursor()
                self._db_conn.isolation_level
                return True
            except (psycopg2.InterfaceError, psycopg2.OperationalError):
                self._db_conn = None
                self._db_cursor = None
        
        db_params = self.get_db_params()
        if not db_params:
            return False
        try:
            self._db_conn = psycopg2.connect(**db_params)
            self._db_cursor = self._db_conn.cursor()
            return True
        except Exception as e:
            print(f"Error connecting panel-specific DB: {e}")
            self._db_conn = None
            self._db_cursor = None
            return False

    def _close_panel_db(self):
        """关闭面板持有的数据库连接。"""
        if self._db_cursor: 
            try: self._db_cursor.close() 
            except Exception as e: print(f"Error closing panel cursor: {e}")
        if self._db_conn: 
            try: self._db_conn.close()
            except Exception as e: print(f"Error closing panel connection: {e}")
        self._db_cursor = None
        self._db_conn = None

    def populate_panel_if_needed(self):
        """[子类可选实现] 当面板被显示时调用。"""
        pass

    def get_panel_config(self) -> dict:
        """[子类必须实现] 返回一个包含该面板特定配置的字典。"""
        raise NotImplementedError("Subclasses must implement get_panel_config")

    def clear_panel_state(self):
        """[子类必须实现] 清除UI元素的状态并重置配置。"""
        raise NotImplementedError("Subclasses must implement clear_panel_state")

    def update_panel_action_buttons_state(self, general_config_ok: bool):
        """[子类可选实现] 由主Tab调用，用于更新面板内按钮的状态。"""
        if hasattr(self, 'filter_items_btn'):
            panel_conditions_ok = True
            if hasattr(self, 'condition_widget') and hasattr(self.condition_widget, 'has_valid_input'):
                panel_conditions_ok = self.condition_widget.has_valid_input()
            self.filter_items_btn.setEnabled(general_config_ok and panel_conditions_ok)

    def get_friendly_source_name(self) -> str:
        """[子类必须实现] 返回一个用户友好的数据源名称。"""
        raise NotImplementedError("Subclasses must implement get_friendly_source_name")
    
    def get_selected_item_ids(self) -> list:
        """
        从名为 'item_list' 的 QListWidget 中获取选中项的ID，并清理空格。
        """
        if hasattr(self, 'item_list'):
            ids = []
            for i in range(self.item_list.count()):
                list_view_item = self.item_list.item(i)
                if list_view_item.isSelected():
                    data = list_view_item.data(Qt.ItemDataRole.UserRole)
                    if data and len(data) > 0 and data[0] is not None:
                        # 核心修复：对获取的ID进行strip()处理
                        ids.append(str(data[0]).strip())
            return ids
        return []

    def __del__(self):
        """确保在面板被销毁时，其独立的数据库连接被关闭。"""
        self._close_panel_db()