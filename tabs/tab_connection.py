# --- START OF FILE tabs/tab_connection.py ---
from PySide6.QtWidgets import QWidget, QVBoxLayout, QFormLayout, QLineEdit, QPushButton, QHBoxLayout, QMessageBox
from PySide6.QtCore import Signal

# REPAIR: Removed import of DEFAULT_DB_NAME and DEFAULT_DB_USER as they are now in profiles.
from app_config import DEFAULT_DB_HOST, DEFAULT_DB_PORT
import psycopg2

class ConnectionTab(QWidget):
    connected_signal = Signal()
    # disconnected_signal = Signal() # Optional: for more robust state management

    def __init__(self, get_db_profile_func, parent=None):
        super().__init__(parent)
        self.get_db_profile = get_db_profile_func
        self.db_params = {}
        self.connected = False
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout(self)
        form_layout = QFormLayout()

        self.db_name_input = QLineEdit()
        self.db_user_input = QLineEdit()
        self.db_password_input = QLineEdit()
        self.db_password_input.setEchoMode(QLineEdit.Password)
        self.db_host_input = QLineEdit(DEFAULT_DB_HOST)
        self.db_port_input = QLineEdit(DEFAULT_DB_PORT)
        
        form_layout.addRow("数据库名称:", self.db_name_input)
        form_layout.addRow("用户名:", self.db_user_input)
        form_layout.addRow("密码:", self.db_password_input)
        form_layout.addRow("主机:", self.db_host_input)
        form_layout.addRow("端口:", self.db_port_input)
        layout.addLayout(form_layout)

        btn_layout = QHBoxLayout()
        self.test_connection_btn = QPushButton("连接测试并应用")
        self.test_connection_btn.clicked.connect(self.connect_database)
        btn_layout.addWidget(self.test_connection_btn)

        self.disconnect_btn = QPushButton("断开连接")
        self.disconnect_btn.clicked.connect(self.reset_connection)
        self.disconnect_btn.setEnabled(False)
        btn_layout.addWidget(self.disconnect_btn)
        layout.addLayout(btn_layout)

    def set_default_params(self, params: dict):
        """Sets default connection parameters from the active profile."""
        self.db_name_input.setText(params.get("dbname", ""))
        self.db_user_input.setText(params.get("user", ""))
        # Host and Port can keep the app-level defaults if profile doesn't specify
        self.db_host_input.setText(params.get("host", DEFAULT_DB_HOST))
        self.db_port_input.setText(params.get("port", DEFAULT_DB_PORT))
        self.db_password_input.clear()

    def connect_database(self):
        if self.connected:
            return
        
        params = {
            'dbname': self.db_name_input.text().strip(),
            'user': self.db_user_input.text().strip(),
            'password': self.db_password_input.text(),
            'host': self.db_host_input.text().strip(),
            'port': self.db_port_input.text().strip()
        }
        
        if not all([params['dbname'], params['user'], params['host'], params['port']]):
            QMessageBox.warning(self, "信息不完整", "数据库名称、用户名、主机和端口不能为空。")
            return
            
        try:
            conn = psycopg2.connect(**params)
            conn.close()
            self.db_params = params
            self.connected = True
            self.lock_inputs(True)
            QMessageBox.information(self, "连接成功", "数据库连接成功，参数已锁定。")
            self.connected_signal.emit()
        except Exception as e:
            QMessageBox.critical(self, "连接失败", f"无法连接到数据库: {str(e)}")

    def lock_inputs(self, lock: bool):
        self.db_name_input.setEnabled(not lock)
        self.db_user_input.setEnabled(not lock)
        self.db_password_input.setEnabled(not lock)
        self.db_host_input.setEnabled(not lock)
        self.db_port_input.setEnabled(not lock)
        self.test_connection_btn.setEnabled(not lock)
        self.disconnect_btn.setEnabled(lock)

    def reset_connection(self):
        """Resets the connection state and unlocks input fields."""
        self.connected = False
        self.db_params = {}
        self.lock_inputs(False)
        QMessageBox.information(self, "已断开", "数据库连接已断开，您可以修改参数或切换数据库类型后重新连接。")
        # Optional: Emit a disconnected signal if other tabs need to react immediately.
        # self.disconnected_signal.emit()

        # Reload default params for the currently selected profile
        profile = self.get_db_profile()
        if profile:
            self.set_default_params(profile.get_default_connection_params())