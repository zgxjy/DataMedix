# --- START OF FILE ui_components/time_window_selector_widget.py ---
from PySide6.QtWidgets import QWidget, QHBoxLayout, QLabel, QComboBox
from PySide6.QtCore import Signal, Slot # Slot 可能不需要，但保留无妨

class TimeWindowSelectorWidget(QWidget):
    time_window_changed = Signal(str) # 发出选中的时间窗口文本

    def __init__(self, label_text="时间窗口:", parent=None):
        super().__init__(parent)
        self._options_with_data = [] # 存储 (display_text, data_value)
        self.init_ui(label_text)

    def init_ui(self, label_text):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0,0,0,0)

        self.label = QLabel(label_text)
        layout.addWidget(self.label)

        self.combo_box = QComboBox()
        # 连接 currentTextChanged 信号，当用户通过UI更改或代码设置currentIndex且文本不同时触发
        self.combo_box.currentTextChanged.connect(self.time_window_changed.emit)
        layout.addWidget(self.combo_box)
        layout.addStretch()
        
        self.setLayout(layout)

    def set_options(self, options: list[str] | list[tuple[str, any]]):
        """
        设置下拉框的选项。
        options: 可以是字符串列表，或者 (显示文本, 关联数据) 的元组列表。
        """
        self.combo_box.blockSignals(True) # 开始修改前阻塞信号
        current_text_before_clear = self.combo_box.currentText() # 记录清除前的当前文本
        self.combo_box.clear()
        self._options_with_data = []

        if not options:
            self.combo_box.addItem("无可用时间窗口", None)
            self.combo_box.setEnabled(False)
            self.combo_box.blockSignals(False) # 完成修改后恢复信号
            # 如果之前有文本，现在没了，也应该发出信号（如果 currentTextChanged 能捕捉到空）
            # 或者手动判断如果 current_text_before_clear 不为空而现在为空，则 emit("")
            if current_text_before_clear != "": # 如果之前有文本，现在变空了
                 self.time_window_changed.emit("")
            return

        self.combo_box.setEnabled(True)
        for option in options:
            if isinstance(option, tuple) and len(option) == 2:
                display_text, data_value = option
                self.combo_box.addItem(display_text, data_value)
                self._options_with_data.append((display_text, data_value))
            else: 
                self.combo_box.addItem(str(option), str(option)) 
                self._options_with_data.append((str(option), str(option)))
        
        initial_emit_text = ""
        if self.combo_box.count() > 0:
            self.combo_box.setCurrentIndex(0) # 默认选中第一个
            initial_emit_text = self.combo_box.currentText() # 获取默认选中的文本

        self.combo_box.blockSignals(False) # 完成修改后恢复信号

        # 关键：在设置完选项并有了默认值后，如果这个默认值与 set_options 调用前的值不同，
        # 或者即使相同，也应该确保下游知道当前的有效选项。
        # currentTextChanged 会在 setCurrentIndex 如果导致文本变化时自动触发。
        # 如果 setCurrentIndex(0) 选中的文本恰好和之前一样，currentTextChanged 可能不触发。
        # 为了确保初始状态总是被传递，可以这样做：
        if initial_emit_text: # 只有当实际有选项时才发射
            # 检查是否真的需要发射：如果 currentTextChanged 没因为 setCurrentIndex(0) 触发
            # (例如，之前就是这个文本，或者之前是空的现在有了默认值)
            # 最简单的方式是总是emit一次，但如果currentTextChanged已经做了，就会重复。
            # 一个折衷是，如果 setCurrentIndex 之后的值和 set_options 之前的值不同，
            # 且 currentTextChanged 没触发（理论上不太可能），或者为了确保，就 emit。
            # 由于 currentTextChanged 的行为，我们通常不需要在这里再次手动 emit，
            # 除非是为了强制一个“初始化完成”的信号。
            # 你之前的代码是 if self.combo_box.count() > 0: self.time_window_changed.emit(self.combo_box.currentText())
            # 这是确保初始值被发出的一个好方法。
             self.time_window_changed.emit(initial_emit_text) # 确保初始值被发送

    def get_current_time_window_text(self) -> str:
        return self.combo_box.currentText()

    def get_current_time_window_data(self) -> any:
        return self.combo_box.currentData()

    def set_current_time_window_by_text(self, text: str):
        # 这个方法如果成功设置，也会触发 currentTextChanged -> time_window_changed
        for i in range(self.combo_box.count()):
            if self.combo_box.itemText(i) == text:
                if self.combo_box.currentIndex() != i: # 只有当索引实际改变时才设置
                    self.combo_box.setCurrentIndex(i)
                else: # 如果已经是这个索引，但想确保信号被处理（例如外部逻辑依赖这个信号）
                    self.time_window_changed.emit(text) # 手动再发一次
                return
        print(f"警告: 未在时间窗口选项中找到文本 '{text}'")

    def set_current_time_window_by_data(self, data_value: any):
        for i in range(self.combo_box.count()):
            if self.combo_box.itemData(i) == data_value:
                if self.combo_box.currentIndex() != i:
                    self.combo_box.setCurrentIndex(i)
                else:
                    self.time_window_changed.emit(self.combo_box.itemText(i))
                return
        print(f"警告: 未在时间窗口选项中找到数据值 '{data_value}'")
        
    def clear_selection(self):
        if self.combo_box.count() > 0:
            if self.combo_box.currentIndex() != 0: # 如果不是第一个，则设为第一个
                self.combo_box.setCurrentIndex(0)
            else: # 如果已经是第一个，手动触发信号，因为currentIndex可能不变
                self.time_window_changed.emit(self.combo_box.currentText())
        elif self.combo_box.currentText() != "": # 如果是空的，但之前有文本
            self.time_window_changed.emit("")


# --- END OF FILE ui_components/time_window_selector_widget.py ---