# tabs/tab_data_merge.py
import sys
import pandas as pd
import chardet

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QFileDialog,
    QTableView, QListWidget, QComboBox, QLineEdit, QSplitter, QGroupBox, QAbstractItemView,
    QHeaderView, QMessageBox, QScrollArea
)
from PySide6.QtCore import Qt, Slot
from PySide6.QtGui import QStandardItemModel, QStandardItem

class PandasTableModel(QStandardItemModel):
    def __init__(self, data):
        super().__init__()
        if data.empty:
            return

        self.setHorizontalHeaderLabels(data.columns.tolist())
        for i, row in data.iterrows():
            items = [QStandardItem(str(val)) for val in row]
            self.appendRow(items)

class DataMergeTab(QWidget):
    def __init__(self):
        super().__init__()
        self.df_left = None
        self.df_right = None
        self.merged_df_result = None

        self.main_layout = QVBoxLayout(self)
        self.setup_ui()

    def detect_encoding(self, file_path):
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read(200000)) # Read a chunk for detection
        return result['encoding']

    def setup_ui(self):
        # 1. 创建一个 QScrollArea 并将其设置为主布局的唯一控件
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)  # 关键！让内部控件能自适应宽度
        self.main_layout.addWidget(scroll_area)

        # 2. 创建一个容器 QWidget，所有的UI元素都将放在这个容器里
        scroll_content_widget = QWidget()
        scroll_area.setWidget(scroll_content_widget)

        # 3. 为容器创建一个布局，之后所有的UI元素都添加到这个布局中
        content_layout = QVBoxLayout(scroll_content_widget)

        # --- 以下是您原有的UI创建代码，只是将 addWidget 的目标从 self.main_layout 改为 content_layout ---
        
        # Top layout for dataset loading and preview
        top_splitter = QSplitter(Qt.Horizontal)

        # Left Dataset Area
        left_group = QGroupBox("左侧数据集")
        left_layout = QVBoxLayout()
        self.btn_load_left = QPushButton("加载左侧数据集 (CSV/Excel)")
        self.btn_load_left.clicked.connect(lambda: self.load_data('left'))
        self.lbl_left_file = QLabel("未加载文件")
        self.table_left_preview = QTableView()
        self.table_left_preview.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_left_preview.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.list_left_cols = QListWidget()
        self.list_left_cols.setSelectionMode(QAbstractItemView.ExtendedSelection)
        left_layout.addWidget(self.btn_load_left)
        left_layout.addWidget(self.lbl_left_file)
        left_preview_columns_splitter = QSplitter(Qt.Vertical)
        left_preview_columns_splitter.addWidget(self.table_left_preview)
        left_preview_columns_splitter.addWidget(self.list_left_cols)
        left_preview_columns_splitter.setSizes([200, 100]) # Initial sizes
        left_layout.addWidget(left_preview_columns_splitter)
        left_group.setLayout(left_layout)
        top_splitter.addWidget(left_group)

        # Right Dataset Area
        right_group = QGroupBox("右侧数据集")
        right_layout = QVBoxLayout()
        self.btn_load_right = QPushButton("加载右侧数据集 (CSV/Excel)")
        self.btn_load_right.clicked.connect(lambda: self.load_data('right'))
        self.lbl_right_file = QLabel("未加载文件")
        self.table_right_preview = QTableView()
        self.table_right_preview.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_right_preview.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.list_right_cols = QListWidget()
        self.list_right_cols.setSelectionMode(QAbstractItemView.ExtendedSelection)
        right_layout.addWidget(self.btn_load_right)
        right_layout.addWidget(self.lbl_right_file)
        right_preview_columns_splitter = QSplitter(Qt.Vertical)
        right_preview_columns_splitter.addWidget(self.table_right_preview)
        right_preview_columns_splitter.addWidget(self.list_right_cols)
        right_preview_columns_splitter.setSizes([200, 100]) # Initial sizes
        right_layout.addWidget(right_preview_columns_splitter)
        right_group.setLayout(right_layout)
        top_splitter.addWidget(right_group)

        # 将 top_splitter 添加到新的 content_layout
        content_layout.addWidget(top_splitter, 2)

        # Middle layout for merge configuration
        merge_config_group = QGroupBox("合并配置")
        merge_config_layout = QHBoxLayout()

        # Left Key Selection
        left_key_group = QGroupBox("左侧合并键 (可多选)")
        left_key_layout_group = QVBoxLayout()
        self.list_left_merge_keys = QListWidget()
        self.list_left_merge_keys.setSelectionMode(QAbstractItemView.ExtendedSelection)
        left_key_layout_group.addWidget(self.list_left_merge_keys)
        left_key_group.setLayout(left_key_layout_group)
        merge_config_layout.addWidget(left_key_group)

        # Right Key Selection
        right_key_group = QGroupBox("右侧合并键 (可多选)")
        right_key_layout_group = QVBoxLayout()
        self.list_right_merge_keys = QListWidget()
        self.list_right_merge_keys.setSelectionMode(QAbstractItemView.ExtendedSelection)
        right_key_layout_group.addWidget(self.list_right_merge_keys)
        right_key_group.setLayout(right_key_layout_group)
        merge_config_layout.addWidget(right_key_group)

        # Merge Type Selection
        merge_type_layout = QVBoxLayout()
        merge_type_layout.addWidget(QLabel("合并类型:"))
        self.combo_merge_type = QComboBox()
        self.combo_merge_type.addItems(["Inner Join", "Left Join", "Right Join", "Outer Join"])
        self.combo_merge_type.setCurrentText("Left Join") # Default
        merge_type_layout.addWidget(self.combo_merge_type)
        merge_config_layout.addLayout(merge_type_layout)
        
        self.btn_perform_merge = QPushButton("执行合并")
        self.btn_perform_merge.clicked.connect(self.perform_merge)
        merge_config_layout.addWidget(self.btn_perform_merge, alignment=Qt.AlignBottom)

        merge_config_group.setLayout(merge_config_layout)
        # 将 merge_config_group 添加到新的 content_layout
        content_layout.addWidget(merge_config_group, 0)

        # Bottom layout for merged result preview
        merged_result_group = QGroupBox("合并结果")
        merged_result_layout = QVBoxLayout()
        self.table_merged_preview = QTableView()
        self.table_merged_preview.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_merged_preview.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive) # Allow resize
        self.btn_export_merged = QPushButton("导出合并结果")
        self.btn_export_merged.clicked.connect(self.export_merged_data)
        self.btn_export_merged.setEnabled(False)
        merged_result_layout.addWidget(self.table_merged_preview)
        merged_result_layout.addWidget(self.btn_export_merged)
        merged_result_group.setLayout(merged_result_layout)
        # 将 merged_result_group 添加到新的 content_layout
        content_layout.addWidget(merged_result_group, 2)

    @Slot(str)
    def load_data(self, side):
        file_path, _ = QFileDialog.getOpenFileName(self, f"加载{('左侧' if side == 'left' else '右侧')}数据集", "", "CSV 文件 (*.csv);;Excel 文件 (*.xlsx *.xls)")
        if not file_path:
            return

        try:
            encoding = None
            if file_path.lower().endswith('.csv'):
                encoding = self.detect_encoding(file_path)
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
            elif file_path.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                QMessageBox.warning(self, "文件类型错误", "仅支持 CSV 和 Excel 文件。")
                return

            if side == 'left':
                self.df_left = df
                self.lbl_left_file.setText(file_path)
                self.update_table_preview(self.table_left_preview, df)
                self.update_column_list(self.list_left_cols, df.columns)
                self.update_column_list(self.list_left_merge_keys, df.columns)
            else: # side == 'right'
                self.df_right = df
                self.lbl_right_file.setText(file_path)
                self.update_table_preview(self.table_right_preview, df)
                self.update_column_list(self.list_right_cols, df.columns)
                self.update_column_list(self.list_right_merge_keys, df.columns)
            
            # Select all columns by default
            if side == 'left':
                for i in range(self.list_left_cols.count()):
                    self.list_left_cols.item(i).setSelected(True)
            else:
                for i in range(self.list_right_cols.count()):
                    self.list_right_cols.item(i).setSelected(True)

        except Exception as e:
            QMessageBox.critical(self, "加载错误", f"加载文件失败: {e}")
            if side == 'left':
                self.df_left = None
                self.lbl_left_file.setText("加载失败")
                self.update_table_preview(self.table_left_preview, pd.DataFrame())
                self.update_column_list(self.list_left_cols, [])
                self.update_column_list(self.list_left_merge_keys, [])
            else:
                self.df_right = None
                self.lbl_right_file.setText("加载失败")
                self.update_table_preview(self.table_right_preview, pd.DataFrame())
                self.update_column_list(self.list_right_cols, [])
                self.update_column_list(self.list_right_merge_keys, [])

    def update_table_preview(self, table_view, df):
        if df is not None and not df.empty:
            preview_df = df.head(100) 
            model = PandasTableModel(preview_df)
            table_view.setModel(model)
        else:
            table_view.setModel(None)

    def update_column_list(self, list_widget, columns):
        list_widget.clear()
        list_widget.addItems(columns)

    @Slot()
    def perform_merge(self):
        if self.df_left is None or self.df_right is None:
            QMessageBox.warning(self, "数据缺失", "请先加载左右两侧的数据集。")
            return

        left_key_items = self.list_left_merge_keys.selectedItems()
        right_key_items = self.list_right_merge_keys.selectedItems()

        if not left_key_items or not right_key_items:
            QMessageBox.warning(self, "合并键缺失", "请为左右数据集选择至少一个合并键。")
            return

        left_keys = [item.text() for item in left_key_items]
        right_keys = [item.text() for item in right_key_items]

        if len(left_keys) != len(right_keys):
            QMessageBox.warning(self, "合并键数量不匹配", "左右两侧选择的合并键数量必须相同。")
            return

        selected_left_cols_items = self.list_left_cols.selectedItems()
        selected_right_cols_items = self.list_right_cols.selectedItems()

        if not selected_left_cols_items or not selected_right_cols_items:
            QMessageBox.warning(self, "列选择缺失", "请至少为每个数据集选择一个要保留的列。")
            return

        selected_left_cols = [item.text() for item in selected_left_cols_items]
        selected_right_cols = [item.text() for item in selected_right_cols_items]

        for key in left_keys:
            if key not in selected_left_cols:
                selected_left_cols.append(key)
        for key in right_keys:
            if key not in selected_right_cols:
                selected_right_cols.append(key)
        
        final_left_cols = list(dict.fromkeys(selected_left_cols))
        final_right_cols = list(dict.fromkeys(selected_right_cols))

        df_left_subset = self.df_left[final_left_cols]
        df_right_subset = self.df_right[final_right_cols]

        merge_type_map = {
            "Left Join": "left",
            "Right Join": "right",
            "Inner Join": "inner",
            "Outer Join": "outer"
        }
        how = merge_type_map.get(self.combo_merge_type.currentText(), "left")

        try:
            suffixes = ('_left', '_right')
            merged_df = pd.merge(df_left_subset, df_right_subset, 
                               left_on=left_keys, right_on=right_keys, 
                               how=how, suffixes=suffixes)
            
            self.merged_df_result = merged_df
            self.update_table_preview(self.table_merged_preview, merged_df)
            self.btn_export_merged.setEnabled(True)
            QMessageBox.information(self, "合并成功", f"数据合并完成，生成 {len(merged_df)} 条记录。")

        except Exception as e:
            QMessageBox.critical(self, "合并错误", f"数据合并失败: {e}")
            self.update_table_preview(self.table_merged_preview, pd.DataFrame())
            self.btn_export_merged.setEnabled(False)

    @Slot()
    def export_merged_data(self):
        if self.merged_df_result is None or self.merged_df_result.empty:
            QMessageBox.warning(self, "无数据", "没有可导出的合并数据。")
            return

        file_path, _ = QFileDialog.getSaveFileName(self, "导出合并结果", "", "CSV 文件 (*.csv);;Excel 文件 (*.xlsx)")
        if not file_path:
            return

        try:
            if file_path.lower().endswith('.csv'):
                self.merged_df_result.to_csv(file_path, index=False, encoding='utf-8-sig')
            elif file_path.lower().endswith('.xlsx'):
                self.merged_df_result.to_excel(file_path, index=False)
            else:
                QMessageBox.warning(self, "文件类型错误", "仅支持导出为 CSV 或 Excel 文件。")
                return
            QMessageBox.information(self, "导出成功", f"合并结果已成功导出到: {file_path}")
        except Exception as e:
            QMessageBox.critical(self, "导出错误", f"导出文件失败: {e}")

if __name__ == '__main__':
    from PySide6.QtWidgets import QApplication
    app = QApplication(sys.argv)
    main_win = DataMergeTab()
    main_win.show()
    sys.exit(app.exec())