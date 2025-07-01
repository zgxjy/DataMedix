# --- START OF PROPOSED MODIFICATION FOR conditiongroup.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, QComboBox, QLabel, QFrame, QGroupBox)
from PySide6.QtCore import Qt, Signal
import psycopg2 # 需要导入
from psycopg2 import sql as pgsql # 确保 pgsql 被正确导入和使用
import re # re is not used in this file from the provided snippet, but good to keep if other parts use it.

class ConditionGroupWidget(QWidget):
    condition_changed = Signal()

    def __init__(self, is_root=False, parent=None): # 移除 search_field 参数
        super().__init__(parent)
        self.is_root = is_root
        # self.search_field 不再是顶层属性，而是每行关键词自己的属性
        self._block_signals = False
        self._available_search_fields = [] # 存储 (db_col_name, display_name)
        self.init_ui()
        if self.is_root: # 确保根节点有一个默认的关键词行
             self.add_keyword()


    def init_ui(self):
        # ... (大部分 init_ui 保持不变，除了不再需要单一的 search_field 设置) ...
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5)
        main_layout.setSpacing(5)

        if not self.is_root:
            group_box = QGroupBox()
            # Set a border for non-root groups to make them visually distinct
            group_box.setStyleSheet("QGroupBox { border: 1px solid gray; margin-top: 0.5em; } QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 3px 0 3px; }")
            group_box.setTitle("子条件组")


            self.layout = QVBoxLayout(group_box)
            main_layout.addWidget(group_box)
        else:
            self.layout = main_layout

        logic_layout = QHBoxLayout()
        self.logic_combo = QComboBox()
        self.logic_combo.addItems(["AND", "OR"])
        self.logic_combo.currentTextChanged.connect(self._emit_condition_changed)
        logic_layout.addWidget(QLabel("组合方式:"))
        logic_layout.addWidget(self.logic_combo)
        logic_layout.addStretch()

        self.add_keyword_btn = QPushButton("添加关键词条件") # 文本稍作修改
        self.add_group_btn = QPushButton("添加子组条件")   # 文本稍作修改
        self.reset_btn = QPushButton("重置条件")
        self.reset_btn.clicked.connect(self.clear_all)
        logic_layout.addWidget(self.reset_btn)
        logic_layout.addWidget(self.add_keyword_btn)
        logic_layout.addWidget(self.add_group_btn)

        if not self.is_root:
            self.del_group_btn = QPushButton("删除本组")
            logic_layout.addWidget(self.del_group_btn)
            self.del_group_btn.clicked.connect(lambda: self.delete_self())
        self.layout.addLayout(logic_layout)

        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.HLine)
        separator.setFrameShadow(QFrame.Shadow.Sunken)
        self.layout.addWidget(separator)

        self.items_layout = QVBoxLayout() # 用于放置关键词行和子组
        self.layout.addLayout(self.items_layout)

        self.add_keyword_btn.clicked.connect(self.add_keyword)
        self.add_group_btn.clicked.connect(self.add_group)

        self.keywords = [] # 存储关键词行UI和数据的字典列表
        self.child_groups = [] # 存储子 ConditionGroupWidget 实例


    def set_available_search_fields(self, fields: list[tuple[str, str]]):
            self._available_search_fields = fields
            for kw_data in self.keywords: # 确保迭代所有已存在的关键词行
                field_combo = kw_data.get("field_combo")
                if field_combo:
                    current_field_db_name = field_combo.currentData()
                    
                    field_combo.blockSignals(True)
                    field_combo.clear()
                    
                    if self._available_search_fields: # 如果有可用字段
                        field_combo.setEnabled(True) # <--- 确保启用
                        for db_col, display_name in self._available_search_fields:
                            field_combo.addItem(display_name, db_col)
                        
                        idx_to_select = -1
                        if current_field_db_name:
                            for i in range(field_combo.count()):
                                if field_combo.itemData(i) == current_field_db_name:
                                    idx_to_select = i
                                    break
                        if idx_to_select != -1:
                            field_combo.setCurrentIndex(idx_to_select)
                        elif field_combo.count() > 0:
                            field_combo.setCurrentIndex(0)
                    else: # 如果没有可用字段
                        field_combo.addItem("无可用字段", None)
                        field_combo.setEnabled(False) # <--- 确保禁用
                    
                    field_combo.blockSignals(False)
                    # 手动触发一次信号，确保依赖它的逻辑（如按钮状态更新）被调用
                    # 特别是对于第一个关键词行，它的初始状态可能依赖这个
                    if field_combo.count() > 0:
                        field_combo.currentTextChanged.emit(field_combo.currentText())


            for child_group in self.child_groups:
                child_group.set_available_search_fields(fields)
            
            # 即使没有关键词行，字段列表变化也应触发 condition_changed
            # 以便主面板可以更新按钮状态等
            self._emit_condition_changed()


    def add_keyword(self, field_db_name=None, keyword_type="包含", keyword_text=""): # 新增 field_db_name 参数
        kw_widget = QWidget()
        kw_layout = QHBoxLayout(kw_widget)
        kw_layout.setContentsMargins(0, 0, 0, 0)

        # 新增：字段选择器 QComboBox
        field_combo = QComboBox()
        current_selection_restored = False
        if self._available_search_fields:
            for db_col, display_name in self._available_search_fields:
                field_combo.addItem(display_name, db_col)
            if field_db_name: # 如果是加载状态，尝试选中指定的字段
                for i in range(field_combo.count()):
                    if field_combo.itemData(i) == field_db_name:
                        field_combo.setCurrentIndex(i)
                        current_selection_restored = True
                        break
            if not current_selection_restored and field_combo.count() > 0:
                field_combo.setCurrentIndex(0) # 默认选中第一个可用字段
        else:
            field_combo.addItem("无可用字段", None) # 占位符
            field_combo.setEnabled(False)
        field_combo.currentTextChanged.connect(self._emit_condition_changed)


        kw_type_combo = QComboBox()
        kw_type_combo.addItems(["包含", "排除", "等于", "不等于", "大于", "小于", "大于等于", "小于等于"]) # 扩展操作符
        kw_type_combo.setCurrentText(keyword_type)
        kw_type_combo.currentTextChanged.connect(self._emit_condition_changed)

        kw_input = QLineEdit()
        kw_input.setPlaceholderText("输入关键词/值...")
        kw_input.setText(keyword_text)
        kw_input.textChanged.connect(self._emit_condition_changed)
        
        del_btn = QPushButton("删除")

        kw_layout.addWidget(QLabel("字段:"))
        kw_layout.addWidget(field_combo)
        kw_layout.addWidget(QLabel("类型:"))
        kw_layout.addWidget(kw_type_combo)
        kw_layout.addWidget(QLabel("值:")) # "关键词" 改为 "值" 更通用
        kw_layout.addWidget(kw_input)
        kw_layout.addWidget(del_btn)
        
        self.items_layout.addWidget(kw_widget)
        keyword_data = {
            "widget": kw_widget, 
            "field_combo": field_combo, # 新增
            "type_combo": kw_type_combo, 
            "input": kw_input, 
            "layout": kw_layout
        }
        self.keywords.append(keyword_data)
        del_btn.clicked.connect(lambda: self.remove_keyword(keyword_data))
        self._emit_condition_changed()
        return keyword_data

    def remove_keyword(self, keyword_data: dict):
        if keyword_data in self.keywords:
            if keyword_data["widget"] is not None:
                keyword_data["widget"].deleteLater()
            self.keywords.remove(keyword_data)
            self._emit_condition_changed()
    
    # This method is not used by the current ConditionGroupWidget logic
    # as search_field is now per-keyword. Keeping it commented for now.
    # def set_search_field(self, field_name):
    #     self._block_signals = True
    #     # self.search_field = field_name # Not used
    #     for group in self.child_groups:
    #         group.set_search_field(field_name) # This would also be problematic
    #     self._block_signals = False
    #     self._emit_condition_changed()

    def add_group(self, group_data=None): # group_data 是用于加载状态的
        group = ConditionGroupWidget(is_root=False, parent=self)
        group.set_available_search_fields(self._available_search_fields)
        group.condition_changed.connect(self._emit_condition_changed)
        self.child_groups.append(group)
        self.items_layout.addWidget(group)
        if group_data:
            group.set_state(group_data) 
        self._emit_condition_changed()
        return group

    def remove_child_group(self, group: 'ConditionGroupWidget'):
         if group in self.child_groups:
            self.child_groups.remove(group)
            self._emit_condition_changed()

    def delete_self(self):
        parent_group = self.parent()
        while parent_group is not None and not isinstance(parent_group, ConditionGroupWidget):
            parent_group = parent_group.parent()
        if isinstance(parent_group, ConditionGroupWidget):
             parent_group.remove_child_group(self)
        self.deleteLater()

    def _emit_condition_changed(self):
        if not self._block_signals:
             self.condition_changed.emit()

    def _build_sql_string_fallback(self, sql_object) -> str:
        """
        Internal helper to convert psycopg2.sql objects to string without a db context.
        This is a simplified version and might not handle all edge cases (e.g., complex Literals).
        """
        parts = []
        def to_list_recursive(obj_to_convert, out_list):
            if isinstance(obj_to_convert, pgsql.Composed):
                for sub_item_in_composed in obj_to_convert:
                    to_list_recursive(sub_item_in_composed, out_list)
            elif isinstance(obj_to_convert, pgsql.SQL):
                out_list.append(obj_to_convert.string)
            elif isinstance(obj_to_convert, pgsql.Identifier):
                temp_quoted_list_for_identifier = []
                for s_part_from_identifier in obj_to_convert.strings:
                    escaped_s_part = s_part_from_identifier.replace('"', '""')
                    temp_quoted_list_for_identifier.append(f'"{escaped_s_part}"')
                out_list.append(".".join(temp_quoted_list_for_identifier))
            elif isinstance(obj_to_convert, pgsql.Literal):
                try:
                    out_list.append(str(obj_to_convert))
                except Exception:
                    out_list.append(f"'LITERAL_CONVERSION_ERROR:{obj_to_convert!r}'")
            elif isinstance(obj_to_convert, str):
                 out_list.append(obj_to_convert)
            else: # Unknown type, use its string representation
                out_list.append(str(obj_to_convert))
        
        to_list_recursive(sql_object, parts)
        return "".join(parts)


    def get_condition(self):
        cond_parts = []
        params = []
        
        for kw_data in self.keywords:
            search_field_for_kw = kw_data["field_combo"].currentData() 
            kw_text = kw_data["input"].text().strip()
            kw_operator_text = kw_data["type_combo"].currentText()

            if search_field_for_kw and kw_text: 
                field_ident = pgsql.Identifier(search_field_for_kw)
                
                is_numeric_target_col = "id" in search_field_for_kw.lower() or \
                                        "version" in search_field_for_kw.lower() or \
                                        "count" in search_field_for_kw.lower() or \
                                        "age" in search_field_for_kw.lower() or \
                                        "num" in search_field_for_kw.lower()

                sql_part = None
                param_val = None

                try:
                    if kw_operator_text == "包含":
                        sql_part = pgsql.SQL("CAST({fld} AS TEXT) ILIKE %s").format(fld=field_ident)
                        param_val = f"%{kw_text}%"
                    elif kw_operator_text == "排除":
                        sql_part = pgsql.SQL("CAST({fld} AS TEXT) NOT ILIKE %s").format(fld=field_ident)
                        param_val = f"%{kw_text}%"
                    elif kw_operator_text in ["等于", "不等于", "大于", "小于", "大于等于", "小于等于"]:
                        op_map = {"等于": "=", "不等于": "!=", "大于": ">", "小于": "<", "大于等于": ">=", "小于等于": "<="}
                        sql_op_py_str = op_map[kw_operator_text] # Python string like "="
                        
                        can_be_numeric_val = False
                        try:
                            float_val_from_text = float(kw_text)
                            can_be_numeric_val = True
                        except ValueError:
                            pass

                        if is_numeric_target_col and can_be_numeric_val:
                            param_val = float_val_from_text
                            # Use named placeholders for clarity
                            sql_part = pgsql.SQL("{identifier} {operator} %s").format(
                                identifier=field_ident, 
                                operator=pgsql.SQL(sql_op_py_str)
                            )
                        else: 
                            param_val = kw_text
                            sql_part = pgsql.SQL("CAST({identifier} AS TEXT) {operator} %s").format(
                                identifier=field_ident,
                                operator=pgsql.SQL(sql_op_py_str)
                            )
                    
                    if sql_part and param_val is not None:
                        cond_parts.append(sql_part)
                        params.append(param_val)
                except Exception as e:
                    print(f"Error processing keyword condition ({search_field_for_kw} {kw_operator_text} {kw_text}): {e}")
                    continue 

        for group in self.child_groups:
            group_sql_template, group_params = group.get_condition()
            if group_sql_template:
                cond_parts.append(pgsql.SQL("({})").format(pgsql.SQL(group_sql_template)))
                params.extend(group_params)

        if not cond_parts:
            return "", []

        logic_operator_sql = pgsql.SQL(f" {self.logic_combo.currentText()} ")
        
        full_sql_composed = None
        if len(cond_parts) == 1:
            full_sql_composed = cond_parts[0]
        else:
            composed_parts_with_ops = []
            for i, part in enumerate(cond_parts):
                composed_parts_with_ops.append(part)
                if i < len(cond_parts) - 1:
                    composed_parts_with_ops.append(logic_operator_sql)
            full_sql_composed = pgsql.Composed(composed_parts_with_ops)
        
        dummy_conn = None
        try:
            dsn_ = ""
            try:
                dsn_ = psycopg2.connect("").dsn
                parsed_dsn = psycopg2.extensions.parse_dsn(dsn_)
                user = parsed_dsn.get('user')
                dbname = parsed_dsn.get('dbname','postgres') 
                dummy_conn_string = f"dbname='{dbname}' user='{user if user else ''}'" 
            except Exception: 
                dummy_conn_string = "dbname='postgres'" 

            dummy_conn = psycopg2.connect(dummy_conn_string)
            return full_sql_composed.as_string(dummy_conn), params
        except psycopg2.Error:
            try:
                sql_string = self._build_sql_string_fallback(full_sql_composed)
                return sql_string, params
            except Exception as e_fallback:
                print(f"Error in fallback SQL generation: {e_fallback}")
                return f"-- Fallback SQL Generation Error: {e_fallback} --", params
        finally:
            if dummy_conn:
                dummy_conn.close()
                
    def has_valid_input(self): 
        for kw_data in self.keywords:
            if kw_data["field_combo"].currentData() and kw_data["input"].text().strip():
                return True
        for group in self.child_groups:
            if group.has_valid_input():
                return True
        return False

    def get_state(self) -> dict:
        state = {
            "logic": self.logic_combo.currentText(),
            "keywords": [],
            "child_groups": []
        }
        for kw_data in self.keywords:
            state["keywords"].append({
                "field_db_name": kw_data["field_combo"].currentData(), 
                "type": kw_data["type_combo"].currentText(),
                "text": kw_data["input"].text()
            })
        for child_group in self.child_groups:
            state["child_groups"].append(child_group.get_state())
        return state

    def set_state(self, state: dict, available_fields_for_state: list[tuple[str,str]] = None):
        self._block_signals = True
        if available_fields_for_state is not None:
            self.set_available_search_fields(available_fields_for_state)
        
        try:
            self.logic_combo.setCurrentText(state.get("logic", "AND"))

            for kw_data in reversed(self.keywords): self.remove_keyword(kw_data)
            for child_group in reversed(self.child_groups): child_group.delete_self()
            self.keywords.clear(); self.child_groups.clear()

            for kw_state in state.get("keywords", []):
                self.add_keyword(
                    field_db_name=kw_state.get("field_db_name"),
                    keyword_type=kw_state.get("type", "包含"),
                    keyword_text=kw_state.get("text", "")
                )
            
            for child_group_state in state.get("child_groups", []):
                new_child = self.add_group() 
                new_child.set_state(child_group_state) 

        except Exception as e:
            print(f"Error setting ConditionGroupWidget state: {e}")
        finally:
            self._block_signals = False
        self._emit_condition_changed()

    def clear_all(self):
        self._block_signals = True
        try:
            for kw_data in reversed(self.keywords): self.remove_keyword(kw_data)
            for child_group in reversed(self.child_groups): child_group.delete_self()
            self.keywords.clear(); self.child_groups.clear()
            if self.is_root: self.add_keyword()
            self.logic_combo.setCurrentIndex(0)
        finally:
            self._block_signals = False
        self._emit_condition_changed()

    def clear_all(self):
        """
        递归地清除所有关键词行、子组，并将根节点重置为包含一个空行。
        """
        self._block_signals = True  # 阻止在清除过程中触发大量信号

        # 从后往前遍历并删除，避免索引问题
        for kw_data in reversed(self.keywords):
            self.remove_keyword(kw_data)
        
        for child_group in reversed(self.child_groups):
            child_group.delete_self() # delete_self 会处理从父节点移除和删除自身

        # 清空列表以防万一
        self.keywords.clear()
        self.child_groups.clear()

        # 如果是根节点，我们希望它保留一个空的关键词行
        if self.is_root:
            self.add_keyword()
        
        # 重置逻辑操作符
        self.logic_combo.setCurrentIndex(0) # 默认为 AND

        self._block_signals = False
        self._emit_condition_changed() # 最后触发一次信号，通知外部UI更新

# --- END OF PROPOSED MODIFICATION FOR conditiongroup.py ---