# --- START OF FILE tests/test_sql_builder_special.py ---
import unittest
import sys
import os

# 确保 sql_logic 和 utils 模块可以被导入
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from sql_logic.sql_builder_special import build_special_data_sql
# 假设 utils.py 中的 validate_column_name 被 sql_builder_special 内部使用，不需要在这里直接测
# from utils import validate_column_name 

# 为了能让 pgsql.SQL().as_string() 工作，需要一个 dummy connection
import psycopg2
import psycopg2.sql as pgsql
from app_config import SQL_BUILDER_DUMMY_DB_FOR_AS_STRING # 从 app_config 获取

class TestSqlBuilderSpecial(unittest.TestCase):

    def setUp(self):
        # 创建一个临时的 dummy connection，仅用于 .as_string() 方法
        try:
            self.dummy_conn = psycopg2.connect(SQL_BUILDER_DUMMY_DB_FOR_AS_STRING)
        except psycopg2.Error as e:
            print(f"Warning: Could not create dummy psycopg2 connection for testing SQL string generation: {e}")
            print("SQL string outputs in tests might be less readable or fail.")
            self.dummy_conn = None


    def tearDown(self):
        if self.dummy_conn:
            self.dummy_conn.close()

    def test_build_chartevents_valuenum_first(self):
        panel_config = {
            "source_event_table": "mimiciv_icu.chartevents",
            "item_id_column_in_event_table": "itemid",
            "value_column_to_extract": "valuenum", # 提取数值
            "time_column_in_event_table": "charttime",
            "selected_item_ids": ["220045"], # 例如心率
            "aggregation_methods": {"first": True}, # 只提取首次
            "event_outputs": None,
            "time_window_text": "整个ICU期间",
            "cte_join_on_cohort_override": None 
        }
        
        target_table = "mimiciv_data.test_cohort"
        base_col_name = "hr"

        # 测试预览SQL
        preview_sql, err_msg, params, gen_cols = build_special_data_sql(
            target_table, base_col_name, panel_config, for_execution=False
        )
        
        self.assertIsNone(err_msg, f"Preview SQL generation failed: {err_msg}")
        self.assertIsNotNone(preview_sql)
        self.assertIsInstance(preview_sql, pgsql.Composed)
        self.assertEqual(len(params), 1) # 对应一个 itemid
        self.assertEqual(params[0], "220045")
        self.assertEqual(len(gen_cols), 1)
        self.assertEqual(gen_cols[0][0], "hr_first") # (列名, 类型字符串)
        self.assertEqual(gen_cols[0][1], "Numeric")

        if self.dummy_conn and preview_sql:
            sql_string = preview_sql.as_string(self.dummy_conn)
            # print("\nPreview SQL (test_build_chartevents_valuenum_first):\n", sql_string) # 打印出来检查
            self.assertIn("FilteredEvents AS", sql_string)
            self.assertIn("evt.valuenum AS event_value", sql_string)
            self.assertIn("evt.itemid = %s", sql_string) # 对应上面的 params
            self.assertIn("array_agg(event_value ORDER BY event_time ASC NULLS LAST)", sql_string)
            self.assertIn("AS hr_first", sql_string)
            self.assertIn("LEFT JOIN MergedDataCTE md ON cohort.hadm_id = md.hadm_id_cohort", sql_string) # 假设默认用hadm_id join

        # 测试执行步骤
        exec_steps, exec_type, exec_desc, exec_gen_cols = build_special_data_sql(
            target_table, base_col_name, panel_config, for_execution=True
        )
        self.assertEqual(exec_type, "execution_list")
        self.assertIsNotNone(exec_steps)
        self.assertEqual(len(exec_steps), 4) # ALTER, CREATE TEMP, UPDATE, DROP TEMP
        self.assertEqual(exec_desc, base_col_name)
        self.assertEqual(exec_gen_cols[0][0], "hr_first")

        if self.dummy_conn and exec_steps:
            alter_sql_str = exec_steps[0][0].as_string(self.dummy_conn)
            create_temp_sql_str = exec_steps[1][0].as_string(self.dummy_conn)
            # print("\nALTER SQL:", alter_sql_str)
            # print("\nCREATE TEMP SQL:", create_temp_sql_str)
            self.assertIn("ALTER TABLE mimiciv_data.test_cohort ADD COLUMN IF NOT EXISTS hr_first NUMERIC", alter_sql_str)
            self.assertIn("CREATE TEMPORARY TABLE temp_merge_data_hr_", create_temp_sql_str) # 部分匹配临时表名

    # 你可以为其他面板类型、不同的聚合方法、时间窗口、文本提取等添加更多的测试用例
    # def test_build_chartevents_value_last_text(self): ...
    # def test_build_medication_exists_prior(self): ...

if __name__ == '__main__':
    unittest.main()
# --- END OF FILE tests/test_sql_builder_special.py ---