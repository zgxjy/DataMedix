# --- START OF FILE tests/test_utils.py ---
import unittest
import sys
import os

# 确保 utils 模块可以被导入
# 获取当前测试文件所在的目录的父目录 (即项目根目录)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils import validate_column_name, sanitize_name_part # 假设这两个函数在 utils.py 中

class TestUtils(unittest.TestCase):

    def test_validate_column_name_valid(self):
        self.assertTrue(validate_column_name("valid_column")[0])
        self.assertTrue(validate_column_name("column_123")[0])
        self.assertTrue(validate_column_name("_valid_starts_underscore")[0])
        self.assertTrue(validate_column_name("c")[0]) # 单字符

    def test_validate_column_name_invalid_start(self):
        self.assertFalse(validate_column_name("123_invalid")[0])
        # self.assertFalse(validate_column_name("_")[0]) # 只有下划线，根据你的规则可能是无效的

    def test_validate_column_name_invalid_chars(self):
        self.assertFalse(validate_column_name("invalid-char")[0])
        self.assertFalse(validate_column_name("invalid space")[0])
        self.assertFalse(validate_column_name("invalid$")[0])

    def test_validate_column_name_keywords(self):
        self.assertFalse(validate_column_name("SELECT")[0])
        self.assertFalse(validate_column_name("table")[0]) # 大小写不敏感

    def test_validate_column_name_too_long(self):
        long_name = "a" * 64
        self.assertFalse(validate_column_name(long_name)[0])
        ok_name = "a" * 63
        self.assertTrue(validate_column_name(ok_name)[0])

    def test_sanitize_name_part_simple(self):
        self.assertEqual(sanitize_name_part("Hello World"), "hello_world")
        self.assertEqual(sanitize_name_part("  leading/trailing_spaces  "), "leading_trailing_spaces")
        self.assertEqual(sanitize_name_part("Test-123!@#$"), "test_123")
        self.assertEqual(sanitize_name_part("123_starts_digit"), "_123_starts_digit")
        self.assertEqual(sanitize_name_part("very_long_name_that_will_be_truncated_for_sure"), "very_long_name_that_will_") # 截断到25
        self.assertEqual(sanitize_name_part(None), "")
        self.assertEqual(sanitize_name_part(""), "")
        self.assertEqual(sanitize_name_part("SQL Keywords Like SELECT"), "sql_keywords_like_select")


if __name__ == '__main__':
    unittest.main()
# --- END OF FILE tests/test_utils.py ---