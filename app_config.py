# --- START OF FILE app_config.py ---

# 应用版本信息
APP_VERSION = "1.0.2" # 版本更新
APP_NAME = "通用医学数据提取与处理工具"

# 默认数据库连接参数 (用户可以在UI中覆盖)
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = "5432"
# 默认数据库名、用户等将由数据库画像提供

# 默认导出路径
DEFAULT_EXPORT_PATH = "USER_DESKTOP"

# SQL构建相关配置
SQL_PREVIEW_LIMIT = 100
SQL_BUILDER_DUMMY_DB_FOR_AS_STRING = "dbname=dummy user=dummy"

# UI相关的配置
DEFAULT_MAIN_WINDOW_WIDTH = 950
DEFAULT_MAIN_WINDOW_HEIGHT = 880
MIN_CONDITION_GROUP_SCROLL_HEIGHT = 150
MIN_PANEL_CONDITION_GROUP_SCROLL_HEIGHT = 200

# 日志配置
LOG_FILE_ENABLED = False
LOG_FILE_PATH = "app_log.log"
LOG_LEVEL = "INFO"

# --- 专项数据聚合方法定义 (通用部分) ---

# 用户在UI上看到的聚合方法及其内部使用的唯一键
AGGREGATION_METHODS_DISPLAY = [
    ("平均值 (Mean)", "MEAN"),
    ("中位数 (Median)", "MEDIAN"),
    ("最小值 (Min)", "MIN"),
    ("最大值 (Max)", "MAX"),
    ("第一次测量值 (First)", "FIRST_VALUE"),
    ("最后一次测量值 (Last)", "LAST_VALUE"),
    ("计数 (Count)", "COUNT"), # 计数值的个数
    ("总和 (Sum)", "SUM"),
    ("标准差 (StdDev)", "STDDEV_SAMP"),    # 样本标准差
    ("方差 (Variance)", "VAR_SAMP"),        # 样本方差
    ("变异系数 (CV)", "CV"),
    ("第25百分位数 (P25)", "P25"),
    ("第75百分位数 (P75)", "P75"),
    ("四分位距 (IQR)", "IQR"),
    ("值域 (Range)", "RANGE"),
    ("原始时间序列 (JSON)", "TIMESERIES_JSON"),
]

# 内部键对应的SQL聚合函数模板
# {val_col} 会被替换为实际的值列名
# {time_col} 会被替换为实际的时间列名
SQL_AGGREGATES = {
    "MEAN": "AVG({val_col})",
    "MEDIAN": "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {val_col})",
    "MIN": "MIN({val_col})",
    "MAX": "MAX({val_col})",
    "COUNT": "COUNT({val_col})",
    "SUM": "SUM({val_col})",
    "STDDEV_SAMP": "STDDEV_SAMP({val_col})",
    "VAR_SAMP": "VAR_SAMP({val_col})",
    "CV": "CASE WHEN AVG({val_col}) IS DISTINCT FROM 0 THEN STDDEV_SAMP({val_col}) / AVG({val_col}) ELSE NULL END",
    "FIRST_VALUE": "(ARRAY_AGG({val_col} ORDER BY {time_col} ASC NULLS LAST))[1]",
    "LAST_VALUE": "(ARRAY_AGG({val_col} ORDER BY {time_col} DESC NULLS LAST))[1]",
    "P25": "PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {val_col})",
    "P75": "PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {val_col})",
    "IQR": "(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {val_col})) - (PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {val_col}))",
    "RANGE": "MAX({val_col}) - MIN({val_col})",
    "TIMESERIES_JSON": "JSONB_AGG(JSONB_BUILD_OBJECT('time', {time_col}, 'value', {val_col}) ORDER BY {time_col} ASC NULLS LAST)",
}

# 内部键对应的SQL结果列类型
AGGREGATE_RESULT_TYPES = {
    "MEAN": "DOUBLE PRECISION",
    "MEDIAN": "DOUBLE PRECISION",
    "MIN": "NUMERIC",
    "MAX": "NUMERIC",
    "COUNT": "INTEGER",
    "SUM": "NUMERIC",
    "STDDEV_SAMP": "DOUBLE PRECISION",
    "VAR_SAMP": "DOUBLE PRECISION",
    "CV": "DOUBLE PRECISION",
    "FIRST_VALUE": "NUMERIC",
    "LAST_VALUE": "NUMERIC",
    "P25": "DOUBLE PRECISION",
    "P75": "DOUBLE PRECISION",
    "IQR": "DOUBLE PRECISION",
    "RANGE": "NUMERIC",
    "TIMESERIES_JSON": "JSONB",
}
# 注意: 文本类型列的结果类型 (MIN, MAX等) 应由 specific_sql_builder 根据面板配置动态处理。