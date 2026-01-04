import psycopg2
import psycopg2.sql as psql
import time
import traceback
from abc import ABC, abstractmethod
from typing import List, Tuple, Dict, Any, Optional

from utils import validate_column_name
from app_config import SQL_AGGREGATES as GENERIC_SQL_AGGREGATES
from app_config import AGGREGATE_RESULT_TYPES as GENERIC_AGGREGATE_RESULT_TYPES
from db_profiles.base_profile import BaseDbProfile

# --- 配置常量 ---
SQL_AGGREGATES = {
    **GENERIC_SQL_AGGREGATES,
    "NOTE_CONCAT": "STRING_AGG({val_col}, E'\\n\\n---NOTE---\\n\\n' ORDER BY {time_col})",
    "NOTE_FIRST": "(ARRAY_AGG({val_col} ORDER BY {time_col} ASC NULLS LAST))[1]",
    "NOTE_LAST": "(ARRAY_AGG({val_col} ORDER BY {time_col} DESC NULLS LAST))[1]",
    "NOTE_COUNT": "COUNT({val_col})",
}

AGGREGATE_RESULT_TYPES = {
    **GENERIC_AGGREGATE_RESULT_TYPES,
    "NOTE_CONCAT": "TEXT",
    "NOTE_FIRST": "TEXT",
    "NOTE_LAST": "TEXT",
    "NOTE_COUNT": "INTEGER",
}

# ==========================================
# 1. 定义策略接口 (Strategy Interface)
# ==========================================

class BaseSqlBuilderStrategy(ABC):
    """SQL构建策略基类，定义不同数据库必须实现的差异化逻辑"""

    def __init__(self, event_alias: psql.Identifier, cohort_alias: psql.Identifier):
        self.evt = event_alias
        self.coh = cohort_alias

    @abstractmethod
    def get_time_window_condition(self, time_col_name: str, window_text: str, is_date_col: bool) -> psql.SQL:
        """生成时间窗口的 WHERE 子句"""
        pass

    @abstractmethod
    def get_value_expression(self, val_col_name: str, table_name: str, is_text_mode: bool) -> psql.SQL:
        """获取数值列的表达式（处理类型转换）"""
        pass

    @abstractmethod
    def get_med_json_columns(self) -> List[psql.SQL]:
        """获取用于生成用药 JSON 的特定列定义 (SELECT 部分)"""
        pass

# ==========================================
# 2. 具体策略实现 (Concrete Strategies)
# ==========================================

class MimicIVStrategy(BaseSqlBuilderStrategy):
    """MIMIC-IV 策略：基于绝对时间戳 (Timestamp)"""

    def get_time_window_condition(self, time_col_name: str, window_text: str, is_date_col: bool) -> psql.SQL:
        time_col = psql.Identifier(time_col_name)
        cohort_admittime = psql.SQL("{}.admittime").format(self.coh)
        cohort_dischtime = psql.SQL("{}.dischtime").format(self.coh)
        cohort_icu_intime = psql.SQL("{}.icu_intime").format(self.coh)
        cohort_icu_outtime = psql.SQL("{}.icu_outtime").format(self.coh)

        # 处理 chartdate (仅日期) 的情况
        start_hosp = psql.SQL("CAST({} AS DATE)").format(cohort_admittime) if is_date_col else cohort_admittime
        end_hosp = psql.SQL("CAST({} AS DATE)").format(cohort_dischtime) if is_date_col else cohort_dischtime

        # 1. 处理 "住院24小时内" (基于 Hospital Admission Time)
        if "住院24小时" in window_text:
            return psql.SQL("{evt}.{time} BETWEEN {start} AND ({start} + interval '24 hours')").format(
                evt=self.evt, time=time_col, start=cohort_admittime)       # 逻辑：事件时间 在 [入院时间, 入院时间 + 24小时] 之间            
        # 2. 处理 "住院48小时内"
        elif "住院48小时" in window_text:
            return psql.SQL("{evt}.{time} BETWEEN {start} AND ({start} + interval '48 hours')").format(
                evt=self.evt, time=time_col, start=cohort_admittime)
        elif "ICU24小时" in window_text:
            return psql.SQL("{evt}.{time} BETWEEN {start} AND ({start} + interval '24 hours')").format(
                evt=self.evt, time=time_col, start=cohort_icu_intime)
        elif "ICU48小时" in window_text:
            return psql.SQL("{evt}.{time} BETWEEN {start} AND ({start} + interval '48 hours')").format(
                evt=self.evt, time=time_col, start=cohort_icu_intime)
        elif "整个ICU期间" in window_text:
            return psql.SQL("{evt}.{time} BETWEEN {start} AND {end}").format(
                evt=self.evt, time=time_col, start=cohort_icu_intime, end=cohort_icu_outtime)
        elif "整个住院期间" in window_text:
            return psql.SQL("{evt}.{time} BETWEEN {start} AND {end}").format(
                evt=self.evt, time=time_col, start=start_hosp, end=end_hosp)
        elif "既往史" in window_text:
            # 既往史通常指入院前，这里简单处理为小于入院时间
            return psql.SQL("{evt}.{time} < {start}").format(
                evt=self.evt, time=time_col, start=start_hosp)
        
        return psql.SQL("TRUE") # 默认不过滤

    def get_value_expression(self, val_col_name: str, table_name: str, is_text_mode: bool) -> psql.SQL:
        # MIMIC 通常不需要特殊转换，直接返回列名
        return psql.SQL("{}.{}").format(self.evt, psql.Identifier(val_col_name))

    def get_med_json_columns(self) -> List[psql.SQL]:
        # MIMIC 标准列名
        return [
            psql.SQL("{}.stoptime").format(self.evt),
            psql.SQL("{}.dose_unit_rx").format(self.evt),
            psql.SQL("{}.form_unit_disp").format(self.evt)
        ]


class EicuStrategy(BaseSqlBuilderStrategy):
    """e-ICU 策略：基于相对偏移量 (Offset, 分钟)"""

    def get_time_window_condition(self, time_col_name: str, window_text: str, is_date_col: bool) -> psql.SQL:
        time_col = psql.Identifier(time_col_name)
        # e-ICU 的 offset 0 通常代表 ICU 入室时间
        
        if "24小时" in window_text:
            return psql.SQL("{evt}.{time} BETWEEN 0 AND 1440").format(evt=self.evt, time=time_col)
        elif "48小时" in window_text:
            return psql.SQL("{evt}.{time} BETWEEN 0 AND 2880").format(evt=self.evt, time=time_col)
        elif "整个ICU期间" in window_text:
            # 0 到 unitdischargeoffset
            return psql.SQL("{evt}.{time} >= 0 AND {evt}.{time} <= {coh}.los_icu_minutes").format(
                evt=self.evt, time=time_col, coh=self.coh)
        
        # e-ICU 暂不支持“整个住院期间”作为精确 Offset 筛选，除非有 hospitaladmitoffset
        # 此处回退到宽泛策略或仅 ICU
        return psql.SQL("{evt}.{time} >= 0").format(evt=self.evt, time=time_col)

    def get_value_expression(self, val_col_name: str, table_name: str, is_text_mode: bool) -> psql.SQL:
        col_ident = psql.Identifier(val_col_name)
        
        # 特殊处理：nursecharting 和 infusiondrug 的数值列实际上是存储在文本字段里的
        if (table_name in ["public.nursecharting", "public.infusiondrug"]) and (not is_text_mode):
            # 需要将文本转为数值，处理空字符串
            return psql.SQL("CAST(NULLIF({evt}.{col}, '') AS NUMERIC)").format(evt=self.evt, col=col_ident)
        
        return psql.SQL("{evt}.{col}").format(evt=self.evt, col=col_ident)

    def get_med_json_columns(self) -> List[psql.SQL]:
        # e-ICU 列映射逻辑
        return [
            psql.SQL("{}.drugstopoffset AS stoptime").format(self.evt),  # 映射停止时间
            psql.SQL("NULL AS dose_unit_rx"),                            # e-ICU 无独立单位列
            psql.SQL("{}.routeadmin AS form_unit_disp").format(self.evt) # 映射给药途径
        ]

# ==========================================
# 3. 工厂函数 (Factory)
# ==========================================

def get_sql_strategy(db_profile: BaseDbProfile, evt_alias: psql.Identifier, coh_alias: psql.Identifier) -> BaseSqlBuilderStrategy:
    display_name = db_profile.get_display_name().lower()
    if "eicu" in display_name:
        return EicuStrategy(evt_alias, coh_alias)
    else:
        return MimicIVStrategy(evt_alias, coh_alias)

# ==========================================
# 4. 主构建函数 (Refactored Main Function)
# ==========================================

def build_special_data_sql(
    target_cohort_table_name: str,
    base_new_column_name: str,
    panel_specific_config: Dict[str, Any],
    db_profile: BaseDbProfile,
    active_db_params: Optional[Dict] = None,
    for_execution: bool = False,
    preview_limit: int = 100
) -> Tuple[Optional[Any], Optional[str], Optional[List[Any]], List[Tuple[str, str]]]:
    
    # 处理特殊的“预处理表合并”模式 (保持原有逻辑)
    if panel_specific_config.get("panel_type") == "merge_preprocessed":
        return build_merge_preprocessed_sql(
            target_cohort_table_name, panel_specific_config, db_profile, active_db_params, for_execution, preview_limit
        )

    generated_column_details_for_preview = [] 

    # --- 1. 参数提取 ---
    source_event_table = panel_specific_config.get("source_event_table")
    id_col_in_event_table = panel_specific_config.get("item_id_column_in_event_table")
    value_column_name = panel_specific_config.get("value_column_to_extract") 
    time_col_name = panel_specific_config.get("time_column_in_event_table")
    time_col_is_date = panel_specific_config.get("time_column_is_date_only", False)
    
    selected_item_ids = panel_specific_config.get("selected_item_ids", [])
    aggregation_methods = panel_specific_config.get("aggregation_methods", {})
    event_outputs = panel_specific_config.get("event_outputs", {})
    quick_extractors = panel_specific_config.get("quick_extractors", {})
    
    time_window_text = panel_specific_config.get("time_window_text")
    is_text_extraction = panel_specific_config.get("is_text_extraction", False)
    
    # 高级过滤器
    text_filter = panel_specific_config.get("text_filter")
    detail_table = panel_specific_config.get("detail_table")
    detail_filters = panel_specific_config.get("detail_filters", [])
    item_filter_conditions = panel_specific_config.get("item_filter_conditions", None) # (sql, params)

    # --- 2. 基础校验 ---
    if not all([source_event_table, time_window_text]):
        return None, "配置不完整 (缺少源表或时间窗口)", [], []
    
    try:
        schema_name, table_only_name = target_cohort_table_name.split('.')
    except ValueError:
        return None, "目标表名格式错误 (Schema.Table)", [], []

    # --- 3. 初始化对象 ---
    target_table_ident = psql.Identifier(schema_name, table_only_name)
    cohort_alias = psql.Identifier("cohort")
    event_alias = psql.Identifier("evt")
    md_alias = psql.Identifier("md")
    target_alias = psql.Identifier("target")
    
    # 获取策略对象
    strategy = get_sql_strategy(db_profile, event_alias, cohort_alias)

    params_for_cte = []
    all_where_conditions = []

    # --- 4. 构建 WHERE 子句 (通用逻辑) ---
    
    # 4.1 文本过滤
    if text_filter:
        all_where_conditions.append(psql.SQL("evt.text ILIKE %s"))
        params_for_cte.append(f"%{text_filter}%")

    # 4.2 项目 ID 过滤
    if id_col_in_event_table and selected_item_ids:
        col_ident = psql.Identifier(id_col_in_event_table)
        safe_ids = [str(i) for i in selected_item_ids]
        
        # 检查是否使用通配符
        if any('%' in s for s in safe_ids):
            ilike_parts = [psql.SQL("TRIM(CAST({}.{} AS TEXT)) ILIKE %s").format(event_alias, col_ident) for _ in safe_ids]
            all_where_conditions.append(psql.SQL("({})").format(psql.SQL(" OR ").join(ilike_parts)))
            params_for_cte.extend(safe_ids)
        else:
            trimmed_expr = psql.SQL("TRIM(CAST({}.{} AS TEXT))").format(event_alias, col_ident)
            if len(safe_ids) == 1:
                all_where_conditions.append(psql.SQL("{} = %s").format(trimmed_expr))
                params_for_cte.append(safe_ids[0])
            else:
                all_where_conditions.append(psql.SQL("{} IN %s").format(trimmed_expr))
                params_for_cte.append(tuple(safe_ids))
    
    # 4.3 自定义高级过滤 (如从 condition_group 传来的)
    if item_filter_conditions:
        custom_sql_str, custom_params = item_filter_conditions
        if custom_sql_str:
            # 注意：这里假设 custom_sql 中的字段是未限定的，或者已经正确限定。
            # 在复杂场景下可能需要更细致的处理，这里简单追加
            all_where_conditions.append(psql.SQL(custom_sql_str))
            params_for_cte.extend(custom_params)

    # 4.4 关联详情表过滤 (如 note 关联 note_detail)
    if detail_table and detail_filters:
        detail_clauses = []
        for field, op, val in detail_filters:
            detail_clauses.append(psql.SQL("{} {} %s").format(psql.Identifier(field), psql.SQL(op)))
            params_for_cte.append(val)
        
        subquery = psql.SQL("(SELECT note_id FROM {} WHERE {})").format(
            psql.SQL(detail_table), psql.SQL(" AND ").join(detail_clauses)
        )
        all_where_conditions.append(psql.SQL("{}.note_id IN {}").format(event_alias, subquery))

    # 4.5 时间窗口过滤 (使用策略)
    if time_col_name:
        time_condition = strategy.get_time_window_condition(time_col_name, time_window_text, time_col_is_date)
        all_where_conditions.append(time_condition)

    # --- 5. 构建 FilteredEvents CTE ---
    cohort_join_key = db_profile.get_cohort_join_key(source_event_table)
    event_join_key = db_profile.get_event_table_join_key(source_event_table)
    
    # 允许面板覆盖默认 JOIN 逻辑 (例如对于“既往史”需要关联 admission 表)
    override_join = panel_specific_config.get("cte_join_on_cohort_override")
    
    if override_join:
        # 如果是 SQL 对象则直接使用，如果是字符串则转换
        from_join_clause = override_join if isinstance(override_join, psql.Composed) or isinstance(override_join, psql.SQL) else psql.SQL(str(override_join))
        # 格式化参数
        from_join_clause = from_join_clause.format(
            event_table=psql.SQL(source_event_table), evt_alias=event_alias,
            cohort_table=target_table_ident, coh_alias=cohort_alias,
            adm_evt=psql.Identifier("adm_evt") # 预留给 admission join 的别名
        )
    else:
        from_join_clause = psql.SQL("FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.{evt_key} = {coh_alias}.{coh_key}").format(
            event_table=psql.SQL(source_event_table), evt_alias=event_alias,
            cohort_table=target_table_ident, coh_alias=cohort_alias,
            evt_key=psql.Identifier(event_join_key), coh_key=psql.Identifier(cohort_join_key)
        )

    # 构建 SELECT 列表
    select_defs = [psql.SQL("{}.*").format(cohort_alias)] # 保留所有队列列
    
    # 添加值列 (使用 event_value 别名)
    if value_column_name:
        # 使用策略获取原始值表达式 (不转换类型，原始值)
        # 注意：我们在聚合阶段再做类型转换 (get_value_expression)，这里为了保持 CTE 通用性，可以先取原始列
        # 或者，为了简化，我们直接在这里取原始列，别名为 event_value
        select_defs.append(psql.SQL("{}.{} AS event_value").format(event_alias, psql.Identifier(value_column_name)))
    
    # 添加时间列 (使用 event_time 别名)
    if time_col_name:
        select_defs.append(psql.SQL("{}.{} AS event_time").format(event_alias, psql.Identifier(time_col_name)))

    # 添加 JSON 所需列 (使用策略)
    if any(m == "MED_TIMESERIES_JSON" for m, s in aggregation_methods.items() if s):
        json_cols = strategy.get_med_json_columns()
        select_defs.extend(json_cols)

    # 组装 CTE SQL
    filtered_events_cte_sql = psql.SQL("FilteredEvents AS (SELECT {selects} {joins} WHERE {conds})").format(
        selects=psql.SQL(', ').join(select_defs),
        joins=from_join_clause,
        conds=psql.SQL(' AND ').join(all_where_conditions) if all_where_conditions else psql.SQL("TRUE")
    )

    # --- 6. 确定聚合列 ---
    selected_methods = []
    type_map = {"NUMERIC": "Numeric", "INTEGER": "Integer", "BOOLEAN": "Boolean", "TEXT": "Text", "JSONB": "JSON"}

    # 处理常规聚合
    for method_key, is_selected in aggregation_methods.items():
        if not is_selected: continue
        template = SQL_AGGREGATES.get(method_key)
        if not template: continue
        
        col_type = AGGREGATE_RESULT_TYPES.get(method_key, "NUMERIC")
        # 文本提取的特殊处理
        if is_text_extraction and method_key in ["MIN", "MAX", "FIRST_VALUE", "LAST_VALUE"]:
            col_type = "TEXT"
            
        final_col_name = f"{base_new_column_name}_{method_key.lower()}"
        is_valid, err = validate_column_name(final_col_name)
        if not is_valid: return None, f"列名 '{final_col_name}' 无效: {err}", [], []
        
        selected_methods.append((final_col_name, psql.Identifier(final_col_name), template, psql.SQL(col_type)))
        generated_column_details_for_preview.append((final_col_name, type_map.get(col_type, col_type)))

    # 处理事件输出 (Exists/Count)
    event_configs = {"exists": ("TRUE", "BOOLEAN"), "countevt": ("COUNT(*)", "INTEGER")}
    for key, is_selected in event_outputs.items():
        if is_selected and key in event_configs:
            tmpl, ctype = event_configs[key]
            final_col_name = f"{base_new_column_name}_{key}"
            selected_methods.append((final_col_name, psql.Identifier(final_col_name), tmpl, psql.SQL(ctype)))
            generated_column_details_for_preview.append((final_col_name, type_map.get(ctype, ctype)))

    # 处理正则提取
    for key, pattern in quick_extractors.items():
        final_col_name = f"{base_new_column_name}_{key}"
        tmpl = "(REGEXP_MATCHES({val_col}, %s, 'i'))" # Placeholder for param
        selected_methods.append((final_col_name, psql.Identifier(final_col_name), (tmpl, [pattern]), psql.SQL("TEXT")))
        generated_column_details_for_preview.append((final_col_name, "Text"))

    if not selected_methods:
        return None, "未选择任何有效的提取列", [], []

    # --- 7. 构建聚合查询 ---
    group_by_key = psql.Identifier(cohort_join_key)
    agg_select_list = []
    extra_params = []

    for _, col_ident, template_obj, _ in selected_methods:
        # 构建取值表达式 (使用策略处理类型转换)
        # 注意：我们在 CTE 中已经将值列别名为 event_value
        # 策略的 get_value_expression 需要知道这一点
        # 为了简化，我们直接在 SQL 模板中替换 {val_col}
        
        val_expr = psql.Identifier('event_value')
        
        # 如果是 e-ICU 且非文本模式，需要转换
        if "eicu" in db_profile.get_display_name().lower() and not is_text_extraction and value_column_name and source_event_table in ["public.nursecharting", "public.infusiondrug"]:
             val_expr = psql.SQL("CAST(NULLIF(event_value, '') AS NUMERIC)")
        
        time_expr = psql.Identifier('event_time')

        # 处理 JSON 特殊情况
        if "JSON" in str(template_obj): # 简单判断，或者是检查 method_key
             # 此时 template_obj 应该对应 SQL_AGGREGATES["MED_TIMESERIES_JSON"]
             # 直接引用 CTE 列
             sql_expr = psql.SQL(template_obj).format(
                 val_col=psql.Identifier('event_value'),
                 time_col=time_expr,
                 stop_col=psql.Identifier('stoptime'),
                 unit_col=psql.Identifier('dose_unit_rx'),
                 form_col=psql.Identifier('form_unit_disp')
             )
        elif isinstance(template_obj, tuple): # 带参数的模板 (如正则)
            tmpl_str, params = template_obj
            sql_expr = psql.SQL("(ARRAY_AGG({}))[1]").format(
                psql.SQL(tmpl_str).format(val_col=val_expr)
            )
            extra_params.extend(params)
        elif template_obj in ["COUNT(*)", "TRUE"]:
            sql_expr = psql.SQL(template_obj)
        else: # 标准聚合
            sql_expr = psql.SQL(template_obj).format(val_col=val_expr, time_col=time_expr)
        
        agg_select_list.append(psql.SQL("{} AS {}").format(sql_expr, col_ident))

    main_query = psql.SQL("SELECT {group}, {aggs} FROM FilteredEvents GROUP BY {group}").format(
        group=group_by_key, aggs=psql.SQL(', ').join(agg_select_list)
    )

    # --- 8. 组装最终 SQL (Execution or Preview) ---
    final_params = params_for_cte + extra_params
    
    # 基础 CTE 部分
    base_cte_part = psql.SQL("WITH {cte} {main}").format(cte=filtered_events_cte_sql, main=main_query)

    if for_execution:
        # 生成 ALTER, CREATE TEMP, UPDATE, DROP 序列
        alter_cols = [psql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(i, t) for _, i, _, t in selected_methods]
        alter_sql = psql.SQL("ALTER TABLE {tgt} ").format(tgt=target_table_ident) + psql.SQL(', ').join(alter_cols) + psql.SQL(";")
        
        tmp_name = f"temp_merge_{base_new_column_name}_{int(time.time())%1000}"[:60]
        tmp_ident = psql.Identifier(tmp_name)
        
        create_tmp = psql.SQL("CREATE TEMPORARY TABLE {tmp} AS {query}").format(tmp=tmp_ident, query=base_cte_part)
        
        updates = [psql.SQL("{col} = {src}.{col}").format(col=i, src=md_alias) for _, i, _, _ in selected_methods]
        update_sql = psql.SQL("UPDATE {tgt} {alias} SET {sets} FROM {tmp} {src} WHERE {alias}.{key} = {src}.{key}").format(
            tgt=target_table_ident, alias=target_alias, sets=psql.SQL(', ').join(updates),
            tmp=tmp_ident, src=md_alias, key=group_by_key
        )
        
        drop_sql = psql.SQL("DROP TABLE IF EXISTS {}").format(tmp_ident)
        
        return [
            (alter_sql, None),
            (create_tmp, final_params),
            (update_sql, None),
            (drop_sql, None)
        ], "execution_list", base_new_column_name, generated_column_details_for_preview

    else:
        # 生成预览 SQL (采样)
        # 这里的技巧是：先对 target table 采样，然后作为 SampledCohort CTE，再与 FilteredEvents 关联
        # 注意：为了性能，我们重写 FilteredEvents 的源头为 SampledCohort
        
        # 重建 CTE 以使用采样表
        sampled_name = psql.Identifier("SampledCohort")
        sampled_cte = psql.SQL("{name} AS (SELECT * FROM {tgt} ORDER BY RANDOM() LIMIT {lim})").format(
            name=sampled_name, tgt=target_table_ident, lim=psql.Literal(preview_limit)
        )
        
        # 替换原始 CTE 中的 JOIN 目标
        cte_sql_preview = filtered_events_cte_sql # 这里其实复用了对象，但 format 参数不同
        # 我们需要重新构建 CTE 字符串，将 target_table_ident 替换为 sampled_name
        # 上面第 5 步使用的是 target_table_ident。为了预览，我们需要修改 join 的右表。
        
        # 简便方法：在预览模式下，我们构建一个新的 JOIN 子句
        if override_join:
             # 复杂情况暂时不替换，直接用全表 (preview limit 会限制最终结果，但中间计算可能慢)
             # 或者简单的在此处只做全表 LIMIT
             pass 
        else:
             # 标准替换
             from_join_clause_preview = psql.SQL("FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.{evt_key} = {coh_alias}.{coh_key}").format(
                event_table=psql.SQL(source_event_table), evt_alias=event_alias,
                cohort_table=sampled_name, coh_alias=cohort_alias, # <--- 关键替换
                evt_key=psql.Identifier(event_join_key), coh_key=psql.Identifier(cohort_join_key)
            )
             # 重建 CTE
             filtered_events_cte_sql = psql.SQL("FilteredEvents AS (SELECT {selects} {joins} WHERE {conds})").format(
                selects=psql.SQL(', ').join(select_defs),
                joins=from_join_clause_preview,
                conds=psql.SQL(' AND ').join(all_where_conditions) if all_where_conditions else psql.SQL("TRUE")
            )

        # 最终预览查询
        preview_selects = [psql.SQL("{}.*").format(cohort_alias)]
        for _, i, _, _ in selected_methods:
            preview_selects.append(psql.SQL("{}.{}").format(md_alias, i))

        preview_sql = psql.SQL(
            "WITH {sampled}, {filtered}, MergedData AS ({agg_query}) "
            "SELECT {cols} FROM {sampled_name} {coh} "
            "LEFT JOIN MergedData {md} ON {coh}.{key} = {md}.{key}"
        ).format(
            sampled=sampled_cte,
            filtered=filtered_events_cte_sql,
            agg_query=main_query, # main_query 依赖 FilteredEvents
            cols=psql.SQL(', ').join(preview_selects),
            sampled_name=sampled_name, coh=cohort_alias, md=md_alias, key=group_by_key
        )
        
        return preview_sql, None, final_params, generated_column_details_for_preview


def build_merge_preprocessed_sql(
    target_cohort_table_name: str,
    panel_specific_config: Dict[str, Any],
    db_profile: BaseDbProfile,
    active_db_params: Optional[Dict] = None,
    for_execution: bool = False,
    preview_limit: int = 100
) -> Tuple[Optional[Any], Optional[str], Optional[List[Any]], List[Tuple[str, str]]]:
    """
    处理预处理表合并的辅助函数 (保持不变)
    """
    source_table_full_name = panel_specific_config.get("source_event_table")
    selected_columns = panel_specific_config.get("selected_columns", [])
    join_key = panel_specific_config.get("join_key")

    if not all([source_table_full_name, selected_columns, join_key]):
        return None, "合并预处理表的配置不完整。", [], []

    try:
        source_schema, source_table_only = source_table_full_name.split('.')
        target_schema, target_table_only = target_cohort_table_name.split('.')
    except ValueError:
        return None, "表名格式错误 (应为 schema.table)。", [], []

    source_table_ident = psql.Identifier(source_schema, source_table_only)
    target_table_ident = psql.Identifier(target_schema, target_table_only)
    join_key_ident = psql.Identifier(join_key)
    
    source_cte = psql.SQL("""
    SourceCTE AS (
        SELECT DISTINCT ON ({key}) *
        FROM {source_table}
        ORDER BY {key}, charttime ASC NULLS LAST, hadm_id
    )
    """).format(key=join_key_ident, source_table=source_table_ident)

    if for_execution:
        alter_clauses = []
        col_details_for_preview = []
        if not active_db_params:
            return None, "数据库连接参数丢失，无法获取列信息。", [], []
        conn = psycopg2.connect(**active_db_params)
        try:
            with conn.cursor() as cur:
                for col_name in selected_columns:
                    cur.execute("""
                        SELECT data_type FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s AND column_name = %s
                    """, (source_schema, source_table_only, col_name))
                    result = cur.fetchone()
                    if result:
                        col_type = result[0]
                        alter_clauses.append(psql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(psql.Identifier(col_name), psql.SQL(col_type)))
                        col_details_for_preview.append((col_name, col_type))
        finally:
            conn.close()

        if not alter_clauses:
            return None, "未能确定要添加的列。", [], []

        alter_sql = psql.SQL("ALTER TABLE {target_table} ").format(target_table=target_table_ident) + psql.SQL(', ').join(alter_clauses) + psql.SQL(";")

        update_sql = psql.SQL(
            "WITH {cte} UPDATE {target} t SET {sets} FROM SourceCTE s WHERE t.{key} = s.{key};"
        ).format(
            cte=source_cte,
            target=target_table_ident,
            sets=psql.SQL(', ').join([psql.SQL("{col} = s.{col}").format(col=psql.Identifier(col_name)) for col_name in selected_columns]),
            key=join_key_ident
        )
        
        return [(alter_sql, None), (update_sql, None)], "execution_list", f"来自 {source_table_only} 表的数据", col_details_for_preview

    else:
        sampled_cohort_cte = psql.SQL(
            "SampledCohort AS (SELECT * FROM {target} ORDER BY RANDOM() LIMIT {limit})"
        ).format(target=target_table_ident, limit=psql.Literal(preview_limit))
        
        select_cols = [psql.SQL("c.*")] + [psql.SQL("s.{}").format(psql.Identifier(c)) for c in selected_columns]

        preview_sql = psql.SQL(
            "WITH {sampled}, {source} "
            "SELECT {cols} FROM SampledCohort c "
            "LEFT JOIN SourceCTE s ON c.{key} = s.{key};"
        ).format(
            sampled=sampled_cohort_cte,
            source=source_cte,
            cols=psql.SQL(', ').join(select_cols),
            key=join_key_ident
        )
        return preview_sql, None, None, []