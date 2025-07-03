# --- START OF FILE sql_logic/sql_builder_special.py ---
import psycopg2
import psycopg2.sql as psql
import time 
import traceback
from utils import validate_column_name

from app_config import SQL_AGGREGATES as GENERIC_SQL_AGGREGATES
from app_config import AGGREGATE_RESULT_TYPES as GENERIC_AGGREGATE_RESULT_TYPES
from typing import List, Tuple, Dict, Any, Optional
from db_profiles.base_profile import BaseDbProfile


def _is_eicu_profile(profile: BaseDbProfile) -> bool:
    """检查数据库画像是否为e-ICU。"""
    return profile and "eicu" in profile.get_display_name().lower()

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


def build_special_data_sql(
    target_cohort_table_name: str,
    base_new_column_name: str,
    panel_specific_config: Dict[str, Any],
    db_profile: BaseDbProfile,
    active_db_params: Optional[Dict] = None,
    for_execution: bool = False,
    preview_limit: int = 100
) -> Tuple[Optional[Any], Optional[str], Optional[List[Any]], List[Tuple[str, str]]]:
    
    if panel_specific_config.get("panel_type") == "merge_preprocessed":
        return build_merge_preprocessed_sql(
            target_cohort_table_name=target_cohort_table_name,
            panel_specific_config=panel_specific_config,
            db_profile=db_profile,
            active_db_params=active_db_params,
            for_execution=for_execution,
            preview_limit=preview_limit
        )

    generated_column_details_for_preview = [] 

    # --- 1. 从配置中提取参数 ---
    source_event_table = panel_specific_config.get("source_event_table")
    id_col_in_event_table = panel_specific_config.get("item_id_column_in_event_table")
    value_column_name_from_panel = panel_specific_config.get("value_column_to_extract") 
    time_col_for_window = panel_specific_config.get("time_column_in_event_table")
    time_col_is_date = panel_specific_config.get("time_column_is_date_only", False)
    selected_item_ids = panel_specific_config.get("selected_item_ids", [])
    aggregation_methods: Optional[Dict[str, bool]] = panel_specific_config.get("aggregation_methods", {})
    event_outputs: Optional[Dict[str, bool]] = panel_specific_config.get("event_outputs", {})
    quick_extractors = panel_specific_config.get("quick_extractors", {})
    current_time_window_text = panel_specific_config.get("time_window_text")
    is_text_extraction = panel_specific_config.get("is_text_extraction", False)
    text_filter = panel_specific_config.get("text_filter")
    detail_table = panel_specific_config.get("detail_table")
    detail_filters = panel_specific_config.get("detail_filters", [])

    # --- 2. 基础校验 ---
    if not all([source_event_table, current_time_window_text]):
        return None, "面板配置信息不完整 (源表, 时间窗口)。", [], generated_column_details_for_preview
    if id_col_in_event_table and not selected_item_ids and not quick_extractors: 
        return None, "已指定项目ID列但未选择任何要提取的项目ID。", [], generated_column_details_for_preview
    if not any(aggregation_methods.values()) and not any(event_outputs.values()) and not quick_extractors:
        return None, "未选择任何聚合方法、事件输出或快捷提取项。", [], generated_column_details_for_preview

    try:
        schema_name, table_only_name = target_cohort_table_name.split('.')
    except ValueError:
        return None, f"目标队列表名 '{target_cohort_table_name}' 格式不正确 (应为 schema.table)。", [], []
        
    # --- 3. 初始化SQL构建组件 ---
    target_table_ident = psql.Identifier(schema_name, table_only_name)
    cohort_alias = psql.Identifier("cohort")
    event_alias = psql.Identifier("evt")
    md_alias = psql.Identifier("md")
    target_alias = psql.Identifier("target")
    
    params_for_cte = []
    all_where_conditions = []

    # --- 4. 构建 WHERE 子句 ---
    if text_filter:
        all_where_conditions.append(psql.SQL("evt.text ILIKE %s"))
        params_for_cte.append(f"%{text_filter}%")

    if id_col_in_event_table and selected_item_ids:
        safe_item_ids = [str(item) for item in selected_item_ids]
        event_table_item_id_col_ident = psql.Identifier(id_col_in_event_table)
        use_ilike = any('%' in s for s in safe_item_ids)

        if use_ilike:
            ilike_parts = [psql.SQL("TRIM(CAST({}.{} AS TEXT)) ILIKE %s").format(event_alias, event_table_item_id_col_ident) for _ in safe_item_ids]
            all_where_conditions.append(psql.SQL("({})").format(psql.SQL(" OR ").join(ilike_parts)))
            params_for_cte.extend(safe_item_ids)
        else:
            trimmed_col_expr = psql.SQL("TRIM(CAST({}.{} AS TEXT))").format(event_alias, event_table_item_id_col_ident)
            if len(safe_item_ids) == 1:
                all_where_conditions.append(psql.SQL("{} = %s").format(trimmed_col_expr))
                params_for_cte.append(safe_item_ids[0])
            elif len(safe_item_ids) > 1:
                all_where_conditions.append(psql.SQL("{} IN %s").format(trimmed_col_expr))
                params_for_cte.append(tuple(safe_item_ids))

    if detail_table and detail_filters:
        detail_where_clauses = []
        note_id_col_in_detail = 'note_id' # 假设详情表总是有note_id
        for field, op, value in detail_filters:
            operator = psql.SQL(op)
            detail_where_clauses.append(psql.SQL("{field} {op} %s").format(field=psql.Identifier(field), op=operator))
            params_for_cte.append(value)
        
        subquery_for_note_ids = psql.SQL("(SELECT {note_id_col} FROM {detail_table} WHERE {conditions})").format(
            note_id_col=psql.Identifier(note_id_col_in_detail),
            detail_table=psql.SQL(detail_table),
            conditions=psql.SQL(" AND ").join(detail_where_clauses)
        )
        all_where_conditions.append(psql.SQL("{evt_alias}.note_id IN {subquery}").format(evt_alias=event_alias, subquery=subquery_for_note_ids))

    is_eicu = _is_eicu_profile(db_profile)
    if time_col_for_window:
        actual_event_time_col_ident = psql.Identifier(time_col_for_window)
        if is_eicu:
            if "24小时" in current_time_window_text: all_where_conditions.append(psql.SQL("{evt}.{time_col} BETWEEN 0 AND 1440").format(evt=event_alias, time_col=actual_event_time_col_ident))
            elif "48小时" in current_time_window_text: all_where_conditions.append(psql.SQL("{evt}.{time_col} BETWEEN 0 AND 2880").format(evt=event_alias, time_col=actual_event_time_col_ident))
            elif "整个ICU期间" in current_time_window_text: all_where_conditions.append(psql.SQL("{evt}.{time_col} >= 0 AND {evt}.{time_col} <= {coh}.los_icu_minutes").format(evt=event_alias, time_col=actual_event_time_col_ident, coh=cohort_alias))
        else:
            cohort_icu_intime = psql.SQL("{}.icu_intime").format(cohort_alias)
            cohort_icu_outtime = psql.SQL("{}.icu_outtime").format(cohort_alias)
            cohort_admittime = psql.SQL("{}.admittime").format(cohort_alias)
            cohort_dischtime = psql.SQL("{}.dischtime").format(cohort_alias)
            
            if "24小时" in current_time_window_text: all_where_conditions.append(psql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND ({start_ts} + interval '24 hours')").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime))
            elif "48小时" in current_time_window_text: all_where_conditions.append(psql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND ({start_ts} + interval '48 hours')").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime))
            elif "整个ICU期间" in current_time_window_text: all_where_conditions.append(psql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime, end_ts=cohort_icu_outtime))
            elif "整个住院期间" in current_time_window_text:
                start_ts_expr = psql.SQL("CAST({} AS DATE)").format(cohort_admittime) if time_col_is_date else cohort_admittime
                end_ts_expr = psql.SQL("CAST({} AS DATE)").format(cohort_dischtime) if time_col_is_date else cohort_dischtime
                all_where_conditions.append(psql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=start_ts_expr, end_ts=end_ts_expr))

    # --- 5. 构建 `FilteredEvents` CTE ---
    cohort_join_key = db_profile.get_cohort_join_key(source_event_table)
    event_join_key = db_profile.get_event_table_join_key(source_event_table)
    
    from_join_clause_for_cte = psql.SQL("FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.{evt_key} = {coh_alias}.{coh_key}").format(
        event_table=psql.SQL(source_event_table), evt_alias=event_alias,
        cohort_table=target_table_ident, coh_alias=cohort_alias,
        evt_key=psql.Identifier(event_join_key), coh_key=psql.Identifier(cohort_join_key)
    )

    select_event_cols_defs = [psql.SQL("{}.*").format(cohort_alias)]
    if value_column_name_from_panel:
        select_event_cols_defs.append(psql.SQL("{}.{} AS event_value").format(event_alias, psql.Identifier(value_column_name_from_panel)))
    if time_col_for_window:
        select_event_cols_defs.append(psql.SQL("{}.{} AS event_time").format(event_alias, psql.Identifier(time_col_for_window)))

    filtered_events_cte_sql = psql.SQL("FilteredEvents AS (SELECT {select_list} {from_join} WHERE {conditions})").format(
        select_list=psql.SQL(', ').join(select_event_cols_defs),
        from_join=from_join_clause_for_cte,
        conditions=psql.SQL(' AND ').join(all_where_conditions) if all_where_conditions else psql.SQL("TRUE")
    )
    
    # --- 6. 确定要生成的列和聚合函数 ---
    selected_methods_details = []
    type_map_display = { "NUMERIC": "Numeric", "INTEGER": "Integer", "BOOLEAN": "Boolean", "TEXT": "Text", "DOUBLE PRECISION": "Numeric (Decimal)", "JSONB": "JSON" }

    if aggregation_methods:
        for method_key, is_selected in aggregation_methods.items():
            if is_selected:
                sql_template = SQL_AGGREGATES.get(method_key)
                if not sql_template: continue
                col_type = AGGREGATE_RESULT_TYPES.get(method_key, "NUMERIC")
                if is_text_extraction and method_key in ["MIN", "MAX", "NOTE_CONCAT", "NOTE_FIRST", "FIRST_VALUE", "LAST_VALUE","NOTE_LAST"]: col_type = "TEXT"
                final_col_name = f"{base_new_column_name}_{method_key.lower()}"
                is_valid, err = validate_column_name(final_col_name)
                if not is_valid: return None, f"生成的列名 '{final_col_name}' 无效: {err}", [], []
                selected_methods_details.append((final_col_name, psql.Identifier(final_col_name), sql_template, psql.SQL(col_type)))
                generated_column_details_for_preview.append((final_col_name, type_map_display.get(col_type, col_type)))

    if event_outputs:
        event_method_configs = {"exists": ("TRUE", psql.SQL("BOOLEAN")), "countevt": ("COUNT(*)", psql.SQL("INTEGER"))}
        for method_key, is_selected in event_outputs.items():
            if is_selected and method_key in event_method_configs:
                agg_template, col_type_obj = event_method_configs[method_key]
                col_type_str = str(col_type_obj).replace("SQL('", "").replace("')", "")
                final_col_name = f"{base_new_column_name}_{method_key.lower()}"
                is_valid, err = validate_column_name(final_col_name)
                if not is_valid: return None, f"生成的列名 '{final_col_name}' 无效: {err}", [], []
                selected_methods_details.append((final_col_name, psql.Identifier(final_col_name), agg_template, col_type_obj))
                generated_column_details_for_preview.append((final_col_name, type_map_display.get(col_type_str, col_type_str)))
    
    if quick_extractors:
        for key, pattern in quick_extractors.items():
            final_col_name = f"{base_new_column_name}_{key}"
            is_valid, err = validate_column_name(final_col_name)
            if not is_valid: return None, f"生成的列名 '{final_col_name}' 无效: {err}", [], []
            sql_template_with_placeholder = "(REGEXP_MATCHES({val_col}, %s, 'i'))"
            sql_template = psql.SQL(sql_template_with_placeholder).format(val_col=psql.Identifier('event_value'))
            template_tuple = (sql_template, [pattern])
            col_type = psql.SQL("TEXT")
            # --- 注意，这里我们传递的是一个元组，表示它需要特殊处理 ---
            selected_methods_details.append((final_col_name, psql.Identifier(final_col_name), template_tuple, col_type))
            generated_column_details_for_preview.append((final_col_name, "Text (Extracted)"))

    if not selected_methods_details:
        return None, "未能构建任何有效的提取列。", [], generated_column_details_for_preview

    # --- 7. 构建最终查询 ---
    group_by_key_ident = psql.Identifier(cohort_join_key)
    aggregated_cols_sql_list = []
    extra_params_for_agg = []
    
    for _, final_col_ident, agg_template_or_tuple, _ in selected_methods_details:
        sql_expr = None

        # --- 核心修复：定义 event_value 列的表达式 ---
        event_value_expression = psql.Identifier('event_value') # 默认为 event_value 列
        is_eicu_text_to_numeric_cast_needed = (
            _is_eicu_profile(db_profile) and
            source_event_table in ["public.nursecharting", "public.infusiondrug"] and # <-- 添加 infusiondrug
            not is_text_extraction # 仅对数值聚合模式生效
        )
        
        # 如果需要转换，则重写值的表达式
        if is_eicu_text_to_numeric_cast_needed:
            event_value_expression = psql.SQL("CAST(NULLIF(event_value, '') AS NUMERIC)")

        if isinstance(agg_template_or_tuple, tuple):
            # 处理正则表达式等特殊情况
            sql_template, params = agg_template_or_tuple
            # 注意：正则表达式通常作用于原始文本，所以这里用原始的 event_value
            sql_expr = psql.SQL("(ARRAY_AGG({}))[1]").format(
                psql.SQL(sql_template).format(val_col=psql.Identifier('event_value'))
            )
            extra_params_for_agg.extend(params)
        elif agg_template_or_tuple in ["COUNT(*)", "TRUE"]:
            # 处理 COUNT(*) 和布尔值
            sql_expr = psql.SQL(agg_template_or_tuple)
        else:
            # 处理所有其他标准聚合函数
            sql_expr = psql.SQL(agg_template_or_tuple).format(
                val_col=event_value_expression, # <--- 使用我们新定义的表达式
                time_col=psql.Identifier('event_time')
            )
        
        if sql_expr:
            aggregated_cols_sql_list.append(psql.SQL("{} AS {}").format(sql_expr, final_col_ident))

    main_agg_select_sql = psql.SQL("SELECT {group_key}, {agg_cols} FROM FilteredEvents GROUP BY {group_key}").format(
        group_key=group_by_key_ident,
        agg_cols=psql.SQL(', ').join(aggregated_cols_sql_list)
    )

    final_query_params = params_for_cte + extra_params_for_agg
    data_gen_query_part = psql.SQL("WITH {filtered_cte} {main_agg_select}").format(filtered_cte=filtered_events_cte_sql, main_agg_select=main_agg_select_sql)

    if for_execution:
        alter_clauses = [psql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(ident, type_obj) for _, ident, _, type_obj in selected_methods_details]
        alter_sql = psql.SQL("ALTER TABLE {target_table} ").format(target_table=target_table_ident) + psql.SQL(', ').join(alter_clauses) + psql.SQL(";")
        temp_table_name = f"temp_merge_{base_new_column_name.lower()}_{int(time.time()) % 100000}"[:63]
        temp_table_ident = psql.Identifier(temp_table_name)
        create_temp_sql = psql.SQL("CREATE TEMPORARY TABLE {temp_table} AS {data_gen};").format(temp_table=temp_table_ident, data_gen=data_gen_query_part)
        set_clauses = [psql.SQL("{col_to_set} = {tmp_alias}.{col_from_tmp}").format(col_to_set=ident, tmp_alias=md_alias, col_from_tmp=ident) for _, ident, _, _ in selected_methods_details]
        update_sql = psql.SQL("UPDATE {target_table} {tgt_alias} SET {set_clauses} FROM {temp_table} {tmp_alias} WHERE {tgt_alias}.{join_key} = {tmp_alias}.{join_key};").format(
            target_table=target_table_ident, tgt_alias=target_alias, set_clauses=psql.SQL(', ').join(set_clauses),
            temp_table=temp_table_ident, tmp_alias=md_alias, join_key=group_by_key_ident
        )
        drop_temp_sql = psql.SQL("DROP TABLE IF EXISTS {temp_table};").format(temp_table=temp_table_ident)
        return [(alter_sql, None), (create_temp_sql, final_query_params), (update_sql, None), (drop_temp_sql, None)], "execution_list", base_new_column_name, generated_column_details_for_preview
    else:
        sampled_cohort_cte_name = psql.Identifier("SampledCohort")
        sampled_cohort_cte = psql.SQL("{sampled_cte_name} AS (SELECT * FROM {target_table} ORDER BY RANDOM() LIMIT {limit})").format(
            sampled_cte_name=sampled_cohort_cte_name,
            target_table=target_table_ident,
            limit=psql.Literal(preview_limit)
        )

        # 修改原始的 filtered_events_cte_sql，让它 join 抽样后的表
        from_join_clause_for_cte = psql.SQL("FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.{evt_key} = {coh_alias}.{coh_key}").format(
            event_table=psql.SQL(source_event_table), evt_alias=event_alias,
            cohort_table=sampled_cohort_cte_name, # <-- 使用抽样后的表
            coh_alias=cohort_alias,
            evt_key=psql.Identifier(event_join_key), coh_key=psql.Identifier(cohort_join_key)
        )
        
        filtered_events_cte_sql = psql.SQL("FilteredEvents AS (SELECT {select_list} {from_join} WHERE {conditions})").format(
            select_list=psql.SQL(', ').join(select_event_cols_defs),
            from_join=from_join_clause_for_cte,
            conditions=psql.SQL(' AND ').join(all_where_conditions) if all_where_conditions else psql.SQL("TRUE")
        )

        data_gen_query_part = psql.SQL("WITH {filtered_cte} {main_agg_select}").format(
            filtered_cte=filtered_events_cte_sql,
            main_agg_select=main_agg_select_sql
        )
        
        # 最终的预览SQL
        preview_select_cols = [psql.SQL("{}.*").format(cohort_alias)]
        for _, final_col_ident, _, _ in selected_methods_details:
            preview_select_cols.append(psql.SQL("{md_alias}.{col_ident}").format(md_alias=md_alias, col_ident=final_col_ident))
            
        preview_sql = psql.SQL(
            "WITH {sampled_cohort}, MergedDataCTE AS ({data_gen_query}) "
            "SELECT {select_cols} FROM {sampled_cohort_name} {coh_alias} "
            "LEFT JOIN MergedDataCTE {md_alias} ON {coh_alias}.{join_key} = {md_alias}.{join_key};"
        ).format(
            sampled_cohort=sampled_cohort_cte,
            data_gen_query=data_gen_query_part,
            select_cols=psql.SQL(', ').join(preview_select_cols),
            sampled_cohort_name=sampled_cohort_cte_name, # <-- 从抽样后的表 select
            coh_alias=cohort_alias,
            md_alias=md_alias, 
            join_key=group_by_key_ident
        )
        # --- 优化逻辑结束 ---
        return preview_sql, None, final_query_params, generated_column_details_for_preview


def build_merge_preprocessed_sql(
    target_cohort_table_name: str,
    panel_specific_config: Dict[str, Any],
    db_profile: BaseDbProfile,
    active_db_params: Optional[Dict] = None,
    for_execution: bool = False,
    preview_limit: int = 100
) -> Tuple[Optional[Any], Optional[str], Optional[List[Any]], List[Tuple[str, str]]]:
    
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
    
    # --- 重要: 解决一个hadm_id可能对应多行预处理结果的问题 ---
    # 我们使用 DISTINCT ON 来保证每个hadm_id只取一行记录（可以基于某个时间或ID排序）
    # 这里我们假设预处理表有一个'charttime'或'row_id'可以排序，如果没有，就随机取一个
    # 这一步保证了合并的确定性
    source_cte = psql.SQL("""
    SourceCTE AS (
        SELECT DISTINCT ON ({key}) *
        FROM {source_table}
        ORDER BY {key}, charttime ASC NULLS LAST, hadm_id -- 优先按时间排序
    )
    """).format(key=join_key_ident, source_table=source_table_ident)

    if for_execution:
        # 1. 获取列类型并构建 ALTER TABLE 语句
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

        # 2. 构建 UPDATE 语句
        set_clauses = [psql.SQL("{col} = s.{col}").format(col=psql.Identifier(col_name)) for col_name in selected_columns]
        update_sql = psql.SQL(
            "WITH {cte} UPDATE {target} t SET {sets} FROM SourceCTE s WHERE t.{key} = s.{key};"
        ).format(
            cte=source_cte,
            target=target_table_ident,
            sets=psql.SQL(', ').join(set_clauses),
            key=join_key_ident
        )
        
        # 返回执行步骤列表
        return [(alter_sql, None), (update_sql, None)], "execution_list", f"来自 {source_table_only} 表的数据", col_details_for_preview

    else: # for_execution=False, 生成预览SQL
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