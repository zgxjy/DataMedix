# --- START OF FILE sql_logic/sql_builder_special.py ---
import psycopg2
import psycopg2.sql as psql
import time 
import traceback
from utils import validate_column_name

from app_config import SQL_AGGREGATES as GENERIC_SQL_AGGREGATES
from app_config import AGGREGATE_RESULT_TYPES as GENERIC_AGGREGATE_RESULT_TYPES
from typing import List, Tuple, Dict, Any, Optional

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
    for_execution: bool = False,
    preview_limit: int = 100
) -> Tuple[Optional[Any], Optional[str], Optional[List[Any]], List[Tuple[str, str]]]:
    
    generated_column_details_for_preview = [] 

    source_event_table = panel_specific_config.get("source_event_table")
    id_col_in_event_table = panel_specific_config.get("item_id_column_in_event_table")
    value_column_name_from_panel = panel_specific_config.get("value_column_to_extract") 
    time_col_for_window = panel_specific_config.get("time_column_in_event_table")
    time_col_is_date = panel_specific_config.get("time_column_is_date_only", False) # 修改：新增此行
    selected_item_ids = panel_specific_config.get("selected_item_ids", [])
    aggregation_methods: Optional[Dict[str, bool]] = panel_specific_config.get("aggregation_methods", {})
    event_outputs: Optional[Dict[str, bool]] = panel_specific_config.get("event_outputs", {})
    quick_extractors = panel_specific_config.get("quick_extractors", {})
    current_time_window_text = panel_specific_config.get("time_window_text")
    cte_join_override = panel_specific_config.get("cte_join_on_cohort_override")
    is_text_extraction = panel_specific_config.get("is_text_extraction", False)
    text_filter = panel_specific_config.get("text_filter")

    if not all([source_event_table, current_time_window_text]):
        return None, "面板配置信息不完整 (源表, 时间窗口)。", [], generated_column_details_for_preview
    
    if id_col_in_event_table and not selected_item_ids: 
        return None, "已指定项目ID列但未选择任何要提取的项目ID。", [], generated_column_details_for_preview
    
    if not any(aggregation_methods.values()) and not any(event_outputs.values()) and not quick_extractors:
        return None, "未选择任何聚合方法、事件输出或快捷提取项。", [], generated_column_details_for_preview

    try:
        schema_name, table_only_name = target_cohort_table_name.split('.')
    except ValueError:
        return None, f"目标队列表名 '{target_cohort_table_name}' 格式不正确 (应为 schema.table)。", [], []
        
    target_table_ident = psql.Identifier(schema_name, table_only_name)
    cohort_alias = psql.Identifier("cohort")
    event_alias = psql.Identifier("evt")
    event_admission_alias = psql.Identifier("adm_evt")
    md_alias = psql.Identifier("md")
    target_alias = psql.Identifier("target")

    params_for_cte = []
    item_id_filter_on_event_table_parts = []
    text_filter_parts = []
    if text_filter:
        text_filter_parts.append(psql.SQL("evt.text ILIKE %s"))
        params_for_cte.append(f"%{text_filter}%")

    if id_col_in_event_table and selected_item_ids:
        event_table_item_id_col_ident = psql.Identifier(id_col_in_event_table)
        
        # 核心修改：判断是否使用ILIKE
        use_ilike = any('%' in str(s) for s in selected_item_ids)

        if use_ilike:
            # 如果ID中包含%，则构建多个ILIKE条件
            # 修复：增加了 CAST(... AS TEXT)
            ilike_parts = [psql.SQL("TRIM(CAST({}.{} AS TEXT)) ILIKE %s").format(event_alias, event_table_item_id_col_ident) for _ in selected_item_ids]
            item_id_filter_on_event_table_parts.append(psql.SQL("({})").format(psql.SQL(" OR ").join(ilike_parts)))
            params_for_cte.extend(selected_item_ids)
        else:
            # 否则，使用常规的 = 或 IN
            # 修复：增加了 CAST(... AS TEXT)
            trimmed_col_expr = psql.SQL("TRIM(CAST({}.{} AS TEXT))").format(event_alias, event_table_item_id_col_ident)
            if len(selected_item_ids) == 1:
                item_id_filter_on_event_table_parts.append(psql.SQL("{} = %s").format(trimmed_col_expr))
                params_for_cte.append(selected_item_ids[0])
            elif len(selected_item_ids) > 1:
                item_id_filter_on_event_table_parts.append(psql.SQL("{} IN %s").format(trimmed_col_expr))
                params_for_cte.append(tuple(selected_item_ids))
    
    cohort_join_key = "hadm_id"
    event_join_key = "hadm_id"
    if source_event_table and "eicu" in source_event_table.lower():
        cohort_join_key = "patientunitstayid"
        event_join_key = "patientunitstayid"
    elif source_event_table and "chartevents" in source_event_table.lower():
        cohort_join_key = "stay_id"
        event_join_key = "stay_id"

    from_join_clause_for_cte = psql.SQL("{event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.{evt_key} = {coh_alias}.{coh_key}").format(
        event_table=psql.SQL(source_event_table), evt_alias=event_alias,
        cohort_table=target_table_ident, coh_alias=cohort_alias,
        evt_key=psql.Identifier(event_join_key), coh_key=psql.Identifier(cohort_join_key)
    )

    if cte_join_override:
        from_join_clause_for_cte_with_from = cte_join_override.format(
            event_table=psql.SQL(source_event_table), evt_alias=event_alias,
            adm_evt=event_admission_alias,
            cohort_table=target_table_ident, coh_alias=cohort_alias)
    else:
        from_join_clause_for_cte_with_from = psql.SQL("FROM ") + from_join_clause_for_cte

    actual_event_time_col_ident = psql.Identifier(time_col_for_window) if time_col_for_window else None
    
    cohort_icu_intime = psql.SQL("{}.icu_intime").format(cohort_alias)
    cohort_icu_outtime = psql.SQL("{}.icu_outtime").format(cohort_alias)
    cohort_admittime = psql.SQL("{}.admittime").format(cohort_alias)
    cohort_dischtime = psql.SQL("{}.dischtime").format(cohort_alias)

    time_filter_conditions_sql_parts = []
    if time_col_for_window:
        if "24小时" in current_time_window_text: time_filter_conditions_sql_parts.append(psql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND ({start_ts} + interval '24 hours')").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime))
        elif "48小时" in current_time_window_text: time_filter_conditions_sql_parts.append(psql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND ({start_ts} + interval '48 hours')").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime))
        elif "整个ICU期间" in current_time_window_text: time_filter_conditions_sql_parts.append(psql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime, end_ts=cohort_icu_outtime))
        # 修改：start
        elif "整个住院期间" in current_time_window_text:
            start_ts_expr = psql.SQL("CAST({} AS DATE)").format(cohort_admittime) if time_col_is_date else cohort_admittime
            end_ts_expr = psql.SQL("CAST({} AS DATE)").format(cohort_dischtime) if time_col_is_date else cohort_dischtime
            time_filter_conditions_sql_parts.append(
                psql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(
                    evt=event_alias, 
                    time_col=actual_event_time_col_ident, 
                    start_ts=start_ts_expr, 
                    end_ts=end_ts_expr
                )
            )
        # 修改：end
        elif "住院以前" in current_time_window_text:
            if not cte_join_override: return None, f"“住院以前”需要JOIN覆盖逻辑。", [], []
            time_filter_conditions_sql_parts.append(psql.SQL("{adm_evt}.admittime < {compare_ts}").format(adm_evt=event_admission_alias, compare_ts=cohort_admittime))

    select_event_cols_defs = [psql.SQL("{}.*").format(cohort_alias)]
    event_value_col_for_select_ident = psql.Identifier(value_column_name_from_panel) if value_column_name_from_panel else None
    if event_value_col_for_select_ident:
        select_event_cols_defs.append(psql.SQL("{}.{} AS event_value").format(event_alias, event_value_col_for_select_ident))
    if actual_event_time_col_ident:
        select_event_cols_defs.append(psql.SQL("{}.{} AS event_time").format(event_alias, actual_event_time_col_ident))

    all_where_conditions = item_id_filter_on_event_table_parts + text_filter_parts + time_filter_conditions_sql_parts
    
    filtered_events_cte_sql = psql.SQL("FilteredEvents AS (SELECT {select_list} {from_join} WHERE {conditions})").format(
        select_list=psql.SQL(', ').join(select_event_cols_defs),
        from_join=from_join_clause_for_cte_with_from,
        conditions=psql.SQL(' AND ').join(all_where_conditions) if all_where_conditions else psql.SQL("TRUE")
    )

    selected_methods_details = []
    type_map_display = { "NUMERIC": "Numeric", "INTEGER": "Integer", "BOOLEAN": "Boolean", "TEXT": "Text", "DOUBLE PRECISION": "Numeric (Decimal)", "JSONB": "JSON" }

    if aggregation_methods:
        for method_key, is_selected in aggregation_methods.items():
            if is_selected:
                sql_template = SQL_AGGREGATES.get(method_key)
                if not sql_template: continue
                col_type = AGGREGATE_RESULT_TYPES.get(method_key, "NUMERIC")
                if is_text_extraction: col_type = "TEXT" if method_key in ["MIN", "MAX", "NOTE_CONCAT", "NOTE_FIRST", "NOTE_LAST"] else col_type
                
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
            
            sql_template = ("(REGEXP_MATCHES({val_col}, %s, 'i'))[2]", [pattern])
            col_type = psql.SQL("TEXT") # Regexp results are text
            
            selected_methods_details.append((final_col_name, psql.Identifier(final_col_name), sql_template, col_type))
            generated_column_details_for_preview.append((final_col_name, "Text (Extracted)"))

    if not selected_methods_details:
        return None, "未能构建任何有效的提取列。", [], generated_column_details_for_preview

    group_by_key = psql.Identifier(cohort_join_key)
    aggregated_cols_sql_list = []
    for _, final_col_ident, agg_template_or_tuple, _ in selected_methods_details:
        if isinstance(agg_template_or_tuple, tuple):
            sql_template_str, regex_params = agg_template_or_tuple
            safe_pattern_literal = psql.Literal(regex_params[0])
            sql_expr = psql.SQL(sql_template_str).format(val_col=psql.Identifier('event_value'))
            sql_expr = psql.SQL(sql_template_str.replace('%s', str(safe_pattern_literal))).format(val_col=psql.Identifier('event_value'))

        else: # Standard aggregation
            params_for_template = {'val_col': psql.Identifier('event_value'), 'time_col': psql.Identifier('event_time')}
            if agg_template_or_tuple == "COUNT(*)" or agg_template_or_tuple == "TRUE":
                 sql_expr = psql.SQL(agg_template_or_tuple)
            else:
                 sql_expr = psql.SQL(agg_template_or_tuple).format(**params_for_template)
        aggregated_cols_sql_list.append(psql.SQL("{} AS {}").format(sql_expr, final_col_ident))

    main_agg_select_sql = psql.SQL("SELECT {group_key}, {agg_cols} FROM FilteredEvents GROUP BY {group_key}").format(
        group_key=group_by_key,
        agg_cols=psql.SQL(', ').join(aggregated_cols_sql_list)
    )

    data_gen_query_part = psql.SQL("WITH {filtered_cte} {main_agg_select}").format(
        filtered_cte=filtered_events_cte_sql, 
        main_agg_select=main_agg_select_sql
    )

    if for_execution:
        alter_clauses = [psql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(ident, type_obj) for _, ident, _, type_obj in selected_methods_details]
        alter_sql = psql.SQL("ALTER TABLE {target_table} ").format(target_table=target_table_ident) + psql.SQL(', ').join(alter_clauses) + psql.SQL(";")
        temp_table_name = f"temp_merge_{base_new_column_name.lower()}_{int(time.time()) % 100000}"[:63]
        temp_table_ident = psql.Identifier(temp_table_name)
        create_temp_sql = psql.SQL("CREATE TEMPORARY TABLE {temp_table} AS {data_gen};").format(temp_table=temp_table_ident, data_gen=data_gen_query_part)
        set_clauses = [psql.SQL("{col_to_set} = {tmp_alias}.{col_from_tmp}").format(col_to_set=ident, tmp_alias=md_alias, col_from_tmp=ident) for _, ident, _, _ in selected_methods_details]
        update_sql = psql.SQL("UPDATE {target_table} {tgt_alias} SET {set_clauses} FROM {temp_table} {tmp_alias} WHERE {tgt_alias}.{join_key} = {tmp_alias}.{join_key};").format(
            target_table=target_table_ident, tgt_alias=target_alias, set_clauses=psql.SQL(', ').join(set_clauses),
            temp_table=temp_table_ident, tmp_alias=md_alias, join_key=group_by_key
        )
        drop_temp_sql = psql.SQL("DROP TABLE IF EXISTS {temp_table};").format(temp_table=temp_table_ident)
        return [(alter_sql, None), (create_temp_sql, params_for_cte), (update_sql, None), (drop_temp_sql, None)], "execution_list", base_new_column_name, generated_column_details_for_preview
    
    else:
        preview_select_cols = [psql.SQL("{}.*").format(cohort_alias)]
        for _, final_col_ident, _, _ in selected_methods_details:
            preview_select_cols.append(psql.SQL("{md_alias}.{col_ident}").format(md_alias=md_alias, col_ident=final_col_ident))
        preview_sql = psql.SQL(
            "WITH MergedDataCTE AS ({data_gen_query}) "
            "SELECT {select_cols} "
            "FROM {target_table} {coh_alias} "
            "LEFT JOIN MergedDataCTE {md_alias} ON {coh_alias}.{join_key} = {md_alias}.{join_key} "
            "ORDER BY RANDOM() LIMIT {limit};"
        ).format(
            data_gen_query=data_gen_query_part,
            select_cols=psql.SQL(', ').join(preview_select_cols),
            target_table=target_table_ident, coh_alias=cohort_alias,
            md_alias=md_alias, join_key=group_by_key, 
            limit=psql.Literal(preview_limit)
        )
        return preview_sql, None, params_for_cte, generated_column_details_for_preview