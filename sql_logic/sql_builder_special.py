# --- START OF FINAL ROBUST VERSION: sql_logic/sql_builder_special.py ---
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
    return profile and "eicu" in profile.get_display_name().lower()

SQL_AGGREGATES = {
    **GENERIC_SQL_AGGREGATES,
    "NOTE_CONCAT": "STRING_AGG(DISTINCT {val_col}, E'\\n\\n---NOTE---\\n\\n' ORDER BY {time_col})",
    "NOTE_FIRST": "(ARRAY_AGG({val_col} ORDER BY {time_col} ASC NULLS LAST))[1]",
    "NOTE_LAST": "(ARRAY_AGG({val_col} ORDER BY {time_col} DESC NULLS LAST))[1]",
    "NOTE_COUNT": "COUNT(DISTINCT {val_col})",
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
    for_execution: bool = False,
    preview_limit: int = 100
) -> Tuple[Optional[Any], Optional[str], Optional[List[Any]], List[Tuple[str, str]]]:
    
    generated_column_details_for_preview = [] 

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

    if not all([source_event_table, current_time_window_text]): return None, "面板配置信息不完整 (源表, 时间窗口)。", [], generated_column_details_for_preview
    if id_col_in_event_table and not selected_item_ids and not quick_extractors: return None, "已指定项目ID列但未选择任何要提取的项目ID。", [], generated_column_details_for_preview
    if not any(aggregation_methods.values()) and not any(event_outputs.values()) and not quick_extractors: return None, "未选择任何聚合方法、事件输出或快捷提取项。", [], generated_column_details_for_preview
    try: schema_name, table_only_name = target_cohort_table_name.split('.')
    except ValueError: return None, f"目标队列表名 '{target_cohort_table_name}' 格式不正确。", [], []
        
    target_table_ident = psql.Identifier(schema_name, table_only_name)
    cohort_alias = psql.Identifier("cohort")
    event_alias = psql.Identifier("evt")
    
    params_for_cte = []
    all_where_conditions = []

    if text_filter: all_where_conditions.append(psql.SQL("evt.text ILIKE %s")); params_for_cte.append(f"%{text_filter}%")
    if id_col_in_event_table and selected_item_ids:
        safe_item_ids = [str(item) for item in selected_item_ids]; event_table_item_id_col_ident = psql.Identifier(id_col_in_event_table)
        if use_ilike := any('%' in s for s in safe_item_ids):
            ilike_parts = [psql.SQL("TRIM(CAST({}.{} AS TEXT)) ILIKE %s").format(event_alias, event_table_item_id_col_ident) for _ in safe_item_ids]
            all_where_conditions.append(psql.SQL("({})").format(psql.SQL(" OR ").join(ilike_parts))); params_for_cte.extend(safe_item_ids)
        else:
            trimmed_col_expr = psql.SQL("TRIM(CAST({}.{} AS TEXT))").format(event_alias, event_table_item_id_col_ident)
            if len(safe_item_ids) == 1: all_where_conditions.append(psql.SQL("{} = %s").format(trimmed_col_expr)); params_for_cte.append(safe_item_ids[0])
            elif len(safe_item_ids) > 1: all_where_conditions.append(psql.SQL("{} IN %s").format(trimmed_col_expr)); params_for_cte.append(tuple(safe_item_ids))
    if detail_table and detail_filters:
        detail_where_clauses = []
        for field, op, value in detail_filters: detail_where_clauses.append(psql.SQL("{field} {op} %s").format(field=psql.Identifier(field), op=psql.SQL(op))); params_for_cte.append(value)
        subquery_for_note_ids = psql.SQL("(SELECT {note_id_col} FROM {detail_table} WHERE {conditions})").format(note_id_col=psql.Identifier('note_id'), detail_table=psql.SQL(detail_table), conditions=psql.SQL(" AND ").join(detail_where_clauses))
        all_where_conditions.append(psql.SQL("{evt_alias}.note_id IN {subquery}").format(evt_alias=event_alias, subquery=subquery_for_note_ids))
    if time_col_for_window:
        actual_event_time_col_ident = psql.Identifier(time_col_for_window)
        if _is_eicu_profile(db_profile):
            if "24小时" in current_time_window_text: all_where_conditions.append(psql.SQL("{evt}.{time_col} BETWEEN 0 AND 1440").format(evt=event_alias, time_col=actual_event_time_col_ident))
            elif "48小时" in current_time_window_text: all_where_conditions.append(psql.SQL("{evt}.{time_col} BETWEEN 0 AND 2880").format(evt=event_alias, time_col=actual_event_time_col_ident))
            elif "整个ICU期间" in current_time_window_text: all_where_conditions.append(psql.SQL("{evt}.{time_col} >= 0 AND {evt}.{time_col} <= {coh}.los_icu_minutes").format(evt=event_alias, time_col=actual_event_time_col_ident, coh=cohort_alias))
        else:
            start_ts = psql.SQL("{}.icu_intime").format(cohort_alias); end_ts = psql.SQL("{}.icu_outtime").format(cohort_alias)
            if "24小时" in current_time_window_text: end_ts = psql.SQL("({} + interval '24 hours')").format(start_ts)
            elif "48小时" in current_time_window_text: end_ts = psql.SQL("({} + interval '48 hours')").format(start_ts)
            elif "整个住院期间" in current_time_window_text: start_ts = psql.SQL("{}.admittime").format(cohort_alias); end_ts = psql.SQL("{}.dischtime").format(cohort_alias)
            if time_col_is_date: start_ts=psql.SQL("CAST({} AS DATE)").format(start_ts); end_ts=psql.SQL("CAST({} AS DATE)").format(end_ts)
            all_where_conditions.append(psql.SQL("{evt}.{time_col} BETWEEN {start} AND {end}").format(evt=event_alias, time_col=actual_event_time_col_ident, start=start_ts, end=end_ts))

    cohort_join_key = db_profile.get_cohort_join_key(source_event_table); event_join_key = db_profile.get_event_table_join_key(source_event_table)
    final_cohort_source_ident = target_table_ident; cohort_source_cte = None
    if not for_execution:
        final_cohort_source_ident = psql.Identifier("SampledCohort")
        cohort_source_cte = psql.SQL("{sampled_cte_name} AS (SELECT * FROM {target_table} ORDER BY RANDOM() LIMIT {limit})").format(sampled_cte_name=final_cohort_source_ident, target_table=target_table_ident, limit=psql.Literal(preview_limit))

    from_join_clause_for_cte = psql.SQL("FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.{evt_key} = {coh_alias}.{coh_key}").format(event_table=psql.SQL(source_event_table), evt_alias=event_alias, cohort_table=final_cohort_source_ident, coh_alias=cohort_alias, evt_key=psql.Identifier(event_join_key), coh_key=psql.Identifier(cohort_join_key))
    
    selected_methods_details = []; type_map_display = {"NUMERIC": "Numeric", "INTEGER": "Integer", "BOOLEAN": "Boolean", "TEXT": "Text", "DOUBLE PRECISION": "Numeric (Decimal)", "JSONB": "JSON"}; final_query_params = list(params_for_cte)
    if aggregation_methods:
        for method_key, is_selected in aggregation_methods.items():
            if is_selected and (sql_template := SQL_AGGREGATES.get(method_key)):
                col_type = AGGREGATE_RESULT_TYPES.get(method_key, "NUMERIC")
                if is_text_extraction and method_key in ["MIN", "MAX", "NOTE_CONCAT", "NOTE_FIRST", "NOTE_LAST"]: col_type = "TEXT"
                final_col_name = f"{base_new_column_name}_{method_key.lower()}"
                if not validate_column_name(final_col_name)[0]: return None, f"生成的列名 '{final_col_name}' 无效", [], []
                selected_methods_details.append({"name": final_col_name, "ident": psql.Identifier(final_col_name), "template": sql_template, "type": psql.SQL(col_type), "mode": "agg"})
                generated_column_details_for_preview.append((final_col_name, type_map_display.get(col_type, col_type)))
    if event_outputs:
        event_method_configs = {"exists": ("TRUE", psql.SQL("BOOLEAN")), "countevt": ("COUNT(*)", psql.SQL("INTEGER"))}
        for method_key, is_selected in event_outputs.items():
            if is_selected and (agg_template_type := event_method_configs.get(method_key)):
                final_col_name = f"{base_new_column_name}_{method_key.lower()}"
                if not validate_column_name(final_col_name)[0]: return None, f"生成的列名 '{final_col_name}' 无效", [], []
                selected_methods_details.append({"name": final_col_name, "ident": psql.Identifier(final_col_name), "template": agg_template_type[0], "type": agg_template_type[1], "mode": "agg"})
                generated_column_details_for_preview.append((final_col_name, type_map_display.get(str(agg_template_type[1]), str(agg_template_type[1]))))
    
    quick_extractor_details = []
    if quick_extractors:
        for key, pattern in quick_extractors.items():
            final_col_name = f"{base_new_column_name}_{key}"
            if not validate_column_name(final_col_name)[0]: return None, f"生成的列名 '{final_col_name}' 无效", [], []
            final_query_params.append(pattern)
            quick_extractor_details.append({"name": final_col_name, "ident": psql.Identifier(final_col_name), "type": psql.SQL("TEXT")})
            generated_column_details_for_preview.append((final_col_name, "Text (Extracted)"))

    if not selected_methods_details and not quick_extractor_details: return None, "未能构建任何有效的提取列。", [], generated_column_details_for_preview

    all_ctes = [];
    if cohort_source_cte: all_ctes.append(cohort_source_cte)
    base_select_list = [psql.SQL("{}.*").format(cohort_alias)]
    if value_column_name_from_panel: base_select_list.append(psql.SQL("{}.{} AS event_value").format(event_alias, psql.Identifier(value_column_name_from_panel)))
    if time_col_for_window: base_select_list.append(psql.SQL("{}.{} AS event_time").format(event_alias, psql.Identifier(time_col_for_window)))
    
    # --- THIS IS THE KEY FIX ---
    # Change how quick extractors are handled
    if quick_extractor_details:
        lateral_joins = []
        for i, _ in enumerate(quick_extractor_details):
            # The LATERAL subquery now directly extracts the first capture group
            # (REGEXP_MATCHES(...))[1] will be NULL if no match, which is safe for LEFT JOIN
            lateral_joins.append(
                psql.SQL("LEFT JOIN LATERAL (SELECT (REGEXP_MATCHES({val_col}, %s, 'i'))[1]) AS {alias}(val) ON TRUE").format(
                    val_col=psql.Identifier('evt', value_column_name_from_panel),
                    alias=psql.Identifier(f"match_{i}")
                )
            )
            base_select_list.append(psql.SQL("{alias}.val AS {col_name}").format(
                alias=psql.Identifier(f"match_{i}"),
                col_name=quick_extractor_details[i]['ident']
            ))
        from_join_clause_for_cte += psql.SQL(" ").join(lateral_joins)

    filtered_events_cte = psql.SQL("FilteredEvents AS (SELECT {select_list} {from_join} WHERE {conditions})").format(
        select_list=psql.SQL(', ').join(base_select_list), from_join=from_join_clause_for_cte,
        conditions=psql.SQL(' AND ').join(all_where_conditions) if all_where_conditions else psql.SQL("TRUE")
    )
    all_ctes.append(filtered_events_cte)
    
    group_by_key_ident = psql.Identifier(cohort_join_key); agg_cols = [psql.SQL("{key}").format(key=group_by_key_ident)]
    for detail in selected_methods_details:
        if detail["mode"] == "agg":
            agg_expr = psql.SQL(detail["template"]).format(val_col=psql.Identifier('event_value'), time_col=psql.Identifier('event_time'))
            agg_cols.append(psql.SQL("{} AS {}").format(agg_expr, detail["ident"]))
    for detail in quick_extractor_details:
        # Now we aggregate the pre-extracted single values
        agg_cols.append(psql.SQL("(ARRAY_AGG(DISTINCT {} ORDER BY {}))[1] AS {}").format(detail["ident"], detail["ident"], detail["ident"]))

    main_agg_select = psql.SQL("SELECT {agg_cols} FROM FilteredEvents GROUP BY {group_key}").format(
        agg_cols=psql.SQL(', ').join(agg_cols), group_key=group_by_key_ident
    )
    all_ctes.append(psql.SQL("AggregatedData AS ({})").format(main_agg_select))
    with_clause = psql.SQL("WITH ") + psql.SQL(', ').join(all_ctes)

    if for_execution:
        all_cols_to_add = selected_methods_details + quick_extractor_details
        alter_clauses = [psql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(d["ident"], d["type"]) for d in all_cols_to_add]
        alter_sql = psql.SQL("ALTER TABLE {target_table} ").format(target_table=target_table_ident) + psql.SQL(', ').join(alter_clauses) + psql.SQL(";")
        temp_table_name = f"temp_merge_{base_new_column_name.lower()}_{int(time.time()) % 100000}"[:63]; temp_table_ident = psql.Identifier(temp_table_name)
        create_temp_sql = psql.SQL("CREATE TEMPORARY TABLE {temp_table} AS SELECT * FROM AggregatedData;").format(temp_table=temp_table_ident)
        set_clauses = [psql.SQL("{col_to_set} = {tmp_alias}.{col_from_tmp}").format(col_to_set=d["ident"], tmp_alias=psql.Identifier("md"), col_from_tmp=d["ident"]) for d in all_cols_to_add]
        update_sql = psql.SQL("UPDATE {target_table} {tgt_alias} SET {set_clauses} FROM {temp_table} {tmp_alias} WHERE {tgt_alias}.{join_key} = {tmp_alias}.{join_key};").format(
            target_table=target_table_ident, tgt_alias=psql.Identifier("target"), set_clauses=psql.SQL(', ').join(set_clauses),
            temp_table=temp_table_ident, tmp_alias=psql.Identifier("md"), join_key=group_by_key_ident
        )
        drop_temp_sql = psql.SQL("DROP TABLE IF EXISTS {temp_table};").format(temp_table=temp_table_ident)
        return [(alter_sql, None), (with_clause + create_temp_sql, final_query_params), (update_sql, None), (drop_temp_sql, None)], "execution_list", base_new_column_name, generated_column_details_for_preview
    else: # Preview
        preview_select_cols = [psql.SQL("{}.*").format(cohort_alias)]
        all_cols_to_add = selected_methods_details + quick_extractor_details
        for d in all_cols_to_add: preview_select_cols.append(psql.SQL("{md_alias}.{col_ident}").format(md_alias=psql.Identifier("md"), col_ident=d["ident"]))
        preview_sql = psql.SQL("SELECT {select_cols} FROM {cohort_source} {coh_alias} LEFT JOIN AggregatedData {md_alias} ON {coh_alias}.{join_key} = {md_alias}.{join_key};").format(
            select_cols=psql.SQL(', ').join(preview_select_cols), cohort_source=final_cohort_source_ident, coh_alias=cohort_alias,
            md_alias=psql.Identifier("md"), join_key=group_by_key_ident
        )
        return with_clause + preview_sql, None, final_query_params, generated_column_details_for_preview

# --- END OF FINAL ROBUST VERSION ---