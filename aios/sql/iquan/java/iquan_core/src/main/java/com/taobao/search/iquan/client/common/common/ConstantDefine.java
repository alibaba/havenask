package com.taobao.search.iquan.client.common.common;

public interface ConstantDefine {
    // common
    String RANGE = "range";
    String GROUPING = "grouping";
    String STRING = "string";
    String INT = "int";
    String LAYER_STRING_ANY = "***layer_string_any***";
    String EMPTY = "";
    String LIST_SEPERATE = ";";

    // sql response
    String ERROR_CODE = "error_code";
    String ERROR_MESSAGE = "error_message";
    String DEBUG_INFOS = "debug_infos";
    String RESULT = "result";

    // model
    String CATALOG_NAME = "catalog_name";
    String DATABASE_NAME = "database_name";

    String TABLE_VERSION = "table_version";
    String TABLE_NAME = "table_name";
    String ALIAS_NAMES = "alias_names";
    String TABLE_TYPE = "table_type";
    String TABLE_CONTENT_VERSION = "table_content_version";
    String TABLE_CONTENT = "table_content";
    String LAYER_TABLE_NAME = "layer_table_name";
    String CONTENT = "content";

    String FUNCTION_VERSION = "function_version";
    String FUNCTION_NAME = "function_name";
    String FUNCTION_TYPE = "function_type";
    String IS_DETERMINISTIC = "is_deterministic";
    String FUNCTION_CONTENT_VERSION = "function_content_version";
    String FUNCTION_CONTENT = "function_content";

    // service
    String FUNCTIONS = "functions";
    String TVF_FUNCTIONS = "tvf_functions";
    String TABLES = "tables";
    String LAYER_TABLES = "layer_tables";
    String COMPUTE_NODES = "compute_nodes";
    String SQLS = "sqls";
    String DEFAULT_CATALOG_NAME = "default_catalog_name";
    String DEFAULT_DATABASE_NAME = "default_database_name";
    String DYNAMIC_PARAMS = "dynamic_params";
    String SQL_PARAMS = "sql_params";

    // other
    String LAYER_TABLE_PLAN_META = "layer_table_plan_meta";
    int INIT_FB_SIZE = 4 * 1024;
}
