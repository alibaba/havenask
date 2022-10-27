#ifndef ISEARCH_SQL_COMMON_H
#define ISEARCH_SQL_COMMON_H

#include <ha3/common.h>
#include <ha3/isearch.h>

BEGIN_HA3_NAMESPACE(sql)

#define SQL_DEFAULT_CATALOG_NAME "default"

#define SQL_CLAUSE_SPLIT "&&"
#define SQL_KV_CLAUSE_SPLIT ";"
#define SQL_KV_SPLIT ":"
#define SQL_QUERY_CLAUSE "query="
#define SQL_KVPAIR_CLAUSE "kvpair="

#define SQL_BIZ_NAME "bizName"
#define SQL_TIMEOUT "timeout"
#define SQL_FORMAT_TYPE "formatType"
#define SQL_FORMAT_TYPE_NEW "format"
#define SQL_FORMAT_DESC "formatDesc"
#define SQL_TRACE_LEVEL "trace"
#define SQL_GET_SEARCH_INFO "searchInfo"
#define SQL_GET_SQL_PLAN "sqlPlan"
#define SQL_FORBIT_MERGE_SEARCH_INFO "forbitMergeSearchInfo"
#define SQL_RESULT_READABLE "resultReadable"
#define SQL_DYNAMIC_PARAMS "dynamic_params"
#define SQL_URLENCODE_DATA "urlencode_data"
#define SQL_PARALLEL_NUMBER "parallel"
#define SQL_PARALLEL_TABLES "parallelTables"
#define SQL_PARALLEL_TABLES_SPLIT "|"
#define SQL_LACK_RESULT_ENABLE "lackResultEnable"
#define SQL_CATALOG_NAME "catalogName"
#define SQL_DATABASE_NAME "databaseName"
#define SQL_PREPARE_LEVEL "iquan.plan.prepare.level"
#define SQL_ENABLE_CACHE "enableSqlCache"
#define SQL_IQUAN_PLAN_FORMAT_VERSION "iquan.plan.format.version"
#define SQL_DEFAULT_VALUE_IQUAN_PLAN_FORMAT_VERSION "plan_version_0.0.1"
#define SQL_IQUAN_OPTIMIZER_TABLE_SUMMARY_SUFFIX "iquan.optimizer.table.summary.suffix"

#define SQL_COLUMN_PREFIX '$'

#define SQL_UNKNOWN_OP "UNKNOWN"
#define SQL_AND_OP "AND"
#define SQL_OR_OP "OR"
#define SQL_EQUAL_OP "="
#define SQL_GT_OP ">"
#define SQL_GE_OP ">="
#define SQL_LT_OP "<"
#define SQL_LE_OP "<="
#define SQL_NOT_OP "NOT"
#define SQL_NOT_EQUAL_OP "<>"
#define HA3_NOT_EQUAL_OP "!="
#define SQL_LIKE_OP "LIKE"
#define SQL_IN_OP "IN"
#define SQL_NOT_IN_OP "NOT IN"
#define SQL_CASE_OP "CASE"
#define SQL_ITEM_OP "ITEM"

#define SQL_UDF_OP "UDF"
#define SQL_UDF_MATCH_OP "MATCHINDEX"
#define SQL_UDF_QUERY_OP "QUERY"
#define SQL_UDF_CAST_OP "CAST"
#define SQL_UDF_MULTI_CAST_OP "MULTICAST"
#define SQL_UDF_ANY_OP "ANY"
#define SQL_UDF_CONTAIN_OP "contain"
#define SQL_UDF_HA_IN_OP "ha_in"
#define SQL_UDF_SP_QUERY_MATCH_OP "sp_query_match"

#define SQL_CONDITION_OPERATOR "op"
#define SQL_CONDITION_TYPE "type"
#define SQL_CONDITION_PARAMETER "params"

#define SQL_FUNC_KVPAIR_SEP ','
#define SQL_FUNC_KVPAIR_KV_SEP ':'
#define SQL_FUNC_KVPAIR_VALUE_SEP ';'
#define SQL_FUNC_KVPAIR_VALUE_VALUE_SEP '#'

#define SQL_IQUAN_SQL_PARAMS_PREFIX "iquan."
#define SQL_IQUAN_EXEC_PARAMS_PREFIX "exec."
#define SQL_IQUAN_EXEC_PARAMS_SOURCE_ID "exec.source.id"
#define SQL_IQUAN_EXEC_PARAMS_SOURCE_SPEC "exec.source.spec"
#define SQL_IQUAN_PLAN_PREPARE_LEVEL "iquan.plan.prepare.level"
#define SQL_IQUAN_PLAN_CACHE_ENABLE "iquan.plan.cache.enable"
#define SQL_SOURCE_SPEC_EMPTY "empty"
// hint
const std::string SQL_SCAN_HINT = "SCAN_ATTR";
const std::string SQL_JOIN_HINT = "JOIN_ATTR";
const std::string SQL_AGG_HINT = "AGG_ATTR";

// join type
const std::string SQL_INNER_JOIN_TYPE = "INNER";
const std::string SQL_LEFT_JOIN_TYPE = "LEFT";
const std::string SQL_SEMI_JOIN_TYPE = "SEMI";
const std::string SQL_ANTI_JOIN_TYPE = "ANTI";

// op name
const std::string EXCHANGE_OP = "Exchange";
const std::string SCAN_OP = "ScanKernel";
const std::string MERGE_OP = "MergeKernel";
const std::string HASH_JOIN_OP = "HashJoinKernel";

// parallel
const std::string PARALLEL_NUM_ATTR = "parallel_num";
constexpr size_t PARALLEL_NUM_MAX_LIMIT = 16;
constexpr size_t PARALLEL_NUM_MIN_LIMIT = 1;
const std::string PARALLEL_INDEX_ATTR = "parallel_index";

const std::string DEFAULT_INPUT_PORT = "input0";
const std::string DEFAULT_OUTPUT_PORT = "output0";

//common
const std::string IQUAN_OP_ID = "op_id";

// scan kernel
const std::string SCAN_TABLE_NAME_ATTR = "table_name";
const std::string SCAN_NORMAL_TABLE_TYPE = "normal";
const std::string SCAN_SUMMARY_TABLE_TYPE = "summary";
const std::string SCAN_KV_TABLE_TYPE = "kv";
const std::string SCAN_KKV_TABLE_TYPE = "kkv";
const std::string SCAN_LOGICAL_TABLE_TYPE = "logical";


// exchange kernel
const std::string EXCHANGE_GRAPH_ATTR = "graph";
const std::string EXCHANGE_INPUT_PORT_ATTR = "input_port";
const std::string EXCHANGE_INPUT_NODE_ATTR = "input_node";
const std::string EXCHANGE_SEARCH_METHOD_NAME = "runGraph";
const std::string EXCHANGE_LACK_RESULT_ENABLE = "lack_result_enable";

// sub graph run param
const std::string SUB_GRAPH_RUN_PARAM_TIMEOUT = "sub_graph_run_param_timeout";
const std::string SUB_GRAPH_RUN_PARAM_TRACE_LEVEL = "sub_graph_run_param_trace_level";
const std::string SUB_GRAPH_RUN_PARAM_THREAD_LIMIT = "sub_graph_run_param_thread_limit";

// agg kernel
const std::string AGG_OUTPUT_FIELD_ATTRIBUTE = "output_fields";
const std::string AGG_OUTPUT_FIELD_TYPE_ATTRIBUTE = "output_fields_type";
const std::string AGG_SCOPE_ATTRIBUTE = "scope";
const std::string AGG_FUNCTION_ATTRIBUTE = "agg_funcs";
const std::string AGG_GROUP_BY_KEY_ATTRIBUTE = "group_fields";
constexpr size_t DEFAULT_GROUP_KEY_COUNT = 5000000;
constexpr size_t DEFAULT_AGG_MEMORY_LIMIT = 512 * 1024 * 1024; // 512M

// table
constexpr uint32_t DEFAULT_BATCH_COUNT = 8 * 1024;
constexpr uint32_t DEFAULT_BATCH_SIZE = 4 * 1024 * 1024; //4MB
constexpr size_t NEED_COMPACT_MEM_SIZE = 4 * 1024 * 1024; // 4MB

// pool size
constexpr size_t MAX_SQL_POOL_SIZE = 512 * 1024 * 1024; // 512MB

// timeout
constexpr double SCAN_TIMEOUT_PERCENT = 0.8;

// formatter
constexpr size_t SQL_TSDB_RESULT_MEM_LIMIT = 256 * 1024 * 1024; // 256M

// scan attribute for nest table join type
enum NestTableJoinType { LEFT_JOIN, INNER_JOIN };

enum class KernelScope {
    NORMAL,
    PARTIAL,
    FINAL,
    UNKNOWN,
};

END_HA3_NAMESPACE(sql)

#endif // ISEARCH_SQL_COMMON_H_
