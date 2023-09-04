/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>

namespace sql {

constexpr char SQL_CLAUSE_SPLIT[] = "&&";
constexpr char SQL_KV_CLAUSE_SPLIT = ';';
constexpr char SQL_KV_SPLIT = ':';
constexpr char SQL_HINT_KV_PAIR_SPLIT = ';';
constexpr char SQL_HINT_KV_SPLIT = ':';
constexpr char SQL_MODEL_KV_PAIR_SPLIT = ',';
constexpr char SQL_MODEL_KV_SPLIT = ':';
constexpr char SQL_QUERY_CLAUSE[] = "query=";
constexpr char SQL_KVPAIR_CLAUSE[] = "kvpair=";
constexpr char SQL_AUTH_TOKEN_CLAUSE[] = "authToken=";
constexpr char SQL_AUTH_SIGNATURE_CLAUSE[] = "authSignature=";

constexpr char SQL_BIZ_NAME[] = "bizName";
constexpr char SQL_TIMEOUT[] = "timeout";
constexpr char SQL_FORMAT_TYPE[] = "formatType";
constexpr char SQL_FORMAT_TYPE_NEW[] = "format";
constexpr char SQL_FORMAT_DESC[] = "formatDesc";
constexpr char SQL_TRACE_LEVEL[] = "trace";
constexpr char SQL_GET_SEARCH_INFO[] = "searchInfo";
constexpr char SQL_GET_RESULT_COMPRESS_TYPE[] = "resultCompressType";
constexpr char SQL_GET_SQL_PLAN[] = "sqlPlan";
constexpr char SQL_FORBIT_MERGE_SEARCH_INFO[] = "forbitMergeSearchInfo";
constexpr char SQL_FORBIT_SEARCH_INFO[] = "forbitSearchInfo";
constexpr char SQL_RESULT_READABLE[] = "resultReadable";
constexpr char SQL_RESULT_ALLOW_SOFT_FAILURE[] = "resultAllowSoftFailure";
constexpr char SQL_DYNAMIC_PARAMS[] = "dynamic_params";
constexpr char SQL_DYNAMIC_KV_PARAMS[] = "dynamic_kv_params";
constexpr char SQL_HINT_PARAMS[] = "hint_params";
constexpr char SQL_URLENCODE_DATA[] = "urlencode_data";
constexpr char SQL_PARALLEL_NUMBER[] = "parallel";
constexpr char SQL_PARALLEL_TABLES[] = "parallelTables";
constexpr char SQL_PARALLEL_TABLES_SPLIT = '|';
constexpr char SQL_LACK_RESULT_ENABLE[] = "lackResultEnable";
constexpr char SQL_CATALOG_NAME[] = "catalogName";
constexpr char SQL_DATABASE_NAME[] = "databaseName";
constexpr char SQL_PREPARE_LEVEL[] = "iquan.plan.prepare.level";
constexpr char SQL_ENABLE_CACHE[] = "enableSqlCache";
constexpr char SQL_IQUAN_PLAN_FORMAT_VERSION[] = "iquan.plan.format.version";
constexpr char SQL_DEFAULT_VALUE_IQUAN_PLAN_FORMAT_VERSION[] = "plan_version_0.0.1";
constexpr char SQL_IQUAN_OPTIMIZER_TABLE_SUMMARY_SUFFIX[] = "iquan.optimizer.table.summary.suffix";

constexpr char SQL_COLUMN_PREFIX = '$';

constexpr char SQL_UNKNOWN_OP[] = "UNKNOWN";
constexpr char SQL_AND_OP[] = "AND";
constexpr char SQL_OR_OP[] = "OR";
constexpr char SQL_EQUAL_OP[] = "=";
constexpr char SQL_MULTI_EQUAL_OP[] = ">>";
constexpr char SQL_GT_OP[] = ">";
constexpr char SQL_GE_OP[] = ">=";
constexpr char SQL_LT_OP[] = "<";
constexpr char SQL_LE_OP[] = "<=";
constexpr char SQL_NOT_OP[] = "NOT";
constexpr char SQL_NOT_EQUAL_OP[] = "<>";
constexpr char HA3_NOT_EQUAL_OP[] = "!=";
constexpr char SQL_LIKE_OP[] = "LIKE";
constexpr char SQL_IN_OP[] = "IN";
constexpr char SQL_NOT_IN_OP[] = "NOT IN";
constexpr char SQL_CASE_OP[] = "CASE";
constexpr char SQL_ITEM_OP[] = "ITEM";

constexpr char SQL_MATCH_TYPE_SIMPLE[] = "simple";
constexpr char SQL_MATCH_TYPE_SUB[] = "sub";
constexpr char SQL_MATCH_TYPE_VALUE[] = "value";
constexpr char SQL_MATCH_TYPE_FULL[] = "full";

constexpr char SQL_UDF_OP[] = "UDF";
constexpr char SQL_UDF_MATCH_OP[] = "MATCHINDEX";
constexpr char SQL_UDF_QUERY_OP[] = "QUERY";
constexpr char SQL_UDF_CAST_OP[] = "CAST";
constexpr char SQL_UDF_MULTI_CAST_OP[] = "MULTICAST";
constexpr char SQL_UDF_ANY_OP[] = "ANY";
constexpr char SQL_UDF_CONTAIN_OP[] = "contain";
constexpr char SQL_UDF_NOT_CONTAIN_OP[] = "notcontain";
constexpr char SQL_UDF_HA_IN_OP[] = "ha_in";
constexpr char SQL_UDF_NOT_HA_IN_OP[] = "not_ha_in";
constexpr char SQL_UDF_PIDVID_CONTAIN_OP[] = "pidvid_contain";
constexpr char SQL_UDF_AITHETA_OP[] = "aitheta";
constexpr char SQL_UDF_SP_QUERY_MATCH_OP[] = "sp_query_match";
constexpr char SQL_UDF_KV_EXTRACT_MATCH_OP[] = "kv_extract_match";

constexpr char SQL_TVF_OP[] = "TVF";

constexpr char SQL_CONDITION_OPERATOR[] = "op";
constexpr char SQL_CONDITION_TYPE[] = "type";
constexpr char SQL_CONDITION_PARAMETER[] = "params";

constexpr char SQL_FUNC_KVPAIR_SEP = ',';
constexpr char SQL_FUNC_KVPAIR_KV_SEP = ':';
constexpr char SQL_FUNC_KVPAIR_VALUE_SEP = ';';
constexpr char SQL_FUNC_KVPAIR_VALUE_VALUE_SEP = '#';

constexpr char SQL_IQUAN_SQL_PARAMS_PREFIX[] = "iquan.";
constexpr char SQL_IQUAN_EXEC_PARAMS_PREFIX[] = "exec.";
constexpr char SQL_IQUAN_EXEC_PARAMS_SOURCE_ID[] = "exec.source.id";
constexpr char SQL_IQUAN_EXEC_PARAMS_SOURCE_SPEC[] = "exec.source.spec";
constexpr char SQL_IQUAN_EXEC_PARAMS_TASK_QUEUE[] = "exec.task.queue";
constexpr char SQL_IQUAN_EXEC_PARAMS_USER_KV[] = "exec.user.kv";
constexpr char SQL_IQUAN_PLAN_PREPARE_LEVEL[] = "iquan.plan.prepare.level";
constexpr char SQL_IQUAN_PLAN_CACHE_ENABLE[] = "iquan.plan.cache.enable";
constexpr char SQL_IQUAN_CTE_OPT_VER[] = "iquan.cte.opt.ver";
constexpr char SQL_IQUAN_OPTIMIZER_TURBOJET_ENABLE[] = "iquan.optimizer.turbojet.enable";
constexpr char SQL_SOURCE_SPEC_EMPTY[] = "empty";

// hint
extern const std::string SQL_SCAN_HINT;
extern const std::string SQL_JOIN_HINT;
extern const std::string SQL_AGG_HINT;

// join type
extern const std::string SQL_INNER_JOIN_TYPE;
extern const std::string SQL_LEFT_JOIN_TYPE;
extern const std::string SQL_SEMI_JOIN_TYPE;
extern const std::string SQL_ANTI_JOIN_TYPE;
extern const std::string SQL_LEFT_MULTI_JOIN_TYPE;

// op name
extern const std::string EXCHANGE_OP;
extern const std::string SCAN_OP;
extern const std::string MERGE_OP;
extern const std::string HASH_JOIN_OP;
extern const std::string UNION_OP;
extern const std::string DELAY_DP_OP;

constexpr char TABLET_ORC_SCAN_OP[] = "sql.TabletOrcScanKernel";
constexpr char KHRONOS_SCAN_OP[] = "sql.KhronosTableScanKernel";
constexpr char ASYNC_SCAN_OP[] = "sql.AsyncScanKernel";
constexpr char EXTERNAL_SCAN_OP[] = "sql.ExternalScanKernel";

// timeline and perf
extern const std::string COLLECT_NAVI_TIMELINE;
extern const std::string COLLECT_NAVI_PERF;

// parallel
extern const std::string PARALLEL_NUM_ATTR;
constexpr size_t PARALLEL_NUM_MAX_LIMIT = 16;
constexpr size_t PARALLEL_NUM_MIN_LIMIT = 1;
extern const std::string PARALLEL_INDEX_ATTR;

extern const std::string DEFAULT_INPUT_PORT;
extern const std::string DEFAULT_OUTPUT_PORT;

// common
extern const std::string IQUAN_OP_ID;

// scan kernel
extern const std::string SCAN_TABLE_NAME_ATTR;
extern const std::string SCAN_NORMAL_TABLE_TYPE;
extern const std::string SCAN_SUMMARY_TABLE_TYPE;
extern const std::string SCAN_KV_TABLE_TYPE;
extern const std::string SCAN_KKV_TABLE_TYPE;
extern const std::string SCAN_LOGICAL_TABLE_TYPE;
extern const std::string SCAN_HOLO_TABLE_TYPE;
extern const std::string SCAN_EXTERNAL_TABLE_TYPE;
extern const std::string SCAN_AGGREGATION_TABLE_TYPE;
extern const std::string SCAN_TARGET_WATERMARK;
extern const std::string SCAN_TARGET_WATERMARK_TYPE;

// exchange kernel
extern const std::string EXCHANGE_GRAPH_ATTR;
extern const std::string EXCHANGE_INPUT_PORT_ATTR;
extern const std::string EXCHANGE_INPUT_NODE_ATTR;
extern const std::string EXCHANGE_SEARCH_METHOD_NAME;
extern const std::string EXCHANGE_LACK_RESULT_ENABLE;

// DelayDp kernel
extern const std::string DELAY_DP_INPUT_NAMES_ATTR;
extern const std::string DELAY_DP_INPUT_DISTS_ATTR;
extern const std::string DELAY_DP_OUTPUT_NAMES_ATTR;
extern const std::string DELAY_DP_DEBUG_ATTR;
extern const std::string DELAY_DP_GRAPH_ATTR;

// sub graph run param
extern const std::string SUB_GRAPH_RUN_PARAM_TIMEOUT;
extern const std::string SUB_GRAPH_RUN_PARAM_TRACE_LEVEL;
extern const std::string SUB_GRAPH_RUN_PARAM_THREAD_LIMIT;
extern const std::string SUB_GRAPH_RUN_PARAM_OTHER_KV;

// agg kernel
extern const std::string AGG_SCOPE_ATTRIBUTE;
extern const std::string AGG_FUNCTION_ATTRIBUTE;
extern const std::string AGG_GROUP_BY_KEY_ATTRIBUTE;
constexpr size_t DEFAULT_GROUP_KEY_COUNT = 5000000;
constexpr size_t DEFAULT_AGG_MEMORY_LIMIT = 512 * 1024 * 1024; // 512M

// table modify kernel
extern const std::string TABLE_OPERATION_UPDATE;
extern const std::string TABLE_OPERATION_INSERT;
extern const std::string TABLE_OPERATION_DELETE;
extern const std::string MESSAGE_KEY_CMD;
extern const std::string MESSAGE_CMD_UPDATE;
extern const std::string MESSAGE_CMD_ADD;
extern const std::string MESSAGE_CMD_DELETE;
extern const std::string DML_REQUEST_ID_FIELD_NAME;

extern const std::string TABLET_MANAGER_RESOURCE_ID;

extern const std::string EXTERNAL_TABLE_TYPE_HA3SQL;
extern const std::string BACKTICK_LITERAL;
extern const std::string ESCAPED_BACKTICK_LITERAL;

// table
constexpr uint32_t DEFAULT_BATCH_COUNT = 8 * 1024;
constexpr uint32_t DEFAULT_BATCH_SIZE = 4 * 1024 * 1024;  // 4MB
constexpr size_t NEED_COMPACT_MEM_SIZE = 4 * 1024 * 1024; // 4MB

// pool size
constexpr size_t MAX_SQL_POOL_SIZE = 512 * 1024 * 1024; // 512MB

// timeout
constexpr double SCAN_TIMEOUT_PERCENT = 0.8;

// formatter
constexpr size_t SQL_TSDB_RESULT_MEM_LIMIT = 256 * 1024 * 1024; // 256M

// scan attribute for nest table join type
enum NestTableJoinType {
    LEFT_JOIN,
    INNER_JOIN
};

enum class KernelScope {
    NORMAL,
    PARTIAL,
    FINAL,
    UNKNOWN,
};

// sql task queue
const char SQL_DEFAULT_TASK_QUEUE_NAME[] = "__sql_runtime_default_task_queue";
} // namespace sql
