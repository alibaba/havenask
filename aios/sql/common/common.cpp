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
#include "sql/common/common.h"

namespace sql {
// hint
const std::string SQL_SCAN_HINT = "SCAN_ATTR";
const std::string SQL_JOIN_HINT = "JOIN_ATTR";
const std::string SQL_AGG_HINT = "AGG_ATTR";

// join type
const std::string SQL_INNER_JOIN_TYPE = "INNER";
const std::string SQL_LEFT_JOIN_TYPE = "LEFT";
const std::string SQL_SEMI_JOIN_TYPE = "SEMI";
const std::string SQL_ANTI_JOIN_TYPE = "ANTI";
const std::string SQL_LEFT_MULTI_JOIN_TYPE = "LEFT_MULTI";

// op name

const std::string EXCHANGE_OP = "sql.ExchangeKernel";
const std::string SCAN_OP = "sql.ScanKernel";
const std::string MERGE_OP = "MergeKernel";
const std::string HASH_JOIN_OP = "sql.HashJoinKernel";
const std::string UNION_OP = "sql.UnionKernel";
const std::string DELAY_DP_OP = "sql.DelayDpKernel";

// timeline and perf
const std::string COLLECT_NAVI_TIMELINE = "navi_timeline";
const std::string COLLECT_NAVI_PERF = "navi_perf";

// parallel
const std::string PARALLEL_NUM_ATTR = "parallel_num";
const std::string PARALLEL_INDEX_ATTR = "parallel_index";
const std::string PARALLEL_BLOCK_COUNT_ATTR = "parallel_block_count";

const std::string DEFAULT_INPUT_PORT = "input0";
const std::string DEFAULT_OUTPUT_PORT = "output0";

// kvpair
const std::string SQL_RESULT_ALLOW_SOFT_FAILURE = "resultAllowSoftFailure";
const std::string SQL_GET_SQL_PLAN = "sqlPlan";

// common
const std::string IQUAN_OP_ID = "op_id";
const std::string SQL_QUERY_CONFIG_NAME = "sql.query_config";

// scan kernel
const std::string SCAN_TABLE_NAME_ATTR = "table_name";
const std::string SCAN_NORMAL_TABLE_TYPE = "normal";
const std::string SCAN_SUMMARY_TABLE_TYPE = "summary";
const std::string SCAN_KV_TABLE_TYPE = "kv";
const std::string SCAN_KKV_TABLE_TYPE = "kkv";
const std::string SCAN_LOGICAL_TABLE_TYPE = "logical";
const std::string SCAN_HOLO_TABLE_TYPE = "holo";
const std::string SCAN_EXTERNAL_TABLE_TYPE = "external";
const std::string SCAN_AGGREGATION_TABLE_TYPE = "aggregation";
const std::string SCAN_TARGET_WATERMARK = "target_watermark";
const std::string SCAN_TARGET_WATERMARK_TYPE = "target_watermark_type";

// exchange kernel
const std::string EXCHANGE_GRAPH_ATTR = "graph";
const std::string EXCHANGE_INPUT_PORT_ATTR = "input_port";
const std::string EXCHANGE_INPUT_NODE_ATTR = "input_node";
const std::string EXCHANGE_SEARCH_METHOD_NAME = "sqlSearch";
const std::string EXCHANGE_LACK_RESULT_ENABLE = "lack_result_enable";

// DelayDp kernel
const std::string DELAY_DP_INPUT_NAMES_ATTR = "input_names";
const std::string DELAY_DP_INPUT_DISTS_ATTR = "input_dists";
const std::string DELAY_DP_OUTPUT_NAMES_ATTR = "output_names";
const std::string DELAY_DP_DEBUG_ATTR = "debug";
const std::string DELAY_DP_GRAPH_ATTR = "graph";

// sub graph run param
const std::string SUB_GRAPH_RUN_PARAM_TIMEOUT = "sub_graph_run_param_timeout";
const std::string SUB_GRAPH_RUN_PARAM_TRACE_LEVEL = "sub_graph_run_param_trace_level";
const std::string SUB_GRAPH_RUN_PARAM_THREAD_LIMIT = "sub_graph_run_param_thread_limit";
const std::string SUB_GRAPH_RUN_PARAM_OTHER_KV = "sub_graph_run_param_other_kv";

// agg kernel
const std::string AGG_SCOPE_ATTRIBUTE = "scope";
const std::string AGG_FUNCTION_ATTRIBUTE = "agg_funcs";
const std::string AGG_GROUP_BY_KEY_ATTRIBUTE = "group_fields";

// table modify kernel
const std::string TABLE_OPERATION_UPDATE = "UPDATE";
const std::string TABLE_OPERATION_INSERT = "INSERT";
const std::string TABLE_OPERATION_DELETE = "DELETE";
const std::string MESSAGE_KEY_CMD = "CMD";
const std::string MESSAGE_CMD_UPDATE = "update_field";
const std::string MESSAGE_CMD_ADD = "add";
const std::string MESSAGE_CMD_DELETE = "delete";
const std::string DML_REQUEST_ID_FIELD_NAME = "dml_request_id";

const std::string TABLET_MANAGER_RESOURCE_ID = "TABLET_MANAGER_R";

const std::string EXTERNAL_TABLE_TYPE_HA3SQL = "HA3SQL";
const std::string BACKTICK_LITERAL = "`";
const std::string ESCAPED_BACKTICK_LITERAL = "``";

} // namespace sql
