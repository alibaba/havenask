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

#include <memory>
#include <string>

enum IquanErrorCode
{
    IQUAN_OK = 0,
    IQUAN_FAIL = -1,
    IQUAN_OOM = -2,
    IQUAN_JSON_FORMAT_ERROR = -3,
    IQUAN_CANNOT_FIND_CLAZZ = -4,
    IQUAN_CANNOT_FIND_METHOD = -5,
    IQUAN_METHOD_THROW_EXCEPTION = -6,
    IQUAN_RESULT_IS_NULL = -7,
    IQUAN_PARAMS_IS_NULL = -8,
    IQUAN_INVALID_FRAME_SIZE = -9,
    IQUAN_JAVA_EXCEPTION_OCCUR = -10,
    IQUAN_FAIL_TO_GET_EXCEPTION_INFO = -11,
    IQUAN_FAIL_TO_TRANSFER_FROM_NORMAL_TO_STATUS = -12,
    IQUAN_FAIL_TO_SET_ENV = -13,
    IQUAN_FAIL_TO_CREATE_JVM = -14,
    IQUAN_INVALID_JVM_TYPE = -15,
    IQUAN_FAILED_TO_CALL_FSLIB = -16,
    IQUAN_INVALID_PARAMS = -17,
    IQUAN_INDEX_INVALID = -18,
    IQUAN_JOIN_INFO_INVALID = -19,
    IQUAN_JNI_EXCEPTION = -20,
    IQUAN_INVALID_JVM_VERSION = -21,
    IQUAN_BINARY_PATH_IS_EMPTY = -22,
    IQUAN_CLASSLOADER_IS_INVALID = -23,
    IQUAN_NEW_INSTANCE_FAILED = -24,
    IQUAN_FAILED_TO_GET_REAL_PATH = -25,
    IQUAN_FAILED_TO_GET_BINARY_PATH = -26,
    IQUAN_UNSUPPORT_TABLE_TYPE = -27,
    IQUAN_INVALID_SUBDOC_FIELD = -28,
    IQUAN_INVALID_NAVI_OBJ = -29,
    IQUAN_FAILED_TO_TRANSFOR_GRAPH = -30,
    IQUAN_RESPONSE_IS_NULL = -31,
    IQUAN_FAILED_TO_CONVERT_FLATTENBUFFER = -32,
    IQUAN_FLATTENBUFFER_FORMAT_ERROR = -33,
    IQUAN_CUSTOMIZED_TABLE_ERROR = -34,
    IQUAN_FILE_DOES_NOT_EXISTED = -35,
    IQUAN_KKV_INVALID_INDEX_FIELDS = -36,
    IQUAN_CACHE_KEY_EXPIRED = -37,
    IQUAN_CACHE_KEY_NOT_FOUND = -38,
    IQUAN_BAD_ANY_CAST = -39,
    IQUAN_FAILED_TO_OPTIMIZE_GRAPH = -40,
    IQUAN_GET_FROM_CACHE_FAILED = -41,
    IQUAN_LAYER_TABLE_ERROR = -42,
};

#define IQUAN_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;

#ifndef likely
#define likely(x) __builtin_expect((x), true)
#endif

#ifndef unlikely
#define unlikely(x) __builtin_expect((x), false)
#endif

#define IQUAN_RETURN_ERROR_IF_NULL(obj, errorCode, errorMessage)                                                       \
    if (unlikely(!obj.get())) {                                                                                        \
        return Status(errorCode, errorMessage);                                                                        \
    }

#define IQUAN_ENSURE(rc)                                                                                               \
    if (unlikely(!rc.ok())) {                                                                                          \
        return rc;                                                                                                     \
    }

#define IQUAN_ENSURE_FUNC(func)                                                                                        \
    {                                                                                                                  \
        Status rc = func;                                                                                              \
        IQUAN_ENSURE(rc)                                                                                               \
    }

constexpr char BINARY_PATH[] = "binaryPath";

// constant string for table type
constexpr char IQUAN_TABLE_TYPE_EXTERNAL[] = "external";
constexpr char IQUAN_TABLE_TYPE_NORMAL[] = "normal";
constexpr char IQUAN_TABLE_TYPE_CUSTOMIZED[] = "customized";
constexpr char IQUAN_TABLE_TYPE_KKV[] = "kkv";
constexpr char IQUAN_TABLE_TYPE_KV[] = "kv";
constexpr char IQUAN_TABLE_TYPE_SUMMARY[] = "summary";
constexpr char IQUAN_TABLE_TYPE_KHRONOS_META[] = "khronos_meta";
constexpr char IQUAN_TABLE_TYPE_KHRONOS_DATA[] = "khronos_data";
constexpr char IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA[] = "khronos_series_data";
constexpr char IQUAN_TABLE_TYPE_ORC[] = "orc";
constexpr char IQUAN_TABLE_TYPE_AGGREGATION[] = "aggregation";

// index
constexpr char KKV_PKEY[] = "prefix";
constexpr char KKV_SKEY[] = "suffix";
constexpr char KKV_PRIMARY_KEY[] = "primary_key";
constexpr char KKV_SECONDARY_KEY[] = "secondary_key";

constexpr char CUSTOM_IDENTIFIER[] = "custom_identifier";
constexpr char CUSTOM_TABLE_KHRONOS[] = "khronos";
constexpr char KHRONOS_FIELD_NAME_TS[] = "timestamp";
constexpr char KHRONOS_FIELD_NAME_WATERMARK[] = "watermark";
constexpr char KHRONOS_FIELD_NAME_METRIC[] = "metric";
constexpr char KHRONOS_FIELD_NAME_TAGK[] = "tagk";
constexpr char KHRONOS_FIELD_NAME_TAGV[] = "tagv";
constexpr char KHRONOS_FIELD_NAME_TAGS[] = "tags";
constexpr char KHRONOS_FIELD_NAME_FIELD_NAME[] = "field_name";
constexpr char KHRONOS_INDEX_TYPE_TIMESTAMP[] = "khronos_timestamp";
constexpr char KHRONOS_INDEX_TYPE_WATERMARK[] = "khronos_watermark";
constexpr char KHRONOS_INDEX_TYPE_TAG_KEY[] = "khronos_tag_key";
constexpr char KHRONOS_INDEX_TYPE_TAG_VALUE[] = "khronos_tag_value";
constexpr char KHRONOS_INDEX_TYPE_METRIC[] = "khronos_metric";
constexpr char KHRONOS_INDEX_TYPE_FIELD_NAME[] = "khronos_field_name";
constexpr char KHRONOS_INDEX_TYPE_VALUE[] = "khronos_value";
constexpr char KHRONOS_INDEX_TYPE_SERIES[] = "khronos_series";
constexpr char KHRONOS_INDEX_TYPE_SERIES_VALUE[] = "khronos_series_value_dict";
constexpr char KHRONOS_INDEX_TYPE_SERIES_STRING[] = "khronos_series_string_dict";
constexpr char KHRONOS_INDEX_TYPE_TAGS[] = "khronos_tags";
constexpr char KHRONOS_META_TABLE_NAME[] = "khronos_meta";
constexpr char KHRONOS_DATA_TABLE_NAME_DEFAULT[] = "__default__";
constexpr char KHRONOS_DATA_TABLE_NAME_SERIES[] = "series";
constexpr char KHRONOS_PARAM_KEY_VALUE_FIELD_SUFFIX[] = "value_field_suffix";
constexpr char KHRONOS_PARAM_KEY_DATA_FIELDS[] = "data_fields";
constexpr char KHRONOS_FIELD_NAME_SERIES_VALUE[] = "series_value";
constexpr char KHRONOS_FIELD_NAME_SERIES_STRING[] = "series_string";

// hash type
constexpr char KINGSO_HASH[] = "KINGSO_HASH";
constexpr char KINGSO_HASH_SEPARATOR[] = "#";
constexpr char KINGSO_HASH_PARTITION_CNT[] = "partition_cnt";

constexpr char SQL_DEFAULT_CATALOG_NAME[] = "default";
constexpr char SQL_DEFAULT_EXTERNAL_DATABASE_NAME[] = "remote";

constexpr char PK64_INDEX_TYPE[] = "PRIMARYKEY64";
constexpr char PK128_INDEX_TYPE[] = "PRIMARYKEY128";

extern const std::string IQUAN_ATTRS;
extern const std::string IQUAN_JSON;
extern const std::string IQUAN_TRUE;
extern const std::string IQUAN_FALSE;

// dynamic params
extern const std::string IQUAN_DYNAMIC_PARAMS_PREFIX;
extern const std::string IQUAN_DYNAMIC_PARAMS_SUFFIX;
extern const std::string IQUAN_REPLACE_PARAMS_PREFIX;
extern const std::string IQUAN_REPLACE_PARAMS_SUFFIX;

extern const std::string IQUAN_DYNAMIC_PARAMS_SEPARATOR;
extern const size_t IQUAN_DYNAMIC_PARAMS_MIN_SIZE;
extern const size_t IQUAN_REPLACE_PARAMS_MIN_SIZE;
extern const std::string IQUAN_DYNAMIC_PARAMS_SIMD_PADDING;

// hint params
extern const std::string IQUAN_HINT_PARAMS_PREFIX;
extern const std::string IQUAN_HINT_PARAMS_SUFFIX;
extern const size_t IQUAN_HINT_PARAMS_MIN_SIZE;

// key of sql params
constexpr char IQUAN_PLAN_PREPARE_LEVEL[] = "iquan.plan.prepare.level";
constexpr char IQUAN_PLAN_CACHE_ENALE[] = "iquan.plan.cache.enable";
constexpr char IQUAN_PLAN_FORMAT_TYPE[] = "iquan.plan.format.type";
constexpr char IQUAN_OPTIMIZER_TABLE_SUMMARY_SUFFIX[] = "iquan.optimizer.table.summary.suffix";

// value of sql param
constexpr char IQUAN_SQL_PARSE[] = "sql.parse";
constexpr char IQUAN_REL_TRANSFORM[] = "rel.transform";
constexpr char IQUAN_REL_POST_OPTIMIZE[] = "rel.post.optimize";
constexpr char IQUAN_JNI_POST_OPTIMIZE[] = "jni.post.optimize";
constexpr char IQUAN_EXEC_SOURCE_ID[] = "exec.source.id";
constexpr char IQUAN_EXEC_SOURCE_SPEC[] = "exec.source.spec";
constexpr char IQUAN_EXEC_LEADER_PREFER_LEVEL[] = "exec.leader.prefer.level";
constexpr char IQUAN_EXEC_TASK_QUEUE[] = "exec.task.queue";
constexpr char IQUAN_EXEC_USER_KV[] = "exec.user.kv";
constexpr char IQUAN_EXEC_INLINE_WORKER[] = "exec.inline.worker";
constexpr char IQUAN_EXEC_ATTR_SOURCE_ID[] = "source_id";
constexpr char IQUAN_EXEC_ATTR_SOURCE_SPEC[] = "source_spec";
constexpr char IQUAN_EXEC_ATTR_TASK_QUEUE[] = "task_queue";
constexpr char IQUAN_EXEC_QRS_BIZ_NAME[] = "exec.qrs.biz_name";
constexpr char IQUAN_EXEC_SEARCHER_BIZ_NAME[] = "exec.searcher.biz_name";
constexpr char IQUAN_EXEC_OVERRIDE_TO_LOGIC_TABLE[] = "exec.override_to_logic_table";
constexpr char IQUAN_EXEC_FORBIT_GRAPH_OUTPUT[] = "exec.forbit.graph_output";

// key of exec params
constexpr char IQUAN_SQL_OPTIMIZER_PLAN_META_GET_CACHE[] = "sql.optimizer.plan.meta.get.cache";
constexpr char IQUAN_SQL_OPTIMIZER_PLAN_META_PUT_CACHE[] = "sql.optimizer.plan.meta.put.cache";
constexpr char IQUAN_SQL_OPTIMIZER_PLAN_GET_CACHE[] = "sql.optimizer.plan.get.cache";
constexpr char IQUAN_SQL_OPTIMIZER_PLAN_PUT_CACHE[] = "sql.optimizer.plan.put.cache";

namespace iquan {
// op name
constexpr char EXCHANGE_OP[] = "Exchange";
constexpr char SCAN_OP[] = "ScanKernel";
constexpr char ASYNC_SCAN_OP[] = "AsyncScanKernel";
constexpr char KHRONOS_SCAN_OP[] = "KhronosTableScanKernel";
constexpr char TABLET_ORC_SCAN_OP[] = "TabletOrcScanKernel";
constexpr char UNION_OP[] = "UnionKernel";
constexpr char HASH_JOIN_OP[] = "HashJoinKernel";

// parallel
constexpr char PARALLEL_NUM_ATTR[] = "parallel_num";
constexpr char PARALLEL_INDEX_ATTR[] = "parallel_index";

constexpr char DEFAULT_INPUT_PORT[] = "input0";
constexpr char DEFAULT_OUTPUT_PORT[] = "output0";

// scan kernel
constexpr char SCAN_TABLE_NAME_ATTR[] = "table_name";

// exchange kernel
constexpr char EXCHANGE_GRAPH_ATTR[] = "graph";
constexpr char EXCHANGE_INPUT_PORT_ATTR[] = "input_port";
constexpr char EXCHANGE_INPUT_NODE_ATTR[] = "input_node";
constexpr char LACK_RESULT_ENABLE[] = "lack_result_enable";

// op attribute
constexpr char OP_ID[] = "op_id";
constexpr int32_t BASE_OP_ID = 10000;

// sub graph run param
constexpr size_t MAX_SUB_GRAPH = 64;
constexpr char SUB_GRAPH_RUN_PARAM_TIMEOUT[] = "sub_graph_run_param_timeout";
constexpr char SUB_GRAPH_RUN_PARAM_TRACE_LEVEL[] = "sub_graph_run_param_trace_level";
constexpr char SUB_GRAPH_RUN_PARAM_THREAD_LIMIT[] = "sub_graph_run_param_thread_limit";
constexpr char SUB_GRAPH_RUN_PARAM_OTHER_KV[] = "sub_graph_run_param_other_kv";

// navi
constexpr char SQL_OUTPUT_NAME[] = "sql_output";
} // namespace iquan
