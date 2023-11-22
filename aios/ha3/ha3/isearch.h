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

#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/LongHashValue.h"
#include "indexlib/indexlib.h" // IWYU pragma: keep
#include "matchdoc/CommonDefine.h"
#include "matchdoc/ValueType.h"

typedef autil::uint128_t primarykey_t;

typedef int32_t fieldboost_t;

typedef uint32_t fieldbitmap_t;
const fieldbitmap_t INVALID_FIELDBITMAP = (fieldbitmap_t)0xFFFFFFFF;

typedef double score_t;

typedef int64_t operation_id_t;
typedef uint32_t FullIndexVersion;
typedef uint32_t quota_t;

const operation_id_t INVALID_OPERATION_ID = -1;

#define END_DOCID ((docid_t)0x7FFFFFFF)
#define MAX_QUOTA ((quota_t)-1)
//#define UNINITIALIZED_DOCID ((docid_t)-2)

const uint8_t SL_QRS = SL_PROXY;

typedef matchdoc::BuiltinType VariableType;

enum VRGroupKey {
    FOR_DISTINCT = 1, // hack flag, to declare DistinctInfo.
    FOR_ATTRIBUTE,    // flag to show attribute in result.
    FOR_USER_DATA,    // flag to show user data in result.
};

enum QueryTermType {
    AND_TERM = 0,
    OR_TERM = 1,
    ANDNOT_TERM = 2,
    RANK_TERM = 3,
};

enum JoinType {
    DEFAULT_JOIN = 0,
    WEAK_JOIN = 1,
    STRONG_JOIN = 2
};

enum SubDocDisplayType {
    SUB_DOC_DISPLAY_NO = 0,
    SUB_DOC_DISPLAY_FLAT = 1,
    SUB_DOC_DISPLAY_GROUP = 2
};

enum TermType {
    TT_STRING = 0,
    TT_NUMBER
};

enum QueryType {
    TERM_QUERY = 0,
    NUMBER_QUERY = 1,
    PHRASE_QUERY = 2,
    AND_QUERY = 3,
    OR_QUERY = 4,
    RANK_QUERY = 5,
    ANDNOT_QUERY = 6,
    MULTI_TERM_QUERY = 7,
    TABLE_QUERY = 8,
    DOCIDS_QUERY = 9,
};

enum AttributeExprScope {
    AE_DEFAULT = 0,
    AE_FILTER = 1,
    AE_SCORER = 2,
    AE_SORTER = 4,
};

enum QueryOperator {
    OP_UNKNOWN = 0,
    OP_AND = 1,
    OP_OR = 2,
    OP_WEAKAND = 3,
};

enum ResultFormatType {
    RF_UNKNOWN,
    RF_XML,
    RF_PROTOBUF,
    RF_FB_SUMMARY,
    RF_JSON
};

enum FetchSummaryType {
    BY_UNKNOWN,
    BY_DOCID,
    BY_PK,
    BY_RAWPK
};

enum QuotaMode {
    QM_PER_DOC,
    QM_PER_LAYER,
    QM_UNKNOWN,
};

enum QuotaType {
    QT_PROPOTION = 0, // propotion by per range doc count, must be 0
    QT_AVERAGE,       // average
    QT_QUOTA,
    QT_UNKNOW
};

enum DispatchType {
    DISPATCH_BY_PARTITION,
    DISPATCH_BY_REPLICA
};

enum MergeInStep {
    MERGE_IN_STEP_FULL,
    MERGE_IN_STEP_BEGIN,
    MERGE_IN_STEP_DOING,
    MERGE_IN_STEP_END
};

enum SummarySearchType {
    SUMMARY_SEARCH_NORMAL = 0,
    SUMMARY_SEARCH_EXTRA,
    SUMMARY_SEARCH_COUNT
};

enum SessionSrcType {
    SESSION_SRC_ARPC = 0,
    SESSION_SRC_HTTP = 1,
    SESSION_SRC_UNKNOWN = 3,
};

// must be 2^n
enum PBResultType {
    SUMMARY_IN_BYTES = 1,
    PB_MATCHDOC_FORMAT = 2,
};

inline std::string transSessionSrcType(SessionSrcType type) {
    if (type == SESSION_SRC_ARPC) {
        return "arpc";
    } else if (type == SESSION_SRC_HTTP) {
        return "http";
    } else {
        return "unknown";
    }
}

constexpr uint32_t SEARCH_PHASE_ONE = 1;
constexpr uint32_t SEARCH_PHASE_TWO = 2;

typedef std::map<std::string, std::string> KeyValueMap;
typedef std::shared_ptr<KeyValueMap> KeyValueMapPtr;

inline std::string getValueFromKeyValueMap(const std::map<std::string, std::string> &keyValueMap,
                                           const std::string &key,
                                           const std::string &defaultValue = "") {
    KeyValueMap::const_iterator iter = keyValueMap.find(key);
    if (iter != keyValueMap.end()) {
        return iter->second;
    }
    return defaultValue;
}

inline std::string getBenchmarkBizName(const std::string &bizName) {
    return bizName + ":t";
}

constexpr uint32_t MAX_AGGREGATE_GROUP_COUNT = 1000;

constexpr char DOCID_PARTITION_MODE[] = "docid";
constexpr char NULL_CLUSTER[] = "";

// build mode
typedef std::string BuildMode;
typedef std::string BuildPhase;

// used to determine which hash range a document belongs to
typedef uint16_t hashid_t;
typedef uint16_t clusterid_t;
constexpr clusterid_t INVALID_CLUSTERID = (clusterid_t)-1;

constexpr int32_t DEFAULT_BOOST_VALUE = 100;
constexpr uint32_t MAX_RERANK_SIZE = 1000000;
constexpr uint32_t MIN_INDEX_SWITCH_LIMIT = 150;
constexpr uint32_t READER_DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024;

constexpr char NULL_STRING[] = "";
constexpr char DYNAMIC_CUT_SEPARATOR[] = "\x1B";
constexpr char LAYERKEY_DOCID[] = "%docid";
constexpr char LAYERKEY_OTHER[] = "%other";
constexpr char LAYERKEY_SEGMENTID[] = "%segmentid";
constexpr char LAYERKEY_PERCENT[] = "%percent";
constexpr char LAYERKEY_UNSORTED[] = "%unsorted";
constexpr char LAYERKEY_SORTED[] = "%sorted";

constexpr char PLUGIN_PATH_NAME[] = "plugins/";

constexpr char DEFAULT_QRS_CHAIN[] = "DEFAULT";
constexpr char DEFAULT_DEBUG_QRS_CHAIN[] = "_@_build_in_DebugQueryMatchQrsChain";
constexpr char DEFAULT_DEBUG_PROCESSOR[] = "_@_build_in_MatchInfoProcessor";
constexpr char DEFAULT_DEBUG_RANK_PROFILE[] = "_@_build_in_DebugQueryMatchRankProfile";
constexpr char DEFAULT_DEBUG_SCORER[] = "_@_build_in_RecordInfoScorer";

constexpr char DEFAULT_RANK_PROFILE[] = "DefaultProfile";

constexpr char DEFAULT_SUMMARY_PROFILE[] = "DefaultProfile";
constexpr char LOCAL_URI_PREFIX[] = "file://";

constexpr char FETCHSUMMARY_GID_SEPERATOR[] = "|";
constexpr char FETCHSUMMARY_GID_SEPERATOR_DEPRECATED[] = "_";

// constexpr char BUILD_IN_REFERENCE_PREFIX[] = "_@_build_in_";
// constexpr char PROVIDER_VAR_NAME_PREFIX[] = "_@_user_data_";
// constexpr char ISEARCH_BUILD_IN_JOIN_DOCID_REF_PREIX[] = "_@_join_docid_";
// constexpr char ISEARCH_BUILD_IN_SUBJOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX[] = "_@_subjoin_docid_";
constexpr char STATUS_CHECK_PREFIX[] = "status";
constexpr char HTTP_SUPPORT_PREFIX[] = "httpsupport:";
constexpr char LIST_CMD[] = "ls";
constexpr char GET_FILE_CONTENT_CMD[] = "file";

constexpr char RESULT_FORMAT_XML[] = "xml";
constexpr char RESULT_FORMAT_PROTOBUF[] = "protobuf";
constexpr char RESULT_FORMAT_JSON[] = "json";
constexpr char RESULT_FORMAT_FB_SUMMARY[] = "fb_summary";

constexpr char LOCAL_USER_NAME_DEFAULT[] = "ha";
constexpr char LOCAL_SERVICE_NAME_DEFAULT[] = "local_service";

constexpr char HEART_BEAT_META_INFO_KEY[] = "WorkerInfo";
constexpr char HEART_BEAT_CLUSTER_NAME[] = "__ha3_admin_cluster_name_";

// raw document format
constexpr char RAW_DOCUMENT_FORMAT_HA3[] = "ha3";
constexpr char RAW_DOCUMENT_FORMAT_ISEARCH[] = "isearch";

constexpr char FSUTIL_BINARY[] = "fs_util_bin";

constexpr uint32_t MAX_PARTITION_RANGE = 65535;
constexpr uint32_t MAX_PARTITION_COUNT = MAX_PARTITION_RANGE + 1;
constexpr int64_t UPDATE_DEPLOY_STATUS_INTERVAL = 5 * 1000 * 1000;   // 5s
constexpr int64_t WORKER_MANAGER_INTERVAL = 30 * 1000 * 1000;        // 30s
constexpr int64_t DOWNLOAD_LOOP_INTERVAL = 5 * 1000 * 1000;          // 5s
constexpr int64_t SERIALIZE_WORKER_ERROR_INTERVAL = 5 * 1000 * 1000; // 5s
constexpr int64_t REOPEN_INDEX_PARTITION_INTERVAL = 2 * 1000 * 1000; // 2s
constexpr int64_t SCAN_INDEX_INTERVAL = 6 * 1000 * 1000;             // 6s
constexpr int32_t KEEP_INCREMENTAL_VERSION_COUNT = 10;
constexpr int64_t APP_ITERATOR_INTERVAL = 100 * 1000;        // 100ms
constexpr int64_t ARPC_RECONNECT_INTERVAL = 3 * 1000 * 1000; // 3s
constexpr int64_t BUILDER_MONITOR_INTERVAL = 500 * 1000;     // 0.5s
constexpr int32_t LOCAL_BUILDER_METRICS_STDOUT_COUNT = 10;   // BUILDER_MONITOR_INTERVAL * 10

constexpr int32_t SERVER_MAX_IDLE_TIME = 100 * 7200 * 1000; // 200h
constexpr int32_t SERVER_TIMEOUT = 5 * 1000;                // 5s

constexpr uint32_t FETCHSUMMARY_GID_FIELD_SIZE = 5;
constexpr uint32_t STATUS_CHECK_PREFIX_LEN = 6;
constexpr uint32_t HTTP_SUPPORT_PREFIX_LEN = 12;
constexpr int64_t GET_FILE_SIZE_LIMIT = 100 * 1024; // 100k

constexpr int DEFAULT_WORKER_RESOURCE = 100;
constexpr int DEFAULT_PARTITION_RESOURCE = DEFAULT_WORKER_RESOURCE;
constexpr int64_t OLD_SNAPSHOT_EXPIRE_TIME = 5 * 1000 * 1000; // 5s

#define HA3_REQUEST_VERSION 3700000
#define HA3_MAJOR_VERSION 3
#define HA3_MINOR_VERSION 8
#define HA3_MICRO_VERSION 0

constexpr int32_t OLD_SEARCH_REQUEST_VERSION
    = HA3_MAJOR_VERSION * 1000000 + HA3_MINOR_VERSION * 10000 + HA3_MICRO_VERSION * 100 + 1;

// compatible suez version, change when request serial
constexpr int32_t SEARCH_REQUEST_VERSION
    = (HA3_REQUEST_VERSION == 0) ? OLD_SEARCH_REQUEST_VERSION : HA3_REQUEST_VERSION;

// HA3_MAJOR_VERSION.HA3_MINOR_VERSION.HA3_MICRO_VERSION_rc1

constexpr uint32_t INVALID_SEARCHER_CACHE_EXPIRE_TIME = uint32_t(-1);

constexpr uint32_t REALTIME_UNLIMIT_QPS = 0;
constexpr uint32_t DEFAULT_WORKER_CAPABILITY = 16;

constexpr char ha3_runtime_version[] = "ha3_runtime_version=3.8.0";
constexpr char DEFAULT_TASK_QUEUE_NAME[] = "__ha3_runtime_default_task_queue";

extern const std::string HA3_EMPTY_STRING;

/// This macro can be appended to a function definition to generate a compiler warning if the result
/// is ignored.
#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
