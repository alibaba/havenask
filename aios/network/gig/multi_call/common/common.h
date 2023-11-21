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
#ifndef ISEARCH_MULTI_CALL_COMMON_H
#define ISEARCH_MULTI_CALL_COMMON_H

#include <limits>
#include <memory>
#include <unordered_set>

#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/Log.h"

namespace google { namespace protobuf {
class Message;
class Arena;
}} // namespace google::protobuf

namespace multi_call {

enum ProtocolType : uint32_t {
    MC_PROTOCOL_TCP = 0,
    MC_PROTOCOL_ARPC = 1,
    MC_PROTOCOL_HTTP = 2,
    MC_PROTOCOL_GRPC = 3,
    MC_PROTOCOL_GRPC_STREAM = 4,
    MC_PROTOCOL_RDMA_ARPC = 5,
    MC_PROTOCOL_UNKNOWN
};

enum RequestType : uint32_t {
    RT_NORMAL = 0,
    RT_PROBE = 1,
    RT_COPY = 2,
};

enum SubscribeType : uint32_t {
    ST_CM2 = 0,
    ST_VIP = 1,
    ST_ISTIO = 2,
    ST_LOCAL = 3,
    ST_HEARTBEAT = 4,
    ST_UNKNOWN
};

enum class HashPolicy { CONSISTENT_HASH, RANDOM_WEIGHTED_HASH, RANDOM_HASH };

extern const std::string MC_PROTOCOL_TCP_STR;
extern const std::string MC_PROTOCOL_RAW_ARPC_STR;
extern const std::string MC_PROTOCOL_ARPC_STR;
extern const std::string MC_PROTOCOL_HTTP_STR;
extern const std::string MC_PROTOCOL_GRPC_STR;
extern const std::string MC_PROTOCOL_GRPC_STREAM_STR;
extern const std::string MC_PROTOCOL_RDMA_ARPC_STR;
extern const std::string MC_PROTOCOL_UNKNOWN_STR;
extern const std::string MC_SUBSCRIBE_TYPE_CM2_STR;
extern const std::string MC_SUBSCRIBE_TYPE_VIP_STR;
extern const std::string MC_SUBSCRIBE_TYPE_ISTIO_STR;
extern const std::string MC_SUBSCRIBE_TYPE_LOCAL_STR;
extern const std::string MC_SUBSCRIBE_TYPE_HEARTBEAT_STR;

ProtocolType convertProtocolType(const std::string &typeStr);
const std::string &convertProtocolTypeToStr(ProtocolType type);
const std::string &convertRequestTypeToStr(RequestType type);
const std::string &convertSubscribeTypeToStr(SubscribeType type);

typedef int32_t PartIdTy;
typedef int64_t VersionTy;
typedef int32_t WeightTy;
typedef uint64_t SourceIdTy;
typedef int32_t RatioTy;

constexpr SourceIdTy INVALID_SOURCE_ID = (SourceIdTy)-1;
constexpr int32_t INVALID_PORT = -1;
constexpr VersionTy DEFAULT_VERSION_ID = 0;
constexpr VersionTy INVALID_VERSION_ID = -1;
constexpr PartIdTy INVALID_PART_COUNT = -1;
constexpr PartIdTy INVALID_PART_ID = -1;
constexpr WeightTy INVALID_WEIGHT = -1;
constexpr uint32_t RPC_CONNECTION_TIMEOUT = 5000;              // ms
constexpr uint64_t DEFAULT_TIMEOUT = 1000;                     // ms
constexpr uint64_t INFINITE_TIMEOUT = (-1);                    // ms
constexpr int32_t COPY_REQUEST_TIMEOUT = 5;                    // ms
constexpr int64_t MULTI_CALL_UPDATE_INTERVAL = 500 * 1000;     // 500 ms
constexpr int64_t REPLICA_STAT_UPDATE_INTERVAL = 30 * 1000;    // 30 ms
constexpr uint32_t UPDATE_TIME_INTERVAL = 2 * 1000 * 1000;     // 2s
constexpr uint32_t MAX_WAIT_TIME = 300 * UPDATE_TIME_INTERVAL; // 600s
constexpr int64_t PROCESS_INTERVAL = 10 * 1000;                // 10ms
constexpr int64_t DEFAULT_TIMEOUT_LOOP_INTERVAL = 10;          // ms
constexpr uint64_t CALL_DELEGATION_QUEUE_SIZE = std::numeric_limits<uint64_t>::max();
constexpr int64_t DEFAULT_THREAD_NUM = 1;
constexpr int64_t DEFAULT_HEARTBEAT_THREAD_NUM = 3;
constexpr int64_t DEFAULT_CALLBACK_THREAD_NUM = 10;
constexpr int64_t DEFAULT_QUEUE_SIZE = 5000;
constexpr int64_t DEFAULT_GRPC_CHANNEL_POLL_INTERVAL_MS = 500; // 500 ms
extern const std::string MULTI_CALL_GROUP_METRIC;
extern const std::string EMPTY_STRING;
extern const std::string GIG_CLUSTER_BIZ_SPLITTER;

extern const std::string GIG_QUERY_CHECKSUM;
extern const std::string GIG_RESPONSE_CHECKSUM;
extern const std::string GIG_DATA;
// just for compatible
extern const std::string GIG_DATA_C;

extern const std::string GIG_GRPC_REQUEST_INFO_KEY;
extern const std::string GIG_GRPC_RESPONSE_INFO_KEY;
extern const std::string GIG_GRPC_TYPE_KEY;
extern const std::string GIG_GRPC_TYPE_STREAM;
extern const std::string GIG_GRPC_HANDLER_ID_KEY;

extern const std::string GIG_GRPC_HEARTBEAT_METHOD_NAME;

extern const std::string GIG_ARPC_HEARTBEAT_METHOD_NAME;

extern const std::string GIG_UNIFY_CALL_METHOD_NAME;

extern const std::string GIG_HEARTBEAT_METHOD_NAME;
constexpr int64_t GIG_STREAM_HEARTBEAT_VERSION = 1;
constexpr int64_t GIG_HEARTBEAT_SKIP_LOW = 2 * 1000 * 1000;
constexpr int64_t GIG_HEARTBEAT_SKIP_HIGH = 600 * 1000 * 1000;
constexpr int64_t GIG_HEARTBEAT_TIMEOUT = 3 * 1000 * 1000;
constexpr int64_t GIG_HEARTBEAT_SUBSCRIBE_TIMEOUT = 30 * 1000 * 1000;

extern const std::string GIG_NEW_HEARTBEAT_METHOD_NAME;

extern const std::string CM2_SERVICE_META_KEY;
extern const std::string CM2_SERVICE_META_CLUSTER;
extern const std::string CM2_SERVICE_META_APP;
extern const std::string CM2_SERVICE_META_ROLE;
extern const std::string CM2_SERVICE_META_GROUP;
extern const std::string CM2_SERVICE_META_PLATFORM;

extern const std::string ENV_META_CLUSTER;
extern const std::string ENV_META_APP;
extern const std::string ENV_META_ROLE;
extern const std::string ENV_META_GROUP;
extern const std::string ENV_META_PLATFORM;

extern const std::string GIG_UNKNOWN;
extern const std::string GIG_TAG_PREFIX;
extern const std::string GIG_TAG_SRC;
extern const std::string GIG_TAG_SRC_AB;

extern const std::string RPC_DATA_SRC;
extern const std::string RPC_DATA_SRC_AB;
extern const std::string RPC_DATA_STRESS_TEST;

extern const std::string GIG_TAG_STRESS_TEST;
extern const std::string GIG_TAG_PLATFORM;
extern const std::string GIG_TAG_GROUP;
extern const std::string GIG_TAG_ROLE;
extern const std::string GIG_TAG_BIZ;
extern const std::string GIG_TAG_TARGET_PREFIX;
extern const std::string GIG_TAG_TARGET_CLUSTER;
extern const std::string GIG_TAG_TARGET_PLATFORM;
extern const std::string GIG_TAG_TARGET_GROUP;
extern const std::string GIG_TAG_TARGET_ROLE;
extern const std::string GIG_TAG_TARGET_APP;
extern const std::string GIG_TAG_TARGET_BIZ;
extern const std::string GIG_TAG_RESERVED_0;
extern const std::string GIG_TAG_RESERVED_1;
extern const std::string GIG_KMONITOR;
extern const std::string DEFAULT_KMONITOR_CONFIG;

extern const std::string GIG_RDMA_PORT_DIFF_KEY;

extern const std::string LOAD_BALANCE_VERSION_GROUP_BIZS_KEY;

#define MULTI_CALL_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;

#define MULTI_CALL_MEMORY_BARRIER() __asm__ __volatile__("" ::: "memory");

union FloatIntUnion {
    int32_t i;
    float f;
};

enum TagMatchType : uint32_t {
    TMT_REQUIRE = 0,
    TMT_PREFER = 1,
};

struct TagMatchInfo {
    TagMatchType type = TMT_PREFER;
    VersionTy version = INVALID_VERSION_ID;
};

typedef std::map<std::string, TagMatchInfo> MatchTagMap;
typedef std::map<std::string, VersionTy> TagMap;
typedef std::map<std::string, std::string> MetaMap;

MULTI_CALL_TYPEDEF_PTR(MatchTagMap);
MULTI_CALL_TYPEDEF_PTR(TagMap);
MULTI_CALL_TYPEDEF_PTR(MetaMap);

typedef uint64_t SignatureTy;
typedef uint64_t PublishGroupTy;
constexpr PublishGroupTy INVALID_PUBLISH_GROUP = 0;

struct BizTopoSignature {
    SignatureTy topo = 0;
    SignatureTy meta = 0;
    SignatureTy tag = 0;
    SignatureTy publishId = 0;
};

struct ServerBizTopoInfo {
    std::string bizName;
    PartIdTy partCount = INVALID_PART_COUNT;
    PartIdTy partId = INVALID_PART_ID;
    VersionTy version = INVALID_VERSION_ID;
    VersionTy protocolVersion = INVALID_VERSION_ID;
    MetaMap metas;
    TagMap tags;
    WeightTy targetWeight = 100;
};

struct BizVolatileInfo {
    TagMap tags;
    WeightTy targetWeight = 100;
};

typedef std::unordered_map<SignatureTy, MetaMapPtr> MetasSignatureMap;

template <typename T>
inline bool cmpxchg(volatile T *addr, T oldValue, T newValue) {
    return __sync_bool_compare_and_swap(addr, oldValue, newValue);
}

void freeProtoMessage(google::protobuf::Message *message);

std::string getTimeStrFromUs(int64_t timeUs);

struct CompatibleFieldInfo {
    // client
    std::string requestInfoField;
    // server
    std::string ecField;
    std::string responseInfoField;
    // eagleeye
    std::string eagleeyeTraceId;
    std::string eagleeyeRpcId;
    std::string eagleeyeUserData;

    // suez request default eagleeye field
    CompatibleFieldInfo()
        : eagleeyeTraceId("traceId")
        , eagleeyeRpcId("rpcId")
        , eagleeyeUserData("userData") {
    }
};

struct ObjectPool {
    autil::ThreadMutex lock;
    std::set<void *> set;
};

MULTI_CALL_TYPEDEF_PTR(ObjectPool);

#define REPORT_METRICS(METHOD)                                                                     \
    do {                                                                                           \
        if (_metricReporterManager) {                                                              \
            const auto &reporter = _metricReporterManager->getWorkerMetricReporter();              \
            if (reporter) {                                                                        \
                reporter->METHOD(1.0);                                                             \
            }                                                                                      \
        }                                                                                          \
    } while (0)

} // namespace multi_call
#endif
