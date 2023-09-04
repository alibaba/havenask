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

#include "aios/network/anet/atomic.h"
#include "autil/CommonMacros.h"
#include "navi/util/NaviTestPool.h"
#include <algorithm>
#include <assert.h>
#include <functional>
#include <memory>
#include <stdint.h>

namespace navi {

#define NAVI_TYPEDEF_PTR(x)                                                                                            \
    typedef std::shared_ptr<x> x##Ptr;                                                                                 \
    typedef std::unique_ptr<x> x##UPtr;

enum ErrorCode : int32_t {
    EC_NONE = 0,
    EC_ABORT = 1,
    EC_DEADLOCK = 2,
    EC_INVALID_ATTRIBUTE = 3,
    EC_INIT_GRAPH = 4,
    EC_INIT_SUB_GRAPH = 5,
    EC_WEAVE_GRAPH = 6,
    EC_QUEUE_FULL = 7,
    EC_FORK = 8,
    EC_TIMEOUT = 9,
    EC_INIT_RESOURCE = 10,
    EC_CREATE_KERNEL = 11,
    EC_LACK_RESOURCE = 12,
    EC_RPC_INPUT_PORT = 13,
    EC_PROTO_DEF = 14,
    EC_DESERIALIZE = 15,
    EC_SERIALIZE = 16,
    EC_NO_DATA = 17,
    EC_DATA_OVERRIDE = 18,
    EC_DATA_AFTER_EOF = 19,
    EC_BUFFER_FULL = 20,
    EC_STREAM_RPC = 21,
    EC_STREAM_RECEIVE = 22,
    EC_STREAM_SEND = 23,
    EC_NO_GRAPH_OUTPUT = 24,
    EC_OVERRIDE_EDGE_DATA = 25,
    EC_KERNEL = 32,
    EC_PART_ID_OVERFLOW = 33,
    EC_ASYNC_TERMINATED = 34,
    EC_PART_CANCEL = 35,
    EC_PART_ID_NOT_EXIST = 36,
    EC_PART_COUNT_MISMATCH = 37,
    EC_LACK_REGISTRY = 38,
    EC_ILLEGAL_RESOURCE_STAGE = 39,
    EC_RESOURCE_CONFIG = 40,
    EC_BIND_RESOURCE = 41,
    EC_BIND_NAMED_DATA = 42,
    EC_LACK_RESOURCE_SNAPSHOT = 43,
    EC_LACK_RESOURCE_BIZ = 44,
    EC_LACK_RESOURCE_BIZ_PART = 45,
    EC_LACK_RESOURCE_GRAPH = 46,
    EC_LACK_RESOURCE_SUB_GRAPH = 47,
    EC_LACK_RESOURCE_SCOPE = 48,
    EC_LACK_RESOURCE_KERNEL = 49,
    EC_LACK_RESOURCE_EXTERNAL = 50,
    EC_LACK_REPLACE_RESOURCE = 51,
    EC_REWRITE_GRAPH = 52,
    EC_ADD_NAMED_DATA = 53,
    EC_NEED_REPLACE = 54,
    EC_UNKNOWN = 127,
    EC_NAVI_MAX,
};

typedef int32_t NaviPartId;
constexpr NaviPartId INVALID_NAVI_PART_COUNT = 0;
constexpr NaviPartId INVALID_NAVI_PART_ID = -1;
constexpr NaviPartId NAVI_BIZ_PART_ID = -2;

enum TypeErrorCode {
    TEC_NONE,
    TEC_NOT_SUPPORT,
    TEC_NULL_DATA,
    TEC_TYPE_MISMATCH,
    TEC_FAILED,
    TEC_UNKNOWN,
};

enum ActivateStrategy {
    AS_REQUIRED,
    AS_OPTIONAL,
    AS_INDEPENDENT,
};

enum EdgeState {
    ES_DATA_EMPTY,
    ES_DATA_READY,
    ES_DATA_EOF,
};

enum IoType {
    IOT_INPUT = 0,
    IOT_OUTPUT = 1,
};

enum PortStoreType {
    PST_DATA,
    PST_SERIALIZE_DATA,
    PST_UNKNOWN,
};

enum PortQueueType {
    PQT_LOCAL_REMOTE_SERVER_1,
    PQT_LOCAL_REMOTE_CLIENT_2,
    PQT_LOCAL_LOCAL_3,
    PQT_REMOTE_LOCAL_SERVER_4,
    PQT_REMOTE_LOCAL_CLIENT_5,
    PQT_REMOTE_REMOTE_CLIENT_6,
    PQT_UNKNOWN,
};

typedef uint32_t InstanceId;
typedef uint32_t QueryId;
struct SessionId {
    SessionId()
        : instance(0)
        , queryId(0)
    {
    }
    bool valid() const {
        return 0 != instance && 0 != queryId;
    }
    InstanceId instance;
    QueryId queryId;
};

typedef uint16_t IndexType;
constexpr IndexType INVALID_INDEX = IndexType(-1);

typedef uint32_t PortId;
constexpr PortId INVALID_PORT_ID = PortId(-1);

struct PortIndex
{
public:
    PortIndex(IndexType index_ = INVALID_INDEX, IndexType group_ = INVALID_INDEX, bool control_ = false)
        : index(index_)
        , group(group_)
        , control(control_)
    {
    }
    bool isValid() const {
        return !(*this == PortIndex());
    }
    bool operator==(const PortIndex &rhs) const {
        return index == rhs.index
            && group == rhs.group;
    }
    bool operator!=(const PortIndex &rhs) const {
        return !(*this == rhs);
    }
    bool isGroup() const {
        return group != INVALID_INDEX;
    }
    bool isControl() const {
        return control;
    }
    IndexType index;
    IndexType group;
    bool control : 1;
};

extern std::ostream &operator<<(std::ostream &os, const PortIndex &portIndex);

enum LogLevel {
    LOG_LEVEL_DISABLE = 0,
/** the fatal message level*/
    LOG_LEVEL_FATAL = 1,
/** the error message level*/
    LOG_LEVEL_ERROR = 2,
/** the warning message level*/
    LOG_LEVEL_WARN = 3,
/** the information level*/
    LOG_LEVEL_INFO = 4,
/** the debug message level*/
    LOG_LEVEL_DEBUG = 5,
/** the trace level 1 */
    LOG_LEVEL_TRACE1 = 6,
/** the trace level 2 */
    LOG_LEVEL_TRACE2 = 7,
/** the trace level 3 */
    LOG_LEVEL_TRACE3 = 8,
/** the schedule info level*/
    LOG_LEVEL_SCHEDULE1 = 9,
    LOG_LEVEL_SCHEDULE2 = 10,
    LOG_LEVEL_SCHEDULE3 = 11,
/** level not set */
    LOG_LEVEL_NOTSET = 12,
/** the count of log level defined above. */
    LOG_LEVEL_COUNT = LOG_LEVEL_NOTSET + 1,
};

inline LogLevel getLevelByString(const std::string &logLevel) {
    auto levelStr = logLevel;
    std::transform(levelStr.begin(), levelStr.end(), levelStr.begin(), ::toupper);
    auto level = LOG_LEVEL_DISABLE;
    if (levelStr == "DISABLE") {
        level = LOG_LEVEL_DISABLE;
    } else if (levelStr == "FATAL") {
        level = LOG_LEVEL_FATAL;
    } else if (levelStr == "ERROR") {
        level = LOG_LEVEL_ERROR;
    } else if (levelStr == "WARN") {
        level = LOG_LEVEL_WARN;
    } else if (levelStr == "INFO") {
        level = LOG_LEVEL_INFO;
    } else if (levelStr == "DEBUG") {
        level = LOG_LEVEL_DEBUG;
    } else if (levelStr == "TRACE1") {
        level = LOG_LEVEL_TRACE1;
    } else if (levelStr == "TRACE2") {
        level = LOG_LEVEL_TRACE2;
    } else if (levelStr == "TRACE3") {
        level = LOG_LEVEL_TRACE3;
    } else if (levelStr == "SCHEDULE1") {
        level = LOG_LEVEL_SCHEDULE1;
    } else if (levelStr == "SCHEDULE2") {
        level = LOG_LEVEL_SCHEDULE2;
    } else if (levelStr == "SCHEDULE3") {
        level = LOG_LEVEL_SCHEDULE3;
    }
    return level;
}

constexpr int64_t NAVI_VERSION = 0;
typedef int32_t GraphId;
constexpr GraphId PARENT_GRAPH_ID = GraphId(-1);
constexpr GraphId USER_GRAPH_ID = GraphId(-2);
constexpr GraphId RS_GRAPH_RESOURCE_GRAPH_ID = GraphId(-3);
constexpr GraphId INVALID_GRAPH_ID = GraphId(-16);

struct ScopeId {
public:
    ScopeId()
        : graphId(INVALID_GRAPH_ID)
        , scopeIndex(-1)
    {
    }
    ScopeId(GraphId graphId_, int32_t scopeIndex_)
        : graphId(graphId_)
        , scopeIndex(scopeIndex_)
    {
    }
public:
    GraphId graphId;
    int32_t scopeIndex;
};

template <typename T>
inline bool cmpxchg(volatile T *addr, T oldValue, T newValue) {
    return __sync_bool_compare_and_swap(addr, oldValue, newValue);
}

#define NAVI_MEMORY_BARRIER() __asm__ __volatile__("" ::: "memory");

extern const std::string NAVI_NAMESPACE_SEPERATOR;
extern const std::string NAVI_BUILDIN_BIZ;
extern const std::string NODE_PORT_SEPERATOR;
extern const std::string GROUP_PORT_SEPERATOR;
extern const std::string NODE_FINISH_PORT;
extern const std::string TERMINATOR_NODE_NAME;
extern const std::string TERMINATOR_KERNEL_NAME;
extern const std::string TERMINATOR_INPUT_PORT;
extern const std::string PLACEHOLDER_KERNEL_NAME;
extern const std::string PLACEHOLDER_OUTPUT_PORT;
extern const std::string TF_BATCH_IDENTITY_KERNEL_NAME;
extern const std::string TF_BATCH_IDENTITY_INPUT;
extern const std::string TF_BATCH_IDENTITY_OUTPUT;
extern const std::string TF_SPLIT_KERNEL;
extern const std::string NAVI_USER_GRAPH_BIZ;
extern const std::string NAVI_FORK_BIZ;
extern const std::string NAVI_FORK_NODE;
extern const std::string NAVI_SERVER_MIRROR_BIZ_PREFIX;

extern const std::string MAP_KERNEL_INPUT_PORT;
extern const std::string MAP_KERNEL_OUTPUT_PORT;

extern const std::string NODE_CONTROL_INPUT_PORT;
extern const std::string CONTROL_DATA_TYPE;
extern const std::string CONTROL_MERGE_KERNEL;

extern const std::string NODE_RESOURCE_INPUT_PORT;
extern const std::string RESOURCE_DATA_TYPE_ID;
extern const std::string RESOURCE_NOTIFY_DATA_TYPE_ID;

extern const std::string DEFAULT_SPLIT_KERNEL;
extern const std::string DEFAULT_MERGE_KERNEL;
extern const std::string PORT_KERNEL_INPUT_PORT;
extern const std::string PORT_KERNEL_OUTPUT_PORT;
extern const std::string PORT_KERNEL_PART_COUNT_KEY;

extern const std::string DEFAULT_LOG_PATTERN;
extern const std::string DEFAULT_LOG_FILE_NAME;

constexpr size_t CONTROL_PORT_FINISH_INDEX = 0;

extern const std::string NAVI_RESULT_TYPE_ID;
extern const std::string ROOT_KMON_RESOURCE_ID;
extern const std::string BIZ_KMON_RESOURCE_ID;
extern const std::string GIG_CLIENT_RESOURCE_ID;
extern const std::string GIG_SESSION_RESOURCE_ID;

extern const std::string SCOPE_TERMINATOR_NODE_NAME_PREFIX;
extern const std::string SCOPE_TERMINATOR_INPUT_PORT;

extern const std::string NAVI_RPC_METHOD_NAME;

extern const std::string NAVI_BUILDIN_ATTR_NODE;
extern const std::string NAVI_BUILDIN_ATTR_KERNEL;

extern const std::string NAVI_BUILDIN_STATIC_GRAPH_META;

extern const std::string SIMD_PADING_STR;

typedef std::function<void()> NaviVoidFunc;
class Creator;
typedef std::function<::navi::Creator *()> NaviCreatorFunc;
typedef std::function<bool()> NaviModuleInitFunc;

class Kernel;
class Resource;
class Data;
class NaviConfigContext;
typedef NaviConfigContext KernelConfigContext;

typedef std::function<bool(void *, Resource *)> StaticResourceBindFunc;
typedef std::function<bool(void *, Resource *, void *)> DynamicResourceBindFunc;
typedef std::function<bool(void *, Data *)> NamedDataBindFunc;
typedef std::function<int(KernelConfigContext &ctx)> ResourceSelectFunc;
constexpr int32_t INVALID_SELECT_INDEX = 65536;
constexpr int32_t IGNORE_SELECT_INDEX = -1;

struct StaticResourceBindInfo {
    std::string name;
    bool require = true;
    StaticResourceBindFunc binder;
};

struct DynamicResourceBindInfo {
    std::string group;
    DynamicResourceBindFunc binder;
};

struct NamedDataBindInfo {
    std::string name;
    bool require = true;
    NamedDataBindFunc binder;
};

struct SelectResourceBindInfo {
    std::vector<std::string> candidates;
    bool require = true;
    ResourceSelectFunc selector;
    StaticResourceBindFunc binder;
};

struct BindInfos {
    std::vector<StaticResourceBindInfo> staticBindVec;
    std::vector<DynamicResourceBindInfo> dynamicBindVec;
    std::vector<NamedDataBindInfo> namedDataBindVec;
    std::vector<SelectResourceBindInfo> selectBindVec;
};

constexpr uint32_t DEFAULT_THREAD_LIMIT = 100000u;
constexpr size_t DEFAULT_THREAD_NUMBER = 0u; // sysconf(_SC_NPROCESSORS_ONLN)
constexpr size_t DEFAULT_MIN_THREAD_NUMBER = 20u;
constexpr size_t DEFAULT_MAX_THREAD_NUMBER = 104u;
constexpr size_t DEFAULT_QUEUE_SIZE = 1000u;
constexpr size_t DEFAULT_PROCESSING_SIZE = 300u;
constexpr int64_t FACTOR_MS_TO_US = 1000;
constexpr int64_t DEFAULT_TIMEOUT_MS = 1000000000;
constexpr int32_t DEFAULT_MAX_INLINE = 3;

enum RegistryType : int32_t {
    RT_RESOURCE = 0,
    RT_KERNEL = 1,
    RT_TYPE = 2,
    RT_MAX = 3,
};

template <int N>
class DependMatcher {
public:
    unsigned int value() { return N; }
};
}

#define NAVI_KERNEL_ERROR_RETURN(code, level, format, args...)                 \
    {                                                                          \
        NAVI_KERNEL_LOG(level, format, ##args);                                \
        return code;                                                           \
    }

namespace google {
namespace protobuf
{
class Arena;
}
}

typedef std::shared_ptr<google::protobuf::Arena> ArenaPtr;

enum TestMode : int32_t {
    TM_NOT_TEST = 0,
    TM_KERNEL_TEST = 1,
    TM_RESOURCE_TEST = 2,
    TM_UNKNOWN = 10
};
