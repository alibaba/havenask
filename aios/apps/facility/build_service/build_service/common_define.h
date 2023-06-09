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

#include <cassert>
#include <map>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "beeper/beeper.h"

#define BS_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;
#define BS_MEMORY_BARRIER() __asm__ __volatile__("" ::: "memory")

#define BS_DECLARE_REFERENCE_CLASS(x, y)                                                                               \
    namespace build_service { namespace x {                                                                            \
    class y;                                                                                                           \
    typedef std::shared_ptr<y> y##Ptr;                                                                                 \
    }}

#define BS_NAMESPACE_USE(x) using namespace build_service::x;

namespace build_service {

typedef std::map<std::string, std::string> KeyValueMap;
extern const std::string EMPTY_STRING;
typedef int64_t checkpointid_t;
constexpr checkpointid_t INVALID_CHECKPOINT_ID = -1;

inline const std::string& getValueFromKeyValueMap(const std::map<std::string, std::string>& keyValueMap,
                                                  const std::string& key,
                                                  const std::string& defaultValue = EMPTY_STRING)
{
    KeyValueMap::const_iterator iter = keyValueMap.find(key);
    if (iter != keyValueMap.end()) {
        return iter->second;
    }
    return defaultValue;
}

template <typename T>
inline bool getValueFromKVMap(const std::map<std::string, std::string>& keyValueMap, const std::string& key, T& value)
{
    std::string valueStr;
    KeyValueMap::const_iterator iter = keyValueMap.find(key);
    if (iter != keyValueMap.end()) {
        valueStr = iter->second;
        return autil::StringUtil::fromString(valueStr, value);
    }
    return false;
}

inline std::string getValueCopyFromKeyValueMap(const std::map<std::string, std::string>& keyValueMap,
                                               const std::string& key, const std::string& defaultValue = EMPTY_STRING)
{
    KeyValueMap::const_iterator iter = keyValueMap.find(key);
    if (iter != keyValueMap.end()) {
        return iter->second;
    }
    return defaultValue;
}

inline std::string keyValueMapToString(const KeyValueMap& kvMap)
{
    std::string ret;
    ret += "{";
    for (KeyValueMap::const_iterator it = kvMap.begin(); it != kvMap.end(); it++) {
        ret += "\t";
        ret += it->first + ":" + it->second;
        ret += "\n";
    }
    ret += "}";
    return ret;
}

extern const std::string BS_COUNTER_PREFIX;

#define BS_COUNTER(counterName) (BS_COUNTER_PREFIX + "." + std::string(#counterName))

#define GET_ACC_COUNTER(counterMap, counterName)                                                                       \
    ({                                                                                                                 \
        indexlib::util::AccumulativeCounterPtr counter;                                                                \
        if ((counterMap)) {                                                                                            \
            std::string bsCounterName = BS_COUNTER(counterName);                                                       \
            counter = (counterMap)->GetAccCounter(bsCounterName);                                                      \
            if (!counter) {                                                                                            \
                std::string errorMsg = "declare counter[" + bsCounterName + "] failed";                                \
                BS_LOG(ERROR, "%s", errorMsg.c_str());                                                                 \
            }                                                                                                          \
        }                                                                                                              \
        counter;                                                                                                       \
    })

#define GET_STATE_COUNTER(counterMap, counterName)                                                                     \
    ({                                                                                                                 \
        indexlib::util::StateCounterPtr counter;                                                                       \
        if ((counterMap)) {                                                                                            \
            std::string bsCounterName = BS_COUNTER(counterName);                                                       \
            counter = (counterMap)->GetStateCounter(bsCounterName);                                                    \
            if (!counter) {                                                                                            \
                std::string errorMsg = "declare counter[" + bsCounterName + "] failed";                                \
                BS_LOG(ERROR, "%s", errorMsg.c_str());                                                                 \
            }                                                                                                          \
        }                                                                                                              \
        counter;                                                                                                       \
    })

#define COMPARE_POINTER(lfs, rhs)                                                                                      \
    do {                                                                                                               \
        bool ret = ((lfs) != NULL && (rhs) != NULL && *(lfs) == *(rhs));                                               \
        if (!ret) {                                                                                                    \
            ret = ((lfs) == NULL && (rhs) == NULL);                                                                    \
        }                                                                                                              \
        if (!ret) {                                                                                                    \
            return false;                                                                                              \
        }                                                                                                              \
    } while (0)

typedef uint16_t hashid_t;
typedef int32_t generationid_t;

constexpr generationid_t INVALID_GENERATIONID = -1;
constexpr int64_t INVALID_PROGRESS = -1;

constexpr uint16_t RANGE_MIN = 0;
constexpr uint16_t RANGE_MAX = 65535;

constexpr size_t MAX_ERROR_COUNT = 7;

extern const std::string BUILDER_EPOCH_ID;
extern const std::string BUILDER_BRANCH_NAME_LEGACY;

extern const std::string REALTIME_INFO_FILE_NAME;
extern const std::string COUNTER_DIR_NAME;
extern const std::string NO_MERGER_CHECKPOINT_FILE_NAME;
extern const std::string SCHEMA_PATCH;
// bs_admin_env
extern const std::string BS_ENV_RECOVER_THREAD_NUM;
extern const std::string BS_ENV_RECOVER_SLEEP_SECOND;
extern const std::string BS_ENV_FILL_COUNTER_INFO;
extern const std::string BS_ENV_ENABLE_GENERATION_METRICS;
extern const std::string BS_ENV_ADMIN_SWIFT_TIMEOUT;
extern const std::string BS_ENV_HEALTH_CHECK_SERVICEKEEPER_TIMEOUT; // second
extern const std::string BS_ENV_HEALTH_CHECK_GENERATIONKEEPER_TIMEOUT;
extern const std::string BS_ENV_SYNC_COUNTER_INTERVAL; // second
extern const std::string BS_ENV_MEM_PRINTING_INTERVAL; // second
extern const std::string BS_ENV_ENABLE_LEGACY_BUILDER_METRICS;
extern const std::string BS_ENV_MEM_RELEASE_INTERVAL; // second
// global resource reader cache
extern const std::string BS_ENV_ENABLE_RESOURCE_READER_CACHE; // second
extern const std::string BS_ENV_ENABLE_RESOURCE_READER_CACHE_CLEAR_INTERVAL;
extern const std::string BS_ENV_ENABLE_RESOURCE_READER_CACHE_EXPIRE_TIME;
extern const std::string BS_ENV_ENABLE_RESOURCE_READER_MAX_ZIP_SIZE; // kilobyte
extern const std::string BS_ENV_TEMP_DIR;
extern const std::string BS_ENV_FLOW_CLEAR_INTERVAL; // second
extern const std::string BS_ENV_DISABLE_BATCH_BROKER_TOPIC;
extern const std::string BS_ENV_BROKER_TOPIC_MAX_BATCH_SIZE;

// checkpoint
extern const std::string BS_RESOURCE_CHECKPOINT_ID;
extern const std::string BS_BUILDER_CHECKPOINT_ID;
extern const std::string BS_INDEX_INFO_ID;
extern const std::string BS_INDEX_CHECKPOINT_ID;
extern const std::string BS_INDEX_TOTAL_SIZE_ID;
extern const std::string BS_BRANCH_INDEX_CHECKPOINT_ID;
extern const std::string BS_PROCESSOR_CHECKPOINT_ID;
extern const std::string BS_PROCESSOR_MAX_ID;
extern const std::string BS_CKP_ID_BUILDER_PATH_VERSION_PREFIX;
extern const std::string BS_CKP_NAME_WORKER_PATH_VERSION;
extern const std::string BS_CKP_ID_DISABLE_OPIDS;
extern const std::string BS_CKP_SCHEMA_PATCH;

// flow related
extern const std::string BS_ENV_LUA_CONFIG_PATH;
extern const std::string BS_FULL_PROCESSOR_FLOW_ID;
extern const std::string BS_INC_PROCESSOR_FLOW_ID;
extern const std::string BS_FULL_BUILD_FLOW_ID;
extern const std::string BS_INC_BUILD_FLOW_ID;
extern const std::string BS_ALTER_FIELD_FLOW_ID;
extern const std::string BS_MERGECRONTAB_FLOW_ID;
extern const std::string BS_PREPARE_DATA_SOURCE_ID;
extern const std::string BS_PREPARE_VERSION_FLOW_ID;
extern const std::string BS_ROLLBACK_FLOW_ID;

extern const std::string BS_FLOW_ID_MERGECRONTAB_SUFFIX;
extern const std::string BS_TASK_ID_MERGECRONTAB;

extern const std::string BS_ENV_INDEX_INFO_REPORT_INTERVAL;
extern const std::string BS_ENV_INDEX_INFO_CLEAR_INTERVAL;

constexpr int32_t UNKNOWN_WORKER_PROTOCOL_VERSION = -1;

extern const std::string BS_ENV_ENABLE_SCHEDULE_METRIC;
extern const std::string BS_ENV_ADMIN_KMON_SERVICE_NAME;
extern const std::string BS_ENV_ADMIN_DEFAULT_KMON_SERVICE_NAME;
extern const std::string BS_ENV_ADMIN_TASK_BEEPER_REPORT_INTERVAL;
extern const std::string BS_ENV_ADMIN_RESTART_THRESHOLD_TO_REPORT;

extern const std::string BS_ENV_KMON_SERVICE_NAME;
extern const std::string BS_ENV_DEFAULT_KMON_SERVICE_NAME;

extern const std::string BS_ENV_DOC_TRACE_FIELD;
extern const std::string BS_ENV_DOC_TRACE_INTERVAL; // second
extern const std::string BS_ENV_READER_CHECKPOINT_STEP;

extern const std::string BS_BACKUP_NODE_LABEL;

}; // namespace build_service
