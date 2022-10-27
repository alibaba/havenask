#ifndef BUILD_SERVICE_COMMON_DEFINE_H
#define BUILD_SERVICE_COMMON_DEFINE_H

#include <map>
#include <vector>
#include <string>
#include <tr1/memory>
#include <cassert>
#include <unistd.h>

#include <autil/CommonMacros.h>
#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>
#include <indexlib/misc/doc_tracer.h>

#define BS_TYPEDEF_PTR(x) typedef std::tr1::shared_ptr<x> x##Ptr;
#define BS_MEMORY_BARRIER() __asm__ __volatile__("" ::: "memory")

#define BS_DECLARE_REFERENCE_CLASS(x, y)       \
namespace build_service {                       \
namespace x {                                   \
class y;                                        \
typedef std::tr1::shared_ptr<y> y##Ptr;         \
}}

#define BS_NAMESPACE_USE(x) using namespace build_service::x;

namespace build_service {

typedef std::map<std::string, std::string> KeyValueMap;
static const std::string EMPTY_STRING;

inline const std::string& getValueFromKeyValueMap(
        const std::map<std::string, std::string>& keyValueMap,
        const std::string &key,
        const std::string &defaultValue = EMPTY_STRING)
{
    KeyValueMap::const_iterator iter = keyValueMap.find(key);
    if (iter != keyValueMap.end()) {
        return iter->second;
    }
    return defaultValue;
}

inline std::string keyValueMapToString(const KeyValueMap &kvMap) {
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


static const std::string BS_COUNTER_PREFIX = "bs";

#define BS_COUNTER(counterName) (BS_COUNTER_PREFIX + "." + std::string(#counterName))

#define GET_ACC_COUNTER(counterMap, counterName)                        \
    ({                                                                  \
        IE_NAMESPACE(util)::AccumulativeCounterPtr counter;           \
        if ((counterMap)) {                                             \
            std::string bsCounterName = BS_COUNTER(counterName);        \
            counter = (counterMap)->GetAccCounter(bsCounterName);       \
            if (!counter) {                                             \
                std::string errorMsg = "declare counter[" + bsCounterName + "] failed"; \
                BS_LOG(ERROR, "%s", errorMsg.c_str());                  \
            }                                                           \
        }                                                               \
        counter;                                                        \
    })

#define GET_STATE_COUNTER(counterMap, counterName)                      \
    ({                                                                  \
        IE_NAMESPACE(util)::StateCounterPtr counter;                  \
        if ((counterMap)) {                                             \
            std::string bsCounterName = BS_COUNTER(counterName);        \
            counter = (counterMap)->GetStateCounter(bsCounterName);     \
            if (!counter) {                                             \
                std::string errorMsg = "declare counter[" + bsCounterName + "] failed"; \
                BS_LOG(ERROR, "%s", errorMsg.c_str());                  \
            }                                                           \
        }                                                               \
        counter;                                                        \
    })

#define COMPARE_POINTER(lfs, rhs) do {                                  \
        bool ret =  ((lfs) != NULL && (rhs) != NULL && *(lfs) == *(rhs)); \
        if (!ret) {                                                     \
            ret = ((lfs) == NULL && (rhs) == NULL);                     \
        }                                                               \
        if (!ret) {                                                     \
            return false;                                               \
        }                                                               \
    } while (0)



typedef uint16_t hashid_t;
typedef int32_t generationid_t;

static const generationid_t INVALID_GENERATIONID = -1;
static const int64_t INVALID_PROGRESS = -1;

static const uint16_t RANGE_MIN = 0;
static const uint16_t RANGE_MAX = 65535;

static const size_t MAX_ERROR_COUNT = 7;

static const std::string REALTIME_INFO_FILE_NAME = "realtime_info.json";
static const std::string COUNTER_DIR_NAME = "counter";
static const std::string NO_MERGER_CHECKPOINT_FILE_NAME = "no_merger_checkpoint";

//bs_admin_env
static const std::string BS_ENV_RECOVER_THREAD_NUM = "recover_thread_num";
static const std::string BS_ENV_RECOVER_SLEEP_SECOND = "bs_recover_sleep_second";
static const std::string BS_ENV_FILL_COUNTER_INFO = "fill_counter_info";
static const std::string BS_ENV_ENABLE_GENERATION_METRICS = "enable_generation_metrics";
static const std::string BS_ENV_ADMIN_SWIFT_TIMEOUT = "bs_admin_swift_timeout";
static const std::string BS_ENV_HEALTH_CHECK_SERVICEKEEPER_TIMEOUT = "bs_health_check_servicekeeper_timeout"; //second
static const std::string BS_ENV_HEALTH_CHECK_GENERATIONKEEPER_TIMEOUT = "bs_health_check_generationkeeper_timeout"; //second
static const std::string BS_ENV_SYNC_COUNTER_INTERVAL = "bs_sync_counter_interval"; // second
static const std::string BS_ENV_MEM_PRINTING_INTERVAL = "bs_tcmalloc_mem_printing_interval"; // second
static const std::string BS_ENV_MEM_RELEASE_INTERVAL = "bs_tcmalloc_mem_release_interval"; // second
//global resource reader cache
static const std::string BS_ENV_ENABLE_RESOURCE_READER_CACHE = "bs_resource_reader_cache"; // second
static const std::string BS_ENV_ENABLE_RESOURCE_READER_CACHE_CLEAR_INTERVAL = "bs_resource_reader_cache_clear_interval"; // second
static const std::string BS_ENV_ENABLE_RESOURCE_READER_CACHE_EXPIRE_TIME = "bs_resource_reader_cache_expire_time"; // second
static const std::string BS_ENV_ENABLE_RESOURCE_READER_MAX_ZIP_SIZE = "bs_resource_reader_max_zip_size"; // kilobyte
static const std::string BS_ENV_TEMP_DIR = "bs_admin_temp_dir";
static const std::string BS_ENV_FLOW_CLEAR_INTERVAL = "bs_admin_flow_clear_interval"; // second

// checkpoint
static const std::string BS_BUILDER_CHECKPOINT_ID = "builder_checkpoint";
static const std::string BS_INDEX_INFO_ID = "index_info";
static const std::string BS_INDEX_CHECKPOINT_ID = "index_checkpoint";
static const std::string BS_INDEX_TOTAL_SIZE_ID = "index_total_size";
static const std::string BS_BRANCH_INDEX_CHECKPOINT_ID = "branch_index_checkpoint";
static const std::string BS_PROCESSOR_CHECKPOINT_ID = "processor_checkpoint";
static const std::string BS_PROCESSOR_MAX_ID = "bs_processor_max_id";
static const std::string BS_CKP_ID_BUILDER_PATH_VERSION_PREFIX = "builder_worker_path_version_";
static const std::string BS_CKP_NAME_WORKER_PATH_VERSION = "worker_path_version";

// flow related
static const std::string BS_ENV_LUA_CONFIG_PATH = "control_flow_config_path";
static const std::string BS_FULL_PROCESSOR_FLOW_ID = "fd_full_processor";
static const std::string BS_INC_PROCESSOR_FLOW_ID = "fd_inc_processor";
static const std::string BS_FULL_BUILD_FLOW_ID = "fd_full_build";
static const std::string BS_INC_BUILD_FLOW_ID = "fd_inc_build";
static const std::string BS_ALTER_FIELD_FLOW_ID = "fd_alter_field";
static const std::string BS_MERGECRONTAB_FLOW_ID = "fd_merge_crontab";
static const std::string BS_PREPARE_DATA_SOURCE_ID = "fd_prepare_data_source";
static const std::string BS_PREPARE_VERSION_FLOW_ID = "fd_prepare_version";
static const std::string BS_ROLLBACK_FLOW_ID = "fd_rollback";

static const std::string BS_FLOW_ID_MERGECRONTAB_SUFFIX = "_merge_crontab";
static const std::string BS_TASK_ID_MERGECRONTAB = "mergeCrontab";

static const std::string BS_ENV_INDEX_INFO_REPORT_INTERVAL = "bs_index_info_report_interval";
static const std::string BS_ENV_INDEX_INFO_CLEAR_INTERVAL = "bs_index_info_clear_interval";
};

static const std::string BS_ENV_ENABLE_SCHEDULE_METRIC = "bs_enable_schedule_metric";
static const std::string BS_ENV_ADMIN_KMON_SERVICE_NAME = "bs_admin_kmon_service_name";
static const std::string BS_ENV_ADMIN_DEFAULT_KMON_SERVICE_NAME = "bs.admin";
static const std::string BS_ENV_ADMIN_TASK_BEEPER_REPORT_INTERVAL = "bs_admin_task_beeper_report_interval";
static const std::string BS_ENV_ADMIN_RESTART_THRESHOLD_TO_REPORT = "bs_admin_restart_threshold_report";

static const std::string BS_ENV_KMON_SERVICE_NAME = "bs_kmon_service_name";
static const std::string BS_ENV_DEFAULT_KMON_SERVICE_NAME = "bs.worker";


#endif /*BUILD_SERVICE_COMMON_DEFINE_H*/
