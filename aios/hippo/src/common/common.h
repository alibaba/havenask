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
#ifndef HIPPO_COMMON_H
#define HIPPO_COMMON_H

//TODO: baisha.lf
//#include "src/config.h"

#include <string>
#include <assert.h>
#include <magic.h>
#include <memory>
#include <memory>
#include <vector>
#include <list>
#include <map>
#include <unordered_map>
#include <set>
#include <climits>
#include "autil/CommonMacros.h"
#include <set>
#include <climits>
#include "autil/CommonMacros.h"
#include <functional>
#include <math.h>
#include <numeric>

#define BEGIN_HIPPO_NAMESPACE(x) namespace hippo { namespace x {
#define END_HIPPO_NAMESPACE(x) } }
#define USE_HIPPO_NAMESPACE(x) using namespace hippo::x
#define HIPPO_NS(x) hippo::x

#define DP2_PROTO_NS deploy_express2::proto
#define DP2_NS deploy_express2

#if __cplusplus >= 201103L
    #define TYPEOF decltype
    #define HIPPO_INTERNAL_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;
    #define HIPPO_FUNCTION std::function
    #define HIPPO_BIND std::bind
    #define HIPPO_PLACEHOLDERS std::placeholders
    #define HIPPO_SHARED_PTR std::shared_ptr

#else
    #define TYPEOF typeof
    #define HIPPO_INTERNAL_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;
    #define HIPPO_FUNCTION std::function
    #define HIPPO_BIND std::bind
    #define HIPPO_PLACEHOLDERS std::placeholders
    #define HIPPO_SHARED_PTR std::shared_ptr

#endif

// basics
const std::string SLASH_SEPARATOR = "/";
const std::string COMMA_SEPARATOR = ",";
const std::string DOT_SEPARATOR = ".";
const std::string COLON_SEPARATOR = ":";
const std::string PATH_SEPARATOR = SLASH_SEPARATOR;
const std::string NAMESPACE_SEPARATOR = SLASH_SEPARATOR;
const std::string MOUNT_SEPARATOR = COLON_SEPARATOR;

// http
const long HTTP_CONN_FAST_EXPIRE_TIME = 3000;
const long HTTP_CONN_EXPIRE_TIME = 30000;
const std::string HTTP_CONTENT_TYPE_APPJSON = "application/json";
const std::string HTTP_CONTENT_TYPE_APPWWW = "application/x-www-form-urlencoded";
const std::string HTTP_CONTENT_TYPE_APPRAWSTREAM = "application/vnd.raw-stream";
const std::string HTTP_CONTENT_TYPE_APPRAWSTREAM_DOCKER = "application/vnd.docker.raw-stream";

// kernel version
const std::string OSVERSION_5U = "5";
const std::string OSVERSION_6U = "6";
const std::string OSVERSION_7U = "7";
const std::string DEFAULT_OS_VERSION = OSVERSION_7U;
const std::string KERNEL_VERSION_FILE = "/proc/version";
const std::string DEFAULT_KERNEL_VERSION = "3.10.0-327.ali2006_search.alios7.x86_64";
const std::string DEFAULT_CPU_MODEL = "Intel(R) Xeon(R) CPU E5-2682 v4 @ 2.50GHz";
const std::string NVME_DISK_DEV_PATH_PREFIX = "/dev/nvme";
const std::string DEFAULT_DISK_DEV_PATH = "/dev/nvme0n1";
const std::string DEFAULT_DISK_DEV_NUMBER = "259:0";
const std::string DEFAULT_TIME_ZONE = "Asia/Shanghai";

// const std::string MACHINE_OS_VERSION = "OS_VERSION";
// const std::string MACHINE_KERNEL_VERSION = "KERNEL_VERSION";
// const std::string MACHINE_ALIDOCKER_VERSION = "ALIDOCKER_VERSION";
// const std::string MACHINE_CPU_MODEL = "CPU_MODEL";

const std::string CORE_FILE_SUSPECTS_NAME = "core";

const std::string MEMORY_URI = "memory-mb";
const std::string VCORES_URI = "vcores";
const std::string IP_URI = "ip";
const std::string DISK_URI = "disk";
const std::string DISK_SIZE_URI = "size";
const std::string DISK_IOPS_URI = "iops";
const std::string RATIO_URI = "ratio";
const std::string ATTRIBUTE_INDEX = "index";
// const std::string GPU_URI = "gpu";
// const std::string FPGA_URI = "fpga";
const std::string GPU_URI = "yarn.io/gpu";
// const std::string FPGA_URI = "yarn.io/fpga";
const std::string FPGA_URI = "yarn.io/alifpga";

const std::string DEFAULT_ENABLE_RESERVED_CPU_TO_SHARE = "true";
const std::string DEFAULT_ENABLE_SHARE_CPU_TO_SHARE = "true";

const std::string DEFAULT_KMONITOR_ADDRESS = "localhost:4141";

const std::string HIPPO_MASTER_SCHEDULER_DOMAIN_ALIYARN = "ALIYARN";
const std::string HIPPO_MASTER_SCHEDULER_DOMAIN_HIPPO = "HIPPO";
//日常 daily, 集成 integration, 预发 staging, 线上 production
const std::string DEFAULT_RUN_ENVIRONMENT = "daily";


typedef int32_t slotid_t;
typedef std::string poduid_t;
typedef std::string appid_t;
typedef std::string clusterid_t;
typedef std::string groupid_t;
typedef std::string diskid_t;
typedef long long jiffies_t;
typedef std::vector<std::string> StringVec;
typedef std::set<std::string> StringSet;
typedef std::map<std::string, std::string> StringMap;
typedef std::map<std::string, std::string> KeyValueMap;
typedef std::map<std::string, std::string> CgroupMounts;

typedef std::map<std::string, int64_t> ResourceQuotas;
typedef std::map<std::string, ResourceQuotas > ClassQuotas;
typedef std::map<groupid_t, ClassQuotas > GroupClassQuotas;
typedef std::map<groupid_t, bool> GroupQuotaCtrls;

typedef std::map<std::string, double> ResourceRatios;
typedef std::map<std::string, ResourceRatios > ClassRatios;
typedef std::map<std::string, ClassRatios > GroupClassRatios;

typedef std::map<groupid_t, std::map<std::string, double> > QuotaGroupWeight;

typedef std::map<std::string, std::map<std::string, int32_t> > StrStrIntMap;

const std::string QUOTA_KEY_CPU_MAX = "cpu.max";
const std::string QUOTA_KEY_MEM_MAX = "mem.max";
const std::string QUOTA_KEY_WEIGHT = "weight";
const std::string PENDING_REASON_QUOTA = "quota";
const std::string PENDING_REASON_ALLOC = "allocate";

typedef std::map<std::string, void*> ProcessorParameters;
struct MetricValue {
    double value;
    int64_t time;
    MetricValue():value(-1.0), time(-1) {};

};
typedef std::map<std::string, MetricValue> MetricMap;
typedef std::map<std::string, double> ResourceUsageMap;
const int32_t MAX_SLOTID_INIT_VALUE = 0;
const int32_t MAX_RESOURCE_AMOUNT = 1 << 30;

struct PriorityRange
{
    int32_t begin;
    int32_t end;
    int32_t percent;
    PriorityRange(int32_t begin, int32_t end, int32_t percent):
        begin(begin), end(end), percent(percent) {};
};

struct AppReleaseRule
{
    std::string appId;
    std::string workDirTag;
    std::string reserveTimeRange;
    int64_t  reserveSecondsWhenRelease;
    int64_t begSeconds;
    int64_t endSeconds;
    AppReleaseRule() {
        reserveSecondsWhenRelease = 0;
        begSeconds = 0;
        endSeconds = 0;
    }
};

struct ReleaseRule {
    std::string defaultReserveTimeRange;
    int64_t  defaultReserveSecondsWhenRelease;
    std::map<std::string, AppReleaseRule> releaseRuleMap;
    ReleaseRule() {
        defaultReserveSecondsWhenRelease = 0;
    }
    bool getAppReleaseRule(const std::string &key, AppReleaseRule &rule) const {
        std::map<std::string, AppReleaseRule>::const_iterator iter = releaseRuleMap.find(key);
        if (iter != releaseRuleMap.end()) {
            rule = iter->second;
            return true;
        }
        return false;
    }
    void addAppReleaseRule(const std::string &key, const AppReleaseRule &rule) {
        releaseRuleMap[key] = rule;
    }
};

struct TimeSliceWMark
{
    int32_t b_hour;
    int32_t b_minute;
    int32_t b_second;

    int32_t e_hour;
    int32_t e_minute;
    int32_t e_second;

    int32_t wmark;
    TimeSliceWMark()
        : b_hour(0), b_minute(0), b_second(0)
        , e_hour(0), e_minute(0), e_second(0)
        , wmark(0) {}
};

struct ClusterConf {
    std::string cluster;
    int32_t weight;
    int32_t min;
    int32_t max;
    ClusterConf()
    : weight(0)
    , min(0)
    , max(INT_MAX)
    {}
};

struct SocketNodePolicy {
    int mempolicy;
    std::map<uint32_t, int32_t> socketsCpus; //放到每个socket多少个cpu

    SocketNodePolicy() : mempolicy(-1) {}
    
    std::vector<uint32_t> getSocketsVec() const{
        std::vector<uint32_t> sockets;
        for (auto &v : socketsCpus) {
            sockets.push_back(v.first);
        }
        return sockets;
    }
}; 

enum APP_RANK_LEVEL {
    HIGH = 0,
    MEDIUM = 1,
    LOW = 2,
    SYSTEM = 3
};

#define DEFAULT_AMON_AGENT_PORT 10086u

#define HIPPO_ANET_IOWORKER_THREAD_NUM 4
#define HIPPO_RPC_SERVER_THREAD_NUM 4
#define HIPPO_RPC_SERVER_QUEUE_SIZE 500
#define HIPPO_HTTP_RPCSERVER_THREAD_NUM 16
#define HIPPO_HTTP_RPCSERVER_QUEUE_SIZE 500

#define DEFAULT_AM_SERVICE_THREAD_NUM 8
#define DEFAULT_AM_SERVICE_QUEUE_SIZE 1000

#define DEFAULT_HEARTBEAT_SERVICE_THREAD_NUM 16
#define DEFAULT_HEARTBEAT_SERVICE_QUEUE_SIZE 10000

#define DEFAULT_SHARD_SIZE 97

#define DEFAULT_CLIENT_SERVICE_THREAD_NUM 4
#define DEFAULT_CLIENT_SERVICE_QUEUE_SIZE 500

#define DEFAULT_ADMIN_SERVICE_THREAD_NUM 1
#define DEFAULT_ADMIN_SERVICE_QUEUE_SIZE 500

#define DEFAULT_SYSTEM_SLOT_COUNT 8

#define LEADER_ELECTION_INTERVAL 10 * 1000000 // 10s

#define THREADNUM_FOR_PACKAGEMANAGER 16
#define QUEUESIZE_FOR_PACKAGEMANAGER 500
#define MAX_INSTALLER_COUNT 3000
#define THREADNUM_FOR_VOLUMEMANAGER 16
#define QUEUESIZE_FOR_VOLUMEMANAGER 500
#define MAX_MOUNTER_COUNT 3000

#define CLEAR_TRASH_RATIO 50 // per 100, means 0.5
#define CLEAR_TRASH_RATIO_TOTAL 100

#define DEFAULT_OPTION_INDEX -1
const std::string DEFAULT_YARN_ACTIVE_RM_PATH = "";
const std::string SLAVE_MANAGER_SERIALIZE_FILE = "slave_list";
const std::string SLAVE_MANAGER_CLUSTER_LIST_SERIALIZE_FILE = "cluster_list";
const std::string SLAVE_MANAGER_OFFLINE_SLOTS_SERIALIZE_FILE = "offline_slots";
const std::string SLAVE_DEFAULT_RUNTIME_CONFIG_FILE = "slave_runtime_config.default";
const std::string SLAVE_RUNTIME_CONFIG_SERIALIZE_FILE = "slave_runtime_config";
const std::string APP_PREDICT_USAGE_SERIALIZE_FILE = "app_predict_usages";
const std::string OLD_RESOURCE_MANAGER_SERIALIZE_FILE = "resource_plan";
const std::string RESOURCE_MANAGER_SERIALIZE_FILE = "resource_plan_0.8.0";
const std::string RESOURCE_MANAGER_SERIALIZE_FILE_NEW = "resource_plan_new";
const std::string MASTER_STRATEGY_SERIALIZE_FILE = "master_strategy";
const std::string APP_MANAGER_SERIALIZE_FILE = "applications";
const std::string PREFERENCE_SERIALIZE_FILE = "app_preference";
const std::string QUEUE_MANAGER_SERIALIZE_FILE = "queues";
const std::string QUEUE_LINK_SERIALIZE_FILE = "queue_links";
const std::string RULES_MANAGER_SERIALIZE_FILE = "rules";
const std::string CONFIG_MANAGER_SERIALIZE_FILE = "configs";
const std::string SYSTEM_SLOT_RESOURCE = "hippo_system";
// quota file
const std::string GROUP_QUOTA_CTRLS_SERIALIZE_FILE =
                                         "__hippo_group_quota_ctrls__";
const std::string GRANTED_QUOTA_SERIALIZE_FILE =
                                         "__hippo_group_class_quotas__";
const std::string DYNAMIC_QUOTA_SERIALIZE_FILE =
                                         "__hippo_dynamic_group_class_quotas__";
const std::string TOTAL_QUOTAS_SERIALIZE_FILE =
                                         "__hippo_total_quotas__";
const std::string REQUIRE_QUOTA_SERIALIZE_FILE =
                                         "__hippo_require_class_quotas__";
const std::string USED_QUOTA_SERIALIZE_FILE =
                                         "__hippo_used_class_quotas__";
const std::string UNPREEMPTABLE_USED_QUOTA_SERIALIZE_FILE =
                                         "__hippo_unpreemptable_used_class_quotas__";
// discard
// const std::string GRANTED_GROUP_QUOTA_SERIALIZE_FILE = "__hippo_group_quotas__";
// const std::string REQUIRE_QUOTA_SERIALIZE_FILE = "__hippo_require_quotas__";
// const std::string REQUIRE_QUOTA_DETAIL_SERIALIZE_FILE = "__hippo_require_detail_quotas__";
// const std::string USED_QUOTA_SERIALIZE_FILE = "__hippo_used_quotas__";
// const std::string USED_QUOTA_DETAIL_SERIALIZE_FILE = "__hippo_used_quotas_detail__";

const std::string REQUEST_MAP_RULES_SERIALIZE_FILE =
                                         "__hippo_map_rules_file__";
const std::string INTERNAL_APPMASTER_RESOURCE_TAG = "__internal_appmaster_resource_tag__";

const std::string LEADER_ELECTION_PATH = "hippo_election_path";
const std::string LEADER_ELECTION_BASE_NAME = "hippo_election";
const std::string AM_LEADER_ELECTION_BASE_NAME = "app_master_leader";
const std::string UNKNOWN_LEADER_ADDRESS = "unknown";
const std::string IS_NEW_START_FILENAME = "is_new_start";
const std::string MASTER_LOCAL_BACKUP_PATH = ".backup";
const std::string MASTER_RESOURCE_SCORE_STRATEGY_SERIALIZE_FILE =
                                         "resoure_score_strategy_file";
// monitor
const std::string ROLE_MASTER = "master";
const std::string ROLE_APP = "app";
const std::string ROLE_CLUSTER = "vcluster";
const std::string ROLE_APP_SLOT = "app_slot";
const std::string ROLE_APP_PROC = "app_proc";
const std::string ROLE_SLAVE = "slave";
const std::string ROLE_SLAVE_DISK = "slave_disk";
const std::string ROLE_SLAVE_APP = "slave_app";
const std::string ROLE_SLAVE_T4_APP = "slave_t4_app";

const std::string DEFAULT_BINARY_NAME = "default_process";

const std::string PRIORITY_CLASS_PRODUCT = "Product";
const std::string PRIORITY_CLASS_NON_PRODUCT = "NonProduct";
const std::string PRIORITY_CLASS_SYSTEM = "System";

const std::string INSTALLER_BIN_FILE = "installer.py";
const std::string INSTALL_TMP_PATH = ".tmpinstallpath";
const std::string PACKAGE_DB_PATH = ".pkg_db";
const std::string TMP_PACKAGE_INFOS_ARGS = "package_list";
const std::string TMP_PACKAGE_TRACE_INFO = "package_info";
const std::string TMP_PACKAGE_DIR = "packages";
const std::string TMP_PACKAGE_INST_ROOT = "root";
const std::string OLD_PKG_CACHE_DIRNAME = "hippo_package_caches";
const std::string PKG_CACHE_DIRNAME = "__hippo_package_caches__";
const std::string PKG_LINKS_DIRNAME = "links";
const std::string PKG_TRASH_DIRNAME = "package_trash";
const std::string PKG_USEINFO_FILENAME = ".__hippo_package_useinfo__";
const std::string LATEST_LINK_SYMBOL = ":latest";
const std::string DATA_CACHE_DIRNAME = "hippo_data_caches";
const std::string APP_INST_ROOT_DIR_NAME = "binary";
const std::string APP_PRE_INST_ROOT_DIR_NAME = ".pre/binary";
const std::string PRIVATE_PKG_CACHE_DIRNAME = "private_package_caches";
const std::string DISK_LINK_DIRNAME = "disk_links";
const std::string APP_LEFT_TAG_DIRNAME = "app_left_tags";
const std::string HIPPO_RESERVED_WORKDIR_FLAG = ".__hippo_reserved_workdir__";
const std::string HIPPO_SSH_WORKDIR = ".ssh";
const std::string HIPPO_PACKAGE_INSTALL_FLAG = ".__hippo__installing__";
const std::string HIPPO_PACKAGE_DONE_FLAG = ".__hippo__done__";
const std::string HIPPO_PACKAGE_POST_INSTALL_SCRIPT = "tmp/post_script.sh";
const std::string HIPPO_PACKAGE_OVERLAY_UPPER = ".__hippo_package_upper";
const std::string HIPPO_PACKAGE_OVERLAY_WORKDIR = ".__hippo_package_workdir";

const std::string HIPPO_COREDUMP_DIR = "__hippo_coredump__";
const std::string HIPPO_COREDUMP_INUSE_FLAG = ".__inuse__";
const std::string HIPPO_CORE_WRITER = "/tmp/coreWriter";
const std::string HIPPO_APP_CORE_LINK = ".coredumps";

const std::string DEFAULT_LOGGER_CONF = "HippoLogger.conf";
const std::string HIPPO_UTIL_BIN_DIR = "lib/python/site-packages/hippo_util/";
const std::string HIPPO_ETC_DIR = "etc/hippo/";

const std::string INTERNAL_T4_RESOURCE_NAME = "T4";
const std::string INTERNAL_DOCKER_RESOURCE_NAME = "docker";
const std::string INTERNAL_IP_RESOURCE_NAME = "ip";

const std::string HIPPO_APP_SHORT_NAME = "HIPPO_APP_SHORT_NAME";
const std::string HIPPO_ROLE_SHORT_NAME = "HIPPO_ROLE_SHORT_NAME";
const std::string HIPPO_APP_TYPE = "HIPPO_APP_TYPE";
const std::string HIPPO_ENV_APP = "HIPPO_APP";
const std::string HIPPO_ENV_ROLE = "HIPPO_ROLE";
const std::string HIPPO_ENV_WORKDIR_TAG = "HIPPO_WORKDIR_TAG";
const std::string HIPPO_ENV_PREFIX = "HIPPO_";
const std::string HIPPO_ENV_SLOT_ID = "HIPPO_SLOT_ID";
const std::string HIPPO_ENV_DOCKERD_HTTPPORT = "HIPPO_ENV_DOCKERD_HTTPPORT";
const std::string HIPPO_ENV_SERVICE_NAME = "HIPPO_SERVICE_NAME";
const std::string HIPPO_ENV_APP_INST_ROOT = "HIPPO_APP_INST_ROOT";
const std::string HIPPO_ENV_VM_IP = "HIPPO_ENV_VM_IP";
const std::string HIPPO_ENV_VM_MASK = "HIPPO_ENV_VM_MASK";
const std::string HIPPO_ENV_VM_GATEWAY = "HIPPO_ENV_VM_GATEWAY";
const std::string HIPPO_ENV_VM_HOSTNAME = "HIPPO_ENV_VM_HOSTNAME";
const std::string HIPPO_ENV_VM_HOSTNIC= "HIPPO_ENV_VM_HOSTNIC";
const std::string HIPPO_ENV_VM_SN = "HIPPO_ENV_VM_SN";
const std::string HIPPO_ENV_SLAVE_IP = "HIPPO_SLAVE_IP";
const std::string HIPPO_ENV_SLAVE_PORT = "HIPPO_SLAVE_PORT";
const std::string HIPPO_ENV_SLAVE_HTTP_PORT = "HIPPO_SLAVE_HTTP_PORT";
const std::string HIPPO_ENV_FORCE_EMPTY_CTL = "HIPPO_FORCE_EMPTY_CTL";
const std::string HIPPO_ENV_DISK_QUOTA_CTL = "HIPPO_DISK_QUOTA_CTL";
const std::string HIPPO_ENV_NO_SYS_OOM_KILLER_CTL = "HIPPO_NO_SYS_OOM_KILLER_CTL";
const std::string HIPPO_ENV_MEM_WMARK = "HIPPO_MEM_WMARK";
const std::string HIPPO_ENV_MEM_EXTRA_BYTES = "HIPPO_MEM_EXTRA_BYTES";
const std::string HIPPO_ENV_MEM_EXTRA_RATIO = "HIPPO_MEM_EXTRA_RATIO";
const std::string HIPPO_ENV_SLAVE_WORKDIR = "HIPPO_SLAVE_WORKDIR";
const std::string HIPPO_ENV_PROC_REAL_WORKDIR = "HIPPO_PROC_REAL_WORKDIR";
const std::string HIPPO_ENV_SLAVE_TRASHDIR = "HIPPO_SLAVE_TRASHDIR";
const std::string HIPPO_ENV_SLAVE_UTILDIR = "HIPPO_SLAVE_UTILDIR";
const std::string HIPPO_ENV_APP_CHECKSUM = "HIPPO_APP_CHECKSUM";
const std::string HIPPO_ENV_APP_DECLARE_TIME = "HIPPO_APP_DECLARE_TIME";
//const std::string HIPPO_ENV_APP_TAG = "HIPPO_APP_ROLE";
const std::string HIPPO_ENV_PROC_INSTANCEID = "HIPPO_PROC_INSTANCEID";
const std::string HIPPO_ENV_PROC_WORKDIR = "HIPPO_PROC_WORKDIR";
const std::string HIPPO_ENV_APP_REAL_WORKDIR = "HIPPO_APP_REAL_WORKDIR";
const std::string HIPPO_ENV_APP_WORKDIR = "HIPPO_APP_WORKDIR";
const std::string HIPPO_ENV_DP2_SLAVE_IP = "HIPPO_DP2_SLAVE_IP"; // todo
const std::string HIPPO_ENV_DP2_SLAVE_PORT = "HIPPO_DP2_SLAVE_PORT";
const std::string HIPPO_ENV_AMON_SLAVE_IP = "HIPPO_AMON_SLAVE_IP"; // todo
const std::string HIPPO_ENV_AMON_SLAVE_PORT = "HIPPO_AMON_SLAVE_PORT"; // todo
const std::string HIPPO_ENV_TOTAL_SLAVE_WORKDIRS = "HIPPO_TOTAL_SLAVE_WORKDIRS";
const std::string HIPPO_ENV_DEFAULT_OSS_ENDPOINT = "HIPPO_DEFAULT_OSS_ENDPOINT";
const std::string HIPPO_ENV_DEFAULT_OSS_ACCESS_KEY = "HIPPO_DEFAULT_OSS_ACCESS_KEY";
const std::string HIPPO_ENV_DEFAULT_OSS_ACCESS_ID = "HIPPO_DEFAULT_OSS_ACCESS_ID";
const std::string HIPPO_ENV_CONTAINER_NAME = "HIPPO_CONTAINER_NAME";
const std::string HIPPO_ENV_OLD_POD = "HIPPO_OLD_POD";
const std::string HIPPO_ENV_K8S_POD = "HIPPO_K8S_POD";
const std::string HIPPO_ENV_REGION_ID = "HIPPO_ENV_REGION_ID";
const std::string HIPPO_ENV_DFS_CONFIG_PATH = "HIPPO_ENV_DFS_CONFIG_PATH";
const std::string HIPPO_ENV_STORAGE_LINKS = "HIPPO_ENV_STORAGE_LINKS";
const std::string HIPPO_ENV_DFS_STORAGE_LIST = "HIPPO_ENV_DFS_STORAGE_LIST";
const std::string HIPPO_ENV_DFS_NUWA = "HIPPO_ENV_DFS_NUWA";
const std::string HIPPO_ENV_CGROUP_PARENT = "CGROUP_PARENT";
const std::string HIPPO_ENV_CGROUP_ROOT = "HIPPO_ENV_CGROUP_ROOT";
const std::string SHENNONG_ENV_TAG_CLUSTER = "SHENNONG_TAG_CLUSTER";
const std::string SHENNONG_ENV_TAG_GROUP_NAME = "SHENNONG_TAG_GROUP_NAME";
const std::string SHENNONG_ENV_TAG_PROCESS_NAME = "SHENNONG_TAG_PROCESS_NAME";
const std::string SHENNONG_ENV_TAG_INSTANCE = "SHENNONG_TAG_INSTANCE";
const std::string SHENNONG_AGENT_SOCK = "/tmp/shennongAgentService.sock";
const std::string SHENNONG_INFLUX_SOCK = "/tmp/shennongInfluxClient.sock";

// labels
const std::string HIPPO_LABEL_NONE = "NONE";
const std::string HIPPO_LABEL_MACHINE_TYPE = "hippo.label.machineType";
const std::string HIPPO_LABEL_HOST_IP = "hippo.label.hostIp";
const std::string HIPPO_LABEL_HOST_PORT = "hippo.label.hostPort";
const std::string HIPPO_LABEL_HIPPO_CLUSTER = "hippo.label.hippoCluster";
const std::string HIPPO_LABEL_KERNEL_VERSION = "hippo.label.kernelVersion";
const std::string HIPPO_LABEL_ALIDOCKER_VERSION = "hippo.label.alidockerVersion";
const std::string HIPPO_LABEL_CPU_MODEL = "hippo.label.cpuModel";
const std::string HIPPO_LABEL_UPTIME = "hippo.label.uptime";

const std::string HIPPO_SLAVE_TRASH_DIRNAME = ".trash";
const std::string HIPPO_APP_WORKDIR_TIMESTAMP = "._timestamp_";
const std::string DEFAULT_HIPPO_SERVICE_NAME = "Hippo";
const std::string SLOT_PLAN_PERSIST_FILE_NAME = "_hippo_slave_slots_.persist";
const std::string SYSTEM_RULES_PERSIST_FILE_NAME = "_hippo_system_rules_.persist";
const std::string RUNTIME_CONFIG_PERSIST_FILE_NAME = "_hippo_slave_runtime_config_.persist";
const std::string POD_VOLUMES_PERSIST_FILE_NAME = "_hippo_pod_volumes_.persist";
const std::string POD_NETWORKS_PERSIST_FILE_NAME = "_hippo_pod_networks_.persist";
const std::string POD_STATUS_PERSIST_FILE_NAME = "_hippo_pod_status_.persist";
// const std::string APP_DATA_REF_TAG = "__app_data_ref_tag__";
// const std::string DONE_TAG_FILE_NAME = "__hippo_done_tag__";
const std::string DATA_INFO_FILE_NAME = "__hippo_data_info__";
const std::string APP_DATA_DIR_NAME = "__data__";

// default resource name in hippo
const double DEFAULT_PRODUCT_CPU_AVERAGE_LOAD = 0.5;
const double DEFAULT_PRODUCT_MEM_AVERAGE_LOAD = 1.0;
const double DEFAULT_NON_PRODUCT_CPU_AVERAGE_LOAD = 1.0; // for testcase
const double DEFAULT_NON_PRODUCT_MEM_AVERAGE_LOAD = 1.0;
const double DEFAULT_NON_PRODUCT_DISK_SIZE_AVERAGE_LOAD = 0.6;
const double DEFAULT_PRODUCT_DISK_SIZE_AVERAGE_LOAD = 0.6;
const double DEFAULT_BUFFER_FACTOR = 0.15;
const double DEFAULT_PROD_CPU_MIN_GUARANTEE = 0.45;
const double DEFAULT_NON_PROD_CPU_MIN_GUARANTEE = 0.4;
const double DEFAULT_PROD_MEM_MIN_GUARANTEE = 1.0;
const double DEFAULT_NON_PROD_MEM_MIN_GUARANTEE = 1.0;
const double DEFAULT_PROD_DISK_SIZE_MIN_GUARANTEE = 0.4;
const double DEFAULT_NON_PROD_DISK_SIZE_MIN_GUARANTEE = 0.4;
const uint32_t DEFAULT_CPU_VARIATION_VALUE = 50;
const uint32_t DEFAULT_MEM_VARIATION_VALUE = 1024;
const uint32_t DEFAULT_DISK_SIZE_VARIATION_VALUE = 1024;
const uint32_t DEFAULT_PREDICT_INTERVAL = 60; //s
// after app start 600s, then do predict
const uint32_t DEFAULT_START_PREDICT_INTERVALS = 10;
const uint32_t DEFAULT_CPU_AMOUNT_WORTH_PREDICT = 400; //4core
const uint32_t DEFAULT_MEM_AMOUNT_WORTH_PREDICT = 8192;//8GB
const std::string CPU_USAGE_PERCENT = "cpuUsagePercent";
const std::string MEM_USAGE_PERCENT = "memoryUsagePercent";
const std::string BOOT_DISK_USED = "bootDiskUsed";
const std::string BOOT_DISK_LIMIT = "bootDiskLimit";
const std::string BOOT_DISK_USAGE_PERCENT = "bootDiskUsagePercent";
const std::string DISK_USED = "diskUsed";
const std::string DISK_LIMIT = "diskLimit";
const std::string DISK_USAGE_PERCENT = "diskUsagePercent";
const std::string MEM_USED = "memoryUsed";
const std::string MEM_LIMIT = "memoryLimit";
const std::string CPU_USED = "cpuUsed";
const std::string OLD_PACKAGE_CACHE_SIZE = "OLD_PACKAGE_CACHE_SIZE";
const std::string NEW_PACKAGE_CACHE_SIZE = "NEW_PACKAGE_CACHE_SIZE";
const std::string NEW_INUSED_PACKAGE_CACHE_SIZE = "NEW_INUSED_PACKAGE_CACHE_SIZE";
const std::string INUSED_IMAGES_SIZE = "inusedImagesSize";
const std::string ALL_IMAGES_SIZE = "allImagesSize";
const std::string MASTER_SCHEDULE_PREFIX = "master.schedule.";
const std::string MASTER_PLUGINS_FILE = "plugins.json";
const std::string K8S_CLIENT_CONF_FILE = "k8s_client_conf.json";
const int64_t DEFAULT_EXPIRE_LAST_APP_TAG_RES_USAGE = 4ll * 3600 * 1000 * 1000; // 4hour

const std::string CPU_MAIN_RANKER = "cpuMainRanker";

// for multiScorePolicy
const double MAX_SCORE =  5000000.0;
const double MIN_SCORE = -5000000.0;
const int32_t SCORE_FACTOR = 1000000;
// resource definition
const std::string RES_NAME_CPU = "cpu";
const std::string RES_NAME_PREDICT_CPU = "predict_cpu";
const std::string RES_NAME_MEM = "mem";
const std::string RES_NAME_PREDICT_MEM = "predict_mem";
const std::string RES_NAME_DISK_PREDICT_SIZE = "disk_predict_size";
const std::string RES_NAME_DISK_LIMIT_SIZE = "disk_limit_size";
const std::string RES_NAME_CPU_QUOTA = "cpu_quota";
const std::string RES_NAME_CPU_LIMIT = "cpu_limit";
const std::string RES_NAME_MEM_LIMIT = "mem_limit";
const std::string RES_NAME_GPU = "gpu";
const std::string RES_NAME_FPGA = "fpga";
const std::string RES_NAME_POV = "pov";
const std::string RES_NAME_NPU = "npu";
const std::string RES_NAME_UNDERLAY = "underlay";
const std::string RES_NAME_DISK = "disk";
const std::string RES_NAME_DISK_SIZE = "disk_size";
const std::string RES_NAME_DISK_IOPS = "disk_iops";
const std::string RES_NAME_DISK_RATIO = "disk_ratio";
const std::string RES_NAME_DISK_READ_IOPS = "disk_read_iops";
const std::string RES_NAME_DISK_WRITE_IOPS = "disk_write_iops";
const std::string RES_NAME_DISK_READ_BPS = "disk_read_bps";   //MB/s
const std::string RES_NAME_DISK_WRITE_BPS = "disk_write_bps"; //MB/s
const std::string RES_NAME_DISK_BUFFER_WRITE_BPS = "disk_buffer_write_bps"; //MB/s

const std::string DADI_TEST_CONTAINER_NAME = "HIPPO_DADI_TEST_CONTAINER";
const std::string DEFAULT_DADI_TEST_IMAGE = "registry.invalid.docker.com/hippo/hippo_agent:8.67_DADI";
const std::string RES_NAME_DADI = "DADI";
const std::string RES_NAME_T4 = "T4";
const std::string RES_NAME_DOCKER = "docker";
const std::string RES_NAME_KATA = "kata";
/* (inet addr:10.125.224.79  Bcast:10.125.227.255  Mask:255.255.252.0
   Hostname:search224079.bja.tbsite.net HostNic:bond0) ===> 
   "ip_10.125.224.79:10.125.227.255:255.255.252.0:search224079.bja.tbsite.net:bond0"
*/
const std::string RES_NAME_IP = "ip";
const std::string RES_NAME_IP_PREFIX = "ip_";
const std::string RES_NAME_IP_SEPARATOR = ":"; 
const std::string RES_NAME_SN_PREFIX = "sn_";
const std::string RES_NAME_DISK_PREFIX = "disk_";
const std::string RES_NAME_DISK_SIZE_PREFIX = "disk_size_";
const std::string RES_NAME_DISK_PREDICT_SIZE_PREFIX = "disk_predict_size_";
const std::string RES_NAME_DISK_LIMIT_SIZE_PREFIX = "disk_limit_size_";
const std::string RES_NAME_DISK_IOPS_PREFIX = "disk_iops_";
const std::string RES_NAME_DISK_RATIO_PREFIX = "disk_ratio_";
const std::string RES_NAME_DISK_READ_IOPS_PREFIX = "disk_read_iops_";
const std::string RES_NAME_DISK_WRITE_IOPS_PREFIX = "disk_write_iops_";
const std::string RES_NAME_DISK_READ_BPS_PREFIX = "disk_read_bps_";
const std::string RES_NAME_DISK_WRITE_BPS_PREFIX = "disk_write_bps_";
const std::string RES_NAME_DISK_BUFFER_WRITE_BPS_PREFIX = "disk_buffer_write_bps_";
const std::string RES_NAME_CPU_MODEL_NUM = "cpu_model_num";
const std::string RES_NAME_NETWORK_BANDWIDTH = "net_bw";

const int32_t DEFAULT_CPU_AMOUNT = 1;
const int32_t DEFAULT_PREDICT_CPU_AMOUNT = 1;
const int32_t DEFAULT_MEM_AMOUNT = 1;
const int32_t DEFAULT_DISK_READ_IOPS_AMOUNT = 1;
const int32_t DEFAULT_DISK_WRITE_IOPS_AMOUNT = 1;
const int32_t DEFAULT_DISK_READ_BPS_AMOUNT = 1;
const int32_t DEFAULT_DISK_WRITE_BPS_AMOUNT = 1;
const int32_t DEFAULT_DISK_SIZE_AMOUNT = 1;
const int32_t DEFAULT_DISK_RATIO_AMOUNT = 1;
const int32_t DEFAULT_DISK_BUFFER_WRITE_BPS_AMOUNT = 1;
const int32_t SYSTEM_APP_DEFAULT_DISK_RATIO_AMOUNT = 0;
const int32_t  DEFAULT_NETWORK_BANDWIDTH = 20000; // Mb/s
const int32_t DEFAULT_CPU_MODEL_NUM = 79;
const double DEFAULT_ROUND_CPU_PERCENT = 0.05;
const double DEFAULT_ROUND_MEM_PERCENT = 0.02;
const double DEFAULT_ROUND_DISK_SIZE_PERCENT = 0.02;

const std::string RES_NAME_ASW = "ASW";
const std::string RES_NAME_PSW = "PSW";
const std::string RES_NAME_RACK = "Rack";
const std::string RES_NAME_FRAME = "ParentServiceTag";
const std::string RES_NAME_PLAYBACK = "PlaybackToYarn";

// for pods
const std::string VIRTUAL_POD_NAME_PREFIX = "__pod_";
const std::string BATCHPOD_POD_NAME_PREFIX = "batchpod-";
const std::string POD_LABEL_APP = "app";
const std::string POD_LABEL_TAG = "tag";
const std::string POD_LABEL_ROLE = "role";
const std::string POD_LABEL_REQUIREMENTID = "requirementId";
const std::string POD_LABEL_HOME_CLUSTER = "homeCluster";

const std::string RULE_REQUEST_APP = "hippo_buddy_buffer_app";
const std::string DEFAULT_BUFFER_GROUP_ID = "/buddy_buffer";
const std::string BUDDY_POD_REQUEST_NAME_PREFIX = "__buddy_pod_";
const std::string BUDDY_RULE_INTERNAL_PREFIX = "__hippo_";
const std::string BUDDY_INFO_SERIALIZE_FILE = "buddy_info";
const int32_t MAX_RESERVE_APP_TTL = 1800; // 0.5h

// used for armory sync, this should be in slot payloads
const std::string ARMORY_DECLARE_PREFIX = "armory_";

const int32_t CGROUP_SLICE_UPDATE_LOOP = 50;

const int64_t METRIC_EXPIRE_INTERVAL = 60 * 1000 * 1000;
const uint64_t DEFAULT_COLLECTLOOP_INTERVAL = 1000 * 1000; // us

const int64_t CGROUP_PATH_CHECK_INTERVAL_FOR_METRIC_COLLECTOR = 60 * 1000 * 1000; // 3 min

const std::string RES_NAME_MACHINE_TYPE_PREFIX = "machineType_";
const std::string MACHINE_INFO_FILE_NAME =  "machine_resource_infos";

const std::string DEFAULT_USER_ADMIN = "admin";
const std::string DEFAULT_USER_HIPPO = "hippo";
const std::string DEFAULT_USER_ROOT = "root";
const std::string ROOT_UID_GID_STRING = "0:0";

// mount
const std::string MOUNT_MNTTYPE_ALL = "all";
const std::string MOUNT_MNTTYPE_OVERLAY = "overlay";
const std::string MOUNT_MNTOPTS_LOWERDIR = "lowerdir";

// queue
const std::string ROOT_QUEUE_NAME = "root";
const std::string DEFAULT_QUEUE_NAME = "default";
const std::string SYSTEM_QUEUE = "system";
const std::string NONPROD_QUEUE = "non_prod";
const std::string DEFAULT_RESOURCE_GROUP = "all_ungrouped_resource";
const int32_t NON_DEFINED_QUEUE_SLAVES_AMOUNT = -1;
const int32_t MAX_QUEUE_SIZE = 10000;

// group
const std::string ROOT_GROUP_ID = "/root";
const std::string UNLIMITED_GROUP_ID = "/__hippo_unlimited__";
const std::string EMPTY_GROUP_ID = "";
const std::string DEFAULT_GROUP_ID = ROOT_GROUP_ID;
const std::string PROD_DEFAULT_GROUP_ID = DEFAULT_GROUP_ID;
const std::string NONPROD_DEFAULT_GROUP_ID = UNLIMITED_GROUP_ID;
const std::string SYSTEM_DEFAULT_GROUP_ID = UNLIMITED_GROUP_ID;

// disk
const std::string DISK_PREFIX = "/ssd/disk";
const std::string DISK_WORKDIR_SUBDIR = "hippo_slave";
const std::string BOOT_DISK_NAME = "9999"; // hippo_slave's boot disk
const std::string BOOT_DISK_SIZE_RES_NAME = RES_NAME_DISK_SIZE_PREFIX +
                                            BOOT_DISK_NAME;
const std::string BOOT_DISK_PREDICT_SIZE_RES_NAME = RES_NAME_DISK_PREDICT_SIZE_PREFIX +
                                            BOOT_DISK_NAME;
const std::string BOOT_DISK_IOPS_RES_NAME = RES_NAME_DISK_IOPS_PREFIX +
                                            BOOT_DISK_NAME;
const std::string BOOT_DISK_READ_IOPS_RES_NAME = RES_NAME_DISK_READ_IOPS_PREFIX +
                                            BOOT_DISK_NAME;
const std::string BOOT_DISK_WRITE_IOPS_RES_NAME = RES_NAME_DISK_WRITE_IOPS_PREFIX +
                                            BOOT_DISK_NAME;
const std::string BOOT_DISK_READ_BPS_RES_NAME = RES_NAME_DISK_READ_BPS_PREFIX +
                                            BOOT_DISK_NAME;
const std::string BOOT_DISK_WRITE_BPS_RES_NAME = RES_NAME_DISK_WRITE_BPS_PREFIX +
                                            BOOT_DISK_NAME;
const std::string BOOT_DISK_BUFFER_WRITE_BPS_RES_NAME = RES_NAME_DISK_BUFFER_WRITE_BPS_PREFIX +
                                            BOOT_DISK_NAME;
const std::string BOOT_DISK_RATIO_RES_NAME = RES_NAME_DISK_RATIO_PREFIX +
                                             BOOT_DISK_NAME;
const int32_t MAX_DISK_RATIO_AMOUNT = 10000;

const uint32_t DIR_QUOTA_MAGIC_NUMBER = 1<<(32-8);

#define SLAVE_HEARTBEAT_INTERVAL_MS 1000u
#define SLAVE_HEARTBEAT_TIMEOUT_MS 1000u
#define SLAVE_LOCATE_ZK_TIMEOUT_S 10
#define AM_LOCATE_ZK_TIMEOUT_S 10

// for metric monitor
const uint64_t DEFAULT_METRICMONITOR_WORKLOOP_INTERVAL = 1000 * 1000; // us

const uint64_t DEFAULT_SLOTGROUPMANAGER_WORKLOOP_INTERVAL = 100 * 1000; // us
const uint64_t DEFAULT_PERF_SAMPLE_LOOP_INTERVAL = 1000 * 1000; // us
const uint32_t DEFAULT_DP_SLAVE_PORT = 9898;
const uint32_t DEFAULT_DOCKER_DAEMON_HTTP_PORT = 4243;
const uint32_t DEFAULT_SLAVE_RECLAIM_INTERVAL = 600;  // s
const uint64_t DEFAULT_PACKAGEMANAGER_WORKLOOP_INTERVAL = 100 * 1000; //us
const uint64_t DEFAULT_VOLUMEEMANAGER_WORKLOOP_INTERVAL = 100 * 1000; //us
const uint64_t DEFAULT_NETWORK_RECLAIMER_WORKLOOP_INTERVAL = 60*1000 * 1000; //us
const int64_t DEFAULT_MAX_SLOT_RESERVE_TIME = 4 * 3600; // sec
const uint32_t ONE_DAY_SECONDS = 86400; // sec

const int64_t STOP_TIMEOUT_FOR_KILL = 0 * 1000 * 1000; // 0 s

const uint32_t DEFAULT_SLAVE_HTTP_PORT = 7012;
const uint32_t DEFAULT_SLAVE_RESTFUL_HTTP_PORT = 7013;

// hippo container label keys
const std::string HIPPO_CONTAINER_LABEL_SLOT_ID = "ali.SlotId";
const std::string HIPPO_CONTAINER_LABEL_BIZ_NAME = "ali.BizName";
const std::string HIPPO_CONTAINER_LABEL_HIPPO_TAG = "ali.HippoTag";
const std::string HIPPO_CONTAINER_LABEL_WORKDIR_TAG = "ali.WorkDirTag";
const std::string HIPPO_CONTAINER_LABEL_PRIORITY = "ali.Priority";
const std::string HIPPO_CONTAINER_LABEL_REQUIRED_CPU = "ali.CpuQuota";
const std::string HIPPO_CONTAINER_LABEL_REQUIRED_MEM = "ali.MemoryLimit";
const std::string HIPPO_CONTAINER_LABEL_GROUP_ID = "ali.GroupId";
const std::string HIPPO_CONTAINER_LABEL_QUOTA_ID = "QuotaId";
const std::string HIPPO_CONTAINER_LABEL_DISK_QUOTA = "DiskQuota";
const std::string HIPPO_CONTAINER_LABEL_APP_NAME = "HIPPOAPP";
const std::string HIPPO_CONTAINER_LABEL_TAG_NAME = "HIPPOTAG";
const std::string HIPPO_CONTAINER_LABEL_APP_WORKDIR = "HIPPO_APP_WORKDIR";
const std::string HIPPO_CONTAINER_LABEL_CGROUP_PATH = "ali.CgroupPath";
const std::string HIPPO_CONTAINER_LABEL_RUN_MODE = "ali.RunMode";

//k8s node resource prefix
const std::string HIPPO_SLAVE_RESOURE_FOR_K8S_NODE = "node.hippo.io/";

const std::string CONTAINER_ENGINE_POUCH = "pouch";
const std::string CONTAINER_ENGINE_DOCKER = "docker";
const std::string DOCKER_POUCH = "`command -v pouch || command -v docker`";
const std::string T4VM_ENTRYPOINT = "/sbin/init";
const std::string T4START = DOCKER_POUCH + " run";
const std::string T4STOP = DOCKER_POUCH + " stop";
const std::string T4REMOVE = DOCKER_POUCH + " rm";
const std::string T4RESIZE = DOCKER_POUCH + " version";
const std::string DOCKERLOGPREFIX = "docker.log.";
const std::string CGMOUNTPOINT = "/cgroup";
const std::string CGMOUNTPOINT_7U = "/sys/fs/cgroup";
const uint64_t DEFAULT_CPU_SHARE = 1024;

// deprecated
const uint64_t MAXMUM_PROD_CPU_SHARE = (1 << 14);
const uint64_t MINMUM_PROD_CPU_SHARE = (1 << 7);
const uint64_t MAXMUM_NON_PROD_CPU_SHARE = (1 << 7);
const uint64_t MINMUM_NON_PROD_CPU_SHARE = 2;

const uint64_t MAX_CPUSHARE = (1 << 18);
const uint64_t PROD_CPUSHARE_FACTOR = 1024;
const uint64_t HIGH_NP_CPUSHARE_FACTOR = (1 << 13);
const uint64_t MIDDLE_NP_CPUSHARE_FACTOR = (1 << 8);
const uint64_t LOW_NP_CPUSHARE_FACTOR = (1 << 1);

const uint64_t CPU_CFS_PERIOD = 100 * 1000; // us, 100ms=>linux default
const int64_t  CPU_CFS_QUOTA_MIN = 1;
const int32_t  MEM_NON_PROD_QUOTA_MIN = 1024; // 1GB

const uint64_t  ONEMB = 1024*1024; // 1MB
const uint64_t  ONE = 1;
const uint64_t  ONE_MILLION = 1000 * 1000;
const uint64_t  ONE_THOUSAND = 1000;
const uint64_t GiB_SHIFT = 30;
const uint64_t MiB_SHIFT = 20;
const uint64_t KiB_SHIFT = 10;

const std::string CONTAINER_WORKDIR = "/hippo_workdir";
const std::string CONTAINER_BINARYCACHE = "/hippo_binary";
const std::string CONTAINER_PRESCRIPT_DIR = "/hippo_scripts";
const std::string CONTAINER_ROOT_PRESCRIPT = "init_root.sh";
const std::string CONTAINER_USER_PRESCRIPT = "init_user.sh";
const std::string CONTAINER_HOME = ".rootfs";
const std::string ROOTFS_HOME = "/home/docker/overlay";
const std::string DEFAULT_MOUNT_FS = "/proc/mounts";
const std::string DEFAULT_CPU_LOADAVG_FILE = "/proc/loadavg";
const std::string DEFAULT_CPU_STAT_FILE = "/proc/stat";
const std::string DEFAULT_MEM_INFO_FILE = "/proc/meminfo";
const std::string DEFAULT_ZONE_INFO_FILE = "/proc/zoneinfo";
const std::string DEFAULT_DISK_STATS_FILE = "/proc/diskstats";
const std::string DEFAULT_NET_DEV_FILE = "/proc/net/dev";
const std::string WORKDIR_PREFIX = "/hippo";

const std::string T4_SSHME = "sshme";
const std::string SSHD_CMDLINE = "/usr/sbin/sshd";
const std::string CNI_STARTER = "cni_starter.py";
const std::string OLD_STARTER = "starter.py";
const std::string STARTER = "starter";
const std::string CGROUP_TYPE = "cgroup";
const std::string CONTAINER_TYPE = "container";
const std::string PROCESS_INFO_FILE = "process_info.json";
const std::string CNI_PROCESS_INFO_FILE = "process_info.py";
const std::string PROCESS_PID_FILE = "process.pid";
const std::string PROCESS_EXITCODE_FILE = "process.exitcode";
const std::string PROCESS_CGROUP_PATH = "PROCESS_CGROUP_PATH";
const std::string OOM_SCORE_ADJ = "OOM_SCORE_ADJ";
const std::string APP_QUOTA_FILE = "quota_id";
const std::string PERSISTENT_VOLUMES_DIR = "persistent_volumes";
const std::string CONTAINER_SCRIPT_DONE = "/var/done/script_done_root";
const std::string ISEARCH5_DATA = "/home/admin/isearch5_data";
const std::string ISEARCH5_DATA_5U = "/home/admin/5u_isearch5_data";
const std::string ISEARCH5_DATA_7U = "/home/admin/7u_isearch5_data";
const std::string ETC_PASSWD = "/etc/passwd";
const std::string ETC_GROUP = "/etc/group";
const std::string ETC_SHADOW = "/etc/shadow";
const std::string ETC_NETWORK = "/etc/sysconfig/network";
const std::string ETC_NETWORK_SCRIPTS = "/etc/sysconfig/network-scripts";
const std::string ETC_INIT_NETWORK = "/etc/init.d/network";
const std::string ETC_SYSCTL_CONF = "/etc/sysctl.conf";
const std::string ETC_RESOLV_CONF = "/etc/resolv.conf";
const std::string ETC_LOCALTIME = "/etc/localtime";
const std::string EMPTY_NETWORK_FILE_PLACEHOLDER = "/tmp/HIPPO_EMPTY_FILE_PLACEHOLDER";

// const std::string EAGLE_LOG = "/home/admin/logs";
const std::string APSARA = "/apsara";
const std::string HADOOP = "/home/hadoop.devel";
const std::string OS_5U_IMAGE = "registry.invalid.docker.com/hippo/hippo_alios5u7_base";
const std::string OS_7U_IMAGE = "registry.invalid.docker.com/hippo/hippo_alios7u2_base";
const std::string HIPPO_IMAGE = "registry.invalid.docker.com/hippo/hippo";
const std::string DEFAULT_7U_IMAGE = "registry.invalid.docker.com/hippo/hippo_alios7u2_base:1.8";

const std::string BALANCE_FACTOR_SCORE_TABLE_KEY = "balance_factor";

const std::string SUPER_CMD = "/opt/hippo/bin/hippo_exec";
const std::string SUDO_CMD = "sudo";
const std::string BIN_SUDO_CMD = "/bin/sudo";
const std::string USR_BIN_SUDO_CMD = "/usr/bin/sudo";

// max restart interval: DEFAULT_PROCESS_RESTART_INTERVAL * 1.3^10
const double RESTART_INTERVAL_BASE_NUMBER = 1.3; 
const int32_t MAX_RESTART_INTERVAL_EXPONENT = 10; 
const int32_t RESTART_INTERVAL_EXPIRE_EXCEED_TIME = 10 * 1000 * 1000;
const double UNLIMITED_CPU_QUOTA_SAFE_RATIO = 1000;
const std::string CORE_FILE_STR_PREFIX = "application/x-coredump";
// proc
const std::string PID_INFO_SPLITTER = ":";

// do spreadout turns number most
const int32_t DEFAULT_MAX_ALLOCATION_SCHEDULE_TURNS = 20;

//metaTag keys
const std::string META_TAG_WORKDIR = "META_TAG_WORKDIR";
const std::string META_TAG_RESOURCE = "META_TAG_RESOURCE";
const std::string META_TAG_HOME_CLUSTER = "META_TAG_HOME_CLUSTER";
const std::string META_TAG_FEDERATED_CLUSTER = "META_TAG_FEDERATED_CLUSTER";
const std::string META_TAG_INTERNAL_REQUIREMENT_ID = "META_TAG_INTERNAL_REQUIREMENT_ID";
const std::string META_USE_RESERVED_RESOURCE = "META_USE_RESERVED_RESOURCE";
const std::string META_PREFER_TEXT = "META_PREFER_TEXT";
const std::string META_TAG_ARMORY_SYNC = "META_TAG_ARMORY_SYNC";

// constraint policy bit map
const uint64_t NOCONS_POLICY_MASK = ((1 << 8) - 1);
const uint64_t NOCONS_MEM = (1<<0);
const uint64_t NOCONS_CPU = (1<<1);
const uint64_t NOCONS_DISK_SPACE = (1<<2);
const uint64_t NOCONS_DISK_IO = (1<<3);
const uint64_t NOCONS_NET_IO = (1<<4);
const uint64_t NOUSE_VM_IP = (1<<5);
const uint64_t NOCONS_MEMCG_RECLAIM = (1<<6);
const uint64_t NO_SYS_OOM_KILLER = (1<<7);

const uint64_t MINOR_PRIORITY_RANGE = (1 << 4);
const uint64_t MEMORY_QOS_SCORE_RANGE = 1000000;

/* container configs*/
const std::string CONFIG_ENABLE = "true";
const std::string CONFIG_DISABLE = "false";
//const std::string NO_MEM_LIMIT = "NO_MEM_LIMIT"; // not support 
const std::string NO_MEMCG_RECLAIM = "NO_MEMCG_RECLAIM";
const std::string MEM_FORCE_EMPTY = "MEM_FORCE_EMPTY";
const std::string MEM_WMARK_RATIO = "MEM_WMARK_RATIO";
const std::string MEM_EXTRA_BYTES = "MEM_EXTRA_BYTES";
const std::string MEM_EXTRA_RATIO = "MEM_EXTRA_RATIO";
const std::string OOM_CONTAINER_STOP = "OOM_CONTAINER_STOP";
const std::string OOM_KILL_DISABLE = "OOM_KILL_DISABLE";
const std::string CPU_BVT_WARP_NS = "CPU_BVT_WARP_NS";
const std::string NET_PRIORITY = "NET_PRIORITY";
const std::string MEM_COLD_THRESHOLD = "MEM_COLD_THRESHOLD";
const std::string CPU_LLC_CACHE = "CPU_LLC_CACHE";
const std::string PREDICT_DELAY_SECONDS = "PREDICT_DELAY_SECONDS";
const std::string CONTAINER_BIND_MOUNTS = "BIND_MOUNTS";
const std::string CONTAINER_LABELS = "LABELS";
const std::string CONTAINER_ENVS = "ENVS";
const std::string CONTAINER_RUNTIME = "RUNTIME";
const std::string KATA_VM_CPUS = "KATA_VM_CPUS";
const std::string KATA_VM_MEM = "KATA_VM_MEM";
const std::string DFS_ENABLE_POV = "DFS_ENABLE_POV";

const std::string DISK_BUFFER_WRITE_BPS = "DISK_BUFFER_WRITE_BPS";
const std::string DISK_WRITE_BPS = "DISK_WRITE_BPS";
const std::string DISK_READ_BPS = "DISK_READ_BPS";
const std::string DISK_WRITE_IOPS = "DISK_WRITE_IOPS";
const std::string DISK_READ_IOPS = "DISK_READ_IOPS";

const std::string CPU_RESERVED_TO_SHARE_RATIO = "CPU_RESERVED_TO_SHARE_RATIO";
const std::string CPU_RESERVED_TO_SHARE_NUM = "CPU_RESERVED_TO_SHARE_NUM";
const std::string TIMEZONE="TIMEZONE";

const uint32_t DEFAULT_INTEL_L3_MASK = 0xfffff;
const uint32_t DEFAULT_INTEL_L3_WAY_SIZE = 2048; //KB

const std::string DEV_DIR = "/dev";
const std::string NVIDIA_DEVICE_PATH_PREFIX = "/dev/nvidia";
const std::string NVIDIA_DEVICE_NAME_PREFIX = "nvidia";
const std::string XILINX_DEVICE_PATH_PREFIX = "/dev/xdma";
const std::string XILINX_DEVICE_NAME_PREFIX = "xdma";
const std::string ALINPU_DEVICE_PATH_PREFIX = "/dev/npu";
const std::string ALINPU_DEVICE_NAME_PREFIX = "npu";
const std::string ALIPOV_DEVICE_PATH_PREFIX = "/dev/pov";
const std::string ALIPOV_DEVICE_NAME_PREFIX = "pov";

const std::string SYSTEM_CONFIG_INIT_VERSION = "-1";
const std::string SLAVE_MOUNT_PREFIX = "slave.mounts.";
const std::string MOUNT_OPT_RW = "rw";
const std::string MOUNT_OPT_RO = "ro";
const std::string MOUNT_OPT_RW_RSLAVE = "rw,rslave";
const std::string KEY_VALUE_CASCADER = "="; 

const std::string RESOURCE_PIPE_FILE = "realtimeAvailableResource.json";
const std::string CONTAINER_DESC_FILE = ".json";
const std::string PROCESS_ALREADY_RUN_FILE = ".processAlreadyRun";
const std::string HTTP_LAUNCH_URI = "/launchSlot";

const std::string RUNTIME_RUNC = "runc";
const std::string RUNTIME_KATA = "kata-runtime";

// dynamic cpu allocater label keys
const std::string HIGHLEVEL_CPUS = "highLevelCpus";
const std::string LABEL_KEY_EXCLUSIVE_CPUNUM = "app.hippo.io/exclusiveModeCpuNum";
const std::string LABEL_KEY_RESERVED_CPUNUM = "app.hippo.io/reserveModeCpuNum";
const std::string LABEL_KEY_SLOT_SAFE_CPUWMARK = "app.hippo.io/safeCpuWmark";
const std::string LABEL_KEY_MIN_CPUNUM_FACTOR = "app.hippo.io/minCpuNumFactor";
const std::string LABEL_KEY_MAX_CPUNUM_FACTOR = "app.hippo.io/maxCpuNumFactor";
const std::string LABEL_KEY_DEADLINE = "app.hippo.io/deadline";
const std::string LABEL_KEY_RESOURCE_QOS = "app.hippo.io/resourceQoS";
const std::string LABEL_KEY_FAILOVER_COST = "app.hippo.io/failoverCost";
const std::string LABEL_KEY_PREEMPTABLE = "app.hippo.io/preemptable";
const std::string LABEL_KEY_GANGREGION = "app.hippo.io/gangRegion";

const std::string LABEL_KEY_APP_SHORT_NAME = "app.hippo.io/appShortName";
const std::string LABEl_KEY_ROLE_SHORT_NAME = "app.hippo.io/roleShortName";
const std::string LABEL_KEY_APP_TYPE = "app.hippo.io/appType";

// app intention labels
const std::string LABEL_KEY_APP_INTENTION_TYPE = "app.hippo.io/appIntentionType";
const std::string LABEL_PRE_INSTALL_PACKAGE_APP = "preInstallPackageApp";

/* used for cgroup parameters */
const std::string CGROUP_SUBSYSTEM_CPU = "cpu";
const std::string CGROUP_SUBSYSTEM_CPUSET = "cpuset";
const std::string CGROUP_SUBSYSTEM_CPUACCT = "cpuacct";
const std::string CGROUP_SUBSYSTEM_MEMORY = "memory";
const std::string CGROUP_SUBSYSTEM_BLKIO = "blkio";
const std::string CGROUP_SUBSYSTEM_DEVICES = "devices";
const std::string CGROUP_SUBSYSTEM_FREEZER = "freezer";
const std::string CGROUP_SUBSYSTEM_NETCLS = "net_cls";
const std::string CGROUP_SUBSYSTEM_NETPRIO = "net_prio";
const std::string CGROUP_SUBSYSTEM_PERFEVENT = "perf_event";
const std::string CGROUP_SUBSYSTEM_HUGETLB = "hugetlb";
const std::string CGROUP_SUBSYSTEM_INTELRDT = "intel_rdt";
const std::string CGROUP_SUBSYSTEM_SYSTEMD = "systemd";
const std::string CGROUP_SUBSYSTEM_PIDS = "pids";

const std::string CGROUP_CPU_CLONE_CHILDREN = "cgroup.clone_children";
const std::string CGROUP_CPU_QUOTA = "cpu.cfs_quota_us";
const std::string CGROUP_CPU_PERIOD = "cpu.cfs_period_us";
const std::string CGROUP_CPU_SHARE = "cpu.shares";
const std::string CGROUP_CPU_BVT = "cpu.bvt_warp_ns";
const std::string CGROUP_CPUSET_CPUS = "cpuset.cpus";
const std::string CGROUP_CPUSET_MEMS = "cpuset.mems";
const std::string CGROUP_MEMORY_HARDLIMIT = "memory.limit_in_bytes";
const std::string CGROUP_MEMORY_PRIORITY = "memory.priority";
const std::string CGROUP_MEMORY_WMARK = "memory.wmark_ratio";
const std::string CGROUP_MEMORY_FORCE_EMPTY_CTL = "memory.force_empty_ctl";
const std::string CGROUP_MEMORY_USE_PRIORITY_OOM = "memory.use_priority_oom";
const std::string CGROUP_MEMORY_OOM_KILL_ALL = "memory.oom_kill_all";
const std::string CGROUP_MEMORY_MOVE_CHARGE_AT_IMMIGRATE = "memory.move_charge_at_immigrate";

const std::string CGROUP_MEMORY_COLD_THRESHOLD = "memory.droppable";
const std::string COLD_MEMORY_RECLAIM = "/var/coldpg_reclaim";

const std::string CGROUP_MEMORY_SYSTEM_SLICE_PRIORITY = "12";
const std::string CGROUP_MEMORY_PROD_SLICE_PRIORITY = "10";
const std::string CGROUP_MEMORY_NON_PROD_SLICE_PRIORITY = "2";
const std::string CGROUP_SYSTEM_SLICE = "system.slice";
const std::string CGROUP_TOP_SLICE = "";

const std::string CGROUP_BLKIO_THROTTLE_READ_IOPS = "blkio.throttle.read_iops_device";
const std::string CGROUP_BLKIO_THROTTLE_WRITE_IOPS = "blkio.throttle.write_iops_device";
const std::string CGROUP_BLKIO_THROTTLE_READ_BPS = "blkio.throttle.read_bps_device";
const std::string CGROUP_BLKIO_THROTTLE_WRITE_BPS = "blkio.throttle.write_bps_device";
const std::string CGROUP_BLKIO_THROTTLE_BUFFERWRITE_BPS = "blkio.throttle.buffered_write_bps_device";
const std::string CGROUP_BLKIO_THROTTLE_BUFFERWRITE_SWITCH = "blkio.throttle.buffered_write_switch";
const std::string CGROUP_INTELRDT_L3CBM = "intel_rdt.l3_cbm";
const std::string CGROUP_SUBSYSTEM_RESCTRL = "resctrl";
const std::string CGROUP_RESCTRL_SCHEMATA = "schemata";
const std::string CGROUP_RESCTRL_PROD_GROUP = "hippo_prod_group";
const std::string CGROUP_RESCTRL_NON_PROD_GROUP = "hippo_non_prod_group";

const std::string HIPPO_CONFIG_SLAVE_PREFIX = "slave.";
const std::string HIPPO_CONFIG_SLAVE_SERVICE_RUN_MODE = "slave.service.runMode";
const std::string RUN_MODE_HIPPO = "hippo";
const std::string RUN_MODE_YARN = "yarn";

// federation
const std::string FEDERATION_DRIVER_CLASS_NATIVE = "NATIVE";
const std::string FEDERATION_DRIVER_CLASS_HIPPO = "HIPPO";
const std::string FEDERATION_DRIVER_CLASS_ASI = "ASI";

const std::string FEDERATION_CLUSTER_NATIVE = "native";

// same as RESOURCE_MANAGER_SERIALIZE_FILE_NEW
const std::string FEDERATION_NATIVE_RESOURCEPLAN_SERIALIZE_FILE = RESOURCE_MANAGER_SERIALIZE_FILE_NEW;
const std::string FEDERATION_NATIVE_RESOURCEPLAN_PATCH_FILE_PREFIX = "resource_plan_patch.";
const std::string FEDERATION_SERIALIZE_PREFIX = "federation";

const std::string FEDERATION_ROUTER_DEFAULT = "default";
const std::string ROUTE_POLICY_DEFAULT_PROD = "default_prod";
const std::string ROUTE_POLICY_DEFAULT_NON_PROD = "default_non_prod";
const std::string ROUTE_POLICY_DEFAULT_SYSTEM = "default_system";
const std::string FEDERATION_ROUTE_POLICY_SERIALIZE_FILE = "route_policy";
const std::string FEDERATION_POLICY_MAPPING_SERIALIZE_FILE = "policy_mapping";
const std::string FEDERATION_LABEL_HOME_CLUSTER = "federation.hippo.io/home-cluster";
const std::string FEDERATION_LABEL_RAW_NAME = "federation.hippo.io/raw-name";
const std::string FEDERATION_LABEL_RAW_UID = "federation.hippo.io/raw-uid";
const std::string FEDERATION_LABEL_RAW_PRIORIYT_CLASS_NAME = "federation.hippo.io/raw-priorityClassName";
const std::string FEDERATION_POD_SHADOW_PREFIX = "fed-";
const std::string HIPPO_PRIORITY_CLASS_NAME_PREFIX = "hippo-priority-";

// yarn basics
const std::string YARN_NODE_SCOPE = "node";
const std::string YARN_ALL_NAMESPACE = "all";
const std::string YARN_HOST_IP = "hostIp";
const std::string YARN_IO_PREFIX = "yarn.io";
const std::string YARN_IO_PREFIX_CENTRALIZED = "rm.yarn.io";
const std::string YARN_IO_PREFIX_DISTRIBUTED = "nm.yarn.io";
const std::string YARN_MEMORY_URI = "memory-mb";
const std::string YARN_VCORES_URI = "vcores";
const std::string YARN_IP_URI = "ip";
const std::string YARN_DISK_URI = "disk";
const std::string YARN_DISK_SIZE_URI = "size";
const std::string YARN_DISK_IOPS_URI = "iops";
const std::string YARN_RATIO_URI = "ratio";
const std::string YARN_ATTRIBUTE_INDEX = "index";
const std::string YARN_GPU_URI = YARN_IO_PREFIX + SLASH_SEPARATOR + RES_NAME_GPU;
const std::string YARN_FPGA_URI = YARN_IO_PREFIX + SLASH_SEPARATOR + "alifpga";
const std::string YARN_DEFAULT_PARTITION = "";

// reason of selected for killing
const std::string KILL_REASON_RELSEASE_MEMORY = "releasing memory";
const std::string KILL_REASON_EXTERNAL = "external killer";
const std::string KILL_REASON_RECLAIM_DISK = "reclaiming disk";

const std::string PACKAGE_VISIBILITY = "package_visibility";
const std::string PRIVATE_PACKAGE = "private";
const std::string CUSTOM_DEVICES = "custom_devices";
#define DEFAULT_HIPPO_POOL_SIZE 10
#define HTTP_STATUS_CODE_INVALID_REQUEST 400
#define HTTP_STATUS_CODE_OK 200
#define HTTP_STATUS_CODE_INTERNAL_SERVER_ERROR 500
#define CONTAINER_EXIT_CODE_FILE  "proc.exitcode"
#define MAX_SOCKETS_NODE_NUM 8

#define DELETE_AND_SET_NULL_HIPPO(x) do {       \
        if ((x) != NULL) {                      \
            delete (x);                         \
            (x) = NULL;                         \
        }                                       \
    } while (false)

#define REPORT_LATENCY(metricObj, startTime, endTime, metricName)       \
    if (metricObj != NULL) {                                            \
        int64_t latency = endTime - startTime;                          \
        if (latency < 0) {                                              \
            latency = 0;                                                \
        }                                                               \
        metricObj->reportMetricValue(metricName, latency);              \
        HIPPO_LOG(DEBUG, "report %s with value [%ld]",                  \
                  metricName, latency);                                 \
    } else {                                                            \
        HIPPO_LOG(TRACE1, "%s metricsObj is NULL", metricName);         \
    }
#endif

/*
 * Force strict CPU ordering.
 * And yes, this is required on UP too when we're talking
 * to devices.
 */
#define mb()    asm volatile("mfence":::"memory")
#define rmb()   asm volatile("lfence":::"memory")

#ifdef CONFIG_UNORDERED_IO
#define wmb()   asm volatile("sfence" ::: "memory")
#else
#define wmb()   asm volatile("" ::: "memory")
#endif

// bool to string
#define BTOS(ret) (ret ? "true" : "false")
