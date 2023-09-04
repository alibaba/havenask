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
#ifndef CARBON_COMMON_H
#define CARBON_COMMON_H

#include <string>
#include <assert.h>
#include <memory>
#include "autil/CommonMacros.h"
#include "hippo/DriverCommon.h"
#include "carbon/CommonDefine.h"
#include "carbon/ErrorDefine.h"

#define BEGIN_CARBON_NAMESPACE(x) namespace carbon { namespace x {
#define END_CARBON_NAMESPACE(x) } }
#define USE_CARBON_NAMESPACE(x) using namespace carbon::x

#define BEGIN_C2_NAMESPACE(x) namespace c2 { namespace x {
#define END_C2_NAMESPACE(x) } }
#define USE_C2_NAMESPACE(x) using namespace c2::x

#define CARBON_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;
#define CARBON_PREDECL_PTR(x) class x; CARBON_TYPEDEF_PTR(x)

#define CARBON_LOG_AND_SET_ERR(level, logFmtStr, msg, ec, errorInfo) {      \
        CARBON_LOG(ERROR, logFmtStr, msg.c_str());                          \
        errorInfo->set_errormsg(msg);                                   \
        errorInfo->set_errorcode(ec);                                   \
    }

#define TESTABLE_BEGIN(pr) public:
#define TESTABLE_END() private:
#define MOCKABLE virtual

#define HISTORY_DUMP_LOOP_INTERVAL (1000*1000)     // 1 sec
#define HISTORY_CONTENT_SEP "\n"

#define DEFAULT_LOST_TIMEOUT (5 * 60 * 1000 * 1000) // 5 min
#define HEALTH_CHECK_INTERVAL (5 * 100 * 1000) // 0.5 sec
#define SERVICE_SYNC_INTERVAL (3 * 1000 * 1000) // 3 sec
#define GROUP_SCHEDULE_INTERVAL (500 * 1000)
#define BUFFER_DEFAULT_CAPCITY (64 * 1000 * 1000)
#define HEARTBEAT_LOST_COUNT_THRESHOLD 5
#define DEFAULT_RUNNER_THREAD_NUM 8
#define DEFAULT_RUNNER_QUEUE_SIZE 100
#define DEFAULT_GLOBAL_CONFIG_MANAGER_LOOPINTERVAL (10 * 1000 * 1000) // 10 sec

#define CARBON_NEW_START_TAG "is_simple_master_new_start"
#define DEFAULT_HEALTH_CHECKER "default"
#define DEFAULT_KMON_SPEC "localhost:4141"
#define LV7_HEALTH_CHECKER "lv7"
#define ADVANCED_LV7_HEALTH_CHECKER "adv_lv7"
#define KEY_PORT  "PORT"
#define KEY_CHECK_PATH "CHECK_PATH"
#define KEY_LOST_TIMEOUT "LOST_TIMEOUT"

#define GROUP_SIGNATURE_META_INFO_KEY "_group_signature_"

#define HIPPO_PREFERENCE_SCOPE_APP "APP"
#define HIPPO_PREFERENCE_SCOPE_ROLE "ROLE"
#define HIPPO_PREFERENCE_TYPE_PROHIBIT "PROHIBIT"
#define HIPPO_PREFERENCE_TYPE_PREFER "PREFER"
#define HIPPO_PREFERENCE_DEFAULT_TTL 600

typedef std::vector<hippo::SlotInfo> SlotInfos;
typedef std::string roleid_t;


// new macro
#define SERIALIZE_DIR "serialize_dir"
#define GROUPS_PLAN_SERIALIZE_FILE_NAME "_carbon_group_plans_"
#define GROUP_PLAN_SERIALIZE_FILE_PREFIX "_carbon_group_plan_."
#define APPCHECKSUM_SERIALIZE_FILE_NAME "_carbon_app_check_sum_"
#define BUFFERED_TAGSLOTS_FILE_NAME "_carbon_buffer_tagslots_"
#define BUFFERED_CONFIG_FILE_NAME "_carbon_buffer_config_"
#define GLOBAL_CONFIG_SERIALIZE_FILE_NAME "_global_config_"

// local dir
#define CARBON_PLAN_LOCAL_RECORD_PATH "_carbon_record_dir_"
#define HIPPO_DRIVER_NAME "hippo_driver"
#define INVALID_VERSION ""
#define SERVICE_SIGNAURE_KEY "carbon_signature"
#define SERVICE_META_KEY "carbon_meta"
#define SCHEDULE_TOO_LONG_TIME 1
#define RECORD_MAX_COUNT 100
#define PREF_PROHIBIT_LEASE_TIME (600*1000) //ms 10min
#define PREF_PREFER_LEASE_TIME (600*1000) //ms 10min
#define CARBON_MODE_LIB "lib"
#define CARBON_MODE_BINARY "binary"
#define DEFAULT_AMONITOR_SERVICE_NAME "CarbonMasterMetrics"

#define PTR_TEST_CALL(ptr, fn) if (ptr) ptr->fn
#define UNITTEST_START(f) public:
#define UNITTEST_END() private:

#define WHEN_FS_FILE_EXIST(serializer) \
    do { \
        bool exist = false; \
        if (!serializer->checkExist(exist)) { \
            CARBON_LOG(ERROR, "check serializer dir exist failed: " #serializer); \
            break; \
        } \
        if (!exist) { \
            CARBON_LOG(DEBUG, "serializer dir not exist, not need recover:" #serializer); \
            break; \
        } \

#define END_WHEN() } while(0)

#endif
