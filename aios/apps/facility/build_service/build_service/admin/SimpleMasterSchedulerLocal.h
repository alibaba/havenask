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
#include "autil/NetUtil.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "hippo/MasterDriver.h"
#include "master_framework/AppPlan.h"
#include "master_framework/SimpleMasterScheduler.h"
#include "master_framework/common.h"

namespace build_service { namespace admin {

class SimpleMasterSchedulerLocal : public master_framework::simple_master::SimpleMasterScheduler
{
public:
    SimpleMasterSchedulerLocal(const std::string& workDir, const std::string& logConfFile);
    virtual ~SimpleMasterSchedulerLocal();

    SimpleMasterSchedulerLocal(const SimpleMasterSchedulerLocal&) = delete;
    SimpleMasterSchedulerLocal& operator=(const SimpleMasterSchedulerLocal&) = delete;
    SimpleMasterSchedulerLocal(SimpleMasterSchedulerLocal&&) = delete;
    SimpleMasterSchedulerLocal& operator=(SimpleMasterSchedulerLocal&&) = delete;

public:
    bool init(const std::string& hippoZkRoot, worker_framework::LeaderChecker* leaderChecker,
              const std::string& applicationId) override;
    void setAppPlan(const master_framework::simple_master::AppPlan& appPlan) override;
    bool start() override { return true; }

    void getAllRoleSlots(std::map<std::string, master_framework::master_base::SlotInfos>& roleSlots) override;

    void releaseSlots(const std::vector<hippo::SlotId>& slots, const hippo::ReleasePreference& pref) override;

    struct ProcessItem {
        pid_t pid;
        bool stopped;
        bool reclaiming;
        master_framework::master_base::RolePlan rolePlan;
        int64_t toReleaseSecond;
        hippo::SlotId slotId;
        ProcessItem() : pid(-1), stopped(true), reclaiming(false), toReleaseSecond(-1) {}
        ProcessItem(int32_t id, pid_t pid_, bool stopped_, const master_framework::master_base::RolePlan& rolePlan_)
            : pid(pid_)
            , stopped(stopped_)
            , reclaiming(false)
            , rolePlan(rolePlan_)
            , toReleaseSecond(-1)
        {
            slotId.id = id;
            slotId.slaveAddress = autil::NetUtil::getBindIp() + ":7007";
        }
    };

private:
    std::pair<pid_t, int32_t> startProcess(const std::string& roleName,
                                           const master_framework::master_base::RolePlan& rolePlan);
    bool stopProcess(pid_t pid);
    bool existProcess(const ProcessItem& item) const;
    void updateWorkerStatus();
    void innerReleaseSlotUnsafe(const hippo::SlotId& slotId);
    std::string genWorkerCmd(const std::string& roleName, const master_framework::master_base::RolePlan& rolePlan,
                             int32_t slotId) const;
    // virtual for test
    virtual int64_t getCurrentSecond() const;

private:
    int32_t _globalSlotCount;
    int64_t _delayReleaseSecond;
    autil::ThreadMutex _roleStatusLock;
    std::map<std::string, std::vector<ProcessItem>> _roleName2ProcessItem; // roleName -> ProcessItem
    autil::LoopThreadPtr _workerStatusUpdaterThread;
    std::string _workDir;
    std::string _logConfFile;
    std::string _binaryPath;

private:
    AUTIL_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SimpleMasterSchedulerLocal);

}} // namespace build_service::admin
