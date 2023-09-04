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
#ifndef MASTER_FRAMEWORK_SIMPLEMASTERSCHEDULERORIGIN_H
#define MASTER_FRAMEWORK_SIMPLEMASTERSCHEDULERORIGIN_H

#include "master_framework/common.h"
#include "master_framework/AppPlan.h"
#include "master_framework/ScheduleUnitManager.h"
#include "master_framework/PartitionScheduleUnitFactory.h"
#include "hippo/LeaderSerializer.h"
#include "worker_framework/LeaderChecker.h"
#include "hippo/MasterDriver.h"
#include "autil/LoopThread.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

class SimpleMasterSchedulerOrigin
{
public:
    static const int32_t INF_SLOT_COUNT = 1000000;
public:
    SimpleMasterSchedulerOrigin();
    virtual ~SimpleMasterSchedulerOrigin();
private:
    SimpleMasterSchedulerOrigin(const SimpleMasterSchedulerOrigin &);
    SimpleMasterSchedulerOrigin& operator=(const SimpleMasterSchedulerOrigin &);
public:
    virtual bool init(const std::string &hippoZkRoot,
                      worker_framework::LeaderChecker * leaderChecker,
                      const std::string &applicationId);

    virtual bool start();
    
    virtual bool stop();

    virtual void setAppPlan(const AppPlan &appPlan);

    virtual std::vector<hippo::SlotInfo> getRoleSlots(
            const std::string &roleName);

    virtual void getAllRoleSlots(
            std::map<std::string, SlotInfos> &roleSlots);

    virtual void reAllocRoleSlots(const std::string &roleName) {
        assert(false);
    }

    virtual hippo::LeaderSerializer* createLeaderSerializer(
            const std::string &zookeeperRoot, const std::string &fileName,
            const std::string &backupRoot = "")
    {
        return _scheduleUnitManagerPtr->createLeaderSerializer(zookeeperRoot,
                fileName, backupRoot);
    }

    hippo::MasterDriver* getMasterDriver() {
        return _scheduleUnitManagerPtr->getMasterDriver();
    }

    virtual void releaseSlots(const std::vector<hippo::SlotId> &slots,
                      const hippo::ReleasePreference &pref);

private:
    void updateScheduleUnits(const master_base::RolePlanMap &rolePlans);

public: // for test
    master_base::ScheduleUnitManagerPtr getScheduleUnitManager() {
        return _scheduleUnitManagerPtr;
    }

    void setMasterDriver(hippo::MasterDriver* masterDriver) {
        _scheduleUnitManagerPtr->setMasterDriver(masterDriver);
    }

    
private:
    master_base::ScheduleUnitManagerPtr _scheduleUnitManagerPtr;
    master_base::PartitionScheduleUnitFactoryPtr _partScheduleUnitFactoryPtr;
    std::map<std::string, master_base::ScheduleUnitPtr> _roleSchedules;
    autil::ThreadMutex _lock;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(SimpleMasterSchedulerOrigin);

//TODO lib 不需要
// class SimpleMasterSchedulerCreator {
// public:
//     virtual SimpleMasterSchedulerOriginPtr create() {
//         return SimpleMasterSchedulerOriginPtr(new SimpleMasterSchedulerOrigin());
//     }
//     virtual ~SimpleMasterSchedulerCreator() {}
// };

// MASTER_FRAMEWORK_TYPEDEF_PTR(SimpleMasterSchedulerCreator);

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

#endif //MASTER_FRAMEWORK_SIMPLEMASTERSCHEDULERORIGIN_H
