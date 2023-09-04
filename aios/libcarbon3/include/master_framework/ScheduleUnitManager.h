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
#ifndef MASTER_FRAMEWORK_SCHEDULEUNITMANAGER_H
#define MASTER_FRAMEWORK_SCHEDULEUNITMANAGER_H

#include "master_framework/common.h"
#include "autil/LoopThread.h"
#include "master_framework/ScheduleUnit.h"
#include "master_framework/ComboScheduleUnit.h"
#include "master_framework/ScheduleUnitFactory.h"
#include "hippo/MasterDriver.h"
#include "worker_framework/LeaderElector.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

class ScheduleUnitManager
{
public:
    ScheduleUnitManager();
    ~ScheduleUnitManager();
private:
    ScheduleUnitManager(const ScheduleUnitManager &);
    ScheduleUnitManager& operator=(const ScheduleUnitManager &);
public:
    bool init(const std::string &hippoZkRoot,
              worker_framework::LeaderElector * leaderElector,
              const std::string &applicationId);
    
    bool start();
    
    bool stop();
    
    void release(const ScheduleUnitPtr &scheduleUnitPtr);
    
    ScheduleUnitPtr createScheduleUnit(const std::string &name,
            const ScheduleUnitFactoryPtr &scheduleUnitFactoryPtr);

    void startSchedule();
    void setProhibitedIps(const std::vector<std::string>& ips);
    void stopSchedule();

    hippo::LeaderSerializer* createLeaderSerializer(
            const std::string &zookeeperRoot,
            const std::string &fileName,
            const std::string &backupRoot = "");

    void getRoleSlotInfos(RoleSlotInfos &roleSlots);
    hippo::MasterDriver* getMasterDriver() {
        return _hippoMasterDriver;
    }

    void releaseSlots(const std::vector<hippo::SlotId> &slotIds,
                      const hippo::ReleasePreference &pref);
    
private:
    void setRolePlans(const RolePlanMap &rolePlans,
                      const std::vector<std::string>& prohibitedIps);

    void releaseUselessRoles(const RoleSlotInfos &roleSlots,
                             const RolePlanMap &rolePlans);

    void modifyProcessRestartCountLimit(
            std::vector<hippo::ProcessInfo> &processInfos);
    void snapshotProhibitedIps(std::vector<std::string>& ips);

public: // public for test
    void schedule();

public: // for test
    void setMasterDriver(hippo::MasterDriver *masterDriver) {
        delete _hippoMasterDriver;
        _hippoMasterDriver = masterDriver;
    }

    std::map<std::string, ScheduleUnitPtr> getScheduleUnits() {
        return _comboScheduleUnitPtr->getScheduleUnits();
    }
    
private:
    hippo::MasterDriver *_hippoMasterDriver;
    ComboScheduleUnitPtr _comboScheduleUnitPtr;
    autil::LoopThreadPtr _scheduleThreadPtr;
    bool _canSchedule;
    mutable autil::ThreadMutex _lock;
    std::vector<std::string> _prohibitedIps;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(ScheduleUnitManager);

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_SCHEDULEUNITMANAGER_H
