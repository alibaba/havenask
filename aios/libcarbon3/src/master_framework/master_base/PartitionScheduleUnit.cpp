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
#include "master_framework/PartitionScheduleUnit.h"
#include "master_framework/Plan.h"
#include "master_base/ScheduleHelper.h"
#include "master_base/SlotClassifier.h"
#include "master_base/SlotChecker.h"
#include "SlotSorter.h"
#include "common/Log.h"

using namespace std;
using namespace autil;
using namespace hippo;

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

MASTER_FRAMEWORK_LOG_SETUP(master_base, PartitionScheduleUnit);

#define PART_SCHEDULE_UNIT_SCOPE "__part_su_scope__"
#define PROP_AUTO_RELEASE_FLAG "auto_release_flag"
//legacy for reclaiming&longTimeNotRunning, new for longTimeNotRunning only

#define PROP_RECLAIMING_AUTO_RELEASE_FLAG "reclaiming_auto_release_flag"
//reclaiming_auto_release_flag is for bs, to do not auto release slot

PartitionScheduleUnit::PartitionScheduleUnit(const std::string &name,
        hippo::MasterDriver *hippoMasterDriver) :
    BasicScheduleUnit(name, hippoMasterDriver)
{
    _slotChecker = new SlotChecker;
}

PartitionScheduleUnit::~PartitionScheduleUnit() {
    delete _slotChecker;
}

bool PartitionScheduleUnit::setPlan(const Plan *plan) {
    const PartitionSchedulePlan *partPlan;
    partPlan = dynamic_cast<const PartitionSchedulePlan *>(plan);
    if (!partPlan) {
        MF_LOG(ERROR, "Fail to cast the Plan to PartitionSchedulePlan.");
        return false;
    }
    
    ScopedLock lock(_lock);
    _plan = *partPlan;

    RolePlanMap rolePlans;
    rolePlans[getName()] = _plan;
    setRolePlans(rolePlans);

    return true;
}

void PartitionScheduleUnit::schedule(const RoleSlotInfos &roleSlotInfos) {
    RolePlan plan = getRolePlan();
    const string &role = getName();
    RoleSlotInfos::const_iterator it = roleSlotInfos.find(role);
    if (it == roleSlotInfos.end()) {
        MF_LOG(DEBUG, "not find role:[%s] in RoleSlotInfos.",
               role.c_str());
        return;
    }

    const SlotInfos &slotInfos = it->second;
    bool reclaimingAutoRelease = true, longTimeNotRunningAutoRelease = true;
    getAutoReleaseFlag(plan, reclaimingAutoRelease, longTimeNotRunningAutoRelease);
    set<SlotId> longTimeNotRunningSlots;
    if (longTimeNotRunningAutoRelease) {
        _slotChecker->checkRunningSlots(slotInfos, longTimeNotRunningSlots);
    }

    vector<const SlotInfo*> onlineSlots;
    vector<const SlotInfo*> needReleaseSlots;
    vector<const SlotInfo*> needResetSlots;
    vector<const SlotInfo*> needRestartProcSlots;

    SlotClassifier::classifySlots(slotInfos,
                                  onlineSlots,
                                  needReleaseSlots,
                                  needResetSlots,
                                  needRestartProcSlots,
                                  reclaimingAutoRelease,
                                  longTimeNotRunningAutoRelease,
                                  longTimeNotRunningSlots);

    MF_LOG(DEBUG, "slotInfos.size() = %zd, onlineSlots.size() = %zd,"
           "needReleaseSlots.size() = %zd, needResetSlots.size() = %zd,"
           "needRestartProcSlots.size() = %zd.", slotInfos.size(),
           onlineSlots.size(), needReleaseSlots.size(),
           needResetSlots.size(),
           needRestartProcSlots.size());

    int32_t redundantSlotCnt = (int32_t)slotInfos.size() - plan.count -
                               (int32_t)needReleaseSlots.size();
    set<SlotId> redundantSlots;
    pickupRedundantSlot(onlineSlots, redundantSlotCnt, needReleaseSlots,
                        redundantSlots);
    resetSlots(needResetSlots);
    restartSlotProcs(needRestartProcSlots);
    releaseSlots(needReleaseSlots, redundantSlots);
}

void PartitionScheduleUnit::pickupRedundantSlot(
        vector<const SlotInfo*> &onlineSlots,
        int32_t redundantSlotCnt,
        vector<const SlotInfo*> &needReleaseSlots,
        set<SlotId> &redundantSlots)
{
    if (redundantSlotCnt <= 0) {
        return;
    }

    SlotSorter sorter;
    sorter.sort(onlineSlots);
    for (int32_t j = 0;
         j < redundantSlotCnt && j < (int32_t)onlineSlots.size();
         j++)
    {
        needReleaseSlots.push_back(onlineSlots[j]);
        redundantSlots.insert(onlineSlots[j]->slotId);
    }
}
    
void PartitionScheduleUnit::releaseSlots(
        const vector<const SlotInfo *> &slots,
        const set<SlotId> &redundantSlots)
{
    MF_LOG(DEBUG, "release slots size:[%zd].", slots.size());
    for (size_t i = 0; i < slots.size(); i++) {
        ReleasePreference pref;
        pref.type = ReleasePreference::RELEASE_PREF_ANY;
        if (redundantSlots.find(slots[i]->slotId) == redundantSlots.end()) {
            pref.type = ReleasePreference::RELEASE_PREF_PROHIBIT;
            pref.leaseMs = 30000; // 30 sec
        }
        
        _hippoMasterDriver->releaseSlot(slots[i]->slotId, pref);
        clearSlotProcessContext(slots[i]->slotId);
        
        MF_LOG(INFO, "release slot [%s:%d], roleName:%s,"
               " isReclaiming:%d, slotStatus:%d.",
               slots[i]->slotId.slaveAddress.c_str(),
               slots[i]->slotId.id, slots[i]->role.c_str(),
               slots[i]->reclaiming, slots[i]->slaveStatus.status);
    }
}

void PartitionScheduleUnit::resetSlots(const vector<const SlotInfo *> &slots) {
    for (size_t i = 0; i < slots.size(); i++) {
        _hippoMasterDriver->resetSlot(slots[i]->slotId);
        MF_LOG(INFO, "reset slot [%s:%d], roleName:%s,"
               "packageStatus:%d.",
               slots[i]->slotId.slaveAddress.c_str(),
               slots[i]->slotId.id, slots[i]->role.c_str(),
               slots[i]->packageStatus.status);
    }
}

void PartitionScheduleUnit::restartSlotProcs(const vector<const SlotInfo *> &slots) {
    for (size_t i = 0; i < slots.size(); i++) {
        const SlotInfo *slotInfo = slots[i];
        set<string> needRestartProcs;
        getNeedRestartProcs(slotInfo->processStatus, needRestartProcs);
        if (!needRestartProcs.empty()) {
            restartProcesses(*slotInfo, needRestartProcs);
        }
    }
}

void PartitionScheduleUnit::getNeedRestartProcs(
        const vector<ProcessStatus> &processStatus,
        set<string> &needRestartProcs)
{
    for (size_t i = 0; i < processStatus.size(); i++) {
        const ProcessStatus &procStatus = processStatus[i];
        if (procStatus.status == ProcessStatus::PS_FAILED &&
            procStatus.restartCount == 10)
        {
            needRestartProcs.insert(procStatus.processName);
        }
    }
}

void PartitionScheduleUnit::restartProcesses(const SlotInfo &slotInfo,
        const set<string> &procNames)
{
    MF_LOG(INFO, "restart process on slot:%s@%d.",
           slotInfo.slotId.slaveAddress.c_str(), slotInfo.slotId.id);
    const SlotId &slotId = slotInfo.slotId;
    vector<string> procNameVect(procNames.begin(), procNames.end());
    if (!_hippoMasterDriver->restartProcess(slotId, procNameVect)) {
        MF_LOG(ERROR, "restart process on slot:%s@%d failed!",
               slotId.slaveAddress.c_str(), slotId.id);
    }
}

RolePlan PartitionScheduleUnit::getRolePlan() {
    ScopedLock lock(_lock);
    return _plan;
}

ProcessContext PartitionScheduleUnit::getSlotProcessContext(
        const SlotId &slotId)
{
    ScopedLock lock(_lock);
    
    if (_slotProcessContextMap.find(slotId) !=
        _slotProcessContextMap.end())
    {
        return _slotProcessContextMap[slotId];
    }
    
    return ScheduleHelper::getProcessContext(_plan);
}

void PartitionScheduleUnit::setSlotProcessContext(
        const SlotId &slotId,
        const ProcessContext &processContext)
{
    ScopedLock lock(_lock);
    _slotProcessContextMap[slotId] = processContext;
}

void PartitionScheduleUnit::clearSlotProcessContext(
        const hippo::SlotId &slotId)
{
    ScopedLock lock(_lock);
    _slotProcessContextMap.erase(slotId);
}

void PartitionScheduleUnit::getAutoReleaseFlag(
        const RolePlan &plan, bool& reclaimingAutoRelease,
        bool& longTimeNotRunningAutoRelease)
{
    auto getFlagFromProperties = [](const Properties& properties, const string& name, bool defaultValue) -> bool
    {
        // if properties has the vaild value, use it; otherwise use defaultValue
        bool value = defaultValue;
        auto it = properties.find(name);
        if (it != properties.end()) {
            const string& valueStr = it->second;
            if (valueStr == "true") {
                value = true;
            } else if (valueStr == "false") {
                value = false;
            } // other valueStrs are invaild
        }
        return value;
    };
    // for bs processor, reclaiming can not auto release.
    // for legacy, reclaimingAutoRelease and longTimeNotRunningAutoRelease are the same
    longTimeNotRunningAutoRelease = getFlagFromProperties(plan.properties, PROP_AUTO_RELEASE_FLAG, true);
    reclaimingAutoRelease = 
        getFlagFromProperties(plan.properties, PROP_RECLAIMING_AUTO_RELEASE_FLAG, longTimeNotRunningAutoRelease);
}

END_MASTER_FRAMEWORK_NAMESPACE(master_base);
