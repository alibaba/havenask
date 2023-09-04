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
#ifndef MASTER_FRAMEWORK_PARTITIONSCHEDULEUNIT_H
#define MASTER_FRAMEWORK_PARTITIONSCHEDULEUNIT_H

#include "master_framework/common.h"
#include "master_framework/BasicScheduleUnit.h"
#include "master_framework/RolePlan.h"


BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

class SlotChecker;
typedef RolePlan PartitionSchedulePlan;


class PartitionScheduleUnit : public BasicScheduleUnit
{
    friend class PartitionScheduleUnitTest;
public:
    PartitionScheduleUnit(const std::string &name,
                          hippo::MasterDriver *hippoMasterDriver);
    
    ~PartitionScheduleUnit();
    
private:
    PartitionScheduleUnit(const PartitionScheduleUnit &);
    PartitionScheduleUnit& operator=(const PartitionScheduleUnit &);

    using BasicScheduleUnit::getSlotInfos;
    
public:
    /* override */ bool setPlan(const Plan *plan);
    
    /* override */ void schedule(const RoleSlotInfos &slotInfos);

    /* override */ const SlotInfos getSlotInfos() const;
    
private:
    void releaseSlots(const std::vector<const hippo::SlotInfo*> &slots,
                      const std::set<hippo::SlotId> &redundantSlots);

    void resetSlots(const std::vector<const hippo::SlotInfo *> &slots);

    
    void restartSlotProcs(const std::vector<const hippo::SlotInfo *> &slots);

    void restartProcesses(const hippo::SlotInfo &slotInfo,
                          const std::set<std::string> &procNames);

    void getNeedRestartProcs(
            const std::vector<hippo::ProcessStatus> &processStatus,
            std::set<std::string> &needRestartProcs);

    hippo::ProcessContext getSlotProcessContext(
            const hippo::SlotId &slotId);

    void setSlotProcessContext(const hippo::SlotId &slotId,
                               const hippo::ProcessContext &processContext);

    void clearSlotProcessContext(const hippo::SlotId &slotId);

    void pickupRedundantSlot(
            std::vector<const hippo::SlotInfo*> &onlineSlots,
            int32_t redundantSlotCnt,
            std::vector<const hippo::SlotInfo*> &needReleaseSlots,
            std::set<hippo::SlotId> &redundantSlots);

    RolePlan getRolePlan();

    void getAutoReleaseFlag(const RolePlan &plan, bool& reclaimingAutoRelease,
                            bool& longTimeNotRunningAutoRelease);

private:
    PartitionSchedulePlan _plan;
    std::map<hippo::SlotId, hippo::ProcessContext> _slotProcessContextMap;
    autil::ThreadMutex _lock;
    SlotChecker *_slotChecker;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(PartitionScheduleUnit);

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_PARTITIONSCHEDULEUNIT_H
