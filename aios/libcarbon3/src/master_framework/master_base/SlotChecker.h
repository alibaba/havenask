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
#ifndef MASTER_FRAMEWORK_SLOTCHECKER_H
#define MASTER_FRAMEWORK_SLOTCHECKER_H

#include "master_framework/common.h"
#include "master_framework/BasicScheduleUnit.h"
#include "master_framework/RolePlan.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

class SlotChecker
{
public:
    SlotChecker();
    ~SlotChecker();
private:
    SlotChecker(const SlotChecker &);
    SlotChecker& operator=(const SlotChecker &);
public:
    void checkRunningSlots(const SlotInfos &slotInfos,
                           std::set<hippo::SlotId> &longTimeNotRunningSlots);

    void checkRunningSlots(const SlotInfos &slotInfos,
                           int64_t curTime,
                           std::set<hippo::SlotId> &longTimeNotRunningSlots);
private:
    int64_t getAbnormalTimeout();
    
    bool isAllProcsRunning(const hippo::SlotInfo &slotInfo);
    
private:
    std::map<hippo::SlotId, int64_t> _lastRunningSlots;
    int64_t _abnormalTimeout;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(SlotChecker);

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_SLOTCHECKER_H
