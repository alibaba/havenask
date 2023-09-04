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
#ifndef CARBON_SLOTUTIL_H
#define CARBON_SLOTUTIL_H

#include "common/common.h"
#include "carbon/Log.h"
#include "hippo/DriverCommon.h"
#include "hippo/ScheduleNode.h"

BEGIN_CARBON_NAMESPACE(common);

class SlotUtil
{
public:
    SlotUtil();
    ~SlotUtil();
private:
    SlotUtil(const SlotUtil &);
    SlotUtil& operator=(const SlotUtil &);
public:
    static bool hasFailedProcess(const hippo::SlotInfo &slotInfo);

    static bool hasRestartingProcess(const hippo::SlotInfo &slotInfo);
    
    static bool isAllProcessRunning(const hippo::SlotInfo &slotInfo);
    
    static bool hasFailedProcess(const hippo::ScheduleNodePtr &scheduleNode);

    static bool hasRestartingProcess(const hippo::ScheduleNodePtr &scheduleNode);
    
    static bool isAllProcessRunning(const hippo::ScheduleNodePtr &scheduleNode);

    static bool isInstanceIdMatched(const hippo::ScheduleNodePtr &scheduleNode);
    
    static std::string getSlotIp(const hippo::SlotId &slotId);
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(SlotUtil);

END_CARBON_NAMESPACE(common);

#endif //CARBON_SLOTUTIL_H
