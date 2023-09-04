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
#include "common/SlotUtil.h"
#include "carbon/Log.h"
#include "autil/StringUtil.h"
#include "hippo/ScheduleNode.h"

using namespace std;
using namespace hippo;
using namespace autil;
BEGIN_CARBON_NAMESPACE(common);

CARBON_LOG_SETUP(common, SlotUtil);

SlotUtil::SlotUtil() {
}

SlotUtil::~SlotUtil() {
}

bool SlotUtil::hasFailedProcess(const SlotInfo &slotInfo) {
    for (size_t j = 0; j < slotInfo.processStatus.size(); j++) {
        if (slotInfo.processStatus[j].status == ProcessStatus::PS_FAILED
            || slotInfo.processStatus[j].status == ProcessStatus::PS_STOPPED)
        {
            return true;
        }
    }
    return false;
}

bool SlotUtil::hasRestartingProcess(const SlotInfo &slotInfo) {
    for (size_t j = 0; j < slotInfo.processStatus.size(); j++) {
        if (slotInfo.processStatus[j].status == ProcessStatus::PS_RESTARTING) {
            return true;
        }
    }
    return false;
}

bool SlotUtil::isAllProcessRunning(const SlotInfo &slotInfo) {
    if (slotInfo.processStatus.size() == 0) {
        return false;
    }
    
    for (size_t j = 0; j < slotInfo.processStatus.size(); j++) {
        if (slotInfo.processStatus[j].status != ProcessStatus::PS_RUNNING) {
            return false;
        }
    }
    return true;
}

bool SlotUtil::hasFailedProcess(const ScheduleNodePtr &scheduleNode) {
    ProcessStatusMap process = scheduleNode->getProcessStatus();
    map<string, ProcessStatus>::const_iterator it = process.begin();
    for (; it != process.end(); ++it) {
        if (it->second.status == ProcessStatus::PS_FAILED) {
            return true;
        }
    }
    return false;
}

bool SlotUtil::hasRestartingProcess(const ScheduleNodePtr &scheduleNode) {
    ProcessStatusMap process = scheduleNode->getProcessStatus();
    map<string, ProcessStatus>::const_iterator it = process.begin();
    for (; it != process.end(); ++it) {
        if (it->second.status == ProcessStatus::PS_RESTARTING) {
            return true;
        }
    }
    return false;
}

bool SlotUtil::isAllProcessRunning(const ScheduleNodePtr &scheduleNode) {
    ProcessStatusMap process = scheduleNode->getProcessStatus();
    map<string, ProcessStatus>::const_iterator it = process.begin();
    if (process.size() == 0) {
        return false;
    }
    for (; it != process.end(); ++it) {
        if (it->second.status != ProcessStatus::PS_RUNNING
            && it->second.status != ProcessStatus::PS_TERMINATED)
        {
            return false;
        }
    }
    return true;
}

bool SlotUtil::isInstanceIdMatched(const ScheduleNodePtr &scheduleNode) {
    ProcessInfos processInfos = scheduleNode->getProcessInfos();
    ProcessStatusMap process = scheduleNode->getProcessStatus();
    ProcessInfos::const_iterator it = processInfos.begin();
    for (; it != processInfos.end(); ++it) {
        ProcessStatusMap::const_iterator processIt =
            process.find(it->first);
        if (processIt == process.end()) {
            CARBON_LOG(DEBUG, "process [%s] is not found in processStatus.",
                       it->first.c_str());
            return false;
        }
        if (it->second.instanceId != processIt->second.instanceId) {
            CARBON_LOG(DEBUG, "instanceId is not matched. processName:[%s]",
                       it->first.c_str());
            return false;
        }
    }
    CARBON_LOG(DEBUG, "instanceId is matched.");
    return true;
}

string SlotUtil::getSlotIp(const SlotId &slotId) {
    const string &slaveAddress = slotId.slaveAddress;
    vector<string> tokens = StringUtil::split(slaveAddress, ":");
    if (tokens.size() != 2) {
        CARBON_LOG(ERROR, "invalid slave address:[%s].", slaveAddress.c_str());
        return "";
    }

    return tokens[0];
}

END_CARBON_NAMESPACE(common);
