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
#include "master/DefaultHealthChecker.h"
#include "carbon/Log.h"
#include "master/SlotUtil.h"
#include "master/WorkerNode.h"
#include "common/common.h"

using namespace std;
using namespace hippo;
using namespace autil;
USE_CARBON_NAMESPACE(common);
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, DefaultHealthChecker);

DefaultHealthChecker::DefaultHealthChecker(const string &id) :
    HealthChecker(id)
{
    _ignoreProcessStatus = false;
    _slaveDeadTimeout = 0;
}

DefaultHealthChecker::~DefaultHealthChecker() {
}

bool DefaultHealthChecker::init(const KVMap &options) {
    string flagStr = getOption(options, KEY_IGNORE_PROCESS_STATUS);
    StringUtil::toLowerCase(flagStr);
    _ignoreProcessStatus = (flagStr == "true");

    string timeoutStr = getOption(options, KEY_SLAVE_DEAD_TIMEOUT);
    if (!timeoutStr.empty()) {
        if (StringUtil::fromString(timeoutStr, _slaveDeadTimeout)) {
            _slaveDeadTimeout = _slaveDeadTimeout * 1000L * 1000L;
        }
    }

    CARBON_LOG(INFO, "init default healthchecker with ignore process status"
               "flag:[%d], slaveDeadTimeout:[%ld].",
               (int)_ignoreProcessStatus,
               _slaveDeadTimeout);
    
    return true;
}

void DefaultHealthChecker::doUpdate(const vector<WorkerNodePtr> &workerNodes) {
    map<nodeid_t, CheckNode> checkNodes;
    for (size_t i = 0; i < workerNodes.size(); i++) {
        const WorkerNodePtr &workerNodePtr = workerNodes[i];
        nodeid_t nodeId = workerNodePtr->getId();
        CheckNode checkNode;
        checkNode.nodeId = nodeId;
        checkNode.slotInfo = workerNodePtr->getSlotInfo();
        checkNode.version = workerNodePtr->getCurVersion();
        checkNodes[nodeId] = checkNode;
    }

    ScopedLock lock(_lock);
    _checkNodes.swap(checkNodes);
    CARBON_LOG(DEBUG, "update check nodes, size:[%zd].", _checkNodes.size());
}

void DefaultHealthChecker::doCheck() {
    map<nodeid_t, CheckNode> checkNodes;
    {
        ScopedLock lock(_lock);
        checkNodes = _checkNodes;
    }
    
    HealthInfoMap healthInfoMap;
    for (map<nodeid_t, CheckNode>::iterator it = checkNodes.begin();
         it != checkNodes.end(); it++)
    {
        HealthInfo &healthInfo = healthInfoMap[it->first];
        CheckNode &checkNode = it->second;
        healthInfo.healthStatus = checkSlotInfo(checkNode);
        healthInfo.version = checkNode.version;
        healthInfo.workerStatus = WT_READY;
    }

    ScopedLock lock(_lock);
    _healthInfoMap.swap(healthInfoMap);

    CARBON_LOG(DEBUG, "check health infos, size:[%zd].", _healthInfoMap.size());
}

HealthType DefaultHealthChecker::checkSlotInfo(CheckNode &checkNode) {
    const SlotInfo &slotInfo = checkNode.slotInfo;
    if (slotInfo.slaveStatus.status == SlaveStatus::DEAD) {
        int64_t curTime = TimeUtility::currentTime();
        if (checkNode.lastSlaveDeadTime == 0) {
            checkNode.lastSlaveDeadTime = curTime;
        }
        
        if (curTime - checkNode.lastSlaveDeadTime >= _slaveDeadTimeout) {
            CARBON_LOG(WARN, "slave status is dead too long for slot[%s], status changed to dead.",
                       ToJsonString(slotInfo.slotId).c_str());
            return HT_DEAD;
        }

        return HT_UNKNOWN;
    } else {
        checkNode.lastSlaveDeadTime = 0;
    }

    if (slotInfo.packageStatus.status == PackageStatus::IS_FAILED ||
        slotInfo.packageStatus.status == PackageStatus::IS_CANCELLED)
    {
        CARBON_LOG(WARN, "package status has error for slot[%s], status changed to dead.",
                   ToJsonString(slotInfo.slotId).c_str());
        return HT_DEAD;
    }
    else if (slotInfo.packageStatus.status == PackageStatus::IS_UNKNOWN ||
             slotInfo.packageStatus.status == PackageStatus::IS_WAITING ||
             slotInfo.packageStatus.status == PackageStatus::IS_INSTALLING)
    {
        return HT_UNKNOWN;
    }

    if (_ignoreProcessStatus) {
        return HT_ALIVE;
    }

    if (SlotUtil::hasTerminatedProcess(slotInfo)) {
        return HT_ALIVE;
    }
    
    if (SlotUtil::hasFailedProcess(slotInfo)) {
        CARBON_LOG(WARN, "has failed process for slot[%s], status changed to dead.", ToJsonString(slotInfo.slotId).c_str());
        return HT_DEAD;
    }
    
    if (SlotUtil::isAllProcessRunning(slotInfo)) {
        return HT_ALIVE;
    }

    return HT_UNKNOWN;
}

string DefaultHealthChecker::getOption(const KVMap &options, const string &k) {
    const auto &it = options.find(k);
    if (it == options.end()) {
        return "";
    }
    return it->second;
}

END_CARBON_NAMESPACE(master);

