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
#include <math.h>
#include "master/ReplicaNodeComp.h"
#include "carbon/Log.h"
#include "master/WorkerNode.h"
#include <cmath>
#include "hippo/DriverCommon.h"

using namespace std;
BEGIN_CARBON_NAMESPACE(master);

CARBON_DECLARE_AND_SETUP_LOGGER(ReplicaNodeComp);

int32_t healthScore(HealthType healthStatus) {
    switch (healthStatus) {
    case HT_DEAD:
        return 0;
    case HT_UNKNOWN:
        return 1;
    case HT_LOST:
        return 2;
    case HT_ALIVE:
        return 3;
    default:
        CARBON_LOG(ERROR, "unsupport health status type:[%s].", enumToCStr(healthStatus));
        return 0;
    }
    return 0;
}

int32_t workerScore(WorkerType workerStatus) {
    switch (workerStatus) {
    case WT_UNKNOWN:
        return 0;
    case WT_NOT_READY:
        return 1;
    case WT_READY:
        return 2;
    default:
        CARBON_LOG(ERROR, "unsupported worker status type:[%s].", enumToCStr(workerStatus));
        return 0;
    }

    return 0;
}

int32_t serviceScore(ServiceType serviceStatus) {
    switch (serviceStatus) {
    case SVT_UNKNOWN:
        return 0;
    case SVT_UNAVAILABLE:
        return 1;
    case SVT_PART_AVAILABLE:
        return 2;
    case SVT_AVAILABLE:
        return 3;
    default:
        CARBON_LOG(ERROR, "unsupported service status type:[%s].", enumToCStr(serviceStatus));
        return 0;
    }
    return 0;
}

int32_t slotScore(SlotType slotStatus) {
    switch (slotStatus) {
    case ST_PKG_FAILED:
        return 0;
    case ST_PROC_FAILED:
        return 1;
    case ST_DEAD:
        return 2;
    case ST_PROC_RESTARTING:
        return 3;
    case ST_UNKNOWN:
        return 4;
    case ST_PROC_RUNNING:
        return 5;
    default:
        CARBON_LOG(ERROR, "unsupported slot status type:[%s].", enumToCStr(slotStatus));
        return 0;
    }
    return 0;
}

int32_t slotPrefScore(const hippo::SlotPreference &slotPref) {
    switch (slotPref.preftag) {
    case hippo::SlotPreference::PREF_RELEASE:
        return 0;
    case hippo::SlotPreference::PREF_NORMAL:
        return 1;
    default:
        CARBON_LOG(ERROR, "unsupported slot pref type:[%d].", (int)slotPref.preftag);
        return 0;
    }
    return 0;
}

// the node's score is higher, the node is more important.
// the nodes with smallest score would be selected to change (updating or release)
int32_t ReplicaNodeScorer::score(const ReplicaNodePtr &nodePtr) {
    //todo: calculate score in workerNode
    WorkerNodePtr workerNodePtr = nodePtr->getCurWorkerNode();
    
    HealthType healthStatus = workerNodePtr->getHealthStatus();
    WorkerType workerStatus = workerNodePtr->getWorkerStatus();
    ServiceType serviceStatus = workerNodePtr->getServiceStatus();
    SlotType slotStatus = workerNodePtr->getSlotStatus();
    bool isReplicaNodeReleasing = nodePtr->isReleasing();
    bool isReclaiming = workerNodePtr->isReclaiming();
    hippo::SlotPreference slotPref = workerNodePtr->getSlotPreference();

    // score factors, more front is more important.
    vector<int32_t> factors = {
        isReplicaNodeReleasing ? 0 : 1,
        workerNodePtr->isAvailable() ? 1 : 0,
        serviceScore(serviceStatus) + workerNodePtr->getServiceScore(),
        workerScore(workerStatus),
        healthScore(healthStatus),
        slotScore(slotStatus),
        isReclaiming ? 0 : 1,
        slotPrefScore(slotPref),
    };

    int32_t score = 0;
    for (size_t i = 0; i < factors.size(); i++) {
        score += factors[i] * pow(10, factors.size() - i - 1);
    }

    CARBON_LOG(DEBUG, "replica node %s get score: %d", nodePtr->getId().c_str(), score);
    return score;
}

END_CARBON_NAMESPACE(master);
