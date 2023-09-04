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
#include "master/WorkerNode.h"
#include "carbon/Log.h"
#include "common/Recorder.h"
#include "master/SlotUtil.h"
#include "master/HealthChecker.h"
#include "master/ServiceSwitch.h"
#include "master/Util.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, WorkerNode);
WorkerNode::WorkerNode() {
    _releasing = false;
    _offline = true;
    _releasing = false;
    _targetHasReached = false;
    _updatingSlotResourceNotMatch = false;
    _lastNotMatchTime = 0;
    _lastNotReadyTime = 0;
    _slotAllocStatus = SAS_UNASSIGNED;
    _slotStatus = ST_UNKNOWN;
    _pref.type = ReleasePreference::RELEASE_PREF_PREFER;
    _pref.leaseMs = PREF_PREFER_LEASE_TIME;
    _pref.workDirTag = Util::workerNodeIdToRoleGUID(_nodeId);
}

WorkerNode::WorkerNode(const nodeid_t &nodeId) {
    _nodeId = nodeId;
    _releasing = false;
    _offline = true;
    _targetHasReached = false;
    _lastNotMatchTime = 0;
    _lastNotReadyTime = 0;
    _slotAllocStatus = SAS_UNASSIGNED;
    _slotStatus = ST_UNKNOWN;
    _updatingSlotResourceNotMatch = false;
    _pref.type = ReleasePreference::RELEASE_PREF_PREFER;
    _pref.leaseMs = PREF_PREFER_LEASE_TIME;
    _pref.workDirTag = Util::workerNodeIdToRoleGUID(_nodeId);
}

WorkerNode::~WorkerNode() {
}

#define RECOVER_PLAN(planVersion, targetPlan)                           \
    {                                                                   \
        ExtVersionedPlanMap::const_iterator it;                         \
        it = versionedPlans.find(planVersion);                          \
        if (it == versionedPlans.end()) {                               \
            CARBON_LOG(ERROR, "workerNode [%s] recover failed: "        \
                       #planVersion " plan not found",                  \
                       _nodeId.c_str());                                \
            return false;                                               \
        }                                                               \
        targetPlan = it->second;                                        \
    }

bool WorkerNode::recover(const version_t &latestVersion,
                         const ExtVersionedPlanMap &versionedPlans)
{
    RECOVER_PLAN(latestVersion, _extFinalPlan);
    RECOVER_PLAN(_curVersion, _extCurPlan);
    RECOVER_PLAN(_nextVersion, _extNextPlan);
    fixPref();
    return true;
}

#undef RECOVER_PLAN

void WorkerNode::fixPref() {
    if (_pref.type == ReleasePreference::RELEASE_PREF_ANY) {
        _pref.type = ReleasePreference::RELEASE_PREF_PREFER;
        _pref.leaseMs = PREF_PREFER_LEASE_TIME;
        _pref.workDirTag = Util::workerNodeIdToRoleGUID(_nodeId);
    }
}

void WorkerNode::assignSlot(const SlotInfo &slotInfo) {
    if (_slotAllocStatus == SAS_UNASSIGNED) {
        if (!SlotUtil::isEmptySlot(slotInfo.slotId)) {
            _slotId = slotInfo.slotId;
            _ip = SlotUtil::getIp(slotInfo);
            _slotInfo = slotInfo;

            CARBON_LOG(INFO, "slot [%s] is assigned to workerNode [%s].",
                       SlotUtil::toString(slotInfo.slotId).c_str(),
                       _nodeId.c_str());
        } else {
            CARBON_LOG(WARN, "assign empty slot to workerNode [%s]", _nodeId.c_str());
        }
    } else {
        CARBON_LOG(ERROR, "try assign to an assigned slot [%s:%d] workerNode [%s], "
                   "assigned slot is [%s:%d]", slotInfo.slotId.slaveAddress.c_str(),
                   slotInfo.slotId.id, _nodeId.c_str(),
                   _slotInfo.slotId.slaveAddress.c_str(),
                   _slotInfo.slotId.id);
    }
}

bool WorkerNode::isUnAssignedSlot() const {
    return _slotAllocStatus == SAS_UNASSIGNED;
}

bool WorkerNode::isAssignedSlot() const {
    return _slotAllocStatus == SAS_ASSIGNED;
}

bool WorkerNode::canReleaseSlot() const {
    return _slotAllocStatus == SAS_RELEASING;
}

bool WorkerNode::isSlotReleased() const {
    return _slotAllocStatus == SAS_RELEASED;
}

bool WorkerNode::hasEmptySlotInfo() const {
    return SlotUtil::isEmptySlot(_slotInfo.slotId);
}

bool WorkerNode::isAvailable() const {
    if (_curVersion != _nextVersion) {
        return false;
    }

    if (!healthInfoReady()) {
        return false;
    }
    return serviceInfoMatch(true);
}

bool WorkerNode::resourcePlanMatch() const {
    return !_updatingSlotResourceNotMatch;
}

bool WorkerNode::launchPlanMatch() const {
    if (SlotUtil::isSlotInfoMatchingPlan(_extCurPlan.plan.launchPlan, _slotInfo)) {
        CARBON_LOG(DEBUG, "slot is matching launch plan.");
        return true;
    }

    CARBON_LOG(DEBUG, "node [%s] is not match the launch plan.", _nodeId.c_str());
    return false;
}

bool WorkerNode::healthInfoMatch() const {
    return _curVersion == _healthInfo.version &&
        _healthInfo.healthStatus == HT_ALIVE &&
        _healthInfo.workerStatus == WT_READY;
}

bool WorkerNode::serviceInfoMatch(bool online) const {
    if (online) {
        return _serviceInfo.status == SVT_AVAILABLE;
    } else {
        return _serviceInfo.status == SVT_UNAVAILABLE;
    }
}

bool WorkerNode::healthInfoReady() const {
    bool ret = _slotAllocStatus == SAS_ASSIGNED &&
               resourcePlanMatch() &&
               launchPlanMatch() &&
               healthInfoMatch();
    return ret;
}

void WorkerNode::setPlan(const version_t &version,
                         const ExtVersionedPlan &plan)
{
    if (_nextVersion != version) {
        _targetHasReached = false;
        _lastNotMatchTime = 0;
    }

    _nextVersion = version;
    _extNextPlan = plan;

    VersionedPlan &curPlan = _extCurPlan.plan;
    VersionedPlan &nextPlan = _extNextPlan.plan;
    if (curPlan.isEmpty()) {
        _curVersion = _nextVersion;
        _extCurPlan = _extNextPlan;
    }

    //take effect immediately.
    curPlan.online = nextPlan.online;
    curPlan.notMatchTimeout = nextPlan.notMatchTimeout;
    curPlan.notReadyTimeout = nextPlan.notReadyTimeout;
    curPlan.updatingGracefully = nextPlan.updatingGracefully;
}

void WorkerNode::setFinalPlan(const version_t &version,
                              const ExtVersionedPlan &plan)
{
    _finalVersion = version;
    _extFinalPlan = plan;
}

void WorkerNode::schedule() {
    if (_slotAllocStatus == SAS_UNASSIGNED) {
        dealwithSlotUnAssigned();
    } else if (_slotAllocStatus == SAS_ASSIGNED) {
        dealwithSlotAssigned();
    } else if (_slotAllocStatus == SAS_LOST) {
        dealwithSlotLost();
    } else if (_slotAllocStatus == SAS_OFFLINING) {
        dealwithSlotOfflining();
    } else if (_slotAllocStatus == SAS_RELEASING) {
        dealwithSlotReleasing();
    } else if (_slotAllocStatus == SAS_RELEASED) {
        dealwithSlotReleased();
    } else {
        CARBON_LOG(ERROR, "workerNode [%s] in unhandled status [%d]",
                   _nodeId.c_str(), _slotAllocStatus);
    }

    CARBON_LOG(DEBUG, "worker node status, nodeId:[%s], isOffline:[%d],"
               "serviceStatus:[%s], slotAllocStatus:[%s].",
               _nodeId.c_str(), _offline, enumToCStr(_serviceInfo.status),
               enumToCStr(_slotAllocStatus));
}

void WorkerNode::dealwithSlotUnAssigned() {
    if (!SlotUtil::isEmptySlot(_slotId)) {
        _slotAllocStatus = SAS_ASSIGNED;
    } else {
        _curVersion = _nextVersion;
        _extCurPlan = _extNextPlan;
        if (_releasing) {
            _slotAllocStatus = SAS_RELEASED;
            _offline = true;
        }
    }
}

void WorkerNode::dealwithSlotAssigned() {
    if (SlotUtil::isEmptySlot(_slotInfo.slotId)) {
        _slotAllocStatus = SAS_LOST;
        return;
    }
    if (_releasing) {
        tryMoveToSlotReleasing();
        return;
    }
    _assignedStep = STEP_BEGIN;
    if (!stepProcessUpdateGracefully()) {
        return;
    }
    _assignedStep = STEP_PROCESS_UPDATE_GRACEFULLY;
    if (!stepProcessResourcePlan()) {
        return;
    }
    _assignedStep = STEP_PROCESS_RESOURCE_PLAN;
    if (!stepProcessLaunchPlan()) {
        return;
    }
    _assignedStep = STEP_PROCESS_LAUNCH_PLAN;
    if (!stepProcessHealthInfo()) {
        return;
    }
    _assignedStep = STEP_PROCESS_HEALTH_INFO;
    if (!stepProcessServiceInfo()) {
        return;
    }
    _assignedStep = STEP_PROCESS_SERVICE_INFO;
}
bool WorkerNode::stepProcessResourcePlan() {
    // for update from old version carbon
    if (_extCurPlan.resourceTag != _extNextPlan.resourceTag) {
        CARBON_LOG(INFO, "workerNode %s resource plan changed, release itself.", _nodeId.c_str());
        release();
        return false;
    }
    if (_extCurPlan.plan.resourcePlan != _extNextPlan.plan.resourcePlan) {
        // this node is selected to upgrade, if resource not match for a long time,
        // release self.
        if (!SlotUtil::requirementIdMatch(_extNextPlan, _slotInfo)) {
            CARBON_LOG(INFO, "request id not match. %s, %s", _extNextPlan.resourceChecksum.c_str(), _slotInfo.requirementId.c_str());
            return false;
        }
        if (SlotUtil::isSlotInfoMatchingResourcePlan(_slotInfo)) {
            _extCurPlan.plan.resourcePlan = _extNextPlan.plan.resourcePlan;
            _extCurPlan.resourceTag = _extNextPlan.resourceTag;
            _updatingSlotResourceNotMatch = false;
            return true;
        } else if (_extNextPlan.plan.recoverResourceNotMatchNode) {
            CARBON_LOG(INFO, "workerNode %s adjust resource in place failed,"
                       "replace self.", _nodeId.c_str());
            _updatingSlotResourceNotMatch = true;
            return false;
        } else {
            CARBON_LOG(INFO, "workerNode %s adjust resource in place failed,"
                       "release self.", _nodeId.c_str());
            release();
            return false;
        }
    }
    _updatingSlotResourceNotMatch = false;
    return true;
}

bool WorkerNode::stepProcessUpdateGracefully() {
    if (_extNextPlan.plan.updatingGracefully) {
        if (_curVersion != _nextVersion) {
            _offline = true;
            if (_serviceInfo.status != SVT_UNAVAILABLE) {
                return false;
            } else {
                return true;
            }
        }
    } else {
        _offline = !_extCurPlan.plan.online;
    }
    return true;
}
bool WorkerNode::stepProcessLaunchPlan() {
    _extCurPlan.plan.launchPlan = _extNextPlan.plan.launchPlan;
    if (launchPlanMatch()) {
        _lastNotMatchTime = 0;
        return true;
    }
    if (_lastNotMatchTime == 0) {
        _lastNotMatchTime = TimeUtility::currentTime();
    }
    return false;
}
bool WorkerNode::stepProcessHealthInfo() {
    _curVersion = _nextVersion;
    _extCurPlan = _extNextPlan;
    if (healthInfoMatch()) {
        _lastNotReadyTime = 0;
        return true;
    }
    if (_lastNotReadyTime == 0) {
        _lastNotReadyTime = TimeUtility::currentTime();
    }
    return false;
}
bool WorkerNode::stepProcessServiceInfo() {
    if (_extNextPlan.plan.updatingGracefully) {
        _offline = !_extCurPlan.plan.online;
    }
    if (serviceInfoMatch(_extCurPlan.plan.online)) {
        _targetHasReached = true;
        return true;
    }
    return false;
}

void WorkerNode::dealwithSlotLost() {
    if (!SlotUtil::isEmptySlot(_slotInfo.slotId)){
        _slotAllocStatus = SAS_ASSIGNED;
        return;
    }
    if (_releasing){
        tryMoveToSlotReleasing();
    }
}

void WorkerNode::dealwithSlotOfflining() {
    _offline = true;
    if (_serviceInfo.status == SVT_UNAVAILABLE) {
        _slotAllocStatus = SAS_RELEASING;
    }
}

void WorkerNode::dealwithSlotReleasing() {
    if (SlotUtil::isEmptySlot(_slotInfo.slotId)) {
        _slotAllocStatus = SAS_RELEASED;
    }
}

void WorkerNode::dealwithSlotReleased() {
    CARBON_LOG(INFO, "workerNode %s has been released.", _nodeId.c_str());
    return;
}

void WorkerNode::releaseWithPref(ReleasePreference pref) {
    CARBON_LOG(INFO, "release worker node[%s] with preference [%s].",
               _nodeId.c_str(), ToJsonString(pref).c_str());
    _releasing = true;
    _pref.type = pref.type;
    _pref.leaseMs = pref.leaseMs;
}

void WorkerNode::release() {
    CARBON_LOG(INFO, "release worker node:[%s].", _nodeId.c_str());
    _releasing = true;
    if (_pref.type == ReleasePreference::RELEASE_PREF_PROHIBIT) {
        return;
    }
    if (isDead() || isLongTimeNotMatchPlan()) {
        _pref.type = ReleasePreference::RELEASE_PREF_PROHIBIT;
        _pref.leaseMs = PREF_PROHIBIT_LEASE_TIME;
    }
}

void WorkerNode::updateSlotInfo(const SlotInfo &slotInfo) {
    _slotInfo = slotInfo;
    _slotStatus = SlotUtil::extractSlotStatus(slotInfo).status;
    common::Recorder::logWhenDiff(_nodeId + ".slotStatus",
                                  string(enumToCStr(_slotStatus)),
                                  _logger);
}

void WorkerNode::updateHealthInfo(const HealthInfo &healthInfo) {
    _healthInfo = healthInfo;
    try {
        string healthInfoStr = ToJsonString(_healthInfo, true);
        common::Recorder::logWhenDiff(_nodeId + ".healthInfo", healthInfoStr,
                                      _logger);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "jsonize WorkerNode healthInfo failed, "
                   "WorkerNodeId:[%s], error:[%s].",
                   _nodeId.c_str(), e.what());
    }
}

void WorkerNode::updateServiceInfo(const ServiceInfo &serviceInfo) {
    _serviceInfo = serviceInfo;
    try {
        string serviceInfoStr = ToJsonString(_serviceInfo, true);
        common::Recorder::logWhenDiff(_nodeId + ".serviceInfo", serviceInfoStr,
                                      _logger);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "jsonize WorkerNode serviceInfo failed, "
                   "WorkerNodeId:[%s], error:[%s].",
                   _nodeId.c_str(), e.what());
    }
}

const string WorkerNode::getGeneralStateString() const {
    if (_slotAllocStatus == SAS_RELEASED) {
        return "released";
    }
    if (_releasing) {
        return "releasing";
    }
    if (_slotInfo.reclaiming) {
        return "reclaiming";
    }
    if (_slotAllocStatus == SAS_LOST) {
        return "lost";
    }
    if (isDead() || isLongTimeNotMatchPlan()) {
        return "broken";
    }
    if (isCompleted()) {
        return "completed";
    }
    return "unknown";
}

bool WorkerNode::inBadState() const {
    return isBroken() || isReclaiming() || isSlotReleased();
}

bool WorkerNode::isBroken() const {
    if (_slotAllocStatus == SAS_LOST) {
        return true;
    }

    if (_slotAllocStatus == SAS_ASSIGNED) {
        return isDead() || isLongTimeNotMatchPlan() || isLongTimeNotReady() || isUpdatingSlotResourceNotMatch();
    }

    return false;
}

bool WorkerNode::isCompleted() const {
    if (_curVersion != _nextVersion) {
        return false;
    }

    if (!healthInfoReady()) {
        return false;
    }

    if (isReclaiming()) {
        return false;
    }

    return serviceInfoMatch(_extCurPlan.plan.online);
}

bool WorkerNode::isReclaiming() const {
    if (_slotAllocStatus == SAS_ASSIGNED) {
        return _slotInfo.reclaiming;
    }
    return false;
}

bool WorkerNode::isDead() const {
    return _healthInfo.healthStatus == HT_DEAD;
}

bool WorkerNode::isLongTimeNotReady() const {
    if (_lastNotReadyTime == 0 || _extCurPlan.plan.notReadyTimeout <= 0) {
        return false;
    }

    int64_t notReadyTimeout = _extCurPlan.plan.notReadyTimeout * 1000L * 1000L;
    int64_t curTime = TimeUtility::currentTime();
    bool ret = curTime - _lastNotReadyTime > notReadyTimeout;
    if (ret) {
        CARBON_LOG(INFO, "node:[%s] is too long to be not ready.",
                   _nodeId.c_str());
    }
    return ret;
}

bool WorkerNode::isLongTimeNotMatchPlan() const {
    if (_lastNotMatchTime == 0) {
        return false;
    }

    int64_t notMatchTimeout = _extCurPlan.plan.notMatchTimeout * 1000L * 1000L;
    int64_t curTime = TimeUtility::currentTime();
    bool ret = curTime - _lastNotMatchTime > notMatchTimeout;
    if (ret) {
        CARBON_LOG(INFO, "node:[%s] is too long to be not match the plan.",
                   _nodeId.c_str());
    }
    return ret;
}


bool WorkerNode::isUpdatingSlotResourceNotMatch() const {
    return _updatingSlotResourceNotMatch;
}

string WorkerNode::getIp() const {
    return _ip;
}

KVMap WorkerNode::getHealthInfoMetas() const {
    return _healthInfo.metas;
}

KVMap WorkerNode::getServiceInfoMetas() const {
    return _serviceInfo.metas;
}

void WorkerNode::fillWorkerNodeStatus(WorkerNodeStatus *status) const {
    status->workerNodeId = _nodeId;
    status->curVersion = _curVersion;
    status->nextVersion = _nextVersion;
    status->finalVersion = _finalVersion;
    status->userDefVersion = _extCurPlan.plan.userDefVersion;
    status->offline = _offline;
    status->releasing = _releasing;
    status->reclaiming = _slotInfo.reclaiming;
    status->lastNotMatchTime = _lastNotMatchTime;
    status->lastNotReadyTime = _lastNotReadyTime;
    status->slotAllocStatus = _slotAllocStatus;
    status->slotInfo = _slotInfo;
    status->healthInfo = _healthInfo;
    status->serviceInfo = _serviceInfo;
    status->ip = _ip;
    status->slotStatus.status = _slotStatus;
    status->slotStatus.slotId = _slotId;
    status->targetSignature = _extNextPlan.plan.signature;
    status->targetCustomInfo = _extNextPlan.plan.customInfo;
    status->readyForCurVersion = isCompleted();
}

void WorkerNode::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("nodeId", _nodeId);
    json.Jsonize("curVersion", _curVersion);
    json.Jsonize("nextVersion", _nextVersion);
    json.Jsonize("finalVersion", _finalVersion, _nextVersion);
    json.Jsonize("slotId", _slotId);
    json.Jsonize("ip", _ip);
    json.Jsonize("offline", _offline);
    json.Jsonize("targetHasReached", _targetHasReached, false);
    json.Jsonize("releasing", _releasing);
    json.Jsonize("pref", _pref);
    json.Jsonize("lastNotMatchTime", _lastNotMatchTime);
    json.Jsonize("lastNotReadyTime", _lastNotReadyTime, 0L);
    json.Jsonize("slotAllocStatus", _slotAllocStatus);
}

bool WorkerNode::isNeedSlot() const {
    if (_slotAllocStatus == SAS_RELEASING ||
        _slotAllocStatus == SAS_RELEASED ||
        _slotAllocStatus == SAS_LOST)
    {
        return false;
    }

    return true;
}

inline void WorkerNode::tryMoveToSlotReleasing() {
    _slotAllocStatus = SAS_OFFLINING;
}

END_CARBON_NAMESPACE(master);
