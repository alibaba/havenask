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
#include "master/ReplicaNode.h"
#include "carbon/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, ReplicaNode);

ReplicaNode::ReplicaNode() {
    _releasing = false;
    _timeStamp = 0;
}

ReplicaNode::ReplicaNode(const nodeid_t &nodeId,
                         const BrokenRecoverQuotaPtr &quotaPtr,
                         const WorkerNodeCreatorPtr &workerNodeCreatorPtr)
{
    _nodeId = nodeId;
    _quotaPtr = quotaPtr;
    _releasing = false;
    _timeStamp = 0;
    if (workerNodeCreatorPtr == NULL) {
        _workerNodeCreatorPtr.reset(new WorkerNodeCreator(""));
    } else {
        _workerNodeCreatorPtr = workerNodeCreatorPtr;
    }
    _curWorkerNodePtr = _workerNodeCreatorPtr->create();
    _smoothRecover = true;
}

ReplicaNode::~ReplicaNode() {
}

ReplicaNode::ReplicaNode(const ReplicaNode &rhs) {
    *this = rhs;
}

ReplicaNode& ReplicaNode::operator=(const ReplicaNode &rhs) {
    if (this == &rhs) {
        return *this;
    }

    _nodeId = rhs._nodeId;
    assert(rhs._curWorkerNodePtr);
    _curWorkerNodePtr.reset(new WorkerNode(*(rhs._curWorkerNodePtr)));
    if (rhs._backupWorkerNodePtr) {
        _backupWorkerNodePtr.reset(new WorkerNode(*(rhs._backupWorkerNodePtr)));
    }
    _version = rhs._version;
    _extPlan = rhs._extPlan;
    _releasing = rhs._releasing;
    _timeStamp = rhs._timeStamp;
    _quotaPtr = rhs._quotaPtr;
    _workerNodeCreatorPtr = rhs._workerNodeCreatorPtr;
    _smoothRecover = rhs._smoothRecover;
    return *this;
}

void ReplicaNode::setPlan(const version_t &version,
                          const ExtVersionedPlan &extPlan)
{
    _version = version;
    _extPlan = extPlan;
}

void ReplicaNode::release() {
    CARBON_LOG(INFO, "release replica node, nodeId:[%s].", _nodeId.c_str());
    _releasing = true;
    _curWorkerNodePtr->release();

    if (_backupWorkerNodePtr) {
        _backupWorkerNodePtr->release();
    }
}

void ReplicaNode::schedule() {
    adjustInternalNode();
    scheduleNodes();
}

void ReplicaNode::adjustInternalNode() {
    if (_releasing) {
        return;
    }
    if (_backupWorkerNodePtr != NULL) {
        if (_backupWorkerNodePtr->isCompleted()) {
            releaseCurNodeAndSwapBackup();
        } else if (_backupWorkerNodePtr->inBadState()
                   || _curWorkerNodePtr->isCompleted()) {
            _backupWorkerNodePtr->release();
        }
        if (_backupWorkerNodePtr->isSlotReleased()) {
            CARBON_LOG(INFO, "totaly release node [%s]",
                       _backupWorkerNodePtr->getId().c_str());
            _backupWorkerNodePtr.reset();
        }
    } else {
        if (_curWorkerNodePtr->inBadState()) {
            if (_curWorkerNodePtr->isBroken() && !_quotaPtr->require()) {
                CARBON_LOG(INFO, "create backup node failed due to not have quota");
                return;
            }
            _backupWorkerNodePtr = _workerNodeCreatorPtr->create();
            CARBON_LOG(INFO, "create backup node [%s] for cur node"
                       " [%s] because cur node is [%s].",
                       _backupWorkerNodePtr->getId().c_str(),
                       _curWorkerNodePtr->getId().c_str(),
                       _curWorkerNodePtr->getGeneralStateString().c_str());
            if (!_smoothRecover) {
                _curWorkerNodePtr->release();
                CARBON_LOG(INFO, "smooth recover flag is FALSE, "
                        "release the current worker node [%s].",
                        _curWorkerNodePtr->getId().c_str());
            }
        }
    }
}

void ReplicaNode::releaseCurNodeAndSwapBackup() {
    CARBON_LOG(INFO, "replace cur node [%s] with backup node [%s]",
               _curWorkerNodePtr->getId().c_str(),
               _backupWorkerNodePtr->getId().c_str());
    _curWorkerNodePtr->release();
    
    WorkerNodePtr tmp = _curWorkerNodePtr;
    _curWorkerNodePtr = _backupWorkerNodePtr;
    _backupWorkerNodePtr = tmp;
}

void ReplicaNode::scheduleNodes() {
    assert(_curWorkerNodePtr);
    _curWorkerNodePtr->setPlan(_version, _extPlan);
    _curWorkerNodePtr->schedule();

    if (_backupWorkerNodePtr) {
        _backupWorkerNodePtr->setPlan(_version, _extPlan);
        _backupWorkerNodePtr->schedule();
    }
}

void ReplicaNode::getWorkerNodes(WorkerNodeVect *workerNodes) const {
    assert(_curWorkerNodePtr);
    workerNodes->push_back(_curWorkerNodePtr);

    if (_backupWorkerNodePtr) {
        workerNodes->push_back(_backupWorkerNodePtr);
    }
}

bool ReplicaNode::isReleased() const {
    assert(_curWorkerNodePtr);
    if (!_releasing) {
        return false;
    }
    bool ret = _curWorkerNodePtr->isSlotReleased();
    if (_backupWorkerNodePtr) {
        ret = ret && _backupWorkerNodePtr->isSlotReleased();
    }
    CARBON_LOG(DEBUG, "replica node is released or not [%d].", ret);
    return ret;
}

void ReplicaNode::setFinalPlan(const version_t &version,
                               const ExtVersionedPlan &plan)
{
    assert(_curWorkerNodePtr);
    _curWorkerNodePtr->setFinalPlan(version, plan);
    if (_backupWorkerNodePtr) {
        _backupWorkerNodePtr->setFinalPlan(version, plan);
    }
}

void ReplicaNode::fillReplicaNodeStatus(ReplicaNodeStatus *status) const {
    status->replicaNodeId = _nodeId;
    status->timeStamp = _timeStamp;
    status->userDefVersion = _extPlan.plan.userDefVersion;
    status->readyForCurVersion = isCompleted();
    assert(_curWorkerNodePtr);
    _curWorkerNodePtr->fillWorkerNodeStatus(&status->curWorkerNodeStatus);

    if (_backupWorkerNodePtr) {
        _backupWorkerNodePtr->fillWorkerNodeStatus(&status->backupWorkerNodeStatus);
    }
}

bool ReplicaNode::isCompleted() const {
    if (_releasing) {
        return false;
    }
    if (_backupWorkerNodePtr) {
        return false;
    }
    if (_curWorkerNodePtr->getCurVersion() != _version) {
        return false;
    }
    return _curWorkerNodePtr->isCompleted();
}

bool ReplicaNode::isAvailable() const {
    if (_releasing) {
        return false;
    }
    return _curWorkerNodePtr->isAvailable();
}

bool ReplicaNode::targetHasReached() const {
    return _curWorkerNodePtr->targetHasReached();
}

bool ReplicaNode::isUnAssignedSlot() const {
    return _curWorkerNodePtr->isUnAssignedSlot();
}

void ReplicaNode::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("nodeId", _nodeId);
    json.Jsonize("version", _version);
    json.Jsonize("curWorkerNode", _curWorkerNodePtr);
    json.Jsonize("backupWorkerNode", _backupWorkerNodePtr);
    json.Jsonize("releasing", _releasing, _releasing);
    json.Jsonize("timeStamp", _timeStamp, _timeStamp);
}

bool ReplicaNode::recover(const version_t &latestVersion,
                          const ExtVersionedPlanMap &versionedPlans,
                          const BrokenRecoverQuotaPtr &brokenRecoverQuotaPtr,
                          const WorkerNodeCreatorPtr &workerNodeCreatorPtr)
{
    ExtVersionedPlanMap::const_iterator extPlanIt = versionedPlans.find(_version);
    if (extPlanIt == versionedPlans.end()) {
        CARBON_LOG(ERROR, "replicaNode [%s] recover failed: %s plan not found",
                   _nodeId.c_str(), _version.c_str());
        return false;
    }
    _extPlan = extPlanIt->second;
    _quotaPtr = brokenRecoverQuotaPtr;
    _workerNodeCreatorPtr = workerNodeCreatorPtr;
    assert(_curWorkerNodePtr);
    if (!_curWorkerNodePtr->recover(latestVersion, versionedPlans)) {
        CARBON_LOG(ERROR, "recover %s's curWorkerNode failed",
                   _nodeId.c_str());
        return false;
    }
    if (_backupWorkerNodePtr) {
        if (!_backupWorkerNodePtr->recover(latestVersion, versionedPlans)) {
            CARBON_LOG(ERROR, "recover %s's backupWorkerNode failed",
                       _nodeId.c_str());
            return false;
        }
    }
    return true;
}

END_CARBON_NAMESPACE(master);
