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
#include "master/ReplicaNodesAdjuster.h"
#include "master/ReplicaNodeComp.h"
#include "carbon/Log.h"
#include "master/Util.h"
#include <cmath>

using namespace std;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, ReplicaNodesAdjuster);

#define LOG_PREFIX _logPrefix.c_str()

ReplicaNodesAdjuster::ReplicaNodesAdjuster(
        const ReplicaNodeCreatorPtr &replicaNodeCreatorPtr,
        const ReplicaNodeVect &replicaNodes,
        const GlobalPlan &globalPlan,
        const ExtVersionedPlanMap &versionedPlans,
        const version_t &latestVersion,
        const ScheduleParams &scheduleParams,
        const string &logPrefix):
    _replicaNodeCreatorPtr(replicaNodeCreatorPtr),
    _replicaNodes(replicaNodes),
    _globalPlan(globalPlan),
    _versionedPlans(versionedPlans),
    _latestVersion(latestVersion),
    _scheduleParams(scheduleParams),
    _logPrefix(logPrefix)
{
    
}

ReplicaNodesAdjuster::~ReplicaNodesAdjuster() {
}

void ReplicaNodesAdjuster::adjust() {
    NodeSelector nodeSelector(_replicaNodes, _scheduleParams.holdingCountMap,
                              _latestVersion);

    int32_t targetLatestCount = 0;
    if (_versionedPlans.size() == 1) {
        //latestVersionRatio is meaningless when only has one version.
        targetLatestCount = _globalPlan.count;
    } else {
        targetLatestCount = (int32_t)ceil(_globalPlan.count * (
                        (double)_globalPlan.latestVersionRatio / 100.0));
    }
    int32_t targetOldCount = _globalPlan.count - targetLatestCount;
    int32_t curLatestCount = nodeSelector.getLatestVersionCount();
    int32_t curOldCount = nodeSelector.getOldVersionCount();

    CARBON_PREFIX_LOG(DEBUG, "targetLatestCount:%d, curLatestCount:%d, "
               "targetOldCount:%d, curOldCount:%d.",
               targetLatestCount, curLatestCount,
               targetOldCount, curOldCount);

    ReplicaNodeVect nodePool;
    collectLatestRedundantNodes(curLatestCount - targetLatestCount,
                                nodeSelector, &nodePool);

    collectOldRedundantNodes(curOldCount - targetOldCount,
                             nodeSelector, &nodePool);

    holdExtraNodes(&nodePool);
    
    supplementLatestNodes(targetLatestCount - curLatestCount,
                          &nodePool);

    supplementOldNodes(targetOldCount - curOldCount,
                       &nodePool);

    replaceNotAssignedNodes(&nodePool);
    
    releaseReplicaNodes(nodePool);
    
    removeReleasedReplicaNodes();
}

void ReplicaNodesAdjuster::replaceNotAssignedNodes(ReplicaNodeVect *nodePool) {
    sort(nodePool->begin(), nodePool->end(),
         [](const ReplicaNodePtr &a, const ReplicaNodePtr &b) {
                return (int32_t)a->isUnAssignedSlot() >
                    (int32_t)b->isUnAssignedSlot();
            });
    size_t assignedIndex = 0;
    for (size_t i = 0; i < _replicaNodes.size(); i++) {
        if (!_replicaNodes[i]->isUnAssignedSlot()) {
            continue;
        }
        if (assignedIndex >= nodePool->size() ||
            (*nodePool)[assignedIndex]->isUnAssignedSlot()) {
            break;
        }
        version_t targetVersion = _replicaNodes[i]->getVersion();
        (*nodePool)[assignedIndex]->setPlan(targetVersion,
                _versionedPlans[targetVersion]);
        (*nodePool)[assignedIndex]->setTimeStamp(_scheduleParams.timeStamp);
        (*nodePool)[assignedIndex++] = _replicaNodes[i];
    }
}

bool replicaNodeCompFunc(const ReplicaNodePtr &a, const ReplicaNodePtr &b) {
    ReplicaNodeComp comp;
    if (a->targetHasReached() == b->targetHasReached()) {
        return comp(a, b);
    }

    return (int32_t)a->targetHasReached() < (int32_t)b->targetHasReached();
}

int32_t ReplicaNodesAdjuster::getHoldCount() const {
    int32_t holdCount = 0;
    for(auto kv : _scheduleParams.holdingCountMap) {
        holdCount += kv.second;
    }
    return holdCount;
}

void ReplicaNodesAdjuster::holdExtraNodes(ReplicaNodeVect *nodePool) {
    int32_t curHoldCount = getHoldCount();
    if (_scheduleParams.minHealthCount <= curHoldCount) {
        return;
    }
    int32_t extraHoldCount = _scheduleParams.minHealthCount - curHoldCount;
    sort(nodePool->begin(), nodePool->end(), replicaNodeCompFunc);
    int32_t targetPoolSize = (int32_t)nodePool->size();
    targetPoolSize = max(0, targetPoolSize - extraHoldCount);
    nodePool->resize(targetPoolSize);
}

void ReplicaNodesAdjuster::collectLatestRedundantNodes(
        int32_t redundantCount, const NodeSelector &nodeSelector,
        ReplicaNodeVect *nodePool)
{
    if (redundantCount <= 0) {
        return;
    }
    
    ReplicaNodeVect redundantNodes;
    nodeSelector.pickupLatestRedundantNodes(redundantCount, &redundantNodes);
    int32_t poolSizeBeforePush = nodePool->size();
    for (size_t i = 0; i < redundantNodes.size(); ++i) {
        nodePool->push_back(redundantNodes[i]);
    }
    int32_t poolSizeAfterPush = nodePool->size();
        
    CARBON_PREFIX_LOG(INFO, "pickup latest version redundant nodes, "
               "latestVersion:[%s], latestRedundantCount:[%d], "
                      "pickedCount:[%d]",
                      _latestVersion.c_str(),
                      (int)redundantCount,
                      (int)(poolSizeAfterPush - poolSizeBeforePush));
}

void ReplicaNodesAdjuster::collectOldRedundantNodes(
        int32_t redundantCount, const NodeSelector &nodeSelector,
        ReplicaNodeVect *nodePool)
{
    if (redundantCount <= 0) {
        return;
    }
    
    ReplicaNodeVect redundantNodes;
    nodeSelector.pickupOldRedundantNodes(redundantCount, &redundantNodes);

    nodePool->insert(nodePool->end(), redundantNodes.begin(), redundantNodes.end());
    
    CARBON_PREFIX_LOG(INFO, "pickup old version redundant nodes, "
                      "oldRedundantCount:[%d], pickedCount:[%zd]",
                      redundantCount, redundantNodes.size());
}

void ReplicaNodesAdjuster::supplementLatestNodes(int32_t count,
        ReplicaNodeVect *nodePool)
{
    if (count <= 0) {
        return;
    }
    
    CARBON_PREFIX_LOG(INFO, "supplement latest version nodes, count:%d, "
               "nodePool size:%zd.", count, nodePool->size());
    
    ExtVersionedPlanMap::const_iterator it =
        _versionedPlans.find(_latestVersion);
    assert(it != _versionedPlans.end());

    const ExtVersionedPlan &latestVersionPlan = it->second;
    supplementNodesWithPlan(_latestVersion, latestVersionPlan,
                            count, nodePool);
}

void ReplicaNodesAdjuster::supplementOldNodes(int32_t count,
        ReplicaNodeVect *nodePool)
{
    if (count <= 0) {
        return;
    }

    assert(_versionedPlans.size() != 0);
    
    version_t version;
    ExtVersionedPlan extPlan;
    selectVersion(_latestVersion, _versionedPlans, &version, &extPlan);

    CARBON_PREFIX_LOG(INFO, "supplement old version nodes, version:%s, "
               "count:%d, nodePool size:%zd.",
               version.c_str(), count, nodePool->size());
    supplementNodesWithPlan(version, extPlan, count, nodePool);
}

void ReplicaNodesAdjuster::selectVersion(
        const version_t &latestVersion,
        const ExtVersionedPlanMap &versionedPlans,
        version_t *version, ExtVersionedPlan *extPlan)
{
    ExtVersionedPlanMap::const_iterator it;
    for (it = versionedPlans.begin(); it != versionedPlans.end(); it++) {
        if (it->first == latestVersion) {
            continue;
        }

        *version = it->first;
        *extPlan = it->second;
        return;
    }

    *version = latestVersion;
    it = versionedPlans.find(latestVersion);
    assert(it != versionedPlans.end());
    *extPlan = it->second;
}

void ReplicaNodesAdjuster::supplementNodesWithPlan(
        const version_t &version,
        const ExtVersionedPlan &extPlan, int32_t count,
        ReplicaNodeVect *nodePool)
{
    int32_t totalCount = (int32_t)_replicaNodes.size();
    int32_t maxExtraCount = _scheduleParams.maxCount - totalCount;
    
    CARBON_PREFIX_LOG(INFO, "supplement replica nodes, version:[%s], count:[%d], "
               "maxExtraCount:[%d], node pool size:[%zd].",
               version.c_str(), count, maxExtraCount,
               nodePool->size());

    ReplicaNodeVect supplementNodes;
    ReplicaNodeVect redundantNodes;

    for (size_t i = 0; i < nodePool->size(); i++) {
        const ReplicaNodePtr &nodePtr = (*nodePool)[i];
        if (nodePtr->isReleasing()) {
            redundantNodes.push_back(nodePtr);
        } else {
            if ((int32_t)supplementNodes.size() < count) {
                CARBON_PREFIX_LOG(INFO, "[%s] is set new plan %s",
                        nodePtr->getId().c_str(), version.c_str());
                supplementNodes.push_back(nodePtr);
            } else {
                redundantNodes.push_back(nodePtr);
            }
        }
    }

    CARBON_PREFIX_LOG(INFO, "supplement nodes from pool size:[%zd].", supplementNodes.size());
    if (maxExtraCount > 0) {
        int32_t lackCount = min(maxExtraCount, count - (int32_t)supplementNodes.size());
        for (int32_t i = 0; i < lackCount; i++) {
            ReplicaNodePtr replicaNodePtr = _replicaNodeCreatorPtr->create();
            _replicaNodes.push_back(replicaNodePtr);
            supplementNodes.push_back(replicaNodePtr);
        }
        CARBON_PREFIX_LOG(INFO, "supplement replica nodes, count:[%d], version:[%s].",
                   lackCount, version.c_str());
    }

    for (size_t i = 0; i < supplementNodes.size(); i++) {
        ReplicaNodePtr nodePtr = supplementNodes[i];
        nodePtr->setPlan(version, extPlan);
        nodePtr->setTimeStamp(_scheduleParams.timeStamp);
    }
    nodePool->swap(redundantNodes);
}

void ReplicaNodesAdjuster::releaseReplicaNodes(const ReplicaNodeVect &nodes) {
    if (nodes.size() > 0) {
        CARBON_PREFIX_LOG(INFO, "release replica nodes, count:%zd.", nodes.size());
    }
    
    for (size_t i = 0; i < nodes.size(); i++) {
        ReplicaNodePtr replicaNodePtr = nodes[i];
        replicaNodePtr->release();
        CARBON_PREFIX_LOG(INFO, "release replica node:[%s], version:[%s].",
                   replicaNodePtr->getId().c_str(),
                   replicaNodePtr->getVersion().c_str());
    }
}

void ReplicaNodesAdjuster::removeReleasedReplicaNodes() {
        ReplicaNodeVect replicaNodes;
    for (size_t i = 0; i < _replicaNodes.size(); i++) {
        const ReplicaNodePtr &replicaNodePtr = _replicaNodes[i];
        if (!replicaNodePtr->isReleased()) {
            replicaNodes.push_back(replicaNodePtr);
        }
    }
    _replicaNodes.swap(replicaNodes);
}

ReplicaNodeVect ReplicaNodesAdjuster::getReplicaNodes() const {
    return _replicaNodes;
}

#undef LOG_PREFIX

END_CARBON_NAMESPACE(master);

