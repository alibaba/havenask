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
#include "master/NodeSelector.h"
#include "master/ReplicaNodeComp.h"

using namespace std;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, NodeSelector);

NodeSelector::NodeSelector(const ReplicaNodeVect &nodes,
                           const map<version_t, int32_t> &holdingCounts,
                           const version_t latestVersion)
{
    _replicaNodes = nodes;
    _holdingCounts = holdingCounts;
    _latestVersion = latestVersion;
    
    init();
}

NodeSelector::~NodeSelector() {
}

void NodeSelector::init() {
    _replicaNodeMap.clear();
    for (size_t i = 0; i < _replicaNodes.size(); i++) {
        ReplicaNodePtr nodePtr = _replicaNodes[i];
        version_t version = nodePtr->getVersion();
        _replicaNodeMap[version].push_back(nodePtr);
    }

    for (map<version_t, ReplicaNodeVect>::iterator it = _replicaNodeMap.begin();
         it != _replicaNodeMap.end(); it++)
    {
        sort(it->second.begin(), it->second.end(), ReplicaNodeComp());
    }
}

void NodeSelector::pickupLatestRedundantNodes(int32_t count,
        ReplicaNodeVect *nodes) const
{
    int32_t holdingCount = getHoldingCount(_latestVersion);

    map<version_t, ReplicaNodeVect>::const_iterator nodesIt =
        _replicaNodeMap.find(_latestVersion);
    if (nodesIt == _replicaNodeMap.end()) {
        return;
    }

    CARBON_LOG(DEBUG, "latest version nodes count:%zd, holdingCount:%d",
               nodesIt->second.size(), holdingCount);
    getFrontNodes(nodesIt->second, count, holdingCount, nodes);
}

void NodeSelector::pickupOldRedundantNodes(int32_t count,
        ReplicaNodeVect *nodes) const 
{
    int32_t pickCount = count;
    map<version_t, ReplicaNodeVect>::const_iterator nodesIt;
    for (nodesIt = _replicaNodeMap.begin();
         nodesIt != _replicaNodeMap.end();
         nodesIt++)
    {
        if (nodesIt->first == _latestVersion) {
            continue;
        }

        int32_t holdingCount = getHoldingCount(nodesIt->first);
        int32_t retCount = getFrontNodes(nodesIt->second,
                pickCount, holdingCount, nodes);
        CARBON_LOG(DEBUG, "pick up old version nodes, version:[%s], "
                   "retCount:[%d], holdingCount:%d",
                   nodesIt->first.c_str(), retCount, holdingCount);
        pickCount -= retCount;
        if (pickCount <= 0) {
            break;
        }
    }
}

int32_t NodeSelector::getLatestVersionCount() const {
    map<version_t, ReplicaNodeVect>::const_iterator it =
        _replicaNodeMap.find(_latestVersion);
    if (it == _replicaNodeMap.end()) {
        return 0;
    }
    
    return (int32_t)(it->second.size());
}

int32_t NodeSelector::getOldVersionCount() const {
    int32_t count = 0;
    map<version_t, ReplicaNodeVect>::const_iterator it;
    for (it = _replicaNodeMap.begin(); it != _replicaNodeMap.end(); it++) {
        if (it->first != _latestVersion) {
            count += (int32_t)(it->second.size());
        }
    }
    return count;
}

int32_t NodeSelector::getFrontNodes(const ReplicaNodeVect &nodes,
                                    int32_t count,
                                    int32_t holdingCount,
                                    ReplicaNodeVect *retNodes) const
{
    int32_t totalCount = (int32_t)nodes.size();
    if (totalCount <= holdingCount) {
        return 0;
    }
    
    int32_t retCount = totalCount - holdingCount;
    retCount = min(count, retCount);
    CARBON_LOG(DEBUG, "retCount: %d", retCount);
    for (int32_t i = 0; i < retCount; i++) {
        retNodes->push_back(nodes[i]);
    }
    return retCount;
}

int32_t NodeSelector::getHoldingCount(const version_t &version) const {
    map<version_t, int32_t>::const_iterator holdingCountIt =
        _holdingCounts.find(version);
    if (holdingCountIt != _holdingCounts.end()) {
        return holdingCountIt->second;
    }

    return 0;
}
    


END_CARBON_NAMESPACE(master);

