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
#ifndef CARBON_NODESELECTOR_H
#define CARBON_NODESELECTOR_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/ReplicaNode.h"

BEGIN_CARBON_NAMESPACE(master);

class NodeSelector
{
public:
    NodeSelector(const ReplicaNodeVect &nodes,
                 const std::map<version_t, int32_t> &holdingCounts,
                 const version_t latestVersion);

    ~NodeSelector();
private:
    NodeSelector(const NodeSelector &);
    NodeSelector& operator=(const NodeSelector &);
    
public:
    void pickupLatestRedundantNodes(int32_t count, ReplicaNodeVect *nodes) const;
    
    void pickupOldRedundantNodes(int32_t count, ReplicaNodeVect *nodes) const;
    
    int32_t getLatestVersionCount() const;

    int32_t getOldVersionCount() const;
    
private:
    void init();

    int32_t getHoldingCount(const version_t &version) const;
    
    int32_t getFrontNodes(const ReplicaNodeVect &nodes,
                          int32_t count,
                          int32_t holdingCount,
                          ReplicaNodeVect *retNodes) const;
    
private:
    ReplicaNodeVect _replicaNodes;
    std::map<version_t, ReplicaNodeVect> _replicaNodeMap;
    std::map<version_t, int32_t> _holdingCounts;
    version_t _latestVersion;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(NodeSelector);


END_CARBON_NAMESPACE(master);

#endif //CARBON_NODESELECTOR_H
