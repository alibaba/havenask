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
#ifndef CARBON_REPLICANODESADJUSTER_H
#define CARBON_REPLICANODESADJUSTER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/ReplicaNodeCreator.h"
#include "carbon/RolePlan.h"
#include "master/Role.h"
#include "master/NodeSelector.h"

BEGIN_CARBON_NAMESPACE(master);

class ReplicaNodesAdjuster
{
public:
    ReplicaNodesAdjuster(const ReplicaNodeCreatorPtr &replicaNodeCreatorPtr,
                         const ReplicaNodeVect &replicaNodes,
                         const GlobalPlan &globalPlan,
                         const ExtVersionedPlanMap &versionedPlans,
                         const version_t &latestVersion,
                         const ScheduleParams &scheduleParams,
                         const std::string &logPrefix);

    ~ReplicaNodesAdjuster();
    
public:
    void adjust();

    ReplicaNodeVect getReplicaNodes() const;

private:
    void collectLatestRedundantNodes(
            int32_t redundantCount,
            const NodeSelector &nodeSelector,
            ReplicaNodeVect *nodePool);

    void collectOldRedundantNodes(
            int32_t redundantCount,
            const NodeSelector &nodeSelector,
            ReplicaNodeVect *nodePool);

    void supplementLatestNodes(int32_t count, ReplicaNodeVect *nodes);
    void supplementOldNodes(int32_t count, ReplicaNodeVect *nodes);
    void supplementNodesWithPlan(const version_t &version,
                                 const ExtVersionedPlan &extPlan, int32_t count,
                                 ReplicaNodeVect *nodes);
    void supplementReplicaNodes(const ReplicaNodeVect &nodes,
                                int32_t count, int32_t extraCount,
                                ReplicaNodeVect *supplementNodes,
                                ReplicaNodeVect *redundantNodes);
    void replaceNotAssignedNodes(ReplicaNodeVect *nodePool);
    void releaseReplicaNodes(const ReplicaNodeVect &nodes);
    void removeReleasedReplicaNodes();

    void holdExtraNodes(ReplicaNodeVect *nodePool);

    int32_t getHoldCount() const;
    
    void selectVersion(
            const version_t &latestVersion,
            const ExtVersionedPlanMap &versionedPlans,
            version_t *version, ExtVersionedPlan *plan);

private:
    ReplicaNodeCreatorPtr _replicaNodeCreatorPtr;
    ReplicaNodeVect _replicaNodes;
    GlobalPlan _globalPlan;
    ExtVersionedPlanMap _versionedPlans;
    version_t _latestVersion;
    ScheduleParams _scheduleParams;
    std::string _logPrefix;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(ReplicaNodesAdjuster);

END_CARBON_NAMESPACE(master);

#endif //CARBON_REPLICANODESADJUSTER_H
