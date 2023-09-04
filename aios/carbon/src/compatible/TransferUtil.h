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
#ifndef CARBON_TRANSFERUTIL_H
#define CARBON_TRANSFERUTIL_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/GroupPlan.h"
#include "carbon/RolePlan.h"
#include "carbon/legacy/RoleManagerWrapperInternal.h"
#include "carbon/proto/Carbon.pb.h"
#include "carbon/Status.h"

BEGIN_CARBON_NAMESPACE(compatible);

class TransferUtil
{
public:
    TransferUtil();
    ~TransferUtil();
private:
    TransferUtil(const TransferUtil &);
    TransferUtil& operator=(const TransferUtil &);
public:
    static void trasferGroupDescToGroupPlan(
            const GroupDesc &groupDesc,
            GroupPlan *groupPlan);
    
    static void transferRoleDescToRolePlan(
            const proto::RoleDescription &roleDesc,
            const std::string &skylineGroup,
            const std::string &skylineAppStage,
            RolePlan *rolePlan);

    static void transferHealthCheckerConfig(
            const proto::RoleDescription &roleDesc,
            HealthCheckerConfig *healthCheckerConfig);

    static void transferServiceConfigs(
            const ServiceVec &serviceVec,
            std::map<std::string, std::vector<ServiceConfig> > *serviceConfigsMap,
            std::string &skylineGroup,
            std::string &skylineAppStage);
    
    static std::map<std::string, std::string> transferKVs(
            const ::google::protobuf::RepeatedPtrField<proto::KVPair> &kvpairs);

    static void transferGroupStatus(const GroupStatus &groupStatus,
                                    GroupStatusWrapper *groupStatusWrapper);

    static void transferRoleStatus(const RoleStatus &roleStatus,
                                   proto::RoleStatus *roleStatusProto);

    static void transferReplicaNodeStatus(
            const std::vector<WorkerNodeStatus> &workerNodeStatusVect,
            proto::RoleInstanceInfo *roleInstanceInfo);

    static void transferReplicaNodeStatus(
            const WorkerNodeStatus &workerNodeStatus,
            proto::ReplicaNode *replicaNodeProto);

    static proto::ReplicaNode::SlotStatus transferSlotStatus(const SlotInfo &slotInfo);

    static proto::ReplicaNode::HealthStatus transferHealthStatus(const HealthType &healthStatus);

    static proto::ReplicaNode::ServiceStatus transferServiceStatus(const ServiceType &serviceType);

    static proto::ReplicaNode::WorkerStatus transferWorkerStatus(const WorkerType &workerType);

protected:
    static void transferConstraintConfig(
            const proto::ConstraintConfig& pbConstraintConfig,
            ConstraintConfig& constraintConfig);

private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(TransferUtil);

END_CARBON_NAMESPACE(compatible);

#endif //CARBON_TRANSFERUTIL_H
