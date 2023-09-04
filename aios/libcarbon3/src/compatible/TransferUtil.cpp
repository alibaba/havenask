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
#include "compatible/TransferUtil.h"
#include "carbon/Log.h"
#include "master/Util.h"
#include "hippo/ProtoWrapper.h"
#include "common/SlotUtil.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
USE_CARBON_NAMESPACE(master);
USE_CARBON_NAMESPACE(common);
BEGIN_CARBON_NAMESPACE(compatible);

CARBON_LOG_SETUP(compatible, TransferUtil);

TransferUtil::TransferUtil() {
}

TransferUtil::~TransferUtil() {
}

void TransferUtil::trasferGroupDescToGroupPlan(
        const GroupDesc &groupDesc,
        GroupPlan *groupPlan)
{
    map<string, vector<ServiceConfig> > serviceConfigsMap;
    transferServiceConfigs(groupDesc.services, &serviceConfigsMap);

    int32_t minHealthCapacity = 0;
    for (size_t i = 0; i < groupDesc.roles.size(); i++) {
        const proto::RoleDescription &roleDesc = groupDesc.roles[i];
        minHealthCapacity = max(minHealthCapacity, roleDesc.minhealthcapacity());
        RolePlan rolePlan;
        transferRoleDescToRolePlan(roleDesc, &rolePlan);
        if (serviceConfigsMap.find(roleDesc.rolename()) != serviceConfigsMap.end()) {
            rolePlan.global.serviceConfigs = serviceConfigsMap[roleDesc.rolename()];
        }
        groupPlan->rolePlans[roleDesc.rolename()] = rolePlan;
    }
    groupPlan->minHealthCapacity = minHealthCapacity;
    groupPlan->extraRatio = groupDesc.stepPercent;
}

void TransferUtil::transferRoleDescToRolePlan(
        const proto::RoleDescription &roleDesc,
        RolePlan *rolePlan)
{
    rolePlan->roleId = roleDesc.rolename();
    GlobalPlan &global = rolePlan->global;
    global.count = roleDesc.count();
    global.latestVersionRatio = roleDesc.latestversionratio();
    transferHealthCheckerConfig(roleDesc, &global.healthCheckerConfig);

    VersionedPlan &versionedPlan = rolePlan->version;
    versionedPlan.online = roleDesc.online();
    versionedPlan.updatingGracefully = roleDesc.updatinggracefully();
    versionedPlan.restartAfterResourceChange = roleDesc.restartafterresourcechange();
    versionedPlan.signature = roleDesc.signature();
    versionedPlan.customInfo = roleDesc.custominfo();
    versionedPlan.userDefVersion = roleDesc.userdefversion();
    versionedPlan.notMatchTimeout = roleDesc.notmatchtimeout();
    versionedPlan.notReadyTimeout = roleDesc.notreadytimeout();
    versionedPlan.preload = roleDesc.preload();
    hippo::ProtoWrapper::convert(roleDesc.packageinfos(),
                                 &versionedPlan.launchPlan.packageInfos);
    hippo::ProtoWrapper::convert(roleDesc.processinfos(),
                                 &versionedPlan.launchPlan.processInfos);
    hippo::ProtoWrapper::convert(roleDesc.datainfos(),
                                 &versionedPlan.launchPlan.dataInfos);

    ResourcePlan &resourcePlan = rolePlan->version.resourcePlan;
    hippo::ProtoWrapper::convert(roleDesc.resources(), &resourcePlan.resources);
    hippo::ProtoWrapper::convert(roleDesc.declarations(), &resourcePlan.declarations);
    hippo::ProtoWrapper::convert(roleDesc.allocatemode(), &resourcePlan.allocateMode);
    if (roleDesc.has_cpusetmode()) {
        resourcePlan.cpusetMode = hippo::CpusetMode(roleDesc.cpusetmode());
    }
    for (auto i = 0; i < roleDesc.containerconfigs_size(); i++) {
        resourcePlan.containerConfigs.push_back(roleDesc.containerconfigs(i));
    }
    hippo::ProtoWrapper::convert(roleDesc.priority(), &resourcePlan.priority);
    resourcePlan.queue = roleDesc.queue();
    resourcePlan.group = roleDesc.groupid();
}

void TransferUtil::transferHealthCheckerConfig(
        const proto::RoleDescription &roleDesc,
        HealthCheckerConfig *healthCheckerConfig)
{
    const proto::HealthCheckerConfig &protoConfig = roleDesc.healthcheckerconfig();
    healthCheckerConfig->name = roleDesc.healthcheckerconfig().name();
    for (int32_t i = 0; i < protoConfig.args_size(); i++) {
        healthCheckerConfig->args[protoConfig.args(i).key()] = protoConfig.args(i).value();
    }
}

void TransferUtil::transferServiceConfigs(
        const ServiceVec &serviceVec,
        map<string, vector<ServiceConfig> > *serviceConfigsMap)
{
    for (size_t i = 0; i < serviceVec.size(); i++) {
        const proto::PublishServiceRequest &serviceRequest = serviceVec[i];
        const string &roleName = serviceRequest.rolename();
        for (int32_t i = 0; i < serviceRequest.serviceconfigs_size(); i++) {
            const proto::ServiceConfig &protoConfig = serviceRequest.serviceconfigs(i);
            ServiceConfig serviceConfig;
            serviceConfig.name = protoConfig.name();
            serviceConfig.type = (ServiceAdapterType)((int32_t)(protoConfig.type()));
            serviceConfig.configStr = protoConfig.configstr();
            serviceConfig.metaStr = protoConfig.metastr();
            serviceConfig.masked = protoConfig.masked();
            serviceConfig.deleteDelay = protoConfig.deletedelay();
            (*serviceConfigsMap)[roleName].push_back(serviceConfig);
        }
    }
}

void TransferUtil::transferGroupStatus(const GroupStatus &groupStatus,
                                       GroupStatusWrapper *groupStatusWrapper)
{
    const map<roleid_t, RoleStatus> &roleStatusMap = groupStatus.roles;
    for (map<roleid_t, RoleStatus>::const_iterator it = roleStatusMap.begin();
         it != roleStatusMap.end(); it++)
    {
        const roleid_t &roleId = it->first;
        const RoleStatus &roleStatus = it->second;
        proto::RoleStatus &roleStatusProto = groupStatusWrapper->roleStatuses[roleId];
        transferRoleStatus(roleStatus, &roleStatusProto);
    }
}

void TransferUtil::transferRoleStatus(const RoleStatus &roleStatus,
                                      proto::RoleStatus *roleStatusProto)
{
    roleStatusProto->set_status(proto::RoleStatus::RS_RUNNING);
    roleStatusProto->set_rolename(roleStatus.roleId);
    roleStatusProto->set_userdefversion(roleStatus.userDefVersion);
    roleStatusProto->set_readyforcurversion(roleStatus.readyForCurVersion);

    version_t latestVersion = roleStatus.latestVersion;
    vector<WorkerNodeStatus> latestWorkerNodes;
    vector<WorkerNodeStatus> curWorkerNodes;

    for (const auto &replicaNodeStatus : roleStatus.nodes) {
        const auto &workerNodeStatusSet = replicaNodeStatus.getValidWorkerNodeStatus();
        for (const auto &workerNodeStatus : workerNodeStatusSet) {
            if (workerNodeStatus.nextVersion == latestVersion) {
                latestWorkerNodes.push_back(workerNodeStatus);
            } else {
                curWorkerNodes.push_back(workerNodeStatus);
            }
        }
    }

    proto::RoleInstanceInfo *curRoleInstanceInfo =
        roleStatusProto->mutable_curinstanceinfo();
    proto::RoleInstanceInfo *nextRoleInstanceInfo =
        roleStatusProto->mutable_nextinstanceinfo();

    transferReplicaNodeStatus(latestWorkerNodes, nextRoleInstanceInfo);
    transferReplicaNodeStatus(curWorkerNodes, curRoleInstanceInfo);
}

void TransferUtil::transferReplicaNodeStatus(
        const vector<WorkerNodeStatus> &workerNodeStatusVect,
        proto::RoleInstanceInfo *roleInstanceInfo)
{
    for (const auto &workerNodeStatus : workerNodeStatusVect) {
        proto::ReplicaNode *replicaNodeProto = roleInstanceInfo->add_replicanodes();
        transferReplicaNodeStatus(workerNodeStatus, replicaNodeProto);
    }
}

void TransferUtil::transferReplicaNodeStatus(
        const WorkerNodeStatus &workerNodeStatus,
        proto::ReplicaNode *replicaNodeProto)
{
    const SlotId &slotId = workerNodeStatus.slotInfo.slotId;
    hippo::proto::SlotId *slotIdProto = replicaNodeProto->mutable_slotid();
    slotIdProto->set_slaveaddress(slotId.slaveAddress);
    slotIdProto->set_id(slotId.id);
    slotIdProto->set_declaretime(slotId.declareTime);

    replicaNodeProto->set_ip(workerNodeStatus.ip);
    replicaNodeProto->set_isofflined(workerNodeStatus.offline);
    replicaNodeProto->set_isreclaiming(workerNodeStatus.reclaiming);
    replicaNodeProto->set_signature(
            Util::getValue(workerNodeStatus.healthInfo.metas,
                           HEALTHCHECK_PAYLOAD_SIGNATURE));
    replicaNodeProto->set_custominfo(
            Util::getValue(workerNodeStatus.healthInfo.metas,
                           HEALTHCHECK_PAYLOAD_CUSTOMINFO));
    replicaNodeProto->set_serviceinfo(
            Util::getValue(workerNodeStatus.healthInfo.metas,
                           HEALTHCHECK_PAYLOAD_SERVICEINFO));
    replicaNodeProto->set_slotstatus(transferSlotStatus(workerNodeStatus.slotInfo));
    replicaNodeProto->set_healthstatus(transferHealthStatus(workerNodeStatus.healthInfo.healthStatus));
    replicaNodeProto->set_workerstatus(transferWorkerStatus(workerNodeStatus.healthInfo.workerStatus));
    replicaNodeProto->set_servicestatus(transferServiceStatus(workerNodeStatus.serviceInfo.status));
    replicaNodeProto->set_targetsignature(workerNodeStatus.targetSignature);
    replicaNodeProto->set_targetcustominfo(workerNodeStatus.targetCustomInfo);
    replicaNodeProto->set_userdefversion(workerNodeStatus.userDefVersion);
    replicaNodeProto->set_readyforcurversion(workerNodeStatus.readyForCurVersion);
    SlotInfoJsonizeWrapper slotInfoWrapper(workerNodeStatus.slotInfo);
    replicaNodeProto->set_slotinfostr(
            autil::legacy::ToJsonString(slotInfoWrapper));
    for (const auto &ps : workerNodeStatus.slotInfo.processStatus) {
        auto processStatus = replicaNodeProto->add_processstatus();
        processStatus->set_isdaemon(ps.isDaemon);
        processStatus->set_status(hippo::proto::ProcessStatus::Status(ps.status));
        processStatus->set_processname(ps.processName);
        processStatus->set_restartcount(ps.restartCount);
        processStatus->set_starttime(ps.startTime);
        processStatus->set_pid(ps.pid);
        processStatus->set_instanceid(ps.instanceId);
    }
    replicaNodeProto->set_isbackup(workerNodeStatus.isBackup);
    replicaNodeProto->set_replicanodeid(workerNodeStatus.replicaNodeId);
}

proto::ReplicaNode::SlotStatus TransferUtil::transferSlotStatus(const SlotInfo &slotInfo) {
    proto::ReplicaNode::SlotStatus slotStatus = proto::ReplicaNode::ST_UNKNOWN;

    if (slotInfo.slaveStatus.status == SlaveStatus::DEAD) {
        slotStatus = proto::ReplicaNode::ST_DEAD;
    } else if (slotInfo.packageStatus.status ==
               PackageStatus::IS_FAILED)
    {
        slotStatus = proto::ReplicaNode::ST_PKG_FAILED;
        return slotStatus;
    } else if (SlotUtil::hasFailedProcess(slotInfo)) {
        slotStatus = proto::ReplicaNode::ST_PROC_FAILED;
    } else if (SlotUtil::isAllProcessRunning(slotInfo)) {
        slotStatus = proto::ReplicaNode::ST_PROC_RUNNING;
    } else if (SlotUtil::hasRestartingProcess(slotInfo)) {
        slotStatus = proto::ReplicaNode::ST_PROC_RESTARTING;
    } else {
        slotStatus = proto::ReplicaNode::ST_UNKNOWN;
    }

    return slotStatus;
}

proto::ReplicaNode::HealthStatus TransferUtil::transferHealthStatus(const HealthType &healthStatus) {
    if (healthStatus  == HT_UNKNOWN) {
        return proto::ReplicaNode::HT_UNKNOWN;
    } else if (healthStatus == HT_ALIVE) {
        return proto::ReplicaNode::HT_ALIVE;
    } else if (healthStatus == HT_DEAD) {
        return proto::ReplicaNode::HT_DEAD;
    }
    return proto::ReplicaNode::HT_UNKNOWN;
}

proto::ReplicaNode::ServiceStatus TransferUtil::transferServiceStatus(const ServiceType &serviceType) {
    if (serviceType == SVT_UNKNOWN) {
        return proto::ReplicaNode::SVT_UNKNOWN;
    } else if (serviceType == SVT_UNAVAILABLE) {
        return proto::ReplicaNode::SVT_UNAVAILABLE;
    } else if (serviceType == SVT_AVAILABLE) {
        return proto::ReplicaNode::SVT_AVAILABLE;
    } else if (serviceType == SVT_PART_AVAILABLE) {
        return proto::ReplicaNode::SVT_PART_AVAILABLE;
    }
    return proto::ReplicaNode::SVT_UNKNOWN;
}

proto::ReplicaNode::WorkerStatus TransferUtil::transferWorkerStatus(const WorkerType &workerType) {
    if (workerType == WT_UNKNOWN) {
        return proto::ReplicaNode::WS_UNKNOWN;
    } else if (workerType == WT_NOT_READY) {
        return proto::ReplicaNode::WS_NOT_READY;
    } else if (workerType == WT_READY) {
        return proto::ReplicaNode::WS_READY;
    }
    return proto::ReplicaNode::WS_UNKNOWN;
}

END_CARBON_NAMESPACE(compatible);
