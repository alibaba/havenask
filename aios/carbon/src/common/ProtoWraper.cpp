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
#include "carbon/ProtoWrapper.h"
#include "hippo/ProtoWrapper.h"

using namespace std;

namespace carbon {

void ProtoWrapper::convert(const proto::GroupStatus &in,
                        carbon::GroupStatus *out)
{
    convert(in.groupid(), &(out->groupId));
    convert(in.roles(), &(out->roles));
}


void ProtoWrapper::convert(const proto::RoleStatusValue &in,
                        carbon::RoleStatus *out)
{
    convert(in.roleid(), &(out->roleId));
    convert(in.latestversion(), &(out->latestVersion));
    convert(in.nodes(), &(out->nodes));
    convert(in.userdefversion(), &(out->userDefVersion));
    convert(in.readyforcurversion(), &(out->readyForCurVersion));
    convert(in.globalplan(), &(out->globalPlan));
    convert(in.versionedplans(), &(out->versionedPlans));
    out->adjustedCount = in.adjustedcount();
    out->adjustedMinHealthCapacity = in.adjustedminhealthcapacity();
}

void ProtoWrapper::convert(const proto::VersionedPlan &in,
                        carbon::VersionedPlan *out){
    convert(in.resourceplan(), &(out->resourcePlan));
}

void ProtoWrapper::convert(const proto::ResourcePlan &in,
                        carbon::ResourcePlan *out){
    out->group = in.group();
    convert(in.resources(), &(out->resources));
}

void ProtoWrapper::convert(const proto::SlotResource &in,
                        hippo::SlotResource *out){
    hippo::ProtoWrapper::convert(in.slotresources(), &(out->resources));
}

void ProtoWrapper::convert(const proto::GlobalPlan &in,
                        carbon::GlobalPlan *out)
{
    out->count = int32_t(in.count());
    out->latestVersionRatio = int32_t(in.latestversionratio());
}

void ProtoWrapper::convert(const proto::ReplicaNodeStatus &in,
                        carbon::ReplicaNodeStatus *out)
{
    convert(in.replicanodeid(), &(out->replicaNodeId));
    convert(in.curworkernodestatus(), &(out->curWorkerNodeStatus));
    convert(in.backupworkernodestatus(), &(out->backupWorkerNodeStatus));
    convert(in.timestamp(), &(out->timeStamp));
    convert(in.userdefversion(), &(out->userDefVersion));
    convert(in.readyforcurversion(), &(out->readyForCurVersion));
}


void ProtoWrapper::convert(const proto::WorkerNodeStatus &in,
                        carbon::WorkerNodeStatus *out)
{
    convert(in.replicanodeid(), &(out->replicaNodeId));
    convert(in.workernodeid(), &(out->workerNodeId));
    convert(in.curversion(), &(out->curVersion));
    convert(in.nextversion(), &(out->nextVersion));
    convert(in.finalversion(), &(out->finalVersion));
    convert(in.offline(), &(out->offline));
    convert(in.releasing(), &(out->releasing));
    convert(in.reclaiming(), &(out->reclaiming));
    convert(in.readyforcurversion(), &(out->readyForCurVersion));
    convert(in.isbackup(), &(out->isBackup));
    convert(in.lastnotmatchtime(), &(out->lastNotMatchTime));
    convert(in.lastnotreadytime(), &(out->lastNotReadyTime));
    out->slotAllocStatus = (carbon::SlotAllocStatus)in.slotallocstatus();
    convert(in.slotinfo(), &(out->slotInfo));
    convert(in.healthinfo(), &(out->healthInfo));
    convert(in.serviceinfo(), &(out->serviceInfo));
    convert(in.slotstatus(), &(out->slotStatus));
    convert(in.ip(), &(out->ip));
    convert(in.userdefversion(), &(out->userDefVersion));
    convert(in.targetsignature(), &(out->targetSignature));
    convert(in.targetcustominfo(), &(out->targetCustomInfo));
    out->workerMode = in.workermode();
    convert(in.labels(),&(out->labels));
}

// convert proto message to hippo struct
void ProtoWrapper::convert(const proto::SlotInfo &in,
                           hippo::SlotInfo *out)
{
    out->role = in.role();
    out->reclaiming = in.reclaiming();
    out->launchSignature = in.launchsignature();
    out->packageChecksum = in.packagechecksum();
    out->preDeployPackageChecksum = in.predeploypackagechecksum();
    out->noLongerMatchQueue = in.nolongermatchqueue();
    out->noLongerMatchResourceRequirement =
        in.nolongermatchresourcerequirement();
    out->requirementId =
        in.requirementid();
    hippo::ProtoWrapper::convert(in.slotid(), &(out->slotId));
    hippo::ProtoWrapper::convert(in.slavestatus(), &(out->slaveStatus));
    hippo::ProtoWrapper::convert(in.slotresource().slotresources(), &(out->slotResource.resources));
    hippo::ProtoWrapper::convert(in.processstatus(), &(out->processStatus));
    hippo::ProtoWrapper::convert(in.packagestatus(), &(out->packageStatus));
    hippo::ProtoWrapper::convert(in.predeploypackagestatus(), &(out->preDeployPackageStatus));
}

void ProtoWrapper::convert(const proto::SlotStatus &in, carbon::SlotStatus *out)
{
    out->status = (carbon::SlotType)in.status();
    hippo::ProtoWrapper::convert(in.slotid(), &(out->slotId));
}

void ProtoWrapper::convert(const proto::ServiceStatus &in, carbon::ServiceInfo *out)
{
    out->status = (carbon::ServiceType)in.status();
    convert(in.metas(), &(out->metas));
}

void ProtoWrapper::convert(const proto::HealthInfo &in, carbon::HealthInfo *out)
{
    out->healthStatus = (carbon::HealthType)in.healthstatus();
    out->workerStatus = (carbon::WorkerType)in.workerstatus();
    convert(in.metas(), &(out->metas));
    convert(in.version(), &(out->version));
}

};