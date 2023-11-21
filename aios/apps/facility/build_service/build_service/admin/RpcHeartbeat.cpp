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
#include "build_service/admin/RpcHeartbeat.h"

#include <arpc/ANetRPCController.h>
#include <arpc/CommonMacros.h>
#include <cstddef>
#include <functional>
#include <google/protobuf/stubs/port.h>
#include <stdint.h>
#include <vector>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "build_service/common/PathDefine.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/WorkerNode.h"
#include "worker_framework/ZkState.h"

using namespace std;
using namespace cm_basic;
using namespace autil;
using namespace google::protobuf;
using namespace build_service::proto;
using namespace build_service::common;
using namespace arpc;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, RpcHeartbeat);

RpcHeartbeat::RpcHeartbeat(const string& zkRoot, ZkWrapper* zkWrapper)
    : _zkRelativePath(worker_framework::ZkState::getZkRelativePath(zkRoot))
    , _leaderClient(zkWrapper)
{
}

RpcHeartbeat::~RpcHeartbeat()
{
    stop(); // stop call syncStatus
}

void RpcHeartbeat::syncStatus()
{
    vector<PartitionId> partIds = getAllPartIds();
    for (size_t i = 0; i < partIds.size(); ++i) {
        call(partIds[i]);
    }
}

arpc::LeaderClient::Identifier RpcHeartbeat::convertStringToIdentifier(const std::string& identifier)
{
    auto [ip, slotId] = WorkerNodeBase::getIpAndSlotId(identifier);
    return arpc::LeaderClient::Identifier(ip, slotId);
}

void RpcHeartbeat::call(const PartitionId& pid)
{
    string targetStateStr, requiredIdentifier;
    getTargetStatus(pid, targetStateStr, requiredIdentifier);
    TargetRequest targetRequest;
    string roleId;
    ProtoUtil::partitionIdToRoleId(pid, roleId);
    targetRequest.set_roleid(roleId);
    *targetRequest.mutable_partitionid() = pid;
    targetRequest.set_targetstate(targetStateStr);
    targetRequest.set_requiredidentifier(requiredIdentifier);
    auto& dependResources = getDependResource(pid);
    if (!dependResources.empty()) {
        *targetRequest.mutable_dependresourcesstr() = dependResources;
    }
    auto zkPath = constructZkPathFromPartitionId(pid);
    auto identifier = convertStringToIdentifier(requiredIdentifier);
    CALL_LEADER(_leaderClient, HeartbeatService_Stub, heartbeat, TargetRequest, CurrentResponse, targetRequest, zkPath,
                std::bind(&RpcHeartbeat::callback, this, std::placeholders::_1),
                /*requiredIdentifier = */ identifier);
}

// callback
void RpcHeartbeat::callback(arpc::OwnControllerClosure<TargetRequest, CurrentResponse>* closure)
{
    auto pid = closure->request.partitionid();
    const CurrentResponse& response = closure->response;
    if (!closure->controller.Failed() && response.has_currentstate() && ProtoUtil::checkHeartbeat(response, pid)) {
        // success
        int64_t workerStartTime = -1;
        if (closure->response.has_starttimestamp()) {
            workerStartTime = closure->response.starttimestamp();
        }
        setCurrentStatus(pid, closure->response.currentstate(), closure->response.identifier(), workerStartTime);
        setUsingResources(pid, closure->response.usingresourcesstr());
        return;
    }

    // set current status / timeout, reset connection
    if (closure->controller.Failed()) {
        BS_LOG(DEBUG, "rpc call failed, errorCode[%d], reason[%s]", closure->controller.GetErrorCode(),
               closure->controller.ErrorText().c_str());
    }
    auto zkPath = constructZkPathFromPartitionId(pid);
    _leaderClient.clearWorkerSpecCache(zkPath);
}

string RpcHeartbeat::constructZkPathFromPartitionId(const PartitionId& pid)
{
    bool ignoreBackupId = pid.role() == ROLE_PROCESSOR ? true : false;
    return PathDefine::getPartitionZkRoot(_zkRelativePath, pid, ignoreBackupId);
}

}} // namespace build_service::admin
