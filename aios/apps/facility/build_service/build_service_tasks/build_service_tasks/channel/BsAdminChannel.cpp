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
#include "build_service_tasks/channel/BsAdminChannel.h"

#include "build_service/util/Log.h"

namespace build_service { namespace task_base {

BS_LOG_SETUP(task_base, BsAdminChannel);

bool BsAdminChannel::GetServiceInfo(const build_service::proto::ServiceInfoRequest& request,
                                    build_service::proto::ServiceInfoResponse& response)
{
    // get rpc channel
    auto rpcChannel = getRpcChannelWithRetry();
    if (!rpcChannel) {
        BS_LOG(ERROR, "get rpc channle failed when getServiceInfo, addr:[%s]", _hostAddr.c_str());
        return false;
    }
    arpc::ANetRPCController controller;
    proto::AdminService_Stub stub(rpcChannel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
    controller.SetExpireTime(RPC_TIMEOUT_MS);
    stub.getServiceInfo(&controller, &request, &response, NULL);

    if (controller.Failed()) {
        BS_LOG(ERROR, "rpc getServiceInfo failed, addr:[%s] ec:[%d] error:[%s]", _hostAddr.c_str(),
               controller.GetErrorCode(), controller.ErrorText().c_str());
        return false;
    }

    return true;
}

bool BsAdminChannel::CreateSnapshot(const build_service::proto::CreateSnapshotRequest& request,
                                    build_service::proto::InformResponse& response)
{
    // get rpc channel
    auto rpcChannel = getRpcChannelWithRetry();
    if (!rpcChannel) {
        BS_LOG(ERROR, "get rpc channle failed when createSnapshot, addr:[%s]", _hostAddr.c_str());
        return false;
    }
    arpc::ANetRPCController controller;
    proto::AdminService_Stub stub(rpcChannel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
    controller.SetExpireTime(RPC_TIMEOUT_MS);
    stub.createSnapshot(&controller, &request, &response, NULL);

    if (controller.Failed()) {
        BS_LOG(ERROR, "rpc createSnapshot failed, addr:[%s] ec:[%d] error:[%s]", _hostAddr.c_str(),
               controller.GetErrorCode(), controller.ErrorText().c_str());
        return false;
    }

    return true;
}

bool BsAdminChannel::RemoveSnapshot(const build_service::proto::RemoveSnapshotRequest& request,
                                    build_service::proto::InformResponse& response)
{
    // get rpc channel
    auto rpcChannel = getRpcChannelWithRetry();
    if (!rpcChannel) {
        BS_LOG(ERROR, "get rpc channle failed when removeSnapshot, addr:[%s]", _hostAddr.c_str());
        return false;
    }
    arpc::ANetRPCController controller;
    proto::AdminService_Stub stub(rpcChannel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
    controller.SetExpireTime(RPC_TIMEOUT_MS);
    stub.removeSnapshot(&controller, &request, &response, NULL);

    if (controller.Failed()) {
        BS_LOG(ERROR, "rpc removeSnapshot failed, addr:[%s] ec:[%d] error:[%s]", _hostAddr.c_str(),
               controller.GetErrorCode(), controller.ErrorText().c_str());
        return false;
    }

    return true;
}

}} // namespace build_service::task_base
