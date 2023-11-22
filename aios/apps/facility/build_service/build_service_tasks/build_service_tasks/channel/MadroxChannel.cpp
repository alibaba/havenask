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
#include "build_service_tasks/channel/MadroxChannel.h"

#include <arpc/ANetRPCController.h>
#include <google/protobuf/service.h>
#include <memory>
#include <stddef.h>

#include "alog/Logger.h"
#include "build_service/util/Log.h"
#include "build_service_tasks/channel/Master.pb.h"

namespace build_service { namespace task_base {

BS_LOG_SETUP(task_base, MadroxChannel);

bool MadroxChannel::GetStatus(const ::madrox::proto::GetStatusRequest& request,
                              ::madrox::proto::GetStatusResponse& response)
{
    // get rpc channel
    auto rpcChannel = getRpcChannelWithRetry();
    if (!rpcChannel) {
        BS_LOG(ERROR, "get rpc channle failed when getStatus to madrox, addr:[%s]", _hostAddr.c_str());
        return false;
    }
    arpc::ANetRPCController controller;
    ::madrox::proto::MasterService_Stub stub(rpcChannel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
    controller.SetExpireTime(RPC_TIMEOUT_MS);
    stub.getStatus(&controller, &request, &response, NULL);

    if (controller.Failed()) {
        BS_LOG(ERROR, "rpc getStatus[%s] to madrox failed, addr:[%s] ec:[%d] error:[%s]",
               request.ShortDebugString().c_str(), _hostAddr.c_str(), controller.GetErrorCode(),
               controller.ErrorText().c_str());
        return false;
    }
    return true;
}

bool MadroxChannel::UpdateRequest(const ::madrox::proto::UpdateRequest& request,
                                  ::madrox::proto::UpdateResponse& response)
{
    // get rpc channel
    auto rpcChannel = getRpcChannelWithRetry();
    if (!rpcChannel) {
        BS_LOG(ERROR, "get rpc channle failed when update, addr:[%s]", _hostAddr.c_str());
        return false;
    }
    arpc::ANetRPCController controller;
    ::madrox::proto::MasterService_Stub stub(rpcChannel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
    controller.SetExpireTime(RPC_TIMEOUT_MS);
    stub.update(&controller, &request, &response, NULL);

    if (controller.Failed()) {
        BS_LOG(ERROR, "rpc update failed, addr:[%s] ec:[%d] error:[%s]", _hostAddr.c_str(), controller.GetErrorCode(),
               controller.ErrorText().c_str());
        return false;
    }

    return true;
}

}} // namespace build_service::task_base
