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
#include "swift/client/SwiftTransportClient.h"

#include <cstddef>
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/port.h>
#include <memory>

#include "arpc/ANetRPCChannel.h" // IWYU pragma: keep
#include "arpc/ANetRPCController.h"
#include "arpc/CommonMacros.h"
#include "arpc/proto/rpc_extensions.pb.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Transport.pb.h"

using namespace std;
using namespace arpc;
using namespace google::protobuf;
using namespace swift::protocol;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftTransportClient);

SwiftTransportClient::SwiftTransportClient(const std::string &address,
                                           const network::SwiftRpcChannelManagerPtr &channelManager,
                                           const string &idStr)
    : ArpcClient(address, channelManager, idStr) {}

SwiftTransportClient::~SwiftTransportClient() {}

#define SWIFTCLIENT_HELPER(functionName, requestName, responseName)                                                    \
    bool SwiftTransportClient::functionName(const requestName *request, responseName *response, int64_t timeout) {     \
        if (!checkRpcChannel()) {                                                                                      \
            return false;                                                                                              \
        }                                                                                                              \
        Transporter_Stub stub(_rpcChannel.get(), Service::STUB_DOESNT_OWN_CHANNEL);                                    \
        arpc::ANetRPCController controller;                                                                            \
        controller.SetExpireTime(timeout);                                                                             \
        stub.functionName(&controller, request, response, NULL);                                                       \
        bool arpcError = controller.Failed();                                                                          \
        if (arpcError) {                                                                                               \
            AUTIL_LOG(WARN,                                                                                            \
                      "[%s] rpc call failed, address [%s], fail reason [%s]",                                          \
                      _idStr.c_str(),                                                                                  \
                      _address.c_str(),                                                                                \
                      controller.ErrorText().c_str());                                                                 \
            auto ec = arpc::ARPC_ERROR_PUSH_WORKITEM == controller.GetErrorCode()                                      \
                          ? protocol::ERROR_BROKER_PUSH_WORK_ITEM                                                      \
                          : protocol::ERROR_CLIENT_ARPC_ERROR;                                                         \
            if (controller.GetErrorCode() == arpc::ARPC_ERROR_TIMEOUT) {                                               \
                if (_channelManager) {                                                                                 \
                    _channelManager->channelTimeout(getAddress());                                                     \
                }                                                                                                      \
            }                                                                                                          \
            response->mutable_errorinfo()->set_errcode(ec);                                                            \
            *response->mutable_errorinfo()->mutable_errmsg() = controller.ErrorText();                                 \
        }                                                                                                              \
        return !arpcError;                                                                                             \
    }

SWIFTCLIENT_HELPER(getMessage, ConsumptionRequest, MessageResponse);
SWIFTCLIENT_HELPER(sendMessage, ProductionRequest, MessageResponse);
SWIFTCLIENT_HELPER(getMaxMessageId, MessageIdRequest, MessageIdResponse);
SWIFTCLIENT_HELPER(getMinMessageIdByTime, MessageIdRequest, MessageIdResponse);

#define SWIFTCLIENT_ASYNC_HELPER(functionName, EnumType)                                                               \
    void SwiftTransportClient::asyncCall(TransportClosureTyped<EnumType> *closure) {                                   \
        closure->setRemoteAddress(_address);                                                                           \
        arpc::ANetRPCController *controller = closure->getController();                                                \
        if (!checkRpcChannel()) {                                                                                      \
            controller->SetFailed("create rpc channel failed!");                                                       \
            closure->Run();                                                                                            \
            return;                                                                                                    \
        }                                                                                                              \
        Transporter_Stub stub(_rpcChannel.get(), Service::STUB_DOESNT_OWN_CHANNEL);                                    \
        stub.functionName(controller, closure->getRequest(), closure->getResponse(), closure);                         \
    }

SWIFTCLIENT_ASYNC_HELPER(getMessage, TRT_GETMESSAGE);
SWIFTCLIENT_ASYNC_HELPER(sendMessage, TRT_SENDMESSAGE);
SWIFTCLIENT_ASYNC_HELPER(getMaxMessageId, TRT_GETMAXMESSAGEID);
SWIFTCLIENT_ASYNC_HELPER(getMinMessageIdByTime, TRT_GETMINMESSAGEIDBYTIME);

} // namespace client
} // namespace swift
