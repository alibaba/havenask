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
#include "swift/network/SwiftAdminClient.h"

#include <cstddef>
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/port.h>
#include <memory>

#include "arpc/ANetRPCChannel.h" // IWYU pragma: keep
#include "arpc/ANetRPCController.h"
#include "swift/protocol/Control.pb.h"
#include "swift/protocol/Heartbeat.pb.h"

using namespace std;
using namespace google::protobuf;
using namespace swift::protocol;

namespace swift {
namespace network {
AUTIL_LOG_SETUP(swift, SwiftAdminClient);

SwiftAdminClient::SwiftAdminClient(const std::string &zkPath,
                                   const SwiftRpcChannelManagerPtr &channelManager,
                                   int64_t timeout)
    : ArpcClient(zkPath, channelManager, "admin"), _timeout(timeout) {}

SwiftAdminClient::~SwiftAdminClient() {}

#define SWIFTCLIENT_HELPER(stubType, functionName, requestName, responseName)                                          \
    bool SwiftAdminClient::functionName(const requestName *request, responseName *response, int64_t timeout) {         \
        if (!checkRpcChannel()) {                                                                                      \
            return false;                                                                                              \
        }                                                                                                              \
        stubType stub(_rpcChannel.get(), Service::STUB_DOESNT_OWN_CHANNEL);                                            \
        arpc::ANetRPCController controller;                                                                            \
        controller.SetExpireTime(timeout);                                                                             \
        stub.functionName(&controller, request, response, NULL);                                                       \
        bool arpcError = controller.Failed();                                                                          \
        if (arpcError) {                                                                                               \
            AUTIL_LOG(WARN,                                                                                            \
                      "rpc call failed, address [%s], fail reason [%s]",                                               \
                      _address.c_str(),                                                                                \
                      controller.ErrorText().c_str());                                                                 \
        }                                                                                                              \
        return !arpcError;                                                                                             \
    }                                                                                                                  \
    bool SwiftAdminClient::functionName(const requestName *request, responseName *response) {                          \
        return functionName(request, response, _timeout);                                                              \
    }

#define SWIFTCLIENT_HEARTBEAT_HELPER(functionName, requestName, responseName)                                          \
    SWIFTCLIENT_HELPER(HeartbeatService_Stub, functionName, requestName, responseName)

#define SWIFTCLIENT_CONTROLLER_HELPER(functionName, requestName, responseName)                                         \
    SWIFTCLIENT_HELPER(Controller_Stub, functionName, requestName, responseName)

SWIFTCLIENT_CONTROLLER_HELPER(createTopic, TopicCreationRequest, TopicCreationResponse);
SWIFTCLIENT_CONTROLLER_HELPER(modifyTopic, TopicCreationRequest, TopicCreationResponse);
SWIFTCLIENT_CONTROLLER_HELPER(deleteTopic, TopicDeletionRequest, TopicDeletionResponse);
SWIFTCLIENT_CONTROLLER_HELPER(getSysInfo, EmptyRequest, SysInfoResponse);
SWIFTCLIENT_CONTROLLER_HELPER(getTopicInfo, TopicInfoRequest, TopicInfoResponse);
SWIFTCLIENT_CONTROLLER_HELPER(getAllTopicName, EmptyRequest, TopicNameResponse);
SWIFTCLIENT_CONTROLLER_HELPER(getAllTopicInfo, EmptyRequest, AllTopicInfoResponse);
SWIFTCLIENT_CONTROLLER_HELPER(getPartitionInfo, PartitionInfoRequest, PartitionInfoResponse);
SWIFTCLIENT_CONTROLLER_HELPER(getRoleAddress, RoleAddressRequest, RoleAddressResponse);
SWIFTCLIENT_CONTROLLER_HELPER(updateBrokerConfig, UpdateBrokerConfigRequest, UpdateBrokerConfigResponse);
SWIFTCLIENT_CONTROLLER_HELPER(rollbackBrokerConfig, RollbackBrokerConfigRequest, RollbackBrokerConfigResponse);
SWIFTCLIENT_CONTROLLER_HELPER(registerSchema, RegisterSchemaRequest, RegisterSchemaResponse);
SWIFTCLIENT_CONTROLLER_HELPER(getSchema, GetSchemaRequest, GetSchemaResponse);
SWIFTCLIENT_CONTROLLER_HELPER(createTopicBatch, TopicBatchCreationRequest, TopicBatchCreationResponse);
SWIFTCLIENT_CONTROLLER_HELPER(deleteTopicBatch, TopicBatchDeletionRequest, TopicBatchDeletionResponse);
SWIFTCLIENT_CONTROLLER_HELPER(getBrokerStatus, GetBrokerStatusRequest, GetBrokerStatusResponse);
SWIFTCLIENT_CONTROLLER_HELPER(getTopicRWTime, GetTopicRWTimeRequest, GetTopicRWTimeResponse);
SWIFTCLIENT_CONTROLLER_HELPER(reportMissTopic, MissTopicRequest, MissTopicResponse);
SWIFTCLIENT_CONTROLLER_HELPER(updateWriterVersion, UpdateWriterVersionRequest, UpdateWriterVersionResponse);
SWIFTCLIENT_CONTROLLER_HELPER(getMasterInfo, EmptyRequest, MasterInfoResponse);
SWIFTCLIENT_CONTROLLER_HELPER(topicAclManage, TopicAclRequest, TopicAclResponse);

SWIFTCLIENT_HEARTBEAT_HELPER(reportBrokerStatus, BrokerStatusRequest, BrokerStatusResponse);

#undef SWIFTCLIENT_HELPER

} // namespace network
} // namespace swift
