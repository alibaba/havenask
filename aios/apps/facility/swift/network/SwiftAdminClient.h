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
#pragma once

#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/network/ArpcClient.h"
#include "swift/network/SwiftRpcChannelManager.h"

namespace swift {
namespace protocol {
class AllTopicInfoResponse;
class BrokerStatusRequest;
class BrokerStatusResponse;
class EmptyRequest;
class GetBrokerStatusRequest;
class GetBrokerStatusResponse;
class GetSchemaRequest;
class GetSchemaResponse;
class GetTopicRWTimeRequest;
class GetTopicRWTimeResponse;
class PartitionInfoRequest;
class PartitionInfoResponse;
class RegisterSchemaRequest;
class RegisterSchemaResponse;
class RoleAddressRequest;
class RoleAddressResponse;
class RollbackBrokerConfigRequest;
class RollbackBrokerConfigResponse;
class SysInfoResponse;
class TopicBatchCreationRequest;
class TopicBatchCreationResponse;
class TopicBatchDeletionRequest;
class TopicBatchDeletionResponse;
class TopicCreationRequest;
class TopicCreationResponse;
class TopicDeletionRequest;
class TopicDeletionResponse;
class TopicInfoRequest;
class TopicInfoResponse;
class TopicNameResponse;
class UpdateBrokerConfigRequest;
class UpdateBrokerConfigResponse;
class MissTopicRequest;
class MissTopicResponse;
class UpdateWriterVersionRequest;
class UpdateWriterVersionResponse;
class MasterInfoResponse;
class TopicAclRequest;
class TopicAclResponse;
} // namespace protocol

namespace network {

class SwiftAdminClient : public ArpcClient {
public:
    SwiftAdminClient(const std::string &zkPath,
                     const SwiftRpcChannelManagerPtr &channelManager,
                     int64_t timeout = 3 * 1000);

    virtual ~SwiftAdminClient();

private:
    SwiftAdminClient(const SwiftAdminClient &);
    SwiftAdminClient &operator=(const SwiftAdminClient &);

public:
#define SWIFTCLIENT_DECL_HELPER(method, requestType, responseType)                                                     \
    virtual bool method(const protocol::requestType *request, protocol::responseType *response, int64_t timeout);      \
    virtual bool method(const protocol::requestType *request, protocol::responseType *response);

    SWIFTCLIENT_DECL_HELPER(createTopic, TopicCreationRequest, TopicCreationResponse);
    SWIFTCLIENT_DECL_HELPER(modifyTopic, TopicCreationRequest, TopicCreationResponse);
    SWIFTCLIENT_DECL_HELPER(deleteTopic, TopicDeletionRequest, TopicDeletionResponse);
    SWIFTCLIENT_DECL_HELPER(getSysInfo, EmptyRequest, SysInfoResponse);
    SWIFTCLIENT_DECL_HELPER(getTopicInfo, TopicInfoRequest, TopicInfoResponse);
    SWIFTCLIENT_DECL_HELPER(getAllTopicName, EmptyRequest, TopicNameResponse);
    SWIFTCLIENT_DECL_HELPER(getAllTopicInfo, EmptyRequest, AllTopicInfoResponse);
    SWIFTCLIENT_DECL_HELPER(getPartitionInfo, PartitionInfoRequest, PartitionInfoResponse);
    SWIFTCLIENT_DECL_HELPER(getRoleAddress, RoleAddressRequest, RoleAddressResponse);
    SWIFTCLIENT_DECL_HELPER(updateBrokerConfig, UpdateBrokerConfigRequest, UpdateBrokerConfigResponse);
    SWIFTCLIENT_DECL_HELPER(rollbackBrokerConfig, RollbackBrokerConfigRequest, RollbackBrokerConfigResponse);
    SWIFTCLIENT_DECL_HELPER(registerSchema, RegisterSchemaRequest, RegisterSchemaResponse);
    SWIFTCLIENT_DECL_HELPER(getSchema, GetSchemaRequest, GetSchemaResponse);
    SWIFTCLIENT_DECL_HELPER(createTopicBatch, TopicBatchCreationRequest, TopicBatchCreationResponse);
    SWIFTCLIENT_DECL_HELPER(deleteTopicBatch, TopicBatchDeletionRequest, TopicBatchDeletionResponse);
    SWIFTCLIENT_DECL_HELPER(getBrokerStatus, GetBrokerStatusRequest, GetBrokerStatusResponse);
    SWIFTCLIENT_DECL_HELPER(getTopicRWTime, GetTopicRWTimeRequest, GetTopicRWTimeResponse);
    SWIFTCLIENT_DECL_HELPER(reportMissTopic, MissTopicRequest, MissTopicResponse);
    SWIFTCLIENT_DECL_HELPER(updateWriterVersion, UpdateWriterVersionRequest, UpdateWriterVersionResponse);
    SWIFTCLIENT_DECL_HELPER(getMasterInfo, EmptyRequest, MasterInfoResponse);
    SWIFTCLIENT_DECL_HELPER(topicAclManage, TopicAclRequest, TopicAclResponse);

    SWIFTCLIENT_DECL_HELPER(reportBrokerStatus, BrokerStatusRequest, BrokerStatusResponse);

#undef SWIFTCLIENT_DECL_HELPER

private:
    int64_t _timeout;
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftAdminClient);

} // namespace network
} // namespace swift
