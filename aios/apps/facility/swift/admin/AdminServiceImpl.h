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

#include <google/protobuf/stubs/callback.h>
#include <string>

#include "autil/Log.h"
#include "swift/admin/AdminLogClosure.h" // IWYU pragma: keep
#include "swift/admin/SwiftAdminServer.h"
#include "swift/admin/SysController.h"
#include "swift/auth/RequestAuthenticator.h"
#include "swift/common/Common.h"
#include "swift/monitor/AdminMetricsReporter.h"
#include "swift/protocol/Control.pb.h"

namespace google {
namespace protobuf {
class RpcController;
} // namespace protobuf
} // namespace google

namespace swift {
namespace config {
class AdminConfig;
} // namespace config
namespace protocol {
class AllTopicInfoResponse;
class AllTopicSimpleInfoResponse;
class ChangeSlotRequest;
class ChangeSlotResponse;
class DeletedNoUseTopicFilesResponse;
class DeletedNoUseTopicRequest;
class DeletedNoUseTopicResponse;
class EmptyRequest;
class ErrorRequest;
class GetBrokerStatusRequest;
class GetBrokerStatusResponse;
class GetSchemaRequest;
class GetSchemaResponse;
class GetTopicRWTimeRequest;
class GetTopicRWTimeResponse;
class LastDeletedNoUseTopicResponse;
class MasterInfoResponse;
class MissTopicRequest;
class MissTopicResponse;
class PartitionErrorResponse;
class PartitionInfoRequest;
class PartitionInfoResponse;
class PartitionTransferRequest;
class PartitionTransferResponse;
class RegisterSchemaRequest;
class RegisterSchemaResponse;
class RoleAddressRequest;
class RoleAddressResponse;
class RollbackBrokerConfigRequest;
class RollbackBrokerConfigResponse;
class SysInfoResponse;
class TopicAclRequest;
class TopicAclResponse;
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
class TurnToMasterRequest;
class TurnToMasterResponse;
class TurnToSlaveRequest;
class TurnToSlaveResponse;
class UpdateBrokerConfigRequest;
class UpdateBrokerConfigResponse;
class UpdateWriterVersionRequest;
class UpdateWriterVersionResponse;
class WorkerErrorResponse;
class WorkerStatusResponse;
} // namespace protocol

namespace admin {

class AdminServiceImpl : public ::swift::protocol::Controller, public swift::admin::SwiftAdminServer {
public:
    AdminServiceImpl(config::AdminConfig *config,
                     admin::SysController *sysController,
                     monitor::AdminMetricsReporter *reporter);
    ~AdminServiceImpl() override;

public:
    RPC_METHOD_DEFINE(getTopicInfo, TopicInfoRequest, TopicInfoResponse, 10, true, checkAuthorizerAndTopicAcl)
    RPC_METHOD_DEFINE(getSchema, GetSchemaRequest, GetSchemaResponse, 1, false, checkAuthorizerAndTopicAcl)
    RPC_METHOD_DEFINE(updateWriterVersion,
                      UpdateWriterVersionRequest,
                      UpdateWriterVersionResponse,
                      1,
                      false,
                      checkAuthorizerAndTopicAcl)

    RPC_METHOD_DEFINE(createTopic, TopicCreationRequest, TopicCreationResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(
        createTopicBatch, TopicBatchCreationRequest, TopicBatchCreationResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(modifyTopic, TopicCreationRequest, TopicCreationResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(deleteTopic, TopicDeletionRequest, TopicDeletionResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(
        deleteTopicBatch, TopicBatchDeletionRequest, TopicBatchDeletionResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(getSysInfo, EmptyRequest, SysInfoResponse, 1, true, checkAuthorizer)
    RPC_METHOD_DEFINE(getAllTopicInfo, EmptyRequest, AllTopicInfoResponse, 10, true, checkAuthorizer)
    RPC_METHOD_DEFINE(getAllTopicSimpleInfo, EmptyRequest, AllTopicSimpleInfoResponse, 10, true, checkAuthorizer)
    RPC_METHOD_DEFINE(getAllTopicName, EmptyRequest, TopicNameResponse, 1, true, checkAuthorizer)
    RPC_METHOD_DEFINE(getPartitionInfo, PartitionInfoRequest, PartitionInfoResponse, 100, true, checkAuthorizer)
    RPC_METHOD_DEFINE(getRoleAddress, RoleAddressRequest, RoleAddressResponse, 1, true, checkAuthorizer)
    RPC_METHOD_DEFINE(getAllWorkerStatus, EmptyRequest, WorkerStatusResponse, 1, true, checkAuthorizer)
    RPC_METHOD_DEFINE(getWorkerError, ErrorRequest, WorkerErrorResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(getPartitionError, ErrorRequest, PartitionErrorResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(registerSchema, RegisterSchemaRequest, RegisterSchemaResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(getBrokerStatus, GetBrokerStatusRequest, GetBrokerStatusResponse, 1, true, checkAuthorizer)
    RPC_METHOD_DEFINE(getTopicRWTime, GetTopicRWTimeRequest, GetTopicRWTimeResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(reportMissTopic, MissTopicRequest, MissTopicResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(getLastDeletedNoUseTopic, EmptyRequest, LastDeletedNoUseTopicResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(
        getDeletedNoUseTopic, DeletedNoUseTopicRequest, DeletedNoUseTopicResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(
        getDeletedNoUseTopicFiles, EmptyRequest, DeletedNoUseTopicFilesResponse, 1, false, checkAuthorizer)
    RPC_METHOD_DEFINE(getMasterInfo, EmptyRequest, MasterInfoResponse, 1, true, checkAuthorizer)
    RPC_METHOD_DEFINE(topicAclManage, TopicAclRequest, TopicAclResponse, 1, false, checkAuthorizer)

    RPC_METHOD_DEFINE(
        updateBrokerConfig, UpdateBrokerConfigRequest, UpdateBrokerConfigResponse, 1, true, checkSysAuthorizer)
    RPC_METHOD_DEFINE(
        rollbackBrokerConfig, RollbackBrokerConfigRequest, RollbackBrokerConfigResponse, 1, true, checkSysAuthorizer)
    RPC_METHOD_DEFINE(
        transferPartition, PartitionTransferRequest, PartitionTransferResponse, 1, false, checkSysAuthorizer)
    RPC_METHOD_DEFINE(changeSlot, ChangeSlotRequest, ChangeSlotResponse, 1, true, checkSysAuthorizer)
    RPC_METHOD_DEFINE(turnToMaster, TurnToMasterRequest, TurnToMasterResponse, 1, true, checkSysAuthorizer)
    RPC_METHOD_DEFINE(turnToSlave, TurnToSlaveRequest, TurnToSlaveResponse, 1, true, checkSysAuthorizer)

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(AdminServiceImpl);

} // end namespace admin
} // end namespace swift
