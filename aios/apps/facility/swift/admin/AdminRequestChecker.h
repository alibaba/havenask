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

#include "autil/Log.h"
#include "autil/result/Result.h"

namespace swift {
namespace protocol {
class ErrorRequest;
class PartitionInfoRequest;
class RoleAddressRequest;
class TopicBatchDeletionRequest;
class TopicCreationRequest;
class TopicDeletionRequest;
class TopicInfoRequest;
} // namespace protocol

namespace config {
class AdminConfig;
}
} // namespace swift

namespace swift {
namespace admin {

class AdminRequestChecker {
public:
    static autil::Result<bool> checkTopicCreationRequest(const protocol::TopicCreationRequest *request,
                                                         config::AdminConfig *config,
                                                         bool checkPartCnt = true);
    static bool checkTopicDeletionRequest(const protocol::TopicDeletionRequest *request);
    static bool checkTopicBatchDeletionRequest(const protocol::TopicBatchDeletionRequest *request);
    static bool checkTopicInfoRequest(const protocol::TopicInfoRequest *request);
    static bool checkPartitionInfoRequest(const protocol::PartitionInfoRequest *request);
    static bool checkRoleAddressRequest(const protocol::RoleAddressRequest *request);
    static bool checkErrorRequest(const protocol::ErrorRequest *request);
    static bool checkLogicTopicModify(const protocol::TopicCreationRequest &target,
                                      const protocol::TopicCreationRequest &current);

private:
    AdminRequestChecker();
    ~AdminRequestChecker();

private:
    AUTIL_LOG_DECLARE();
};

} // namespace admin
} // namespace swift
