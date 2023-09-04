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

#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/result/Result.h"
#include "swift/auth/TopicAclDataManager.h"
#include "swift/common/Common.h"

namespace swift {
namespace protocol {
class AccessAuthInfo;
class TopicAccessInfo;
class TopicAclRequest;
class TopicAclResponse;
} // namespace protocol

namespace auth {

class TopicAclRequestHandler {
public:
    TopicAclRequestHandler(TopicAclDataManagerPtr topicAclDataManager);
    ~TopicAclRequestHandler();
    TopicAclRequestHandler(const TopicAclRequestHandler &) = delete;
    TopicAclRequestHandler &operator=(const TopicAclRequestHandler &) = delete;

public:
    void handleRequest(const protocol::TopicAclRequest *request, protocol::TopicAclResponse *response);
    autil::Result<bool> checkTopicAcl(const std::string &topicName,
                                      const std::string &accessId,
                                      const std::string &accessKey,
                                      bool needWrite);

    autil::Result<bool> createTopicAclItems(const std::vector<std::string> &topicNames);
    autil::Result<bool> deleteTopicAclItems(const std::vector<std::string> &topicName);
    autil::Result<bool> getTopicAccessInfo(const std::string &topicName,
                                           const protocol::AccessAuthInfo &accessAuthInfo,
                                           protocol::TopicAccessInfo &authInfo);

private:
    void setHandleError(protocol::TopicAclResponse *response, const std::string &msgStr);

private:
    TopicAclDataManagerPtr _topicAclDataManager;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(TopicAclRequestHandler);

} // namespace auth
} // namespace swift
