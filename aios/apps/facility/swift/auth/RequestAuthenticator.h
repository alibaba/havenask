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

#include <map>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/result/Result.h"
#include "swift/auth/TopicAclRequestHandler.h"
#include "swift/common/Common.h"
#include "swift/config/AuthorizerInfo.h"
#include "swift/protocol/AdminRequestResponse.pb.h"

namespace swift {
namespace protocol {
class AuthenticationInfo;
} // namespace protocol

namespace auth {

class RequestAuthenticator {
public:
    RequestAuthenticator(const config::AuthorizerInfo &conf, const TopicAclRequestHandlerPtr &requestHandler);
    ~RequestAuthenticator();

private:
    RequestAuthenticator(const RequestAuthenticator &);
    RequestAuthenticator &operator=(const RequestAuthenticator &);

public:
    template <typename T>
    bool checkAuthorizer(const T *request);
    template <typename T>
    bool checkSysAuthorizer(const T *request);
    template <typename T>
    bool checkTopicAcl(const T *request);
    template <typename T>
    bool checkAuthorizerAndTopicAcl(const T *request);
    template <typename T>
    bool checkTopicAclInBroker(const T *request, bool needWrite);

    autil::Result<bool> handleTopicAclRequest(const protocol::TopicAclRequest *request,
                                              protocol::TopicAclResponse *response);

    void updateTopicAclRequestHandler(TopicAclRequestHandlerPtr requestHandler);
    autil::Result<bool> createTopicAclItems(const std::vector<std::string> &topicNames);
    autil::Result<bool> deleteTopicAclItems(const std::vector<std::string> &topicName);
    protocol::TopicAccessInfo getTopicAccessInfo(const std::string &topicName,
                                                 const protocol::AuthenticationInfo &authInfo);

public:
    static autil::Result<bool> validateTopicAclRequest(const protocol::TopicAclRequest *request);

private:
    bool checkAuthorizer(const std::string &username,
                         const std::string &passwd,
                         const std::map<std::string, std::string> &users);
    bool checkTopicAcl(const std::string &topicName,
                       const std::string &accessId,
                       const std::string &accessKey,
                       bool needWrite);

private:
    config::AuthorizerInfo _config;
    TopicAclRequestHandlerPtr _topicAclRequestHandler;
    autil::ReadWriteLock _rwLock;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
bool RequestAuthenticator::checkSysAuthorizer(const T *request) {
    if (!_config.getEnable()) {
        return true;
    }
    if (!request->has_authentication()) {
        AUTIL_LOG(INFO, "No authentication, request refused[%s]", request->ShortDebugString().c_str());
        return false;
    }
    const auto &authInfo = request->authentication();
    return checkAuthorizer(authInfo.username(), authInfo.passwd(), _config.getSysUsers());
}

template <typename T>
bool RequestAuthenticator::checkAuthorizer(const T *request) {
    if (!checkSysAuthorizer(request)) {
        const auto &authInfo = request->authentication();
        return checkAuthorizer(authInfo.username(), authInfo.passwd(), _config.getNormalUsers());
    }
    return true;
}

template <typename T>
bool RequestAuthenticator::checkTopicAclInBroker(const T *request, bool needWrite) {
    if (!_config.getEnable()) {
        const auto &authInfo = request->authentication();
        return checkTopicAcl(request->topicname(),
                             authInfo.accessauthinfo().accessid(),
                             authInfo.accessauthinfo().accesskey(),
                             needWrite);
    } else {
        if (!checkAuthorizer(request)) {
            const auto &authInfo = request->authentication();
            return checkTopicAcl(request->topicname(),
                                 authInfo.accessauthinfo().accessid(),
                                 authInfo.accessauthinfo().accesskey(),
                                 needWrite);
        }
        return true;
    }
}

template <typename T>
bool RequestAuthenticator::checkAuthorizerAndTopicAcl(const T *request) {
    if (!checkAuthorizer(request)) {
        const auto &authInfo = request->authentication();
        return checkTopicAcl(
            request->topicname(), authInfo.accessauthinfo().accessid(), authInfo.accessauthinfo().accesskey(), false);
    }
    return true;
}

template <>
bool RequestAuthenticator::checkAuthorizerAndTopicAcl(const protocol::UpdateWriterVersionRequest *request);

SWIFT_TYPEDEF_PTR(RequestAuthenticator);

} // namespace auth
} // end namespace swift
