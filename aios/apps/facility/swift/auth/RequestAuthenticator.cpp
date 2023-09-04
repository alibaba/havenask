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
#include "swift/auth/RequestAuthenticator.h"

#include <iosfwd>
#include <memory>
#include <utility>

#include "autil/TimeUtility.h"
#include "autil/result/Errors.h"
#include "autil/result/ForwardList.h"
#include "swift/auth/TopicAclDataSyncher.h"
#include "swift/protocol/Common.pb.h"

using namespace std;
using namespace autil;
using namespace autil::result;
using namespace swift::protocol;

namespace swift {
namespace auth {

AUTIL_LOG_SETUP(swift, RequestAuthenticator);

RequestAuthenticator::RequestAuthenticator(const config::AuthorizerInfo &conf,
                                           const TopicAclRequestHandlerPtr &requestHandler)
    : _config(conf), _topicAclRequestHandler(requestHandler) {}

RequestAuthenticator::~RequestAuthenticator() {}

Result<bool> RequestAuthenticator::validateTopicAclRequest(const protocol::TopicAclRequest *request) {
    const auto &topicAccessInfo = request->topicaccessinfo();
    const auto &accessAuthInfo = topicAccessInfo.accessauthinfo();
    switch (request->accessop()) {
    case ADD_TOPIC_ACCESS:
    case UPDATE_TOPIC_ACCESS_KEY:
        if (request->topicname().empty() || accessAuthInfo.accessid().empty() || accessAuthInfo.accesskey().empty()) {
            return RuntimeError::make("request has empty field, topicName [%s], accessid [%s], accesskey [%s]",
                                      request->topicname().c_str(),
                                      accessAuthInfo.accessid().c_str(),
                                      accessAuthInfo.accesskey().c_str());
        }
        return true;
    case UPDATE_TOPIC_ACCESS:
    case UPDATE_TOPIC_ACCESS_TYPE:
    case UPDATE_TOPIC_ACCESS_PRIORITY:
    case DELETE_TOPIC_ACCESS:
        if (request->topicname().empty() || accessAuthInfo.accessid().empty()) {
            return RuntimeError::make("request has empty field, topicName [%s], accessid [%s]",
                                      request->topicname().c_str(),
                                      accessAuthInfo.accessid().c_str());
        }
        return true;
    case UPDATE_TOPIC_AUTH_STATUS:
    case CLEAR_TOPIC_ACCESS:
    case LIST_ONE_TOPIC_ACCESS:
        if (request->topicname().empty()) {
            return RuntimeError::make("request has empty field, topicName [%s]", request->topicname().c_str());
        }
        return true;
    case LIST_ALL_TOPIC_ACCESS:
    default:
        return true;
    }
}

bool RequestAuthenticator::checkAuthorizer(const string &username,
                                           const string &passwd,
                                           const map<string, string> &users) {
    auto iter = users.find(username);
    if (iter != users.end() && iter->second == passwd) {
        return true;
    } else {
        return false;
    }
}

bool RequestAuthenticator::checkTopicAcl(const std::string &topicName,
                                         const std::string &accessId,
                                         const std::string &accessKey,
                                         bool needWrite) {
    ScopedReadLock lock(_rwLock);
    if (_topicAclRequestHandler == nullptr) {
        return false;
    }
    auto ret = _topicAclRequestHandler->checkTopicAcl(topicName, accessId, accessKey, needWrite);
    if (!ret.is_ok()) {
        AUTIL_LOG(INFO, "check topic acl failed [%s]", ret.get_error().message().c_str());
        return false;
    }
    return true;
}

void RequestAuthenticator::updateTopicAclRequestHandler(TopicAclRequestHandlerPtr requestHandler) {
    ScopedWriteLock lock(_rwLock);
    _topicAclRequestHandler = requestHandler;
}

autil::Result<bool> RequestAuthenticator::handleTopicAclRequest(const protocol::TopicAclRequest *request,
                                                                protocol::TopicAclResponse *response) {
    ScopedReadLock lock(_rwLock);
    if (_topicAclRequestHandler == nullptr) {
        return RuntimeError::make("topic acl request handler not initialized [%s]", request->topicname().c_str());
    }
    _topicAclRequestHandler->handleRequest(request, response);
    return true;
}

autil::Result<bool> RequestAuthenticator::createTopicAclItems(const vector<string> &topicNames) {
    ScopedReadLock lock(_rwLock);
    if (_topicAclRequestHandler == nullptr) {
        return RuntimeError::make("request handler not initialized");
    }
    auto ret = _topicAclRequestHandler->createTopicAclItems(topicNames);
    if (!ret.is_ok()) {
        return ret;
    }
    return true;
}

autil::Result<bool> RequestAuthenticator::deleteTopicAclItems(const vector<string> &topicNames) {
    ScopedReadLock lock(_rwLock);
    if (_topicAclRequestHandler == nullptr) {
        return false;
    }
    return _topicAclRequestHandler->deleteTopicAclItems(topicNames);
}

template <>
bool RequestAuthenticator::checkAuthorizerAndTopicAcl(const protocol::UpdateWriterVersionRequest *request) {
    if (!checkAuthorizer(request)) {
        const auto &authInfo = request->authentication();
        return checkTopicAcl(request->topicwriterversion().topicname(),
                             authInfo.accessauthinfo().accessid(),
                             authInfo.accessauthinfo().accesskey(),
                             true);
    }
    return true;
}

TopicAccessInfo RequestAuthenticator::getTopicAccessInfo(const string &topicName, const AuthenticationInfo &authInfo) {
    TopicAccessInfo retAccessInfo;
    if (_config.getEnable() && authInfo.accessauthinfo().accessid().empty()) {
        retAccessInfo.set_accesspriority(ACCESS_PRIORITY_P0);
        retAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
        return retAccessInfo;
    }
    ScopedReadLock lock(_rwLock);
    if (_topicAclRequestHandler == nullptr) {
        retAccessInfo.set_accesspriority(ACCESS_PRIORITY_P5);
        retAccessInfo.set_accesstype(TOPIC_ACCESS_NONE);
        return retAccessInfo;
    }
    auto ret = _topicAclRequestHandler->getTopicAccessInfo(topicName, authInfo.accessauthinfo(), retAccessInfo);
    if (ret.is_err()) {
        retAccessInfo.set_accesspriority(ACCESS_PRIORITY_P5);
        retAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
        return retAccessInfo;
    }
    return retAccessInfo;
}

} // namespace auth
} // end namespace swift
