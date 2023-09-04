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
#include "swift/auth/TopicAclRequestHandler.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <utility>

#include "autil/result/Errors.h"
#include "autil/result/ForwardList.h"
#include "swift/auth/TopicAclDataSyncher.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

using namespace autil::result;
using namespace swift::protocol;
using namespace std;

namespace swift {
namespace auth {
AUTIL_LOG_SETUP(swift, TopicAclRequestHandler);

TopicAclRequestHandler::TopicAclRequestHandler(TopicAclDataManagerPtr topicAclDataManager)
    : _topicAclDataManager(topicAclDataManager) {}

TopicAclRequestHandler::~TopicAclRequestHandler() {}

void TopicAclRequestHandler::handleRequest(const protocol::TopicAclRequest *request,
                                           protocol::TopicAclResponse *response) {
    autil::Result<bool> ret;
    switch (request->accessop()) {
    case ADD_TOPIC_ACCESS:
        ret = _topicAclDataManager->addTopicAccess(request->topicname(), request->topicaccessinfo());
        if (!ret.is_ok()) {
            setHandleError(response, ret.get_error().message());
        }
        return;
    case UPDATE_TOPIC_ACCESS:
        ret = _topicAclDataManager->updateTopicAccess(request->topicname(), request->topicaccessinfo());
        if (!ret.is_ok()) {
            setHandleError(response, ret.get_error().message());
        }
        return;
    case UPDATE_TOPIC_ACCESS_KEY:
        ret = _topicAclDataManager->updateTopicAccessKey(request->topicname(), request->topicaccessinfo());
        if (!ret.is_ok()) {
            setHandleError(response, ret.get_error().message());
        }
        return;
    case UPDATE_TOPIC_ACCESS_PRIORITY:
        ret = _topicAclDataManager->updateTopicAccessPriority(request->topicname(), request->topicaccessinfo());
        if (!ret.is_ok()) {
            setHandleError(response, ret.get_error().message());
        }
        return;
    case UPDATE_TOPIC_ACCESS_TYPE:
        ret = _topicAclDataManager->updateTopicAccessType(request->topicname(), request->topicaccessinfo());
        if (!ret.is_ok()) {
            setHandleError(response, ret.get_error().message());
        }
        return;
    case DELETE_TOPIC_ACCESS:
        ret = _topicAclDataManager->deleteTopicAccess(request->topicname(), request->topicaccessinfo());
        if (!ret.is_ok()) {
            setHandleError(response, ret.get_error().message());
        }
        return;

    case UPDATE_TOPIC_AUTH_STATUS:
        ret = _topicAclDataManager->updateTopicAuthCheckStatus(request->topicname(), request->authcheckstatus());
        if (!ret.is_ok()) {
            setHandleError(response, ret.get_error().message());
        }
        return;
    case CLEAR_TOPIC_ACCESS:
        ret = _topicAclDataManager->clearTopicAccess(request->topicname());
        if (!ret.is_ok()) {
            setHandleError(response, ret.get_error().message());
        }
        return;
    case LIST_ONE_TOPIC_ACCESS: {
        TopicAclData *topicAclData = response->mutable_topicacldatas()->add_alltopicacldata();
        ret = _topicAclDataManager->getTopicAclDatas(request->topicname(), *topicAclData);
        if (!ret.is_ok()) {
            setHandleError(response, ret.get_error().message());
        }
        return;
    }
    case LIST_ALL_TOPIC_ACCESS: {
        const auto &topicAclDataMap = _topicAclDataManager->getTopicAclDataMap();
        for (const auto &iter : topicAclDataMap) {
            *response->mutable_topicacldatas()->add_alltopicacldata() = iter.second;
        }
        return;
    }
    default:
        return;
    }
}

void TopicAclRequestHandler::setHandleError(protocol::TopicAclResponse *response, const string &msgStr) {
    protocol::ErrorInfo *ei = response->mutable_errorinfo();
    ei->set_errcode(ERROR_ADMIN_OPERATION_FAILED);
    std::string errMsg = ErrorCode_Name(ei->errcode());
    if (msgStr.size() > 0) {
        errMsg += "[" + msgStr + "]";
    }
    ei->set_errmsg(errMsg);
}

autil::Result<bool> TopicAclRequestHandler::checkTopicAcl(const std::string &topicName,
                                                          const std::string &accessId,
                                                          const std::string &accessKey,
                                                          bool needWrite) {
    protocol::TopicAclData topicAclData;
    auto ret = _topicAclDataManager->getTopicAclDatas(topicName, topicAclData);
    if (!ret.is_ok()) {
        return true; // only check topic acl data exist
    }
    if (topicAclData.checkstatus() != TOPIC_AUTH_CHECK_ON) {
        return true;
    }

    if (accessId.empty() || accessKey.empty()) {
        return RuntimeError::make(
            "topic access info has empty field, topic [%s], accessid [%s]", topicName.c_str(), accessId.c_str());
    }
    topicAclData.Clear();
    ret = _topicAclDataManager->getTopicAclData(topicName, accessId, topicAclData);
    if (!ret.is_ok()) {
        return ret;
    }
    if (topicAclData.topicaccessinfos_size() != 1) {
        return RuntimeError::make("check topic access failed, access not exist, topic [%s] accessid [%s]",
                                  topicName.c_str(),
                                  accessId.c_str());
    }
    const auto &accessInfo = topicAclData.topicaccessinfos(0);
    if (accessKey != accessInfo.accessauthinfo().accesskey()) {
        return RuntimeError::make(
            "check topic access failed, accesskey not equal, topic [%s], accessid [%s], accesskey [%s]",
            topicName.c_str(),
            accessId.c_str(),
            accessKey.c_str());
    }
    if (accessInfo.accesstype() == TOPIC_ACCESS_NONE) {
        return RuntimeError::make("check topic access failed, access is none, topic [%s], accessid [%s]",
                                  topicName.c_str(),
                                  accessId.c_str());
    }
    if (needWrite && accessInfo.accesstype() != TOPIC_ACCESS_READ_WRITE) {
        return RuntimeError::make("check topic access failed, write not allow, topic [%s], accessid [%s], type [%d], ",
                                  topicName.c_str(),
                                  accessId.c_str(),
                                  accessInfo.accesstype());
    }
    return true;
}

autil::Result<bool> TopicAclRequestHandler::createTopicAclItems(const vector<string> &topicNames) {
    return _topicAclDataManager->createTopicAclItems(topicNames);
}

autil::Result<bool> TopicAclRequestHandler::deleteTopicAclItems(const vector<string> &topicNames) {
    return _topicAclDataManager->deleteTopicAclItems(topicNames);
}

autil::Result<bool> TopicAclRequestHandler::getTopicAccessInfo(const string &topicName,
                                                               const AccessAuthInfo &accessAuthInfo,
                                                               TopicAccessInfo &topicAccessInfo) {
    const string &accessId = accessAuthInfo.accessid();
    const string &accessKey = accessAuthInfo.accesskey();
    TopicAclData topicAclData;
    auto ret = _topicAclDataManager->getTopicAclData(topicName, accessId, topicAclData);
    if (!ret.is_ok()) {
        return ret;
    }
    if (topicAclData.checkstatus() != TOPIC_AUTH_CHECK_ON) {
        return RuntimeError::make("topic check status off");
    }
    if (topicAclData.topicaccessinfos_size() == 0) {
        topicAccessInfo.set_accesstype(TOPIC_ACCESS_NONE);
        return true;
    }
    const auto &accessInfo = topicAclData.topicaccessinfos(0);
    if (accessKey != accessInfo.accessauthinfo().accesskey()) {
        topicAccessInfo.set_accesstype(TOPIC_ACCESS_NONE);
        return true;
    } else {
        topicAccessInfo = accessInfo;
    }
    return true;
}

} // namespace auth
} // namespace swift
