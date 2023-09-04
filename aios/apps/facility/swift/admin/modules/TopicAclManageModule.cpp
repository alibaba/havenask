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
#include "swift/admin/modules/TopicAclManageModule.h"

#include "swift/auth/RequestAuthenticator.h"
#include "swift/auth/TopicAclDataManager.h"
#include "swift/auth/TopicAclRequestHandler.h"
#include "swift/common/CreatorRegistry.h"
#include "swift/config/AdminConfig.h"

using namespace swift::auth;
using namespace swift::common;

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, TopicAclManageModule);

TopicAclManageModule::TopicAclManageModule() : BaseModule() {}

TopicAclManageModule::~TopicAclManageModule() { _aclDataManager.reset(); }

bool TopicAclManageModule::doInit() {
    _requestAuthenticator.reset(new auth::RequestAuthenticator(_adminConfig->getAuthenticationConf(), nullptr));
    return true;
}

bool TopicAclManageModule::doLoad() { return true; }

bool TopicAclManageModule::doUnload() {
    _requestAuthenticator.reset();
    _aclDataManager.reset();
    return true;
}

bool TopicAclManageModule::doUpdate(Status status) {
    _aclDataManager.reset(new TopicAclDataManager());
    if (!_aclDataManager->init(
            _adminConfig->getZkRoot(), !status.isLeader(), _adminConfig->getMaxTopicAclSyncIntervalUs())) {
        AUTIL_LOG(ERROR, "Topic Access Manage Module init failed");
        _aclDataManager.reset();
        return false;
    }
    AUTIL_LOG(INFO, "update topic acl manage module status %d", !status.isLeader());
    TopicAclRequestHandlerPtr requestHandler(new TopicAclRequestHandler(_aclDataManager));
    _requestAuthenticator->updateTopicAclRequestHandler(requestHandler);
    return true;
}

RequestAuthenticatorPtr TopicAclManageModule::getRequestAuthenticator() { return _requestAuthenticator; }

REGISTER_MODULE(TopicAclManageModule, "M|S", "L|F");

} // namespace admin
} // namespace swift
