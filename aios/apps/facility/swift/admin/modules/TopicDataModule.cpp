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
#include "swift/admin/modules/TopicDataModule.h"

#include <string>

#include "swift/admin/SysController.h"
#include "swift/admin/TopicInfo.h"
#include "swift/admin/TopicTable.h"
#include "swift/common/Common.h"
#include "swift/common/CreatorRegistry.h"
#include "swift/common/TopicWriterController.h"
#include "swift/config/AdminConfig.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, TopicDataModule);

using namespace swift::common;
using namespace swift::protocol;

TopicDataModule::TopicDataModule() {}

TopicDataModule::~TopicDataModule() {}

bool TopicDataModule::doInit() {
    _zkDataAccessor = _sysController->getAdminZkDataAccessor();
    if (!_zkDataAccessor) {
        return false;
    }
    _topicTable = _sysController->getTopicTable();
    return true;
}

bool TopicDataModule::doLoad() {
    _topicWriterController = std::make_unique<TopicWriterController>(_zkDataAccessor, _adminConfig->getZkRoot());
    if (!_topicWriterController) {
        return false;
    }
    return true;
}

bool TopicDataModule::doUnload() { return true; }

void TopicDataModule::updateWriterVersion(const protocol::UpdateWriterVersionRequest *request,
                                          protocol::UpdateWriterVersionResponse *response) {
    protocol::ErrorInfo *errorInfo = response->mutable_errorinfo();
    errorInfo->set_errcode(ERROR_NONE);
    const auto &topicName = request->topicwriterversion().topicname();
    std::string errorMsg;
    if (!_topicWriterController->validateRequest(request, errorMsg)) {
        errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
        errorInfo->set_errmsg(ErrorCode_Name(errorInfo->errcode()));
        errorInfo->set_errmsg(errorMsg);
        AUTIL_LOG(ERROR, "request validate, topic[%s] request[%s]", topicName.c_str(), request->DebugString().c_str());
        return;
    }
    auto topicInfo = _topicTable->findTopic(topicName);
    if (!topicInfo) {
        errorInfo->set_errcode(ERROR_ADMIN_TOPIC_NOT_EXISTED);
        errorInfo->set_errmsg(ErrorCode_Name(errorInfo->errcode()));
        AUTIL_LOG(ERROR, "topic not exist, topic[%s]", topicName.c_str());
        return;
    }
    if (!topicInfo->enableWriterVersionController()) {
        errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
        errorInfo->set_errmsg(ErrorCode_Name(errorInfo->errcode()));
        AUTIL_LOG(ERROR, "writer version controller disable, topic[%s]", topicName.c_str());
        return;
    }
    _topicWriterController->updateWriterVersion(request, response);
}

REGISTER_MODULE(TopicDataModule, "M|S|U", "L|F|U");

} // namespace admin
} // namespace swift
