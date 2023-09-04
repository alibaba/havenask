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
#include "swift/common/TopicWriterController.h"

#include <iosfwd>
#include <memory>

#include "autil/Lock.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"

using namespace std;
using namespace swift::protocol;

namespace swift {
namespace common {
AUTIL_LOG_SETUP(swift, TopicWriterController);

TopicWriterController::TopicWriterController(util::ZkDataAccessorPtr zkDataAccessor, const std::string &zkRoot)
    : _zkDataAccessor(zkDataAccessor), _zkRoot(zkRoot) {}

TopicWriterController::~TopicWriterController() {}

bool TopicWriterController::validateRequest(const protocol::UpdateWriterVersionRequest *request,
                                            std::string &errorMsg) {
    if (!request->has_topicwriterversion()) {
        errorMsg = "topic write version empty";
        return false;
    }
    const auto &topicWriterVersion = request->topicwriterversion();
    if (topicWriterVersion.writerversions_size() == 0) {
        errorMsg = "write version empty";
        return false;
    }

    if (!topicWriterVersion.has_topicname() || topicWriterVersion.topicname().empty()) {
        errorMsg = "topic name empty";
        return false;
    }
    return true;
}

void TopicWriterController::updateWriterVersion(const UpdateWriterVersionRequest *request,
                                                UpdateWriterVersionResponse *response) {
    const auto &topicWriterVersion = request->topicwriterversion();
    const auto &topicName = topicWriterVersion.topicname();
    std::shared_ptr<SingleTopicWriterController> singleWriterController;
    {
        autil::ScopedLock lock(_lock);
        auto &versionController = _writerVersionControllers[topicName];
        if (!versionController) {
            versionController = std::make_shared<SingleTopicWriterController>(_zkDataAccessor, _zkRoot, topicName);
            if (!versionController->init()) {
                setErrorInfo(response, ERROR_ADMIN_OPERATION_FAILED, "version controller init failed");
                return;
            }
        }
        singleWriterController = versionController;
    }

    auto errorInfo = singleWriterController->updateWriterVersion(request->topicwriterversion());
    if (errorInfo.errcode() != ERROR_NONE) {
        setErrorInfo(response, errorInfo.errcode(), errorInfo.errmsg());
        return;
    } else {
        auto *errorInfo = response->mutable_errorinfo();
        errorInfo->set_errcode(ERROR_NONE);
    }
}

void TopicWriterController::setErrorInfo(protocol::UpdateWriterVersionResponse *response,
                                         protocol::ErrorCode ec,
                                         const string &msgStr) {
    auto *errorInfo = response->mutable_errorinfo();
    errorInfo->set_errcode(ec);
    string errMsg = ErrorCode_Name(errorInfo->errcode());
    if (msgStr.size() > 0) {
        errMsg += "[" + msgStr + "]";
    }
    errorInfo->set_errmsg(errMsg);
    AUTIL_LOG(ERROR, "Error Message: %s", errMsg.c_str());
}

} // namespace common
} // namespace swift
