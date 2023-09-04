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
#include "swift/admin/WorkerErrorHandler.h"

#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, WorkerErrorHandler);

WorkerErrorHandler::WorkerErrorHandler() {}

WorkerErrorHandler::~WorkerErrorHandler() {}

void WorkerErrorHandler::addError(const std::string &workerAddress,
                                  protocol::RoleType role,
                                  protocol::ErrorCode errCode,
                                  int64_t errTime,
                                  const std::string &errMsg) {
    protocol::WorkerError workerError;
    workerError.set_workeraddress(workerAddress);
    workerError.set_role(role);
    workerError.set_errcode(errCode);
    workerError.set_errtime(errTime);
    workerError.set_errmsg(errMsg);

    if (WorkerErrorHandlerBase::addError(workerAddress, workerError)) {
        AUTIL_LOG(ERROR, "Get Error:%s", workerError.ShortDebugString().c_str());
    }
}

} // namespace admin
} // namespace swift
