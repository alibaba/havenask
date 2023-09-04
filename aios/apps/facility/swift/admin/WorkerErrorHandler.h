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

#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "swift/admin/ErrorHandlerTemplate.h"
#include "swift/common/Common.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace admin {
typedef ErrorHandlerTemplate<std::string, protocol::WorkerError, protocol::WorkerErrorResponse> WorkerErrorHandlerBase;

class WorkerErrorHandler : public WorkerErrorHandlerBase {
public:
    WorkerErrorHandler();
    ~WorkerErrorHandler();

private:
    WorkerErrorHandler(const WorkerErrorHandler &);
    WorkerErrorHandler &operator=(const WorkerErrorHandler &);

public:
    void addError(const std::string &workerAddress,
                  protocol::RoleType role,
                  protocol::ErrorCode errCode,
                  int64_t errTime,
                  const std::string &errMsg);

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(WorkerErrorHandler);

} // namespace admin
} // namespace swift
