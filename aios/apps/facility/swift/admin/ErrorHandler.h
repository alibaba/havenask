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

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/admin/PartitionErrorHandler.h"
#include "swift/admin/WorkerErrorHandler.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace protocol {
class ErrorRequest;
class HeartbeatInfo;
class PartitionErrorResponse;
class WorkerErrorResponse;
} // namespace protocol
} // namespace swift

namespace swift {
namespace admin {

class ErrorHandler {
public:
    ErrorHandler();
    ~ErrorHandler();

private:
    ErrorHandler(const ErrorHandler &);
    ErrorHandler &operator=(const ErrorHandler &);

public:
    void extractError(const protocol::HeartbeatInfo &heartbeatInfo);

    void addPartitionError(const protocol::PartitionId &partitionId,
                           protocol::ErrorCode errorCode,
                           int64_t errTime,
                           const std::string &errMsg);

    void addWorkerError(const std::string &workerAddress,
                        protocol::RoleType role,
                        protocol::ErrorCode errCode,
                        int64_t errTime,
                        const std::string &errMsg);

    void getPartitionError(const protocol::ErrorRequest &request, protocol::PartitionErrorResponse &response);
    void getWorkerError(const protocol::ErrorRequest &request, protocol::WorkerErrorResponse &response);
    void clear();

private:
    void extractPartitionError(const protocol::HeartbeatInfo &heartbeatInfo);
    void extractWorkerError(const protocol::HeartbeatInfo &heartbeatInfo);

private:
    autil::ThreadMutex _lock;

    WorkerErrorHandler _workerHandler;
    PartitionErrorHandler _partitionHandler;

private:
    friend class ErrorHandlerTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ErrorHandler);

} // namespace admin
} // namespace swift
