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
#include "swift/admin/ErrorHandler.h"

#include "autil/TimeUtility.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"

namespace swift {
namespace protocol {
class ErrorRequest;
class PartitionErrorResponse;
class WorkerErrorResponse;
} // namespace protocol
} // namespace swift

using namespace autil;

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, ErrorHandler);
using namespace swift::protocol;

ErrorHandler::ErrorHandler() {}

ErrorHandler::~ErrorHandler() {}

void ErrorHandler::extractError(const protocol::HeartbeatInfo &heartbeatInfo) {
    extractPartitionError(heartbeatInfo);
    extractWorkerError(heartbeatInfo);
}

void ErrorHandler::addPartitionError(const protocol::PartitionId &partitionId,
                                     protocol::ErrorCode errorCode,
                                     int64_t errTime,
                                     const std::string &errMsg) {
    ScopedLock lock(_lock);
    _partitionHandler.addError(partitionId, errorCode, errTime, errMsg);
}

void ErrorHandler::addWorkerError(const std::string &workerAddress,
                                  protocol::RoleType role,
                                  protocol::ErrorCode errCode,
                                  int64_t errTime,
                                  const std::string &errMsg) {
    ScopedLock lock(_lock);
    _workerHandler.addError(workerAddress, role, errCode, errTime, errMsg);
}

void ErrorHandler::getPartitionError(const protocol::ErrorRequest &request,
                                     protocol::PartitionErrorResponse &response) {
    ScopedLock lock(_lock);
    _partitionHandler.getError(request, response);
}
void ErrorHandler::getWorkerError(const protocol::ErrorRequest &request, protocol::WorkerErrorResponse &response) {
    ScopedLock lock(_lock);
    _workerHandler.getError(request, response);
}

void ErrorHandler::extractPartitionError(const protocol::HeartbeatInfo &heartbeatInfo) {
    int32_t count = heartbeatInfo.tasks_size();
    for (int32_t i = 0; i < count; i++) {
        const TaskStatus &task = heartbeatInfo.tasks(i);
        if (!task.has_errcode() || !task.has_errtime() || ERROR_NONE == task.errcode()) {
            continue;
        }
        if (task.has_errmsg()) {
            addPartitionError(task.taskinfo().partitionid(), task.errcode(), task.errtime(), task.errmsg());
        } else {
            addPartitionError(task.taskinfo().partitionid(), task.errcode(), task.errtime(), "");
        }
    }
}
void ErrorHandler::extractWorkerError(const protocol::HeartbeatInfo &heartbeatInfo) {

    if (!heartbeatInfo.alive()) {
        addWorkerError(heartbeatInfo.address(),
                       heartbeatInfo.role(),
                       ERROR_WORKER_DEAD,
                       TimeUtility::currentTime(),
                       ErrorCode_Name(ERROR_WORKER_DEAD));
        return;
    }

    if (!heartbeatInfo.has_errcode() || !heartbeatInfo.has_errtime()) {
        return;
    }
    if (heartbeatInfo.has_errmsg()) {
        addWorkerError(heartbeatInfo.address(),
                       heartbeatInfo.role(),
                       heartbeatInfo.errcode(),
                       heartbeatInfo.errtime(),
                       heartbeatInfo.errmsg());
    } else {
        addWorkerError(
            heartbeatInfo.address(), heartbeatInfo.role(), heartbeatInfo.errcode(), heartbeatInfo.errtime(), "");
    }
}

void ErrorHandler::clear() {
    ScopedLock lock(_lock);
    _partitionHandler.clear();
    _workerHandler.clear();
}
} // namespace admin
} // namespace swift
