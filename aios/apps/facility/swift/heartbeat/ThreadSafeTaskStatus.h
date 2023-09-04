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

#include <assert.h>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"

namespace swift {
namespace heartbeat {

class ThreadSafeTaskStatus {
public:
    ThreadSafeTaskStatus(const protocol::TaskInfo &taskInfo) {
        _taskStatus.Clear();
        *(_taskStatus.mutable_taskinfo()) = taskInfo;
        _taskStatus.set_status(protocol::PARTITION_STATUS_STARTING);
        _taskStatus.set_errcode(protocol::ERROR_NONE);
        _taskStatus.set_errtime((int64_t)-1);
        _taskStatus.set_errmsg("");
    }
    ~ThreadSafeTaskStatus() {}

private:
    ThreadSafeTaskStatus(const ThreadSafeTaskStatus &);
    ThreadSafeTaskStatus &operator=(const ThreadSafeTaskStatus &);

public:
    const protocol::TaskInfo getTaskInfo() const {
        autil::ScopedLock lock(_mutex);
        assert(_taskStatus.has_taskinfo());
        return _taskStatus.taskinfo();
    }

    const protocol::PartitionId getPartitionId() const {
        autil::ScopedLock lock(_mutex);
        assert(_taskStatus.has_taskinfo());
        assert(_taskStatus.taskinfo().has_partitionid());
        return _taskStatus.taskinfo().partitionid();
    }

    protocol::PartitionStatus getPartitionStatus() const {
        autil::ScopedLock lock(_mutex);
        return _taskStatus.status();
    }

    void setPartitionStatus(protocol::PartitionStatus status) {
        autil::ScopedLock lock(_mutex);
        _taskStatus.set_status(status);
    }

    void setError(protocol::ErrorCode errorCode, const std::string &errorMsg = "") {
        autil::ScopedLock lock(_mutex);
        _taskStatus.set_errcode(errorCode);
        _taskStatus.set_errtime(autil::TimeUtility::currentTime());
        _taskStatus.set_errmsg(errorMsg);
    }

    void setSessionId(int64_t sessionId) {
        autil::ScopedLock lock(_mutex);
        _taskStatus.set_sessionid(sessionId);
    }

    protocol::ErrorCode getErrorCode() const {
        autil::ScopedLock lock(_mutex);
        assert(_taskStatus.has_errcode());
        return _taskStatus.errcode();
    }

    const protocol::TaskStatus getTaskStatus() const {
        autil::ScopedLock lock(_mutex);
        return _taskStatus;
    }

    bool sealed() const {
        autil::ScopedLock lock(_mutex);
        return _taskStatus.taskinfo().sealed();
    }

    bool enableTTlDel() const {
        autil::ScopedLock lock(_mutex);
        assert(_taskStatus.has_taskinfo());
        return _taskStatus.taskinfo().enablettldel();
    }

    bool versionControl() const {
        autil::ScopedLock lock(_mutex);
        return _taskStatus.taskinfo().has_versioncontrol() && _taskStatus.taskinfo().versioncontrol();
    }

private:
    mutable autil::ThreadMutex _mutex;
    protocol::TaskStatus _taskStatus;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ThreadSafeTaskStatus);

} // namespace heartbeat
} // namespace swift
