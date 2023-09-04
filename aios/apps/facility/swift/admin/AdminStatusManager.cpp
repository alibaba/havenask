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
#include "swift/admin/AdminStatusManager.h"

#include <memory>
#include <utility>

#include "autil/TimeUtility.h"

namespace swift {
namespace admin {
using namespace autil;

AUTIL_LOG_SETUP(swift, AdminStatusManager);

Status::Status() : msStatus(MS_UNKNOWN), lfStatus(LF_UNKNOWN) {}

Status::Status(MasterSlaveStatus ms) : msStatus(ms), lfStatus(LF_UNKNOWN) {}

Status::Status(LeaderFollowerStatus lf) : msStatus(MS_UNKNOWN), lfStatus(lf) {}

Status::Status(MasterSlaveStatus ms, LeaderFollowerStatus lf) : msStatus(ms), lfStatus(lf) {}

bool Status::isMaster() const { return msStatus == MS_MASTER; }

bool Status::isLeader() const { return lfStatus == LF_LEADER; }

bool Status::operator==(const Status &other) {
    return this->msStatus == other.msStatus && this->lfStatus == other.lfStatus;
}
bool Status::operator!=(const Status &other) { return !(*this == other); }

std::string Status::toString() const {
    std::string str;
    if (msStatus == MS_UNKNOWN) {
        str = "UNKNOWN_";
    } else {
        str = (msStatus == MS_MASTER) ? "MASTER_" : "SLAVE_";
    }
    if (lfStatus == LF_UNKNOWN) {
        str += "UNKNOWN";
    } else {
        str += (lfStatus == LF_LEADER) ? "LEADER" : "FOLLOWER";
    }
    return str;
}

AdminStatusManager::AdminStatusManager() {}

AdminStatusManager::~AdminStatusManager() { stop(); }

bool AdminStatusManager::start() {
    if (!_stopped) {
        AUTIL_LOG(INFO, "admin status manager has already started.");
        return true;
    }
    if (!_transitionThread) {
        _transitionThread = Thread::createThread(std::bind(&AdminStatusManager::workLoop, this), "transition");
        if (!_transitionThread) {
            AUTIL_LOG(ERROR, "admin status manager start failed.");
            return false;
        }
    }
    _stopped = false;
    AUTIL_LOG(INFO, "admin status manager start.");
    return true;
}

void AdminStatusManager::stop() {
    if (_stopped) {
        AUTIL_LOG(INFO, "admin status manager has already stopped.");
        return;
    }
    _stopped = true;
    _notifier.notify();
    if (_transitionThread) {
        _transitionThread->join();
        _transitionThread.reset();
    }
    AUTIL_LOG(INFO, "admin status manager stop.");
}

bool AdminStatusManager::transition() {
    Status lastStatus, currentStatus;
    {
        ScopedReadLock lock(_lock);
        if (_currentStatus == _lastStatus) {
            AUTIL_LOG(DEBUG, "skip action, current equal target, status[%s]", _currentStatus.toString().c_str());
            return true;
        }
        AUTIL_LOG(INFO,
                  "admin status transition from[%s] to[%s] ",
                  _lastStatus.toString().c_str(),
                  _currentStatus.toString().c_str());
        lastStatus = _lastStatus;
        currentStatus = _currentStatus;
    }
    if (_handler) {
        if (!_handler(lastStatus, currentStatus)) {
            AUTIL_LOG(ERROR,
                      "invoke handler failed, lastStatus[%s] currentStatus[%s]",
                      lastStatus.toString().c_str(),
                      currentStatus.toString().c_str());
            return false;
        }
    }
    ScopedWriteLock lock(_lock);
    _lastStatus = currentStatus;
    return true;
}

void AdminStatusManager::setStatusUpdateHandler(StatusUpdateHandler handler) { _handler = std::move(handler); }

void AdminStatusManager::setMaster(bool isMaster) {
    ScopedWriteLock lock(_lock);
    auto status = isMaster ? MS_MASTER : MS_SLAVE;
    if (_currentStatus.msStatus != status) {
        _currentStatus.msStatus = status;
        _notifier.notify();
    }
}

void AdminStatusManager::setLeader(bool isLeader) {
    ScopedWriteLock lock(_lock);
    auto status = isLeader ? LF_LEADER : LF_FOLLOWER;
    if (_currentStatus.lfStatus != status) {
        _currentStatus.lfStatus = status;
        _notifier.notify();
    }
}

bool AdminStatusManager::isMaster() const {
    ScopedReadLock lock(_lock);
    return _currentStatus.isMaster();
}

bool AdminStatusManager::isLeader() const {
    ScopedReadLock lock(_lock);
    return _currentStatus.isLeader();
}

void AdminStatusManager::workLoop() {
    while (!_stopped) {
        if (!transition()) {
            AUTIL_LOG(WARN, "admin status transition failed.");
        }
        _notifier.waitNotification(TRANSITION_WAIT_TIME);
    }
}

} // namespace admin
} // namespace swift
