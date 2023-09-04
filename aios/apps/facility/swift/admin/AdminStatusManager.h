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

#include <atomic>
#include <functional>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/Thread.h"

namespace swift {
namespace admin {
enum MasterSlaveStatus {
    MS_UNKNOWN = 0,
    MS_MASTER = 1,
    MS_SLAVE = 2
};

enum LeaderFollowerStatus {
    LF_UNKNOWN = 3,
    LF_LEADER = 4,
    LF_FOLLOWER = 5
};

class Status {
public:
    Status();
    Status(MasterSlaveStatus ms);
    Status(LeaderFollowerStatus lf);
    Status(MasterSlaveStatus ms, LeaderFollowerStatus lf);

public:
    bool isMaster() const;
    bool isLeader() const;
    bool operator==(const Status &other);
    bool operator!=(const Status &other);
    std::string toString() const;

public:
    MasterSlaveStatus msStatus;
    LeaderFollowerStatus lfStatus;
};

class AdminStatusManager {

    using StatusUpdateHandler = std::function<bool(Status lastStaus, Status currentStatus)>;

public:
    AdminStatusManager();
    ~AdminStatusManager();
    AdminStatusManager(const AdminStatusManager &) = delete;
    AdminStatusManager &operator=(const AdminStatusManager &) = delete;

public:
    bool start();
    void stop();
    bool transition();
    void setStatusUpdateHandler(StatusUpdateHandler handler);
    void setMaster(bool isMaster);
    void setLeader(bool isLeader);
    bool isMaster() const;
    bool isLeader() const;

private:
    void workLoop();

private:
    mutable autil::ReadWriteLock _lock;
    Status _lastStatus;
    Status _currentStatus;
    autil::Notifier _notifier;
    StatusUpdateHandler _handler;
    std::atomic_bool _stopped = true;
    autil::ThreadPtr _transitionThread;

private:
    static constexpr int64_t TRANSITION_WAIT_TIME = 10 * 1000 * 1000;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace admin
} // namespace swift
