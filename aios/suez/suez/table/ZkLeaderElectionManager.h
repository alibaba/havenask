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
#include <map>
#include <memory>

#include "aios/network/gig/multi_call/common/common.h"
#include "autil/Lock.h"
#include "suez/table/LeaderElectionManager.h"
#include "worker_framework/LeaderElector.h"

namespace cm_basic {
class ZkWrapper;
}

namespace suez {

class LeaderElectionStrategy;

using LeaderElector = worker_framework::LeaderElector;

struct LeaderElectorWrapper {
    std::unique_ptr<cm_basic::ZkWrapper> zkWrapper;
    std::unique_ptr<LeaderElector> impl;
    std::map<PartitionId, LeaderChangeHandler> subscribers;
    // state for release leader
    mutable bool forceFollower;
    mutable int64_t forceFollowerExpireTime;
    mutable autil::ThreadMutex mutex;
    int64_t forceFollowerTimeInMs;
    int64_t becomeLeaderTimestamp;
    bool hasStarted;

    LeaderElectorWrapper(std::unique_ptr<cm_basic::ZkWrapper> zk, std::unique_ptr<LeaderElector> leaderElector);
    ~LeaderElectorWrapper();

    bool isLeader() const;

    void releaseLeader();

    bool start();
    void stop();

    size_t getRefCount() const;
    void registerPartition(const PartitionId &pid, LeaderChangeHandler handler);
    void removePartition(const PartitionId &pid);

    void becomeLeader();
    void nolongerLeader();

    int64_t getLeaderTimestamp() const;

    void leaderChange(bool becomeLeader);
};

class ZkLeaderElectionManager : public LeaderElectionManager {
public:
    ZkLeaderElectionManager(const LeaderElectionConfig &config);
    ~ZkLeaderElectionManager();

private:
    ZkLeaderElectionManager(const ZkLeaderElectionManager &) = delete;
    ZkLeaderElectionManager &operator=(const ZkLeaderElectionManager &) = delete;

public:
    bool init() override;
    void stop() override;

    bool registerPartition(const PartitionId &pid, LeaderChangeHandler handler = nullptr) override;
    void removePartition(const PartitionId &pid) override;

    void releaseLeader(const PartitionId &pid) override;

    RoleType getRoleType(const PartitionId &pid) const override;
    void seal(const PartitionId &pid) override;
    int64_t getLeaderTimestamp(const PartitionId &pid) override;
    void allowCampaginLeader(const PartitionId &pid) override;
    void setCampaginLeaderIndication(const PartitionId &pid, bool allowCampaginLeader) override;
    std::pair<bool, int64_t> getLeaderElectionInfo(const PartitionId &pid) override;

public:
    LeaderElectorWrapper *findLeaderElectorLocked(const PartitionId &pid);
    const LeaderElectorWrapper *findLeaderElectorLocked(const PartitionId &pid) const;

    // for test
    LeaderElectorWrapper *getLeaderElector(const PartitionId &pid);

private:
    std::unique_ptr<LeaderElectorWrapper> createLeaderElectorWrapper(const PartitionId &pid);
    std::unique_ptr<cm_basic::ZkWrapper> createZkWrapper() const;

private:
    std::string _leaderElectionRoot;
    std::string _leaderInfoRoot;
    std::unique_ptr<LeaderElectionStrategy> _strategy;
    std::unique_ptr<cm_basic::ZkWrapper> _zkWrapper;

    mutable autil::ReadWriteLock _lock;
    std::map<uint64_t, std::unique_ptr<LeaderElectorWrapper>> _leaderElectors;
    struct AllowLeaderInfo {
        bool allowCampaginLeader = false;
        bool campaginStatusReady = false;
    };
    std::map<PartitionId, AllowLeaderInfo> _allowLeaderInfos;
};

} // namespace suez
