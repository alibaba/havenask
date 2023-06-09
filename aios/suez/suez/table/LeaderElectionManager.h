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

#include <functional>

#include "suez/sdk/PartitionId.h"
#include "suez/table/LeaderElectionConfig.h"
#include "suez/table/TableMeta.h"

namespace multi_call {
class GigRpcServer;
}

namespace suez {

using LeaderChangeHandler = std::function<void(const PartitionId &, bool becomeLeader)>;

class LeaderElectionManager {
public:
    LeaderElectionManager(const LeaderElectionConfig &config);
    virtual ~LeaderElectionManager();

public:
    virtual bool init() = 0;
    virtual void stop() = 0;

    virtual bool registerPartition(const PartitionId &pid, LeaderChangeHandler handler = nullptr) = 0;
    virtual void removePartition(const PartitionId &pid) = 0;
    virtual void seal(const PartitionId &pid) = 0;
    virtual void releaseLeader(const PartitionId &pid) = 0;

    virtual RoleType getRoleType(const PartitionId &pid) const = 0;
    virtual int64_t getLeaderTimestamp(const PartitionId &pid) = 0;
    virtual void allowCampaginLeader(const PartitionId &pid) = 0;
    virtual void setCampaginLeaderIndication(const PartitionId &pid, bool allowCampaginLeader) = 0;
    virtual std::pair<bool, int64_t> getLeaderElectionInfo(const PartitionId &pid) = 0;

public:
    bool isLeader(const PartitionId &pid) const { return getRoleType(pid) == RT_LEADER; }
    bool isFollower(const PartitionId &pid) const { return getRoleType(pid) == RT_FOLLOWER; }
    void setLeaderInfo(const std::string &leaderInfo) { _leaderInfo = leaderInfo; }
    const LeaderElectionConfig &getLeaderElectionConfig() const { return _config; }

public:
    static LeaderElectionManager *create(const std::string &zoneName);

protected:
    std::string _leaderInfo;
    LeaderElectionConfig _config;
};

} // namespace suez
