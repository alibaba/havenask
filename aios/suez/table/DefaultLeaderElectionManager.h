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

#include "suez/table/LeaderElectionManager.h"

namespace suez {

class DefaultLeaderElectionManager : public LeaderElectionManager {
public:
    DefaultLeaderElectionManager(RoleType roleType = RT_LEADER);
    ~DefaultLeaderElectionManager();

private:
    DefaultLeaderElectionManager(const DefaultLeaderElectionManager &);
    DefaultLeaderElectionManager &operator=(const DefaultLeaderElectionManager &);

public:
    bool init() override { return true; }
    void stop() override {}

    bool registerPartition(const PartitionId &pid, LeaderChangeHandler handler = nullptr) override { return true; }
    void removePartition(const PartitionId &pid) override {}

    void releaseLeader(const PartitionId &pid) override {}

    RoleType getRoleType(const PartitionId &pid) const override { return _roleType; }
    int64_t getLeaderTimestamp(const PartitionId &pid) override { return 0; }
    void seal(const PartitionId &pid) override {}
    void allowCampaginLeader(const PartitionId &pid) override {}
    void setCampaginLeaderIndication(const PartitionId &pid, bool allowCampaginLeader) override {}
    std::pair<bool, int64_t> getLeaderElectionInfo(const PartitionId &pid) override { return std::make_pair(false, 0); }

private:
    RoleType _roleType;
};

} // namespace suez
