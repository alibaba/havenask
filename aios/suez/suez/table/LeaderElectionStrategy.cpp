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
#include "suez/table/LeaderElectionStrategy.h"

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/MurmurHash.h"
#include "autil/StringUtil.h"
#include "suez/sdk/CmdLineDefine.h"
#include "suez/table/TablePathDefine.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, LeaderElectionStrategy);

LeaderElectionStrategy::~LeaderElectionStrategy() {}

class TableBasedStrategy : public LeaderElectionStrategy {
public:
    uint64_t getKey(const PartitionId &pid) const override {
        auto path = getPath(pid);
        return autil::MurmurHash::MurmurHash64A(path.c_str(), path.length(), 0);
    }
    std::string getPath(const PartitionId &pid) const override {
        return TablePathDefine::constructPartitionLeaderElectPath(pid, false);
    }

    LeaderElectionStrategyType getType() const override { return ST_TABLE; }

public:
    static const std::string TYPE;
};
const std::string TableBasedStrategy::TYPE = "table";

class TableIgnoreVersionStrategy final : public TableBasedStrategy {
public:
    std::string getPath(const PartitionId &pid) const override {
        return TablePathDefine::constructPartitionLeaderElectPath(pid, true);
    }

    LeaderElectionStrategyType getType() const override { return ST_TABLE_IGNORE_VERSION; }

public:
    static const std::string TYPE;
};
const std::string TableIgnoreVersionStrategy::TYPE = "table_ignore_version";

class RangeBasedStrategy final : public LeaderElectionStrategy {
public:
    uint64_t getKey(const PartitionId &pid) const override {
        uint64_t key = pid.from;
        key <<= 32;
        key |= pid.to;
        return key;
    }
    std::string getPath(const PartitionId &pid) const override {
        return autil::StringUtil::toString(pid.from) + "_" + autil::StringUtil::toString(pid.to);
    }

    LeaderElectionStrategyType getType() const override { return ST_RANGE; }

public:
    static const std::string TYPE;
};
const std::string RangeBasedStrategy::TYPE = "range";

class WorkerBasedStrategy final : public LeaderElectionStrategy {
public:
    WorkerBasedStrategy(const std::string &roleName) : _roleName(roleName) {}

public:
    uint64_t getKey(const PartitionId &pid) const override { return 0; }
    std::string getPath(const PartitionId &pid) const override { return _roleName; }

    LeaderElectionStrategyType getType() const override { return ST_WORKER; }

public:
    static const std::string TYPE;

private:
    std::string _roleName;
};
const std::string WorkerBasedStrategy::TYPE = "worker";

LeaderElectionStrategy *LeaderElectionStrategy::create(const std::string &type) {
    if (type == TableBasedStrategy::TYPE) {
        AUTIL_LOG(INFO, "create a table based LeaderElectionStrategy");
        return new TableBasedStrategy();
    } else if (type == TableIgnoreVersionStrategy::TYPE) {
        AUTIL_LOG(INFO, "create a table ignore version LeaderElectionStrategy");
        return new TableIgnoreVersionStrategy();
    } else if (type.empty() || type == RangeBasedStrategy::TYPE) {
        AUTIL_LOG(INFO, "create a range based LeaderElectionStrategy");
        return new RangeBasedStrategy();
    } else if (type == WorkerBasedStrategy::TYPE) {
        auto roleName = autil::EnvUtil::getEnv(ROLE_NAME, "");
        if (roleName.empty()) {
            AUTIL_LOG(ERROR, "worker based leader election strategy must has role name");
            return nullptr;
        }
        return new WorkerBasedStrategy(roleName);
    } else {
        AUTIL_LOG(ERROR, "unsupported LeaderElectionStrategy type: %s", type.c_str());
        return nullptr;
    }
}

} // namespace suez
