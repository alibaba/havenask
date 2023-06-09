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
#include <memory>
#include <set>
#include <stddef.h>
#include <stdint.h>

#include "autil/Lock.h"
#include "suez/table/InnerDef.h"
#include "suez/table/ScheduleConfig.h"
#include "suez/table/SuezPartition.h"
#include "suez/table/TableLeaderInfoPublisher.h"
#include "suez/table/TableMeta.h"
#include "suez/table/TableVersion.h"

namespace suez {
class DataOptionWrapper;
class LeaderElectionManager;
class SuezPartitionFactory;
class TableWriter;

// TODO: maybe remove Table, TableManager use std::map<PartitionId, SuezPartitionPtr> directly
class Table {
public:
    Table(const TableResource &tableResource, SuezPartitionFactory *factory);
    Table(const TableResource &tableResource);
    ~Table();

private:
    Table(const Table &);
    Table &operator=(const Table &);

public:
    const PartitionId &getPid() const;
    std::string getIdentifier() const;

public:
    void init(const TargetPartitionMeta &target);
    void deploy(const TargetPartitionMeta &target, bool distDeploy);
    void cancelDeploy();
    void cancelLoad();
    void load(const TargetPartitionMeta &target);
    void forceLoad(const TargetPartitionMeta &target);
    void reload(const TargetPartitionMeta &target);
    void preload(const TargetPartitionMeta &target);
    void unload();
    void updateRt(const TargetPartitionMeta &target);
    void setKeepCount(size_t keepCount);
    void setConfigKeepCount(size_t configKeepCount);

    void finalTargetToTarget();

    // for write
    bool needCommit() const;
    std::pair<bool, TableVersion> commit();
    void becomeLeader(const TargetPartitionMeta &target);
    void nolongerLeader(const TargetPartitionMeta &target);

    TableStatus getTableStatus() const;
    DeployStatus getTableDeployStatus(IncVersion incVersion) const;
    SuezPartitionDataPtr getSuezPartitionData(bool allowForceLoad, const TargetPartitionMeta &target) const;
    SuezPartitionPtr getSuezPartition() const;
    std::shared_ptr<TableWriter> getTableWriter() const;
    CurrentPartitionMeta getPartitionMeta() const;
    SuezPartitionType determineSuezPartitionType(const TargetPartitionMeta &target) const;
    void cleanIncVersion(const std::set<IncVersion> &inuseVersions);
    void shutdown();
    bool targetReached(const TargetPartitionMeta &target, bool allowForceLoad) const;

    void setLeaderElectionManager(LeaderElectionManager *leaderElectionMgr);
    void setLeaderInfoPublisher(const std::shared_ptr<TableLeaderInfoPublisher> &leaderInfoPublisher);

    void reportStatus();
    void setAllowFollowerWrite(bool allowFollowerWrite);

    const std::shared_ptr<TableLeaderInfoPublisher> &getLeaderInfoPublisher() const { return _leaderInfoPublisher; }
    bool isRecovered() const;

private:
    bool tryReload(const TargetPartitionMeta &target);
    void doRoleSwitch(const TargetPartitionMeta &target);
    void updateTableStatus(const StatusAndError<TableStatus> &status);
    void setTableStatus(TableStatus status);

    static bool
    targetReached(const TargetPartitionMeta &target, const CurrentPartitionMeta &current, bool allowForceLoad);

    void reportLatency(const std::string &op, int64_t latency);
    bool isAvailable(bool allowForceLoady, const TargetPartitionMeta &target) const;
    void flushDeployStatusMetric() const;

private:
    TableResource _tableResource;
    SuezPartitionPtr _suezPartition;
    CurrentPartitionMetaPtr _partitionMeta;
    SuezPartitionFactory *_factory = nullptr;
    bool _ownFactory = false;
    LeaderElectionManager *_leaderElectionMgr = nullptr;
    std::shared_ptr<TableLeaderInfoPublisher> _leaderInfoPublisher;
    bool _allowFollowerWrite = false;

    struct SwitchState;
    std::shared_ptr<SwitchState> _switchState;
    mutable autil::ThreadMutex _switchMutex;
};

using TablePtr = std::shared_ptr<Table>;

} // namespace suez
