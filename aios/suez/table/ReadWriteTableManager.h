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

#include <memory>

#include "suez/table/TableManager.h"

namespace multi_call {
class GigRpcServer;
};

namespace suez {

class LeaderElectionManager;
class VersionManager;
class VersionPublisher;
class VersionSynchronizer;

class ReadWriteTableManager : public TableManager {
public:
    ReadWriteTableManager();
    ~ReadWriteTableManager();

public:
    bool init(const InitParam &param) override;
    void releaseLeader();

    void setLeaderElectionManager(LeaderElectionManager *leaderElectionMgr);
    void setVersionManager(VersionManager *versionMgr);
    void setVersionSynchronizer(VersionSynchronizer *versionSync);
    void setGigRpcServer(multi_call::GigRpcServer *gigRpcServer);
    void setZoneName(std::string zoneName);
    void processTableIndications(const std::set<PartitionId> &releaseLeaderTables,
                                 const std::set<PartitionId> &campaginLeaderTables,
                                 const std::set<PartitionId> &publishLeaderTables,
                                 const std::map<PartitionId, int32_t> &tablesWeight) override;

protected:
    UPDATE_RESULT
    generateTodoList(const HeartbeatTarget &target, const HeartbeatTarget &finalTarget, TodoList &todoList) override;
    TablePtr createTable(const PartitionId &pid, const PartitionMeta &meta) override;
    void removeTable(const PartitionId &pid) override;

private:
    void generateSyncVersionOperations(const TableMetas &tableMetas, TodoList &todoList) const;

    void handleCommitResult(const PartitionId &pid, const std::pair<bool, TableVersion> &version) override;
    void leaderChange(const PartitionId &pid, bool isLeader);

private:
    std::string _zoneName;
    VersionManager *_versionManager;                   // owned by TaskExecutor
    LeaderElectionManager *_leaderElectionMgr;         // owned by TaskExecutor
    VersionSynchronizer *_versionSync;                 // owned by TaskExecutor
    multi_call::GigRpcServer *_gigRpcServer = nullptr; // owned by TaskExecutor
    std::unique_ptr<VersionPublisher> _versionPublisher;
};

} // namespace suez
