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
#include "suez/table/ReadWriteTableManager.h"

#include <algorithm>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "suez/heartbeat/HeartbeatTarget.h"
#include "suez/sdk/TableWriter.h"
#include "suez/table/Commit.h"
#include "suez/table/LeaderElectionManager.h"
#include "suez/table/SyncVersion.h"
#include "suez/table/TodoRunner.h"
#include "suez/table/VersionManager.h"
#include "suez/table/VersionPublisher.h"
#include "suez/table/VersionSynchronizer.h"

using namespace std;
using namespace autil;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, ReadWriteTableManager);

ReadWriteTableManager::ReadWriteTableManager()
    : _versionManager(nullptr), _leaderElectionMgr(nullptr), _versionSync(nullptr) {}

ReadWriteTableManager::~ReadWriteTableManager() {
    // clear tables first
    stop();
}

bool ReadWriteTableManager::init(const InitParam &param) {
    if (!TableManager::init(param)) {
        return false;
    }

    _todoListExecutor->addRunner(std::make_unique<AsyncRunner>(param.deployThreadPool, "sync_version"),
                                 {OP_SYNC_VERSION});

    _versionPublisher = VersionPublisher::create();
    if (!_versionPublisher) {
        AUTIL_LOG(ERROR, "create version publisher failed");
        return false;
    }

    return true;
}

UPDATE_RESULT ReadWriteTableManager::generateTodoList(const HeartbeatTarget &target,
                                                      const HeartbeatTarget &finalTarget,
                                                      TodoList &todoList) {
    auto ret = TableManager::generateTodoList(target, finalTarget, todoList);

    generateSyncVersionOperations(target.getTableMetas(), todoList);
    return ret;
}

void ReadWriteTableManager::setLeaderElectionManager(LeaderElectionManager *leaderElectionMgr) {
    _leaderElectionMgr = leaderElectionMgr;
}

void ReadWriteTableManager::setVersionManager(VersionManager *versionMgr) { _versionManager = versionMgr; }

void ReadWriteTableManager::setVersionSynchronizer(VersionSynchronizer *versionSync) { _versionSync = versionSync; }

void ReadWriteTableManager::setGigRpcServer(multi_call::GigRpcServer *gigRpcServer) { _gigRpcServer = gigRpcServer; }

void ReadWriteTableManager::setZoneName(string zoneName) { _zoneName = zoneName; }

void ReadWriteTableManager::generateSyncVersionOperations(const TableMetas &tableMetas, TodoList &todoList) const {
    for (const auto &it : tableMetas) {
        const auto &pid = it.first;
        const auto &target = it.second;

        if (target.getTableMode() != TM_READ_WRITE) {
            continue;
        }

        auto table = getTable(pid);
        if (!table) {
            continue;
        }
        int event = SyncVersion::SE_NONE;

        // check is rollback
        const auto &current = table->getPartitionMeta();
        IncVersion headVersionId = INVALID_VERSION;
        TableVersion headTableVersion;
        if (_versionManager->getHeadVersion(pid, headTableVersion)) {
            headVersionId = headTableVersion.getVersionId();
        }
        if (table->getTableStatus() == TS_LOADED && headVersionId > current.getIncVersion() &&
            current.getIncVersion() != INVALID_VERSION) {
            event = event | SyncVersion::SE_ROLLBACK;
        }

        // check has commit version
        TableVersion publishedVersion;
        if (!(_versionManager->getPublishedVersion(pid, publishedVersion) && publishedVersion.isFinished())) {
            event |= SyncVersion::SE_COMMIT;
        }
        if (event != SyncVersion::SE_NONE) {
            todoList.addOperation(std::make_shared<SyncVersion>(
                table, target, _leaderElectionMgr, _versionManager, _versionPublisher.get(), _versionSync, event));
        }
    }
}

void ReadWriteTableManager::handleCommitResult(const PartitionId &pid, const std::pair<bool, TableVersion> &result) {
    if (result.first && result.second.isLeaderVersion()) {
        const auto &version = result.second;
        AUTIL_LOG(
            INFO, "partition[%s] commit version: %s", ToJsonString(pid, true).c_str(), version.toString().c_str());
        _versionManager->updateLocalVersion(pid, version);
    } else if (!result.first) {
        AUTIL_LOG(WARN, "partition[%s] commit version failed, release leader", ToJsonString(pid, true).c_str());
        _leaderElectionMgr->releaseLeader(pid);
    }
}

TablePtr ReadWriteTableManager::createTable(const PartitionId &pid, const PartitionMeta &meta) {
    if (meta.getTableMode() == TM_READ_WRITE) {
        TableVersion leaderVersion, publishedVersion;
        if (!SyncVersion::recoverVersion(
                pid, meta, _versionSync, _versionPublisher.get(), leaderVersion, publishedVersion)) {
            AUTIL_LOG(WARN, "recover version for %s failed", ToJsonString(pid, true).c_str());
            return TablePtr();
        }

        auto fn = [this](const PartitionId &pid, bool isLeader) mutable { leaderChange(pid, isLeader); };
        if (!publishedVersion.isFinished() && !_leaderElectionMgr->registerPartition(pid, std::move(fn))) {
            AUTIL_LOG(WARN, "start leader election for %s failed", ToJsonString(pid, true).c_str());
            return TablePtr();
        }

        if (leaderVersion.getVersionId() != INVALID_VERSION) {
            _versionManager->updateLeaderVersion(pid, leaderVersion);
        }
        if (publishedVersion.getVersionId() != INVALID_VERSION) {
            _versionManager->updatePublishedVersion(pid, publishedVersion);
        }
    }

    auto t = TableManager::createTable(pid, meta);
    t->setLeaderElectionManager(_leaderElectionMgr);
    auto leaderInfoPublisher = make_shared<TableLeaderInfoPublisher>(pid, _zoneName, _gigRpcServer, _leaderElectionMgr);
    t->setLeaderInfoPublisher(leaderInfoPublisher);
    return t;
}

void ReadWriteTableManager::removeTable(const PartitionId &pid) {
    AUTIL_LOG(INFO, "remove table %s", ToJsonString(pid, true).c_str());
    _leaderElectionMgr->removePartition(pid);
    _versionManager->remove(pid);
    _versionSync->remove(pid);
    _versionPublisher->remove(pid);
    TableManager::removeTable(pid);
}

void ReadWriteTableManager::releaseLeader() {
    AUTIL_LOG(INFO, "releaseLeader begin");
    for (const auto &it : _tableMap) {
        const auto &pid = it.first;
        _leaderElectionMgr->releaseLeader(pid);
        it.second->getLeaderInfoPublisher()->publish();
    }
    AUTIL_LOG(INFO, "releaseLeader end");
}

void ReadWriteTableManager::leaderChange(const PartitionId &pid, bool isLeader) {
    auto table = getTable(pid);
    if (!table) {
        return;
    }
    if (table->getLeaderInfoPublisher()->publish()) {
        AUTIL_LOG(INFO, "[%s]: update leader info success, isLeader: [%d]", ToJsonString(pid, true).c_str(), isLeader);
        auto writer = table->getTableWriter();
        if (writer) {
            // TODO: 1. maybe update writer version
            //       2. change writer states before publish leader info
            bool enableWrite = isLeader || _scheduleConfig.allowFollowerWrite;
            writer->setEnableWrite(enableWrite);
            writer->setRoleType(isLeader ? RT_LEADER : RT_FOLLOWER);
        }
    } else {
        AUTIL_LOG(INFO, "[%s]: update leader info failed, isLeader: [%d]", ToJsonString(pid, true).c_str(), isLeader);
    }
}

void ReadWriteTableManager::processTableIndications(const set<PartitionId> &releaseLeaderTables,
                                                    const set<PartitionId> &campaginLeaderTables,
                                                    const set<PartitionId> &publishLeaderTables,
                                                    const map<PartitionId, int32_t> &tablesWeight) {
    for (const auto &pid : releaseLeaderTables) {
        AUTIL_LOG(INFO, "release leader for pid[%s]", ToJsonString(pid, true).c_str());
        _leaderElectionMgr->releaseLeader(pid);
    }
    for (const auto &pid : campaginLeaderTables) {
        AUTIL_LOG(INFO, "set allow campagin leader for pid[%s]", ToJsonString(pid, true).c_str());
        _leaderElectionMgr->setCampaginLeaderIndication(pid, true);
        auto table = getTable(pid);
        if (table) {
            table->reportStatus();
        }
    }
    for (const auto &pid : publishLeaderTables) {
        auto table = getTable(pid);
        if (table) {
            AUTIL_LOG(INFO, "set allow publish leader for pid[%s]", ToJsonString(pid, true).c_str());
            table->getLeaderInfoPublisher()->setAllowPublishLeader(true);
            table->reportStatus();
        }
    }
    for (const auto &kv : tablesWeight) {
        auto table = getTable(kv.first);
        if (table) {
            AUTIL_LOG(INFO, "set weight[%d] for pid[%s]", kv.second, ToJsonString(kv.first, true).c_str());
            table->getLeaderInfoPublisher()->updateWeight(kv.second);
        }
    }
}

} // namespace suez
