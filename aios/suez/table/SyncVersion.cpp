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
#include "suez/table/SyncVersion.h"

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/VersionLoader.h"
#include "suez/common/TablePathDefine.h"
#include "suez/table/LeaderElectionManager.h"
#include "suez/table/VersionManager.h"
#include "suez/table/VersionPublisher.h"
#include "suez/table/VersionSynchronizer.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SyncVersion);

SyncVersion::SyncVersion(const TablePtr &table,
                         const TargetPartitionMeta &target,
                         LeaderElectionManager *leaderElectionMgr,
                         VersionManager *versionMgr,
                         VersionPublisher *versionPublisher,
                         VersionSynchronizer *versionSync,
                         int event)
    : TodoWithTarget(OP_SYNC_VERSION, table, target)
    , _leaderElectionMgr(leaderElectionMgr)
    , _versionMgr(versionMgr)
    , _versionPublisher(versionPublisher)
    , _versionSync(versionSync)
    , _event(event) {}

void SyncVersion::run() {
    if (_event & SyncEvent::SE_ROLLBACK) {
        if (!syncRollbackVersion()) {
            AUTIL_LOG(WARN, "[%s]: sync rollback  version failed", FastToJsonString(getPartitionId(), true).c_str());
            return;
        }
    }
    if (_event & SyncEvent::SE_COMMIT) {
        syncCommitVersion();
    }
}

bool SyncVersion::syncRollbackVersion() {
    AUTIL_LOG(DEBUG, "syncRollbackVersion begin");
    if (!isLeader()) {
        AUTIL_LOG(DEBUG, "syncRollbackVersion stop, is not leader");
        return true;
    }
    std::vector<TableVersion> versions;
    const auto &pid = getPartitionId();
    if (!_versionSync->getVersionList(pid, versions)) {
        AUTIL_LOG(ERROR, "[%s]: get version list  failed  ", FastToJsonString(pid, true).c_str());
        return false;
    }
    auto currentVersionId = _table->getPartitionMeta().getIncVersion();
    if (currentVersionId == INVALID_VERSION) {
        AUTIL_LOG(ERROR, "[%s]: current version id invalid  ", FastToJsonString(pid, true).c_str());
        return false;
    }
    while (!versions.empty() && versions.back().getVersionId() > currentVersionId) {
        versions.pop_back();
    }
    if (!_versionSync->updateVersionList(pid, versions)) {
        AUTIL_LOG(ERROR, "[%s]: update version list failed  ", FastToJsonString(pid, true).c_str());
    }
    AUTIL_LOG(INFO,
              "[%s]: update version list to [%s] success",
              FastToJsonString(pid, true).c_str(),
              FastToJsonString(versions, true).c_str());
    TableVersion headVersion;
    if (!versions.empty()) {
        headVersion = versions.back();
    }
    _versionMgr->updateHeadVersion(pid, headVersion);
    AUTIL_LOG(DEBUG, "syncRollbackVersion end");
    return true;
}

void SyncVersion::syncCommitVersion() {
    AUTIL_LOG(DEBUG, "syncCommitVersion begin");
    PartitionVersion version;
    const auto &pid = getPartitionId();
    _versionMgr->getVersion(pid, version);
    if (isLeader()) {
        syncForLeader(version);
    } else {
        syncForFollower(version);
    }
    if (version.publishedVersion.isFinished()) {
        // only table strategy need optimize
        // TODO: maybe use a freeze interface, which keeps the original state
        AUTIL_LOG(INFO, "[%s]: published version is sealed, stop leader election", FastToJsonString(pid, true).c_str());
        _leaderElectionMgr->seal(pid);
    }
    AUTIL_LOG(DEBUG, "syncCommitVersion end");
}

void SyncVersion::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    // log太多，冲掉有价值的信息
    bool logEnabled = autil::EnvUtil::getEnv("debug_sync_version", false);
    if (logEnabled) {
        TodoWithTarget::Jsonize(json);
    }
}

bool SyncVersion::isLeader() const { return _leaderElectionMgr->getRoleType(getPartitionId()) == RT_LEADER; }

bool SyncVersion::updateVersionList(const TableVersion &newVersion) {
    const auto &pid = getPartitionId();
    std::vector<TableVersion> versions;
    if (!_versionSync->getVersionList(pid, versions)) {
        AUTIL_LOG(WARN, "[%s]: get list failed, retry later", FastToJsonString(pid, true).c_str());
        return false;
    }
    std::set<versionid_t> versionsInIndexRoot;
    std::string partitionRoot = TablePathDefine::constructIndexPath(_target.getRawIndexRoot(), pid);
    auto dir = indexlib::file_system::Directory::GetPhysicalDirectory(partitionRoot);
    fslib::FileList fileList;
    auto status = indexlibv2::framework::VersionLoader::ListVersion(dir, &fileList);
    if (!status.IsOK()) {
        AUTIL_LOG(
            WARN, "[%s]: list index root [%s] failed", FastToJsonString(pid, true).c_str(), partitionRoot.c_str());
        return false;
    }
    for (auto &fileName : fileList) {
        versionsInIndexRoot.insert(indexlibv2::framework::VersionLoader::GetVersionId(fileName));
    }

    std::vector<TableVersion> newVersions;
    for (const auto &version : versions) {
        if (versionsInIndexRoot.find(version.getVersionId()) != versionsInIndexRoot.end()) {
            newVersions.push_back(version);
        }
    }
    newVersions.emplace_back(newVersion);
    if (!_versionSync->updateVersionList(pid, newVersions)) {
        AUTIL_LOG(WARN,
                  "[%s]: update version list failed, retry later, version[%s]",
                  FastToJsonString(pid, true).c_str(),
                  FastToJsonString(newVersion, true).c_str());
        return false;
    }
    AUTIL_LOG(INFO,
              "[%s]: update version list updated, version[%s]",
              FastToJsonString(pid, true).c_str(),
              FastToJsonString(newVersion, true).c_str());
    return true;
}

void SyncVersion::syncForLeader(PartitionVersion &version) {
    const auto &pid = getPartitionId();
    // persist local version to persistent storage
    if (version.localVersion.getVersionId() > version.leaderVersion.getVersionId()) {
        std::string localConfigPath =
            TablePathDefine::constructLocalConfigPath(pid.getTableName(), _target.getConfigPath());
        if (version.localVersion.isLeaderVersion() &&
            _versionSync->persistVersion(pid, localConfigPath, version.localVersion)) {
            AUTIL_LOG(INFO,
                      "[%s]: leader version updated from %s to %s",
                      FastToJsonString(pid, true).c_str(),
                      FastToJsonString(version.leaderVersion, true).c_str(),
                      FastToJsonString(version.localVersion, true).c_str());
            _versionMgr->updateLeaderVersion(pid, version.localVersion);
            version.leaderVersion = version.localVersion;
            if (!updateVersionList(version.localVersion)) {
                AUTIL_LOG(INFO, "[%s]: update version list failed, retry later", FastToJsonString(pid, true).c_str());
                return;
            }
        }
    }
    if (version.leaderVersion.getVersionId() > version.publishedVersion.getVersionId()) {
        if (_versionPublisher->publish(_target.getRawIndexRoot(), pid, version.leaderVersion)) {
            AUTIL_LOG(INFO,
                      "[%s]: published version updated from %s to %s",
                      FastToJsonString(pid, true).c_str(),
                      FastToJsonString(version.publishedVersion, true).c_str(),
                      FastToJsonString(version.leaderVersion, true).c_str());
            _versionMgr->updatePublishedVersion(pid, version.leaderVersion);
            version.publishedVersion = version.leaderVersion;
        }
    }
}

void SyncVersion::syncForFollower(PartitionVersion &version) {
    const auto &pid = getPartitionId();
    if (!version.leaderVersion.isFinished() && _versionSync->supportSyncFromPersist(_target)) {
        TableVersion leaderVersion;
        if (_versionSync->syncFromPersist(pid, _target.getConfigPath(), leaderVersion) &&
            leaderVersion.getVersionId() > version.leaderVersion.getVersionId()) {
            AUTIL_LOG(INFO,
                      "[%s]: leader version updated from %s to %s",
                      FastToJsonString(pid, true).c_str(),
                      FastToJsonString(version.leaderVersion, true).c_str(),
                      FastToJsonString(leaderVersion, true).c_str());
            version.leaderVersion = leaderVersion;
            _versionMgr->updateLeaderVersion(pid, version.leaderVersion);
        }
    }
    if (version.leaderVersion.isFinished() && !version.publishedVersion.isFinished()) {
        if (_versionPublisher->getPublishedVersion(
                _target.getRawIndexRoot(), pid, version.leaderVersion.getVersionId(), version.publishedVersion)) {
            AUTIL_LOG(INFO,
                      "[%s]: the sealed version %s is published",
                      FastToJsonString(pid, true).c_str(),
                      FastToJsonString(version.publishedVersion, true).c_str());
            _versionMgr->updatePublishedVersion(pid, version.publishedVersion);
        }
    }
}

bool SyncVersion::recoverVersion(const PartitionId &pid,
                                 const TargetPartitionMeta &target,
                                 VersionSynchronizer *versionSync,
                                 VersionPublisher *versionPublisher,
                                 TableVersion &leaderVersion,
                                 TableVersion &publishedVersion) {
    if (!versionSync->supportSyncFromPersist(target)) {
        AUTIL_LOG(INFO, "[%s] no need sync from remote", FastToJsonString(pid, true).c_str());
        return true;
    }
    if (!versionSync->syncFromPersist(pid, target.getConfigPath(), leaderVersion)) {
        AUTIL_LOG(ERROR, "[%s]: recover leader version failed", FastToJsonString(pid, true).c_str());
        return false;
    }
    if (leaderVersion.isFinished()) {
        versionPublisher->getPublishedVersion(
            target.getRawIndexRoot(), pid, leaderVersion.getVersionId(), publishedVersion);
    }
    return true;
}

} // namespace suez
