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
#include "suez/table/BsVersionSynchronizer.h"

#include "autil/Log.h"
#include "build_service/common/RemoteVersionCommitter.h"
#include "build_service/config/ResourceReader.h"
#include "fslib/fs/FileSystem.h"
#include "suez/table/TablePathDefine.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, BsVersionSynchronizer);

BsVersionSynchronizer::BsVersionSynchronizer(const VersionSynchronizerConfig &config) {
    if (config.syncFromBsAdmin == "leader_only") {
        _syncFromPersistType = BST_LEADER_ONLY;
    } else if (config.syncFromBsAdmin == "all") {
        _syncFromPersistType = BST_ALL;
    } else {
        _syncFromPersistType = BST_NONE;
    }
    AUTIL_LOG(
        INFO, "sync from bs admin config: [%s], sync type [%d]", config.syncFromBsAdmin.c_str(), _syncFromPersistType);
}
BsVersionSynchronizer::~BsVersionSynchronizer() {}

bool BsVersionSynchronizer::init() { return true; }

void BsVersionSynchronizer::remove(const PartitionId &pid) {}

bool BsVersionSynchronizer::supportSyncFromPersist(const PartitionMeta &target) const {
    if (_syncFromPersistType == BST_NONE) {
        return false;
    }
    if (_syncFromPersistType == BST_ALL) {
        return true;
    }
    assert(_syncFromPersistType == BST_LEADER_ONLY);
    return target.getRoleType() == RT_LEADER;
}

bool BsVersionSynchronizer::syncFromPersist(const PartitionId &pid,
                                            const std::string &configPath,
                                            TableVersion &version) {
    autil::ScopedLock lock(_mutex);
    auto committer = createCommitter(pid, configPath);
    std::vector<indexlibv2::versionid_t> versions;
    if (!committer || !committer->GetCommittedVersions(1, versions).IsOK()) {
        AUTIL_LOG(ERROR, "get committed version  for pid [%s] failed", ToJsonString(pid, true).c_str());
        return false;
    }
    IncVersion versionId = INVALID_VERSION;
    if (!versions.empty()) {
        assert(versions.size() == 1);
        versionId = versions[0];
    }
    version = TableVersion(versionId);
    return true;
}

bool BsVersionSynchronizer::persistVersion(const PartitionId &pid,
                                           const std::string &localConfigPath,
                                           const TableVersion &version) {
    autil::ScopedLock lock(_mutex);
    auto committer = createCommitter(pid, localConfigPath);
    if (!committer) {
        AUTIL_LOG(ERROR, "create version committer for pid [%s] failed", ToJsonString(pid, true).c_str());
        return false;
    }
    if (!committer->Commit(version.getVersionMeta()).IsOK()) {
        AUTIL_LOG(ERROR,
                  "commit version [%s] failed, versionMeta [%s]",
                  autil::legacy::ToJsonString(pid, true).c_str(),
                  autil::legacy::ToJsonString(version.getVersionMeta(), true).c_str());
        return false;
    }
    AUTIL_LOG(INFO,
              "commit version for [%s] success, versionMeta [%s]",
              autil::legacy::ToJsonString(pid, true).c_str(),
              autil::legacy::ToJsonString(version.getVersionMeta(), true).c_str());
    return true;
}

bool BsVersionSynchronizer::getVersionList(const PartitionId &pid, std::vector<TableVersion> &versions) { return true; }

bool BsVersionSynchronizer::updateVersionList(const PartitionId &pid, const std::vector<TableVersion> &versions) {
    return true;
}

std::shared_ptr<build_service::common::RemoteVersionCommitter> BsVersionSynchronizer::createCommitter() const {
    return std::make_shared<build_service::common::RemoteVersionCommitter>();
}

std::shared_ptr<build_service::common::RemoteVersionCommitter>
BsVersionSynchronizer::createCommitter(const PartitionId &pid, const std::string &configPath) const {
    std::string existConfigPath = TablePathDefine::constructLocalConfigPath(pid.getTableName(), configPath);
    if (fslib::EC_TRUE != fslib::fs::FileSystem::isExist(configPath)) {
        existConfigPath = configPath;
    }
    auto committer = createCommitter();
    assert(committer);
    build_service::config::ResourceReader resourceReader(configPath);
    std::string dataTableName;
    if (!resourceReader.getDataTableFromClusterName(pid.getTableName(), dataTableName)) {
        AUTIL_LOG(ERROR,
                  "get data table for cluster [%s] from local config path [%s] failed",
                  pid.getTableName().c_str(),
                  configPath.c_str());
        return nullptr;
    }
    build_service::common::RemoteVersionCommitter::InitParam initParam;
    initParam.generationId = pid.getFullVersion();
    initParam.dataTable = dataTableName;
    initParam.clusterName = pid.getTableName();
    initParam.rangeFrom = pid.from;
    initParam.rangeTo = pid.to;
    initParam.configPath = configPath;
    if (!committer->Init(initParam).IsOK()) {
        AUTIL_LOG(ERROR, "init version committer for pid [%s] failed", ToJsonString(pid, true).c_str());
        return nullptr;
    }
    return committer;
}

} // namespace suez
