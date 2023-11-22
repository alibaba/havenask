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
#include "build_service/admin/CheckpointSynchronizer.h"

#include <algorithm>
#include <assert.h>

#include "alog/Logger.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, CheckpointSynchronizer);

#define CS_LOG(level, format, args...) BS_LOG(level, "[%s] " format, _buildId.ShortDebugString().c_str(), ##args)
#define CS_INTERVAL_LOG(interval, level, format, args...)                                                              \
    BS_INTERVAL_LOG(interval, level, "[%s] " format, _buildId.ShortDebugString().c_str(), ##args)

CheckpointSynchronizer::CheckpointSynchronizer(const proto::BuildId& buildId) : _buildId(buildId) {}

CheckpointSynchronizer::~CheckpointSynchronizer() { clear(); }

void CheckpointSynchronizer::clear()
{
    autil::ScopedWriteLock lock(_rwlock);
    _clusterSynchronizers.clear();
}

bool CheckpointSynchronizer::init(
    const std::map<std::string, std::shared_ptr<ClusterCheckpointSynchronizer>>& clusterSynchronizers,
    bool isLeaderFollowerMode)
{
    clear();
    autil::ScopedWriteLock lock(_rwlock);
    _clusterSynchronizers = clusterSynchronizers;
    _isLeaderFollowerMode = isLeaderFollowerMode;
    std::vector<std::string> clusterNames;
    for (const auto& [clusterName, _] : clusterSynchronizers) {
        clusterNames.push_back(clusterName);
    }
    CS_LOG(INFO, "Init checkpoint synchronizer with clusters[%s], isLeaderFollowerMode[%d]",
           autil::legacy::ToJsonString(clusterNames, /*isCompact=*/true).c_str(), isLeaderFollowerMode);
    return true;
}

std::shared_ptr<ClusterCheckpointSynchronizer>
CheckpointSynchronizer::getClusterSynchronizer(const std::string& clusterName) const
{
    autil::ScopedReadLock lock(_rwlock);
    auto iter = _clusterSynchronizers.find(clusterName);
    if (iter == _clusterSynchronizers.end()) {
        CS_LOG(ERROR, "invalid cluster name[%s]", clusterName.c_str());
        assert(false);
        return nullptr;
    }
    return iter->second;
}

std::vector<proto::Range> CheckpointSynchronizer::getRanges(const std::string& clusterName) const
{
    auto clusterSynchronizer = getClusterSynchronizer(clusterName);
    if (!clusterSynchronizer) {
        return {};
    }
    return clusterSynchronizer->getRanges();
}

bool CheckpointSynchronizer::empty() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _clusterSynchronizers.empty();
}

void CheckpointSynchronizer::setIsLeaderFollowerMode(bool isLeaderFollowerMode)
{
    autil::ScopedWriteLock lock(_rwlock);
    _isLeaderFollowerMode = isLeaderFollowerMode;
}

bool CheckpointSynchronizer::publishPartitionLevelCheckpoint(const std::string& clusterName, const proto::Range& range,
                                                             const std::string& versionMetaStr, std::string& errMsg)
{
    auto clusterSynchronizer = getClusterSynchronizer(clusterName);
    if (!clusterSynchronizer) {
        return false;
    }
    return clusterSynchronizer->publishPartitionLevelCheckpoint(range, versionMetaStr, _isLeaderFollowerMode, errMsg);
}

bool CheckpointSynchronizer::getCommittedVersions(const std::string& clusterName, const proto::Range& range,
                                                  uint32_t limit, std::vector<indexlib::versionid_t>& versions,
                                                  std::string& errorMsg)
{
    auto clusterSynchronizer = getClusterSynchronizer(clusterName);
    if (!clusterSynchronizer) {
        return false;
    }
    return clusterSynchronizer->getCommittedVersions(range, limit, versions, errorMsg);
}

bool CheckpointSynchronizer::isLeaderFollowerMode() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _isLeaderFollowerMode;
}

bool CheckpointSynchronizer::syncCluster(const std::string& clusterName,
                                         const ClusterCheckpointSynchronizer::SyncOptions& options)
{
    auto clusterSynchronizer = getClusterSynchronizer(clusterName);
    if (!clusterSynchronizer) {
        return false;
    }
    return clusterSynchronizer->sync(options);
}

bool CheckpointSynchronizer::sync()
{
    for (auto& [clusterName, clusterSynchronizer] : _clusterSynchronizers) {
        if (!clusterSynchronizer->sync()) {
            CS_LOG(ERROR, "Sync failed: cluster %s", clusterName.c_str());
            return false;
        }
    }
    return true;
}

bool CheckpointSynchronizer::rollback(const std::string& clusterName, checkpointid_t checkpointId,
                                      const ::google::protobuf::RepeatedPtrField<proto::Range>& rangeVec,
                                      bool needAddIndexInfos, checkpointid_t* resId, std::string& errorMsg)
{
    auto clusterSynchronizer = getClusterSynchronizer(clusterName);
    if (!clusterSynchronizer) {
        return false;
    }
    return clusterSynchronizer->rollback(checkpointId, rangeVec, needAddIndexInfos, resId, errorMsg);
}

bool CheckpointSynchronizer::getReservedVersions(
    const std::string& clusterName, const proto::Range& range,
    std::set<indexlibv2::framework::VersionCoord>* reservedVersionList) const
{
    auto clusterSynchronizer = getClusterSynchronizer(clusterName);
    if (!clusterSynchronizer) {
        return false;
    }
    return clusterSynchronizer->getReservedVersions(range, reservedVersionList);
}

bool CheckpointSynchronizer::createSavepoint(
    const std::string& clusterName, checkpointid_t checkpointId, const std::string& comment,
    ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult* checkpoint, std::string& errMsg)
{
    auto clusterSynchronizer = getClusterSynchronizer(clusterName);
    if (!clusterSynchronizer) {
        return false;
    }
    return clusterSynchronizer->createSavepoint(checkpointId, comment, checkpoint, errMsg);
}

bool CheckpointSynchronizer::removeSavepoint(const std::string& clusterName, checkpointid_t checkpointId,
                                             std::string& errMsg)
{
    auto clusterSynchronizer = getClusterSynchronizer(clusterName);
    if (!clusterSynchronizer) {
        return false;
    }
    return clusterSynchronizer->removeSavepoint(checkpointId, errMsg);
}

bool CheckpointSynchronizer::getCheckpoint(
    const std::string& clusterName, checkpointid_t checkpointId,
    ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult* result) const
{
    auto clusterSynchronizer = getClusterSynchronizer(clusterName);
    if (!clusterSynchronizer) {
        return false;
    }
    return clusterSynchronizer->getCheckpoint(checkpointId, result);
}

std::pair<bool, common::PartitionLevelCheckpoint>
CheckpointSynchronizer::getPartitionCheckpoint(const std::string& clusterName, const proto::Range& range) const
{
    auto clusterSynchronizer = getClusterSynchronizer(clusterName);
    if (!clusterSynchronizer) {
        return {false, common::PartitionLevelCheckpoint()};
    }
    return clusterSynchronizer->getPartitionCheckpoint(range);
}

bool CheckpointSynchronizer::listCheckpoint(
    const std::string& clusterName, bool savepointOnly, uint32_t offset, uint32_t limit,
    std::vector<ClusterCheckpointSynchronizer::SingleGenerationLevelCheckpointResult>* result) const
{
    auto clusterSynchronizer = getClusterSynchronizer(clusterName);
    if (!clusterSynchronizer) {
        return false;
    }
    return clusterSynchronizer->listCheckpoint(savepointOnly, offset, limit, result);
}

}} // namespace build_service::admin
