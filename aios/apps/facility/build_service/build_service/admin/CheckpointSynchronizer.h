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

#include "autil/Lock.h"
#include "build_service/admin/CheckpointMetricReporter.h"
#include "build_service/admin/ClusterCheckpointSynchronizer.h"
#include "build_service/common/Checkpoint.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/framework/VersionMeta.h"

namespace build_service { namespace admin {

class CheckpointSynchronizer
{
public:
    explicit CheckpointSynchronizer(const proto::BuildId& buildId);
    virtual ~CheckpointSynchronizer();

    virtual bool init(const std::map<std::string, std::shared_ptr<ClusterCheckpointSynchronizer>>& clusterSynchronizers,
                      bool isLeaderFollowerMode);
    virtual void clear();

    virtual bool sync();

    bool publishPartitionLevelCheckpoint(const std::string& clusterName, const proto::Range& range,
                                         const std::string& versionMetaStr, std::string& errMsg);
    bool createSavepoint(const std::string& clusterName, checkpointid_t checkpointId, const std::string& comment,
                         ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult* checkpoint,
                         std::string& errMsg);
    bool removeSavepoint(const std::string& clusterName, checkpointid_t checkpointId, std::string& errMsg);
    bool getReservedVersions(const std::string& clusterName, const proto::Range& range,
                             std::set<indexlibv2::framework::VersionCoord>* reservedVersionList) const;
    bool
    listCheckpoint(const std::string& clusterName, bool savepointOnly, uint32_t offset, uint32_t limit,
                   std::vector<ClusterCheckpointSynchronizer::SingleGenerationLevelCheckpointResult>* result) const;
    bool getCheckpoint(const std::string& clusterName, checkpointid_t checkpointId,
                       ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult* result) const;
    std::pair<bool, common::PartitionLevelCheckpoint> getPartitionCheckpoint(const std::string& clusterName,
                                                                             const proto::Range& range) const;

    std::vector<proto::Range> getRanges(const std::string& clusterName) const;

    bool getCommittedVersions(const std::string& clusterName, const proto::Range& range, uint32_t limit,
                              std::vector<indexlib::versionid_t>& versions, std::string& errorMsg);

    bool rollback(const std::string& clusterName, checkpointid_t checkpointId,
                  const ::google::protobuf::RepeatedPtrField<proto::Range>& rangeVec, bool needAddIndexInfos,
                  checkpointid_t* resId, std::string& errorMsg);

    bool syncCluster(const std::string& clusterName, const ClusterCheckpointSynchronizer::SyncOptions& options);
    bool empty() const;

    const proto::BuildId& getBuildId() const { return _buildId; }
    void setIsLeaderFollowerMode(bool isLeaderFollowerMode);
    bool isLeaderFollowerMode() const;

protected:
    std::shared_ptr<ClusterCheckpointSynchronizer> getClusterSynchronizer(const std::string& clusterName) const;

protected:
    proto::BuildId _buildId;
    std::map<std::string, std::shared_ptr<ClusterCheckpointSynchronizer>> _clusterSynchronizers;
    bool _isLeaderFollowerMode = false;

    mutable autil::ReadWriteLock _rwlock;

    BS_LOG_DECLARE();
};

}} // namespace build_service::admin
