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
#include "build_service/admin/LegacyCheckpointAdaptor.h"

#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/config/BuildRuleConfig.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, LegacyCheckpointAdaptor);
LegacyCheckpointAdaptor::LegacyCheckpointAdaptor(const std::shared_ptr<config::ResourceReader>& resourceReader,
                                                 const common::IndexCheckpointAccessorPtr& accessor,
                                                 const proto::BuildId& buildId, const std::string& clusterName,
                                                 const std::string& indexRoot)
    : ClusterCheckpointSynchronizer(buildId, clusterName)
    , _resourceReader(resourceReader)
    , _accessor(accessor)
{
    _root = indexRoot;
}

LegacyCheckpointAdaptor::~LegacyCheckpointAdaptor() {}

bool LegacyCheckpointAdaptor::listCheckpoint(
    bool savepointOnly, uint32_t offset, uint32_t limit,
    std::vector<ClusterCheckpointSynchronizer::SingleGenerationLevelCheckpointResult>* result)
{
    assert(result);
    result->clear();
    if (!_accessor) {
        BS_LOG(ERROR, "index checkpoint accessor is nullptr for cluster [%s]", _clusterName.c_str());
        return false;
    }
    std::vector<proto::CheckpointInfo> ckptInfos;
    if (!_accessor->listCheckpoint(_clusterName, savepointOnly, offset + limit, &ckptInfos)) {
        BS_LOG(ERROR, "list checkpoint failed for cluster [%s]", _clusterName.c_str());
        return false;
    }
    for (size_t i = offset; i < offset + limit && i < ckptInfos.size(); ++i) {
        ClusterCheckpointSynchronizer::SingleGenerationLevelCheckpointResult singleResult;
        auto& ckptInfo = ckptInfos[i];
        auto ckptId = ckptInfo.versionid();
        if (_accessor->isSavepoint(_clusterName, ckptId)) {
            singleResult.isSavepoint = true;
        }
        singleResult.generationLevelCheckpoint.checkpointId = ckptInfo.versionid();
        singleResult.generationLevelCheckpoint.checkpointId = ckptInfo.versionid();
        singleResult.generationLevelCheckpoint.createTime = autil::TimeUtility::us2ms(ckptInfo.versiontimestamp());
        singleResult.generationLevelCheckpoint.maxTimestamp = ckptInfo.versiontimestamp();
        singleResult.generationLevelCheckpoint.minTimestamp = ckptInfo.versiontimestamp();
        singleResult.generationLevelCheckpoint.readSchemaId = ckptInfo.schemaid();

        result->push_back(singleResult);
    }
    return true;
}

bool LegacyCheckpointAdaptor::getCheckpoint(checkpointid_t checkpointId,
                                            ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult* result)
{
    if (!_accessor) {
        BS_LOG(ERROR, "index checkpoint accessor is nullptr for cluster [%s]", _clusterName.c_str());
        return false;
    }
    uint32_t partitionCount = 0;
    if (!getPartitionCount(&partitionCount)) {
        BS_LOG(ERROR, "get partition count failed");
        return false;
    }
    proto::CheckpointInfo checkpointInfo;
    if (!_accessor->getIndexCheckpoint(_clusterName, (versionid_t)checkpointId, checkpointInfo)) {
        BS_LOG(ERROR, "get checkpoint for cluster[%s], checkpointid [%ld] failed", _clusterName.c_str(), checkpointId);
        return false;
    }
    // TODO(xiaohao.yxh) add comment, checkpointInfoFile
    if (_accessor->isSavepoint(_clusterName, checkpointId)) {
        result->isSavepoint = true;
        result->checkpointInfoFile = getCheckpointFilePath(checkpointId);
    }
    result->generationLevelCheckpoint.checkpointId = checkpointId;
    result->generationLevelCheckpoint.maxTimestamp = checkpointInfo.versiontimestamp();
    result->generationLevelCheckpoint.minTimestamp = checkpointInfo.versiontimestamp();
    result->generationLevelCheckpoint.createTime = checkpointInfo.versiontimestamp();
    result->generationLevelCheckpoint.readSchemaId = checkpointInfo.schemaid();
    auto rangeVec = util::RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, partitionCount);
    for (auto& range : rangeVec) {
        result->generationLevelCheckpoint.versionIdMapping[common::PartitionLevelCheckpoint::genRangeKey(range)] =
            (versionid_t)checkpointId;
    }
    return true;
}

bool LegacyCheckpointAdaptor::removeSavepoint(checkpointid_t checkpointId)
{
    if (!_accessor->removeIndexSavepoint("user", _clusterName, checkpointId)) {
        BS_LOG(ERROR, "remove index savepoint for cluster [%s], checkpointid [%ld] failed", _clusterName.c_str(),
               checkpointId);
        return false;
    }
    return true;
}

bool LegacyCheckpointAdaptor::createSavepoint(checkpointid_t checkpointId,
                                              ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult* result)
{
    if (!getCheckpoint(checkpointId, result)) {
        BS_LOG(ERROR, "create savepoint failed because get checkpoint failed");
        return false;
    }
    result->isSavepoint = true;
    result->checkpointInfoFile = getCheckpointFilePath(checkpointId);
    result->comment = "not support comment for legacy generation";
    string checkpointStr = autil::legacy::ToJsonString(result->generationLevelCheckpoint);
    if (!saveCheckpointInRoot(checkpointId, checkpointStr)) {
        BS_LOG(ERROR, "save checkpoint [%ld] failed, , checkpoint[%s]", checkpointId, checkpointStr.c_str());
        return false;
    }
    if (!_accessor->createIndexSavepoint("user", _clusterName, checkpointId)) {
        BS_LOG(ERROR, "create index save point failed");
    }
    BS_LOG(INFO, "create savepooint for cluster [%s] checkpointid [%ld] success", _clusterName.c_str(), checkpointId);
    return true;
}

bool LegacyCheckpointAdaptor::getPartitionCount(uint32_t* partitionCount)
{
    if (!_resourceReader) {
        BS_LOG(ERROR, "resource reader is nullptr");
        return false;
    }
    config::BuildRuleConfig buildRuleConfig;
    if (!_resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.builder_rule_config",
                                                       buildRuleConfig)) {
        BS_LOG(ERROR, "get builder_rule_config failed, cluster[%s]", _clusterName.c_str());
        return false;
    }
    *partitionCount = buildRuleConfig.partitionCount;
    return true;
}

}} // namespace build_service::admin
