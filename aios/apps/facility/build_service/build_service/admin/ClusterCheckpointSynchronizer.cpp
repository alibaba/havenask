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
#include "build_service/admin/ClusterCheckpointSynchronizer.h"

#include <cstdint>

#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/VersionCoord.h"

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ClusterCheckpointSynchronizer);

#define CS_LOG(level, format, args...)                                                                                 \
    BS_LOG(level, "[%s][%s] " format, _buildId.ShortDebugString().c_str(), _clusterName.c_str(), ##args)
#define CS_INTERVAL_LOG(interval, level, format, args...)                                                              \
    BS_INTERVAL_LOG(interval, level, "[%s][%s] " format, _buildId.ShortDebugString().c_str(), _clusterName.c_str(),    \
                    ##args)

ClusterCheckpointSynchronizer::ClusterCheckpointSynchronizer(const proto::BuildId& buildId,
                                                             const std::string& clusterName)
    : _buildId(buildId)
    , _clusterName(clusterName)
{
}

ClusterCheckpointSynchronizer::~ClusterCheckpointSynchronizer()
{
    while (0 != _mutex.trylock()) {
        CS_LOG(WARN, "mutex is still holded");
        usleep(100 * 1000);
    }
    _mutex.unlock();
}

bool ClusterCheckpointSynchronizer::checkPublishParam(const proto::Range& range,
                                                      const indexlibv2::framework::VersionMeta& versionMeta,
                                                      bool checkVersionLine) const
{
    if (versionMeta.GetVersionId() == indexlibv2::INVALID_VERSIONID) {
        CS_LOG(ERROR, "VersionId %d is invalid.", versionMeta.GetVersionId());
        return false;
    }
    bool isRangeValid = false;
    for (const auto& validRange : _ranges) {
        if (validRange.from() == range.from() && validRange.to() == range.to()) {
            isRangeValid = true;
            break;
        }
    }
    if (!isRangeValid) {
        CS_LOG(ERROR, "Range [%d_%d] does not match with any ranges.", range.from(), range.to());
        return false;
    }

    auto [ret, lastCheckpoint] = getPartitionCheckpoint(range);
    if (!ret) {
        CS_LOG(ERROR, "Get latest partition [%d_%d] checkpoint failed.", range.from(), range.to());
        return false;
    }
    static bool skipCheckVersionLine = autil::EnvUtil::getEnv("BS_SKIP_CHECK_VERSION_LINE", false);

    if (checkVersionLine && !skipCheckVersionLine) {
        auto parentVersionId = versionMeta.GetVersionLine().GetParentVersion().GetVersionId();
        if (lastCheckpoint.isRollbackVersion && parentVersionId != lastCheckpoint.versionId) {
            CS_LOG(ERROR,
                   "publish version [%s] failed  expected parent version "
                   "[%d], but [%d], clusterName[%s], range [%d, %d]",
                   autil::legacy::ToJsonString(versionMeta, true).c_str(), lastCheckpoint.versionId, parentVersionId,
                   _clusterName.c_str(), range.from(), range.to());
            return false;
        }

        auto lastVersion = indexlibv2::framework::VersionCoord(lastCheckpoint.versionId, lastCheckpoint.fenceName);
        if (!versionMeta.GetVersionLine().CanFastFowardFrom(lastVersion, /*hasBuildingSegment*/ false)) {
            CS_LOG(ERROR,
                   "publish version [%s] can not fast from [%s], "
                   "clusterName[%s], range [%d, %d]",
                   autil::legacy::ToJsonString(versionMeta.GetVersionLine(), true).c_str(),
                   lastVersion.DebugString().c_str(), _clusterName.c_str(), range.from(), range.to());
            return false;
        }
    }

    return true;
}

bool ClusterCheckpointSynchronizer::publishPartitionLevelCheckpoint(const proto::Range& range,
                                                                    const std::string& versionMetaStr,
                                                                    bool checkVersionLine, std::string& errMsg)
{
    indexlibv2::framework::VersionMeta versionMeta;
    try {
        autil::legacy::FromJsonString(versionMeta, versionMetaStr);
    } catch (...) {
        CS_LOG(ERROR, "Deserialize version meta exception: invalid version meta str %s.", versionMetaStr.c_str());
        errMsg = "Deserialize version meta exception";
        return false;
    }
    if (!checkPublishParam(range, versionMeta, checkVersionLine)) {
        CS_LOG(ERROR, "Check publish param failed: version meta str %s", versionMetaStr.c_str());
        errMsg = "Check publish param failed";
        return false;
    }
    common::PartitionLevelCheckpoint checkpoint(versionMeta, range, getCurrentTime());
    std::string checkpointStr = autil::legacy::ToJsonString(checkpoint);
    std::string checkpointName = autil::StringUtil::toString(versionMeta.GetVersionId());
    std::string checkpointKey =
        common::PartitionLevelCheckpoint::genPartitionLevelCheckpointKey(_buildId, _clusterName, range);
    std::vector<std::pair<std::string, std::string>> removeCheckpoints;
    _checkpointAccessor->addOrUpdateCheckpoint(checkpointKey, checkpointName, checkpointStr);
    _checkpointAccessor->updateReservedCheckpoint(checkpointKey, PARTITION_RESERVE_COUNT, removeCheckpoints);
    if (_metricsReporter) {
        kmonitor::MetricsTags tags;
        tags.AddTag("partition",
                    autil::StringUtil::toString(range.from()) + "_" + autil::StringUtil::toString(range.to()));
        tags.AddTag("cluster", _clusterName);
        tags.AddTag("checkpointKey", checkpointKey);
        _metricsReporter->reportIndexVersion(checkpoint.versionId, tags);
        _metricsReporter->reportIndexSchemaVersion(checkpoint.readSchemaId, tags);
        _metricsReporter->calculateAndReportIndexTimestampFreshness(checkpoint.minTimestamp, tags);
    }
    CS_LOG(INFO, "Publish partition checkpoint [%d_%d], version meta %s successfully.", range.from(), range.to(),
           versionMetaStr.c_str());
    return true;
}

bool ClusterCheckpointSynchronizer::getCommittedVersions(const proto::Range& range, uint32_t limit,
                                                         std::vector<indexlibv2::versionid_t>& versions,
                                                         std::string& errorMsg)
{
    versions.clear();
    std::string checkpointKey =
        common::PartitionLevelCheckpoint::genPartitionLevelCheckpointKey(_buildId, _clusterName, range);
    std::vector<std::pair<std::string, std::string>> checkpoints;
    _checkpointAccessor->listCheckpoints(checkpointKey, checkpoints);
    std::reverse(checkpoints.begin(), checkpoints.end());
    if (checkpoints.size() > limit) {
        checkpoints.resize(limit);
    }
    for (const auto& [versionIdStr, _] : checkpoints) {
        indexlibv2::versionid_t versionId = indexlibv2::INVALID_VERSIONID;
        if (!autil::StringUtil::strToInt32(versionIdStr.c_str(), versionId)) {
            CS_LOG(ERROR, "failed to convert invalid versionId[%s] to number", versionIdStr.c_str());
            return false;
        }
        if (versionId == indexlibv2::INVALID_VERSIONID) {
            continue;
        }
        versions.push_back(versionId);
    }
    return true;
}

bool ClusterCheckpointSynchronizer::createPartitionSavepointByGeneration(
    const common::GenerationLevelCheckpoint& checkpoint)
{
    auto applyRole = getGenerationUserName(checkpoint.checkpointId);
    for (const auto& range : _ranges) {
        std::string errMsg;
        std::string checkpointKey =
            common::PartitionLevelCheckpoint::genPartitionLevelCheckpointKey(_buildId, _clusterName, range);
        auto iter = checkpoint.versionIdMapping.find(common::PartitionLevelCheckpoint::genRangeKey(range));
        assert(iter != checkpoint.versionIdMapping.end());
        versionid_t versionId = iter->second;
        if (versionId == indexlibv2::INVALID_VERSIONID) {
            continue;
        }
        std::string checkpointName = autil::StringUtil::toString(versionId);
        if (!_checkpointAccessor->createSavepoint(applyRole, checkpointKey, checkpointName, errMsg)) {
            CS_LOG(ERROR,
                   "create savepoint for generation user failed, "
                   "errMsg[%s], checkpointKey[%s], savepointId[%s]",
                   errMsg.c_str(), checkpointKey.c_str(), checkpointName.c_str());
            return false;
        }
    }
    return true;
}

bool ClusterCheckpointSynchronizer::removePartitionSavepointByGeneration(
    const common::GenerationLevelCheckpoint& checkpoint, const std::vector<proto::Range>& ranges,
    bool cleanPartitionCheckpoint)
{
    auto applyRole = getGenerationUserName(checkpoint.checkpointId);
    for (const auto& range : ranges) {
        std::string errMsg;
        std::string checkpointKey =
            common::PartitionLevelCheckpoint::genPartitionLevelCheckpointKey(_buildId, _clusterName, range);
        auto iter = checkpoint.versionIdMapping.find(common::PartitionLevelCheckpoint::genRangeKey(range));
        assert(iter != checkpoint.versionIdMapping.end());
        std::string checkpointName = autil::StringUtil::toString(iter->second);
        if (!_checkpointAccessor->removeSavepoint(applyRole, checkpointKey, checkpointName, errMsg)) {
            CS_LOG(ERROR,
                   "remove savepoint for generation user failed, "
                   "errMsg[%s], checkpointKey[%s], savepointId[%s]",
                   errMsg.c_str(), checkpointKey.c_str(), checkpointName.c_str());
            return false;
        }
        std::vector<std::pair<std::string, std::string>> removeCheckpoints;
        _checkpointAccessor->updateReservedCheckpoint(checkpointKey, PARTITION_RESERVE_COUNT, removeCheckpoints);
        std::string comment;
        if (cleanPartitionCheckpoint && !_checkpointAccessor->isSavepoint(checkpointKey, checkpointName, &comment)) {
            _checkpointAccessor->cleanCheckpoint(checkpointKey, checkpointName);
        }
    }
    return true;
}

bool ClusterCheckpointSynchronizer::checkNeedSync(bool isDaily, bool instant,
                                                  common::GenerationLevelCheckpoint* latestCheckpoint, bool* needSync)
{
    *needSync = true;
    std::string checkpointKey =
        common::GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(_buildId, _clusterName, isDaily);
    auto syncIntervalMs = isDaily ? DAYTIME_IN_MS : _options.syncIntervalMs;
    if (instant) {
        syncIntervalMs = 0;
    }
    std::string tmpCheckpointName;
    std::string tmpCheckpointValue;
    const auto currentTime = getCurrentTime();
    if (_checkpointAccessor->getLatestCheckpoint(checkpointKey, tmpCheckpointName, tmpCheckpointValue)) {
        if (!generationCheckpointfromStr(tmpCheckpointValue, latestCheckpoint)) {
            CS_LOG(ERROR, "convert checkpoint from str [%s] failed", tmpCheckpointValue.c_str());
            return false;
        }
        if (currentTime - latestCheckpoint->createTime < syncIntervalMs) {
            *needSync = false;
            return true;
        }
    }
    return true;
}

bool ClusterCheckpointSynchronizer::generateCheckpointId(checkpointid_t* checkpointId)
{
    std::vector<SingleGenerationLevelCheckpointResult> result;
    if (!listCheckpoint(/*savepointOnly*/ false, /*offset*/ 0,
                        /*limit*/ 1, &result)) {
        CS_LOG(ERROR, "list checkpoint failed for cluster [%s]", _clusterName.c_str());
        return false;
    }
    *checkpointId = result.empty() ? 0 : result[0].generationLevelCheckpoint.checkpointId + 1;
    return true;
}

bool ClusterCheckpointSynchronizer::publishGenerationCheckpoint(
    const common::GenerationLevelCheckpoint& checkpoint,
    const std::vector<common::PartitionLevelCheckpoint>& partitionCheckpoints, const SyncOptions& options)
{
    std::string checkpointKey =
        common::GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(_buildId, _clusterName, options.isDaily);
    std::string checkpointName = autil::StringUtil::toString(checkpoint.checkpointId);
    std::string checkpointValue = autil::legacy::ToJsonString(checkpoint);
    std::vector<std::pair<std::string, std::string>> removeCheckpoints;
    auto reserveCheckpointCount =
        options.isDaily ? _options.reserveDailyCheckpointCount : _options.reserveCheckpointCount;
    _checkpointAccessor->addOrUpdateCheckpoint(checkpointKey, checkpointName, checkpointValue);
    _checkpointAccessor->updateReservedCheckpoint(checkpointKey, reserveCheckpointCount, removeCheckpoints);
    if (reserveCheckpointCount == 0) {
        return true;
    }
    if (!createPartitionSavepointByGeneration(checkpoint)) {
        CS_LOG(ERROR, "create partition savepoints failed for cluster [%s]", _clusterName.c_str());
        return false;
    }
    for (const auto& [name, value] : removeCheckpoints) {
        common::GenerationLevelCheckpoint generationLevelCheckpoint;
        if (!generationCheckpointfromStr(value, &generationLevelCheckpoint)) {
            CS_LOG(ERROR, "parse checkpoint failed. checkpoint[%s]", value.c_str());
            return false;
        }
        if (!removePartitionSavepointByGeneration(generationLevelCheckpoint, _ranges,
                                                  /*cleanCheckpoint=*/false)) {
            CS_LOG(ERROR,
                   "remove generation level savepoint failed. "
                   "checkpoint[%s]",
                   value.c_str());
            return false;
        }
    }
    if (_metricsReporter) {
        kmonitor::MetricsTags tags;
        tags.AddTag("cluster", _clusterName);
        tags.AddTag("checkpointKey", checkpointKey);
        _metricsReporter->reportIndexSchemaVersion(checkpoint.readSchemaId, tags);
        _metricsReporter->calculateAndReportIndexTimestampFreshness(checkpoint.minTimestamp, tags);
    }
    CS_LOG(INFO,
           "generation checkpoint key [%s] sync succeed, checkpointName[%s], "
           "checkpointValue[%s], isDaily[%d]",
           checkpointKey.c_str(), checkpointName.c_str(), checkpointValue.c_str(), options.isDaily);
    if (!options.isDaily && options.addIndexInfos) {
        addCheckpointToIndexInfo(checkpoint.readSchemaId, partitionCheckpoints, options.indexInfoParam,
                                 options.isOffline);
    }
    return true;
}

bool ClusterCheckpointSynchronizer::generateGenerationCheckpoint(
    common::GenerationLevelCheckpoint* checkpoint, std::vector<common::PartitionLevelCheckpoint>* partitionCheckpoints)
{
    indexlibv2::schemaid_t readSchemaId = indexlibv2::INVALID_SCHEMAID;
    const auto currentTime = getCurrentTime();
    bool hasValidVersion = false;
    // for generate unique checkpoint id
    if (!generateCheckpointId(&(checkpoint->checkpointId))) {
        CS_LOG(ERROR, "generate checkpoint id for cluster clusterName[%s] failed", _clusterName.c_str());
        return false;
    }

    for (const auto& range : _ranges) {
        auto [ret, partitionCheckpoint] = getPartitionCheckpoint(range);
        if (!ret) {
            CS_LOG(ERROR,
                   "get partition checkpoint failed, clusterName[%s], "
                   "range [%d, %d]",
                   _clusterName.c_str(), range.from(), range.to());
            return false;
        }
        if (partitionCheckpoint.versionId != indexlibv2::INVALID_VERSIONID) {
            hasValidVersion = true;
            if (readSchemaId == indexlibv2::INVALID_SCHEMAID && readSchemaId != partitionCheckpoint.readSchemaId) {
                readSchemaId = partitionCheckpoint.readSchemaId;
            }
            if (readSchemaId != partitionCheckpoint.readSchemaId) {
                CS_LOG(ERROR,
                       "read schema [%u] is not the same with [%u] for "
                       "cluster [%s]. create generation checkpoint "
                       "until all partition read schema id is the same.",
                       readSchemaId, partitionCheckpoint.readSchemaId, _clusterName.c_str());
                return false;
            }
        }
        std::string rangeKey = common::PartitionLevelCheckpoint::genRangeKey(range);
        checkpoint->maxTimestamp = std::max(checkpoint->maxTimestamp, partitionCheckpoint.maxTimestamp);
        checkpoint->minTimestamp = std::min(checkpoint->minTimestamp, partitionCheckpoint.minTimestamp);
        checkpoint->versionIdMapping[rangeKey] = partitionCheckpoint.versionId;
        partitionCheckpoints->push_back(partitionCheckpoint);
    }
    checkpoint->createTime = currentTime;
    checkpoint->readSchemaId = readSchemaId;
    return hasValidVersion;
}

bool ClusterCheckpointSynchronizer::sync(const SyncOptions& options)
{
    autil::ScopedLock lock(_mutex);
    common::GenerationLevelCheckpoint latestCheckpoint;
    bool needSync = false;
    if (!checkNeedSync(options.isDaily, options.isInstant, &latestCheckpoint, &needSync)) {
        return false;
    }
    if (!needSync) {
        reportPartitionLevelCheckpointMetric();
        return true;
    }
    common::GenerationLevelCheckpoint checkpoint;
    std::vector<common::PartitionLevelCheckpoint> releatedPartitionCheckpoints;
    if (!generateGenerationCheckpoint(&checkpoint, &releatedPartitionCheckpoints)) {
        CS_INTERVAL_LOG(60, ERROR, "Generate generation checkpoint for cluster [%s] failed.", _clusterName.c_str());
        return false;
    }
    if (checkpoint.versionIdMapping == latestCheckpoint.versionIdMapping) {
        CS_LOG(DEBUG, "Not create checkpoint: new checkpoint version id mapping is same with "
                      "the latest checkpoint version id mapping.");
        return true;
    }
    if (checkpoint.readSchemaId == indexlibv2::INVALID_SCHEMAID) {
        CS_LOG(WARN, "Not create checkpoint: read schema id is invalid.");
        return true;
    }
    return publishGenerationCheckpoint(checkpoint, releatedPartitionCheckpoints, options);
}

bool ClusterCheckpointSynchronizer::sync()
{
    if (_checkpointAccessor == nullptr) {
        CS_LOG(ERROR, "Sync failed: checkpoint accessor is uninitialized.");
        return false;
    }
    SyncOptions options1;
    options1.isDaily = false;
    options1.isInstant = false;
    SyncOptions options2;
    options2.isDaily = true;
    options2.isInstant = false;
    if (!sync(options1) || !sync(options2)) {
        return false;
    }
    return true;
}

bool ClusterCheckpointSynchronizer::checkRollbackLegal(checkpointid_t checkpointId)
{
    std::vector<SingleGenerationLevelCheckpointResult> result;
    GenerationLevelCheckpointGetResult getResult;
    if (!getCheckpoint(checkpointId, &getResult)) {
        CS_LOG(ERROR, "checkpoint[%ld] not found", checkpointId);
        return false;
    }
    if (!listCheckpoint(/*savepointOnly*/ true, /*offset*/ 0,
                        /*limit*/ std::numeric_limits<uint32_t>::max(), &result)) {
        CS_LOG(ERROR, "list checkpoint failed");
        return false;
    }
    for (const auto& checkpoint : result) {
        if (checkpoint.generationLevelCheckpoint.checkpointId > checkpointId) {
            CS_LOG(ERROR, "has savepoint[%ld], not rollback checkpointId[%ld]",
                   checkpoint.generationLevelCheckpoint.checkpointId, checkpointId);
            return false;
        }
    }
    return true;
}

bool ClusterCheckpointSynchronizer::getRemoveGenerationLevelCheckpoints(
    checkpointid_t checkpointId, std::vector<common::GenerationLevelCheckpoint>* removeCheckpoints)
{
    assert(removeCheckpoints);
    removeCheckpoints->clear();
    std::vector<SingleGenerationLevelCheckpointResult> result;
    if (!listCheckpoint(/*savepointOnly=*/false, /*offset=*/0,
                        /*limit=*/std::numeric_limits<uint32_t>::max(), &result)) {
        CS_LOG(ERROR, "list checkpoint failed");
        return false;
    }
    for (auto& checkpointResult : result) {
        auto& checkpoint = checkpointResult.generationLevelCheckpoint;
        if (checkpoint.checkpointId > checkpointId) {
            removeCheckpoints->push_back(checkpoint);
        }
    }
    return true;
}

bool ClusterCheckpointSynchronizer::cleanPartitionCheckpoint(
    const std::vector<proto::Range>& ranges, const std::vector<common::GenerationLevelCheckpoint>& removeCheckpoints)
{
    for (const auto& checkpoint : removeCheckpoints) {
        if (!removePartitionSavepointByGeneration(checkpoint, ranges,
                                                  /*cleanCheckpoint*/ true)) {
            CS_LOG(ERROR, "remove generation level savepoint failed. checkpoint[%s]",
                   autil::legacy::ToJsonString(checkpoint, true).c_str());
            return false;
        }
        std::string checkpointName = autil::StringUtil::toString(checkpoint.checkpointId);
        auto updateCheckpoint = checkpoint;
        updateCheckpoint.isObsoleted = true;
        std::string checkpointValue = autil::legacy::ToJsonString(updateCheckpoint);
        if (!_checkpointAccessor->updateCheckpoint(common::GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(
                                                       _buildId, _clusterName, /*isDaily*/ true),
                                                   checkpointName, checkpointValue) &&
            !_checkpointAccessor->updateCheckpoint(common::GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(
                                                       _buildId, _clusterName, /*isDaily*/ false),
                                                   checkpointName, checkpointValue)) {
            CS_LOG(ERROR, "update legacy mark failed. checkpoint [%s]", checkpointName.c_str());
            return false;
        }
    }
    return true;
}

bool ClusterCheckpointSynchronizer::updateRollbackMark(const std::vector<proto::Range>& ranges)
{
    for (const auto& range : ranges) {
        auto [ret, partitionCheckpoint] = getPartitionCheckpoint(range);
        if (!ret) {
            CS_LOG(ERROR,
                   "get partition checkpoint for cluster [%s], range [%d, %d] "
                   "failed",
                   _clusterName.c_str(), range.from(), range.to());
            return false;
        }
        partitionCheckpoint.isRollbackVersion = true;
        _checkpointAccessor->addOrUpdateCheckpoint(
            common::PartitionLevelCheckpoint::genPartitionLevelCheckpointKey(_buildId, _clusterName, range),
            autil::StringUtil::toString(partitionCheckpoint.versionId),
            autil::legacy::ToJsonString(partitionCheckpoint));
    }
    return true;
}

std::vector<proto::Range>
ClusterCheckpointSynchronizer::convertRanges(const ::google::protobuf::RepeatedPtrField<proto::Range>& rangeVec)
{
    std::vector<proto::Range> ranges;
    if (rangeVec.empty()) {
        ranges = _ranges;
    } else {
        for (const auto& range : rangeVec) {
            ranges.push_back(range);
        }
    }
    return ranges;
}

bool ClusterCheckpointSynchronizer::rollback(checkpointid_t checkpointId,
                                             const ::google::protobuf::RepeatedPtrField<proto::Range>& rangeVec,
                                             bool needAddIndexInfo, checkpointid_t* resId, std::string& errorMsg)
{
    // 1. 校验savepoint，1 2 3回滚到2，3有savepoint的场景
    // 2. 清理partition checkpoint
    // 3. 设置正在回滚的状态（持久化到partition level的checkpoint中）
    // 4. 生成一个新checkpoint，如果 needAddIndexInfo == true 立即同步到index info上
    auto ranges = convertRanges(rangeVec);
    if (!checkRollbackLegal(checkpointId)) {
        CS_LOG(ERROR, "rollback is not legal, clusterName[%s], checkpointId[%ld]", _clusterName.c_str(), checkpointId);
        return false;
    }
    std::vector<common::GenerationLevelCheckpoint> removeCheckpoints;
    if (!getRemoveGenerationLevelCheckpoints(checkpointId, &removeCheckpoints)) {
        CS_LOG(ERROR,
               "get remove generation level checkpoints failed, "
               "clusterName[%s], checkpointId[%ld]",
               _clusterName.c_str(), checkpointId);
        return false;
    }
    if (!cleanPartitionCheckpoint(ranges, removeCheckpoints)) {
        CS_LOG(ERROR,
               "clean spatial partition checkpoint failed, "
               "clusterName[%s], checkpointId[%ld]",
               _clusterName.c_str(), checkpointId);
        return false;
    }
    if (!updateRollbackMark(ranges)) {
        CS_LOG(ERROR,
               "update rollback mark failed, clusterName[%s], "
               "checkpointId[%ld]",
               _clusterName.c_str(), checkpointId);
        return false;
    }
    // generate new checkpoint that equal to rollback checkpoint, then return new checkpoint id
    SyncOptions syncOptions;
    syncOptions.isDaily = false;
    syncOptions.isInstant = true;
    syncOptions.addIndexInfos = needAddIndexInfo;
    if (!sync(syncOptions)) {
        CS_LOG(ERROR,
               "publish rollback version to index info failed, "
               "clusterName[%s], checkpointId[%ld]",
               _clusterName.c_str(), checkpointId);
        return false;
    }
    CS_LOG(INFO, "rollback success, clusterName[%s], checkpointId[%ld]", _clusterName.c_str(), checkpointId);
    std::vector<SingleGenerationLevelCheckpointResult> result;
    if (!listCheckpoint(/*savepointOnly*/ false, /*offset*/ 0, /*limit*/ 1, &result) || result.empty()) {
        CS_LOG(ERROR, "list checkpoint failed for cluster [%s]", _clusterName.c_str());
        return false;
    }
    *resId = result[0].generationLevelCheckpoint.checkpointId;
    return true;
}

void ClusterCheckpointSynchronizer::addCheckpointToIndexInfo(
    indexlibv2::schemaid_t schemaVersion, const std::vector<common::PartitionLevelCheckpoint>& partitionCheckpoints,
    const IndexInfoParam& indexInfoParam, bool isOffline)
{
    ::google::protobuf::RepeatedPtrField<proto::IndexInfo> indexInfos;
    for (const auto& checkpoint : partitionCheckpoints) {
        proto::IndexInfo indexInfo;
        // add legacy info
        indexInfo.set_isvalid(true);
        indexInfo.set_processordatasourceidx(0);
        indexInfo.set_processorcheckpoint(0);
        indexInfo.set_ongoingmodifyopids("");

        // add basic info
        indexInfo.set_versiontimestamp(checkpoint.minTimestamp);
        if (indexInfoParam.startTimestamp >= 0) {
            indexInfo.set_starttimestamp(indexInfoParam.startTimestamp);
        }
        if (indexInfoParam.finishTimestamp >= 0) {
            indexInfo.set_finishtimestamp(indexInfoParam.finishTimestamp);
        }
        indexInfo.set_clustername(_clusterName);
        proto::Range range;
        range.set_from(checkpoint.from);
        range.set_to(checkpoint.to);
        *indexInfo.mutable_range() = range;
        indexInfo.set_indexversion(checkpoint.versionId);
        if (checkpoint.indexSize >= 0) {
            indexInfo.set_indexsize(checkpoint.indexSize);
        }
        auto iter = indexInfoParam.totalRemainIndexSizes.find(range);
        if (iter != indexInfoParam.totalRemainIndexSizes.end()) {
            indexInfo.set_totalremainindexsize(iter->second);
        } else {
            indexInfo.set_totalremainindexsize(0);
        }
        indexInfo.set_schemaversion(schemaVersion);
        *indexInfos.Add() = indexInfo;
    }
    common::IndexCheckpointAccessor indexCheckpointAccessor(_checkpointAccessor);
    if (isOffline && !indexCheckpointAccessor.hasFullIndexInfo(_clusterName)) {
        // for fullIndexInfos
        indexCheckpointAccessor.updateIndexInfo(true, _clusterName, indexInfos);
    }
    indexCheckpointAccessor.updateIndexInfo(false, _clusterName, indexInfos);
}

bool ClusterCheckpointSynchronizer::init(const std::string& root, const std::vector<proto::Range>& ranges,
                                         const std::shared_ptr<common::CheckpointAccessor>& checkpointAccessor,
                                         const std::shared_ptr<CheckpointMetricReporter>& metricsReporter,
                                         const Options& options)
{
    if (ranges.empty()) {
        CS_LOG(ERROR, "init failed. ranges are empty");
        return false;
    }
    if (checkpointAccessor == nullptr) {
        CS_LOG(ERROR, "init failed. checkpoint accessor is nullptr");
        return false;
    }
    _root = root;
    _metricsReporter = metricsReporter;
    _options = options;

    if (_options.reserveCheckpointCount < 1) {
        CS_LOG(WARN, "[%s] reserveCheckpointCount [%u] is too small, reset to 1", _clusterName.c_str(),
               options.reserveCheckpointCount);
        _options.reserveCheckpointCount = 1;
    }
    CS_LOG(INFO,
           "clusterName[%s] syncIntervalMs[%ld] "
           "reserveCheckpointCount[%u] reserveDailyCheckpointCount[%u]",
           _clusterName.c_str(), _options.syncIntervalMs, _options.reserveCheckpointCount,
           _options.reserveDailyCheckpointCount);

    _checkpointAccessor = checkpointAccessor;
    autil::ScopedLock lock(_mutex);
    _ranges = ranges;
    common::IndexCheckpointAccessor indexCheckpointAccessor(_checkpointAccessor);
    ::google::protobuf::RepeatedPtrField<proto::IndexInfo> indexInfos;
    if (indexCheckpointAccessor.getIndexInfo(/*isFull=*/false, _clusterName, indexInfos)) {
        addIndexInfoToCheckpoint(indexInfos);
    } else if (indexCheckpointAccessor.getIndexInfo(/*isFull=*/true, _clusterName, indexInfos)) {
        addIndexInfoToCheckpoint(indexInfos);
    }
    CS_LOG(INFO, "cluster checkpoint synchronizer init succeed.");
    return true;
}

void ClusterCheckpointSynchronizer::addIndexInfoToCheckpoint(
    const ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos)
{
    for (const auto& indexInfo : indexInfos) {
        auto range = indexInfo.range();
        std::string checkpointKey =
            common::PartitionLevelCheckpoint::genPartitionLevelCheckpointKey(_buildId, _clusterName, range);
        std::string checkpointName, checkpointValue;
        if (_checkpointAccessor->getLatestCheckpoint(checkpointKey, checkpointName, checkpointValue)) {
            continue;
        }
        common::PartitionLevelCheckpoint checkpoint;
        std::vector<std::pair<std::string, std::string>> removeCheckpoints;
        checkpoint.versionId = indexInfo.indexversion();
        checkpoint.readSchemaId = indexInfo.schemaversion();
        checkpoint.indexSize = indexInfo.indexsize();
        checkpoint.from = range.from();
        checkpoint.to = range.to();
        checkpoint.createTime = getCurrentTime();
        checkpointName = autil::StringUtil::toString(indexInfo.indexversion());
        _checkpointAccessor->addOrUpdateCheckpoint(checkpointKey, checkpointName,
                                                   autil::legacy::ToJsonString(checkpoint));
        _checkpointAccessor->updateReservedCheckpoint(checkpointKey, PARTITION_RESERVE_COUNT, removeCheckpoints);
    }
}

bool ClusterCheckpointSynchronizer::getReservedVersions(
    const proto::Range& range, std::set<indexlibv2::framework::VersionCoord>* reservedVersionList) const
{
    if (!getReservedVersions(range, reservedVersionList,
                             /*isDaily=*/false)) {
        CS_LOG(ERROR, "get normal reserved versions failed, clusterName[%s]", _clusterName.c_str());
        return false;
    }
    if (!getReservedVersions(range, reservedVersionList,
                             /*isDaily=*/true)) {
        CS_LOG(ERROR, "get daily reserved versions failed, clusterName[%s]", _clusterName.c_str());
        return false;
    }
    return true;
}

bool ClusterCheckpointSynchronizer::getReservedVersions(
    const proto::Range& range, std::set<indexlibv2::framework::VersionCoord>* reservedVersionList, bool isDaily) const
{
    if (_checkpointAccessor == nullptr) {
        CS_LOG(ERROR, "get reserved versions failed. _checkpointAccessor is nullptr");
        assert(false);
        return false;
    }
    // generation level
    std::string generationLevelCheckpointKey =
        common::GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(_buildId, _clusterName, isDaily);
    std::vector<std::pair<std::string, std::string>> generationLevelCheckpoints;
    _checkpointAccessor->listCheckpoints(generationLevelCheckpointKey, generationLevelCheckpoints);
    for (const auto& [_, value] : generationLevelCheckpoints) {
        common::GenerationLevelCheckpoint checkpoint;
        if (!generationCheckpointfromStr(value, &checkpoint)) {
            return false;
        }
        std::string rangeKey = common::PartitionLevelCheckpoint::genRangeKey(range);
        auto iter = checkpoint.versionIdMapping.find(rangeKey);
        if (iter == checkpoint.versionIdMapping.end()) {
            CS_LOG(ERROR,
                   "not found range key[%s] in version id mapping, "
                   "checkpointKey[%s], value[%s]",
                   rangeKey.c_str(), generationLevelCheckpointKey.c_str(), value.c_str());
            return false;
        }
        const auto& versionId = iter->second;
        if (versionId == indexlibv2::INVALID_VERSIONID) {
            continue;
        }
        (*reservedVersionList).emplace(versionId);
    }
    // partition level
    std::string partitionLevelCheckpointKey =
        common::PartitionLevelCheckpoint::genPartitionLevelCheckpointKey(_buildId, _clusterName, range);
    std::vector<std::pair<std::string, std::string>> partitionLevelCheckpoints;
    _checkpointAccessor->listCheckpoints(partitionLevelCheckpointKey, partitionLevelCheckpoints);
    for (const auto& [versionIdStr, _] : partitionLevelCheckpoints) {
        indexlibv2::versionid_t versionId = indexlibv2::INVALID_VERSIONID;
        if (!autil::StringUtil::strToInt32(versionIdStr.c_str(), versionId)) {
            CS_LOG(ERROR, "failed to convert invalid versionId[%s] to number", versionIdStr.c_str());
            return false;
        }
        if (versionId == indexlibv2::INVALID_VERSIONID) {
            continue;
        }
        (*reservedVersionList).emplace(versionId);
    }
    CS_LOG(INFO, "range[%u %u] get reserved version list [%s]", range.from(), range.to(),
           autil::legacy::ToJsonString(*reservedVersionList, /*isCompact=*/true).c_str());
    return true;
}

bool ClusterCheckpointSynchronizer::saveCheckpointInRoot(checkpointid_t checkpointId,
                                                         const std::string& checkpointContent)
{
    std::string directory = getCheckpointDirectory();
    std::string checkpointFile = getCheckpointFilePath(checkpointId);

    bool dirExist = false;
    if (!fslib::util::FileUtil::isDir(directory, dirExist)) {
        CS_LOG(ERROR, "check whether [%s] is dir failed.", directory.c_str());
        return false;
    }

    if (!dirExist && !fslib::util::FileUtil::mkDir(directory, true)) {
        CS_LOG(ERROR, "make dir [%s] failed.", directory.c_str());
        return false;
    }

    bool fileExist = false;
    if (!fslib::util::FileUtil::isExist(checkpointFile, fileExist)) {
        CS_LOG(ERROR, "is exist io error, path [%s], content[%s].", checkpointFile.c_str(), checkpointContent.c_str());
        return false;
    }

    if (fileExist) {
        CS_LOG(INFO, "checkpoint is exist, path [%s], content[%s].", checkpointFile.c_str(), checkpointContent.c_str());
        return true;
    }

    if (!indexlib::file_system::FslibWrapper::AtomicStore(checkpointFile, checkpointContent).OK()) {
        CS_LOG(ERROR, "write checkpoint file [%s] failed, content[%s].", checkpointFile.c_str(),
               checkpointContent.c_str());
        return false;
    }
    CS_LOG(INFO, "write checkpoint, path [%s], content[%s].", checkpointFile.c_str(), checkpointContent.c_str());
    return true;
}

bool ClusterCheckpointSynchronizer::deleteCheckpointInRoot()
{
    std::string directory = getCheckpointDirectory();
    bool dirExist = false;
    if (!fslib::util::FileUtil::isDir(directory, dirExist)) {
        CS_LOG(WARN, "check whether [%s] is dir failed.", directory.c_str());
        return false;
    }
    if (!dirExist) {
        return true;
    }

    std::vector<std::string> fileList;
    if (!fslib::util::FileUtil::listDir(directory, fileList)) {
        CS_LOG(WARN, "list dir [%s] failed.", directory.c_str());
        return false;
    }

    std::vector<SingleGenerationLevelCheckpointResult> result;
    if (!listCheckpoint(/*savepointOnly=*/true, /*offset=*/0,
                        /*limit=*/std::numeric_limits<uint32_t>::max(), &result)) {
        CS_LOG(WARN, "list checkpoints failed, clusterName [%s].", _clusterName.c_str());
        return false;
    }

    std::set<std::string> currentSavepointFiles;
    for (const auto& checkpoint : result) {
        std::string checkpointFileName = getCheckpointFileName(checkpoint.generationLevelCheckpoint.checkpointId);
        currentSavepointFiles.insert(checkpointFileName);
    }
    for (const auto& file : fileList) {
        if (currentSavepointFiles.find(file) == currentSavepointFiles.end()) {
            std::string fullPath = fslib::util::FileUtil::joinFilePath(directory, file);
            if (!fslib::util::FileUtil::removeIfExist(fullPath)) {
                CS_LOG(WARN, "remove checkpoint file failed, path [%s] file [%s]", directory.c_str(), file.c_str());
                return false;
            }
            CS_LOG(INFO, "remove checkpoint file success, path [%s] file [%s]", directory.c_str(), file.c_str());
        }
    }
    return true;
}

bool ClusterCheckpointSynchronizer::createSavepoint(int64_t savepointId, const std::string& comment,
                                                    GenerationLevelCheckpointGetResult* checkpoint, std::string& errMsg)
{
    if (createSavepoint(savepointId, comment, checkpoint, /*isDaily*/ true, errMsg)) {
        return true;
    }
    if (createSavepoint(savepointId, comment, checkpoint, /*isDaily*/ false, errMsg)) {
        return true;
    }
    CS_LOG(ERROR, "create savepoint failed, clusterName[%s], savepointId[%lu]", _clusterName.c_str(), savepointId);
    return false;
}

bool ClusterCheckpointSynchronizer::createSavepoint(checkpointid_t checkpointId, const std::string& comment,
                                                    GenerationLevelCheckpointGetResult* checkpoint, bool isDaily,
                                                    std::string& errMsg)
{
    autil::ScopedLock lock(_mutex);
    std::string checkpointKey =
        common::GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(_buildId, _clusterName, isDaily);
    std::string checkpointIdStr;
    if (checkpointId == INVALID_CHECKPOINT_ID) {
        [[maybe_unused]] std::string checkpointValue;
        if (_checkpointAccessor->getLatestCheckpoint(checkpointKey, checkpointIdStr, checkpointValue)) {
            checkpointId = autil::StringUtil::fromString<checkpointid_t>(checkpointIdStr);
            CS_LOG(INFO,
                   "not assign checkpoint id, use latest "
                   "checkpointId[%s], checkpointKey[%s]",
                   checkpointIdStr.c_str(), checkpointKey.c_str());
        } else {
            errMsg = "not found latest checkpoint for [" + checkpointKey + "]";
            CS_LOG(ERROR, "%s", errMsg.c_str());
            return false;
        }
    } else {
        checkpointIdStr = autil::StringUtil::toString(checkpointId);
    }
    std::string checkpointStr;
    if (!_checkpointAccessor->getCheckpoint(checkpointKey, checkpointIdStr, checkpointStr) ||
        !generationCheckpointfromStr(checkpointStr, &(checkpoint->generationLevelCheckpoint))) {
        CS_LOG(ERROR, "get checkpoint [%s] failed, checkpointId[%s], checkpoint[%s]", checkpointKey.c_str(),
               checkpointIdStr.c_str(), checkpointStr.c_str());
        return false;
    }
    checkpoint->isSavepoint = true;
    checkpoint->checkpointInfoFile = getCheckpointFilePath(checkpointId);
    checkpoint->comment = comment;

    if (!saveCheckpointInRoot(checkpointId, checkpointStr)) {
        CS_LOG(ERROR, "save checkpoint [%s] failed, checkpointId[%s], checkpoint[%s]", checkpointKey.c_str(),
               checkpointIdStr.c_str(), checkpointStr.c_str());
        return false;
    }
    if (!_checkpointAccessor->createSavepoint("user", checkpointKey, checkpointIdStr, errMsg, comment)) {
        CS_LOG(ERROR,
               "create savepoint failed, errMsg[%s], checkpointKey[%s], "
               "checkpointId[%s]",
               errMsg.c_str(), checkpointKey.c_str(), checkpointIdStr.c_str());
        return false;
    }
    return true;
}

bool ClusterCheckpointSynchronizer::removeSavepoint(checkpointid_t checkpointId, std::string& errMsg)
{
    autil::ScopedLock lock(_mutex);
    std::string checkpointKey =
        common::GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(_buildId, _clusterName, /*isDaily=*/false);
    _checkpointAccessor->removeSavepoint("user", checkpointKey, autil::StringUtil::toString(checkpointId), errMsg);
    checkpointKey = common::GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(_buildId, _clusterName,
                                                                                       /*isDaily=*/true);
    _checkpointAccessor->removeSavepoint("user", checkpointKey, autil::StringUtil::toString(checkpointId), errMsg);
    if (!deleteCheckpointInRoot()) {
        CS_LOG(WARN,
               "delete checkpoint in root, errMsg[%s], checkpointKey[%s], "
               "checkpointId[%ld]",
               errMsg.c_str(), checkpointKey.c_str(), checkpointId);
    }
    return true;
}
bool ClusterCheckpointSynchronizer::getCheckpoint(checkpointid_t checkpointId,
                                                  GenerationLevelCheckpointGetResult* result) const
{
    if (getCheckpoint(checkpointId, result, /*isDaily*/ false) ||
        getCheckpoint(checkpointId, result, /*isDaily*/ true)) {
        return true;
    }
    CS_LOG(ERROR, "get checkpoint failed, clusterName[%s], checkpointId[%lu]", _clusterName.c_str(), checkpointId);
    return false;
}

bool ClusterCheckpointSynchronizer::getCheckpoint(checkpointid_t checkpointId,
                                                  GenerationLevelCheckpointGetResult* result, bool isDaily) const
{
    std::string checkpointKey =
        common::GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(_buildId, _clusterName, isDaily);
    std::string checkpointName = autil::StringUtil::toString(checkpointId);
    std::string checkpointValue;
    if (!_checkpointAccessor->getCheckpoint(checkpointKey, checkpointName, checkpointValue)) {
        CS_LOG(WARN, "get checkpoint [%ld] for cluster [%s] failed", checkpointId, _clusterName.c_str());
        return false;
    }
    common::GenerationLevelCheckpoint checkpoint;
    if (!generationCheckpointfromStr(checkpointValue, &checkpoint)) {
        CS_LOG(WARN, "get checkpoint [%ld] for cluster [%s] failed, checkpoint value [%s]", checkpointId,
               _clusterName.c_str(), checkpointValue.c_str());
        return false;
    }
    result->generationLevelCheckpoint = checkpoint;
    result->comment.clear();
    result->isSavepoint = _checkpointAccessor->isSavepoint(checkpointKey, checkpointName, &(result->comment));
    if (result->isSavepoint) {
        result->checkpointInfoFile = getCheckpointFilePath(checkpointId);
    }
    return true;
}

bool ClusterCheckpointSynchronizer::listCheckpoint(bool savepointOnly, uint32_t offset, uint32_t limit,
                                                   std::vector<SingleGenerationLevelCheckpointResult>* result) const
{
    assert(result);
    result->clear();
    std::vector<SingleGenerationLevelCheckpointResult> checkpoints;
    auto getCheckpoints = [this, &checkpoints, savepointOnly](bool isDaily) -> bool {
        std::vector<std::pair<std::string, std::string>> checkpointPairs;
        _checkpointAccessor->listCheckpoints(
            common::GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(_buildId, _clusterName, isDaily),
            checkpointPairs);
        for (auto iter = checkpointPairs.rbegin(); iter != checkpointPairs.rend(); iter++) {
            const auto& [checkpointName, checkpointStr] = *iter;
            SingleGenerationLevelCheckpointResult singleResult;
            if (!generationCheckpointfromStr(checkpointStr, &singleResult.generationLevelCheckpoint)) {
                CS_LOG(ERROR, "get checkpoint failed. isDaily[%d], checkpointName[%s]", isDaily,
                       checkpointName.c_str());
                return false;
            }
            singleResult.isDailySavepoint = isDaily;
            std::string comment;
            std::string checkpointKey =
                common::GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(_buildId, _clusterName, isDaily);
            bool isSavepoint = _checkpointAccessor->isSavepoint(checkpointKey, checkpointName, &comment);
            if (isSavepoint) {
                singleResult.isSavepoint = true;
            }
            if (savepointOnly == false || isSavepoint) {
                checkpoints.push_back(singleResult);
            }
        }
        return true;
    };

    if (!getCheckpoints(/*isDaily*/ true) || !getCheckpoints(/*isDaily*/ false)) {
        CS_LOG(ERROR,
               "getCheckpoints failed. clusterName[%s] savepointOnly[%d], "
               "offset[%u], limit[%u]",
               _clusterName.c_str(), savepointOnly, offset, limit);
        return false;
    }
    std::sort(checkpoints.begin(), checkpoints.end(), [](const auto& left, const auto& right) {
        if (left.generationLevelCheckpoint.createTime == right.generationLevelCheckpoint.createTime) {
            return left.generationLevelCheckpoint.checkpointId > right.generationLevelCheckpoint.checkpointId;
        }
        return left.generationLevelCheckpoint.createTime > right.generationLevelCheckpoint.createTime;
    });
    for (size_t i = offset; i < offset + limit && i < checkpoints.size(); ++i) {
        result->push_back(checkpoints[i]);
    }

    return true;
}

bool ClusterCheckpointSynchronizer::generationCheckpointfromStr(const std::string& str,
                                                                common::GenerationLevelCheckpoint* checkpoint)
{
    try {
        autil::legacy::FromJsonString(*checkpoint, str);
    } catch (const autil::legacy::ExceptionBase& e) {
        // CS_LOG(ERROR, "generation level checkpoint load failed,
        // value[%s], exception[%s]", str.c_str(), e.what());
        return false;
    } catch (const std::exception& e) {
        // CS_LOG(ERROR, "generation level checkpoint load failed,
        // value[%s], exception[%s]", str.c_str(), e.what());
        return false;
    } catch (...) {
        // CS_LOG(ERROR, "generation level checkpoint load failed,
        // value[%s], unknown exception", str.c_str());
        return false;
    }
    if ((*checkpoint).versionIdMapping.empty()) {
        // CS_LOG(ERROR, "invalid generation level checkpoint [%s], empty
        // version id mapping", str.c_str());
        return false;
    }
    return true;
}

std::pair<bool, common::PartitionLevelCheckpoint>
ClusterCheckpointSynchronizer::getPartitionCheckpoint(const proto::Range& range) const
{
    common::PartitionLevelCheckpoint checkpoint;
    std::string partitionLevelCheckpointKey =
        common::PartitionLevelCheckpoint::genPartitionLevelCheckpointKey(_buildId, _clusterName, range);
    std::string checkpointName;
    std::string checkpointValue;
    if (!_checkpointAccessor->getLatestCheckpoint(partitionLevelCheckpointKey, checkpointName, checkpointValue)) {
        CS_LOG(DEBUG, "get version meta for cluster [%s], range [%d, %d] failed", _clusterName.c_str(), range.from(),
               range.to());
        return {true, common::PartitionLevelCheckpoint()};
    }
    try {
        autil::legacy::FromJsonString(checkpoint, checkpointValue);
    } catch (const autil::legacy::ExceptionBase& e) {
        CS_LOG(ERROR, "checkpoint load failed, value[%s], exception[%s]", checkpointValue.c_str(), e.what());
        return {false, common::PartitionLevelCheckpoint()};
    } catch (const std::exception& e) {
        CS_LOG(ERROR, "checkpoint load failed, value[%s], exception[%s]", checkpointValue.c_str(), e.what());
        return {false, common::PartitionLevelCheckpoint()};
    } catch (...) {
        CS_LOG(ERROR, "checkpoint load failed, value[%s], unknown exception", checkpointValue.c_str());
        return {false, common::PartitionLevelCheckpoint()};
    }
    return {true, checkpoint};
}

std::string ClusterCheckpointSynchronizer::getCheckpointFileName(const checkpointid_t checkpointId) const
{
    std::string fileName = std::string(CHECKPOINT_FILE_PREFIX) + "." + autil::StringUtil::toString(checkpointId);
    return fileName;
}

std::string ClusterCheckpointSynchronizer::getCheckpointDirectory() const
{
    std::string generationPath =
        util::IndexPathConstructor::getGenerationIndexPath(_root, _clusterName, _buildId.generationid());
    std::string directory = fslib::util::FileUtil::joinFilePath(generationPath, CHECKPOINT_DIRECTORY_NAME);
    return directory;
}

std::string ClusterCheckpointSynchronizer::getCheckpointFilePath(const checkpointid_t checkpointId) const
{
    std::string directory = getCheckpointDirectory();
    std::string fileName = getCheckpointFileName(checkpointId);
    std::string filePath = fslib::util::FileUtil::joinFilePath(directory, fileName);
    return filePath;
}

int64_t ClusterCheckpointSynchronizer::getCurrentTime() const
{
    return autil::TimeUtility::currentTimeInMilliSeconds();
}

std::string ClusterCheckpointSynchronizer::getGenerationUserName(const checkpointid_t checkpointId) const
{
    return std::string(GENERATION_USER_PREFIX) + "_" + autil::StringUtil::toString(checkpointId);
}

void ClusterCheckpointSynchronizer::reportPartitionLevelCheckpointMetric()
{
    for (const auto& range : _ranges) {
        auto [ret, partitionCheckpoint] = getPartitionCheckpoint(range);
        if (!ret) {
            CS_LOG(ERROR,
                   "get partition checkpoint for cluster [%s], range [%d, %d] "
                   "failed",
                   _clusterName.c_str(), range.from(), range.to());
            continue;
        }
        if (partitionCheckpoint.versionId == indexlibv2::INVALID_VERSIONID) {
            continue;
        }
        std::string rangeKey = common::PartitionLevelCheckpoint::genRangeKey(range);
        if (_metricsReporter) {
            kmonitor::MetricsTags tags;
            tags.AddTag("partition", rangeKey);
            tags.AddTag("cluster", _clusterName);
            _metricsReporter->reportIndexVersion(partitionCheckpoint.versionId, tags);
            _metricsReporter->reportIndexSchemaVersion(partitionCheckpoint.readSchemaId, tags);
            _metricsReporter->calculateAndReportIndexTimestampFreshness(partitionCheckpoint.minTimestamp, tags);
            _metricsReporter->reportIndexSize(partitionCheckpoint.indexSize, tags);
        }
    }
}

}} // namespace build_service::admin
