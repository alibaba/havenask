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
#include "build_service/admin/taskcontroller/SingleBuilderTaskV2.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <google/protobuf/stubs/status.h>
#include <google/protobuf/util/json_util.h>
#include <memory>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "build_service/admin/CheckpointCreator.h"
#include "build_service/admin/CheckpointMetricReporter.h"
#include "build_service/admin/ClusterCheckpointSynchronizer.h"
#include "build_service/admin/DefaultBrokerTopicCreator.h"
#include "build_service/common/BuilderCheckpointAccessor.h"
#include "build_service/common/Checkpoint.h"
#include "build_service/common/IndexCheckpointFormatter.h"
#include "build_service/common/IndexTaskRequestQueue.h"
#include "build_service/common/SwiftResourceKeeper.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/config/DataLinkModeUtil.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/util/DataSourceHelper.h"
#include "build_service/util/RangeUtil.h"
#include "hippo/proto/Common.pb.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionMeta.h"
#include "kmonitor/client/core/MetricsTags.h"

using namespace std;
using namespace build_service::config;
using namespace autil;
using namespace autil::legacy;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, SingleBuilderTaskV2);

SingleBuilderTaskV2::SingleBuilderTaskV2(const std::string& taskId, const std::string& taskName,
                                         const TaskResourceManagerPtr& resMgr)
    : TaskController(taskId, taskName, resMgr)
    , _lastAlignVersionTimestamp(autil::TimeUtility::currentTimeInSeconds())
    , _indexCkpAccessor(CheckpointCreator::createIndexCheckpointAccessor(_resourceManager))
    , _builderCkpAccessor(CheckpointCreator::createBuilderCheckpointAccessor(_resourceManager))
    , _schemaVersion(config::INVALID_SCHEMAVERSION)
{
}

SingleBuilderTaskV2::SingleBuilderTaskV2(const SingleBuilderTaskV2& other)
    : TaskController(other)
    , _clusterName(other._clusterName)
    , _partitionCount(other._partitionCount)
    , _parallelNum(other._parallelNum)
    , _configPath(other._configPath)
    , _rollbackInfo(other._rollbackInfo)
    , _alignVersionId(other._alignVersionId)
    , _lastAlignVersionTimestamp(other._lastAlignVersionTimestamp)
    , _checkpointKeepCount(other._checkpointKeepCount)
    , _buildStep(other._buildStep)
    , _builderDataDesc(other._builderDataDesc)
    , _swiftResourceGuard(other._swiftResourceGuard)
    , _builders(other._builders)
    , _indexCkpAccessor(other._indexCkpAccessor)
    , _builderCkpAccessor(other._builderCkpAccessor)
    , _schemaVersion(other._schemaVersion)
{
}

SingleBuilderTaskV2::~SingleBuilderTaskV2() { _swiftResourceGuard.reset(); }

bool SingleBuilderTaskV2::init(const std::string& clusterName, const std::string& taskConfigFilePath,
                               const std::string& initParam)
{
    _clusterName = clusterName;
    return true;
}

bool SingleBuilderTaskV2::start(const KeyValueMap& param)
{
    std::string buildStep = getValueFromKeyValueMap(param, "buildStep");
    BS_LOG(INFO, "begin start [%s] builder for [%s]", buildStep.c_str(), _clusterName.c_str());
    if (buildStep != "full") {
        auto iter = param.find("hasRealtimeDataDesc");
        if (iter != param.end()) {
            if (!StringUtil::fromString(iter->second, _needBuildData)) {
                BS_LOG(ERROR, "invalid needBuildRealtimeData [%s]", iter->second.c_str());
                return false;
            }
        }
    }
    int64_t schemaId = config::INVALID_SCHEMAVERSION;
    auto iter = param.find("schemaId");
    if (iter != param.end()) {
        if (!StringUtil::numberFromString(iter->second, schemaId)) {
            BS_LOG(ERROR, "invalid schema id[%s]", iter->second.c_str());
            return false;
        }
        if (schemaId < _schemaVersion) {
            BS_LOG(ERROR, "invalid schema id[%s], last schema id[%ld] ", iter->second.c_str(), _schemaVersion);
            return false;
        }
        _schemaVersion = schemaId;
    }

    // parse config
    ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    ResourceReaderPtr resourceReader;
    if (_schemaVersion == config::INVALID_SCHEMAVERSION) {
        resourceReader = readerAccessor->getLatestConfig();
        auto tabletSchema = resourceReader->getTabletSchema(_clusterName);
        if (!tabletSchema) {
            BS_LOG(ERROR, "get schema failed[%s]", _clusterName.c_str());
            return false;
        }
        _schemaVersion = tabletSchema->GetSchemaId();
    } else {
        resourceReader = readerAccessor->getConfig(_clusterName, _schemaVersion);
    }
    _configPath = resourceReader->getOriginalConfigPath();

    BuildRuleConfig buildRuleConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.builder_rule_config",
                                                      buildRuleConfig)) {
        string errorMsg = "parse cluster_config.builder_rule_config for [" + _clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    // partition count
    _partitionCount = buildRuleConfig.partitionCount;
    if (buildStep == "full") {
        _buildStep = proto::BUILD_STEP_FULL;
        _parallelNum = buildRuleConfig.buildParallelNum;
    } else {
        _buildStep = proto::BUILD_STEP_INC;
        _parallelNum = buildRuleConfig.incBuildParallelNum;
    }
    if (!_needBuildData) {
        _parallelNum = 1;
    }
    if (!processRollback(param)) {
        return false;
    }
    if (!parseImportParam(param)) {
        return false;
    }
    if (!updateKeepCheckpointCount(resourceReader)) {
        return false;
    }
    if (_needBuildData) {
        if (!prepareBuilderDataDescription(resourceReader, param, _builderDataDesc)) {
            return false;
        }
    }
    prepareBuilders(_builders, true);
    BS_LOG(INFO, "end start [%s] builder for [%s]", buildStep.c_str(), _clusterName.c_str());
    return true;
}

bool SingleBuilderTaskV2::parseImportParam(const KeyValueMap& param)
{
    std::string importedVersionIdStr = getValueFromKeyValueMap(param, "importedVersionId");
    if (!importedVersionIdStr.empty()) {
        if (!autil::StringUtil::numberFromString(importedVersionIdStr, _importedVersionId)) {
            BS_LOG(ERROR, "invalid imported version [%s]", importedVersionIdStr.c_str());
            return false;
        }
    }
    return true;
}
bool SingleBuilderTaskV2::processRollback(const KeyValueMap& param)
{
    std::string rollbackCheckpointIdStr = getValueFromKeyValueMap(param, BS_ROLLBACK_SOURCE_CHECKPOINT);
    RollbackInfo rollbackInfo = _rollbackInfo;
    if (!rollbackCheckpointIdStr.empty()) {
        if (autil::StringUtil::numberFromString<checkpointid_t>(rollbackCheckpointIdStr,
                                                                rollbackInfo.rollbackCheckpointId)) {
            auto checkpointSynchronizer = getCheckpointSynchronizer();
            if (!checkpointSynchronizer) {
                BS_LOG(ERROR, "get checkpoint synchronizer failed, cluster[%s]", _clusterName.c_str());
                return false;
            }
            ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult result;
            if (!checkpointSynchronizer->getCheckpoint(_clusterName, rollbackInfo.rollbackCheckpointId, &result)) {
                BS_LOG(ERROR, "get checkpoint [%ld] failed, clusterName[%s]", rollbackInfo.rollbackCheckpointId,
                       _clusterName.c_str());
                return false;
            }
            if (result.isSavepoint == false) {
                BS_LOG(ERROR, "checkpoint [%ld] is not savepoint, rollback operation is unsafe, clusterName[%s]",
                       rollbackInfo.rollbackCheckpointId, _clusterName.c_str());
                return false;
            }
            rollbackInfo.rollbackVersionIdMapping = result.generationLevelCheckpoint.versionIdMapping;
            if (rollbackInfo.rollbackVersionIdMapping.empty()) {
                BS_LOG(ERROR, "invalid empty version id mapping, checkpoint [%ld], clusterName[%s]",
                       rollbackInfo.rollbackCheckpointId, _clusterName.c_str());
                return false;
            }
        } else {
            BS_LOG(ERROR, "parse rollbackCheckpointId [%s] for [%s] failed", rollbackCheckpointIdStr.c_str(),
                   _clusterName.c_str());
            return false;
        }
    } else {
        // do nothing
        return true;
    }
    for (size_t i = 0; i < _partitionCount; i++) {
        auto range = util::RangeUtil::getRangeInfo(0, 65535, _partitionCount, /*parallelNum=*/1, /*partitionIdx=*/i,
                                                   /*parallelIdx=*/0);
        std::string rangeKey = common::PartitionLevelCheckpoint::genRangeKey(range);
        if (rollbackInfo.rollbackVersionIdMapping.find(rangeKey) == rollbackInfo.rollbackVersionIdMapping.end()) {
            BS_LOG(ERROR, "not found rangeKey[%s] from versionIdMapping[%s].", rangeKey.c_str(),
                   autil::legacy::ToJsonString(rollbackInfo.rollbackVersionIdMapping, /*isCompact=*/true).c_str());
            assert(false);
            return false;
        }
    }
    _rollbackInfo = rollbackInfo;
    _rollbackInfo.branchId++;
    BS_LOG(INFO,
           "start rollback, rollbackVersionId[%d], rollbackCheckpointId[%ld], rollbackVersionIdMapping[%s], "
           "clusterName[%s], after rolling back branchId is [%lu]",
           _rollbackInfo.rollbackVersionIdMapping.begin()->second, _rollbackInfo.rollbackCheckpointId,
           autil::legacy::ToJsonString(_rollbackInfo.rollbackVersionIdMapping, /*isCompact=*/true).c_str(),
           _clusterName.c_str(), _rollbackInfo.branchId);
    std::vector<BuilderGroup> builders;
    prepareBuilders(builders, false);
    cleanBuilderCheckpoint(builders, /*keepMasterCheckpoint=*/false);
    return true;
}

bool SingleBuilderTaskV2::updateKeepCheckpointCount(ResourceReaderPtr resourceReader)
{
    bool isConfigExist = false;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.keep_checkpoint_count",
                                                      _checkpointKeepCount, isConfigExist)) {
        BS_LOG(ERROR, "parse cluster_config.keep_checkpoint_count failed for cluster[%s]", _clusterName.c_str());
        return false;
    }
    if (isConfigExist) {
        return true;
    }
    auto tabletOptions = resourceReader->getTabletOptions(_clusterName);
    if (!tabletOptions) {
        BS_LOG(ERROR, "parse tablet options failed for cluster[%s]", _clusterName.c_str());
        return false;
    }

    _checkpointKeepCount = tabletOptions->GetOfflineConfig().GetBuildConfig().GetKeepVersionCount();
    return true;
}

bool SingleBuilderTaskV2::finish(const KeyValueMap& kvMap)
{
    int64_t stopTimeInterval = autil::EnvUtil::getEnv("builder_sync_stoptime_interval", int64_t(5));
    int64_t finishTimeInSeconds = autil::TimeUtility::currentTimeInSeconds() + stopTimeInterval;
    uint64_t readAfterFinishTsInSeconds = 0;
    if (_needBuildData) {
        auto iter = kvMap.find("finishTimeInSecond");
        if (iter != kvMap.end()) {
            // "finishTimeInSecond" tells finishTs (version stopTS)
            // "readAfterFinishTsInSeconds" tells reader read time after finishedTs, default 0
            if (!StringUtil::fromString(iter->second, finishTimeInSeconds)) {
                BS_LOG(ERROR, "invalid stopTimestampInSecond [%s] from kvMap", iter->second.c_str());
                return false;
            }

            string readAfterFinishTsInSecondsStr = getValueFromKeyValueMap(kvMap, "readAfterFinishTsInSeconds", "0");
            if (!StringUtil::fromString(readAfterFinishTsInSecondsStr, readAfterFinishTsInSeconds)) {
                BS_LOG(ERROR, "invaild readAfterFinishTsInSeconds [%s] from kvMap",
                       readAfterFinishTsInSecondsStr.c_str());
                return false;
            }
        }
        _builderDataDesc["stopTimestamp"] =
            StringUtil::toString(autil::TimeUtility::sec2us(finishTimeInSeconds + readAfterFinishTsInSeconds));
    }
    auto iter = kvMap.find("mergeConfig");
    if (iter != kvMap.end()) {
        _specificMergeConfig = iter->second;
        BS_LOG(INFO, "finish task with specific merge: [%s]", _specificMergeConfig.c_str());
    } else {
        if (_buildStep == proto::BUILD_STEP_FULL) {
            _specificMergeConfig = "full_merge";
        }
    }
    BS_LOG(INFO, "end finish, fill stopTimestamp is datadescription, cluster[%s], step[%s]", _clusterName.c_str(),
           getBuildStepStr().c_str());
    return true;
}

config::ResourceReaderPtr SingleBuilderTaskV2::getConfigReader()
{
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    auto reader = readerAccessor->getConfig(_configPath);
    if (!reader) {
        reader.reset(new config::ResourceReader(_configPath));
        reader->init();
    }
    return reader;
}

bool SingleBuilderTaskV2::prepareBuilderDataDescription(ResourceReaderPtr resourceReader, const KeyValueMap& kvMap,
                                                        proto::DataDescription& ds)
{
    auto iter = kvMap.find(DATA_DESCRIPTION_KEY);
    if (iter == kvMap.end()) {
        //以中转topic作为数据源
        registBrokerTopic();
        auto swiftResourceKeeper =
            dynamic_pointer_cast<common::SwiftResourceKeeper>(_swiftResourceGuard->getResourceKeeper());
        ds[config::READ_SRC] = "swift";
        ds[config::READ_SRC_TYPE] = SWIFT_PROCESSED_READ_SRC;
        ds[SWIFT_TOPIC_NAME] = swiftResourceKeeper->getTopicName();
        ds[SWIFT_ZOOKEEPER_ROOT] = swiftResourceKeeper->getSwiftRoot();
        ds[SWIFT_START_TIMESTAMP] = "0";
        auto configReader = getConfigReader();
        config::SwiftConfig swiftConfig;
        if (!swiftResourceKeeper->getSwiftConfig(configReader, swiftConfig)) {
            BS_LOG(ERROR, "get swift config failed");
            return false;
        }
        if (_buildStep == proto::BUILD_STEP_FULL && !swiftConfig.isFullIncShareBrokerTopic()) {
            ds[config::SRC_SIGNATURE] = std::to_string(SwiftTopicConfig::FULL_TOPIC_SRC_SIGNATURE);
        } else {
            ds[config::SRC_SIGNATURE] = std::to_string(SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE);
        }
        int64_t stopTs;
        auto iter = kvMap.find("stopTimestamp");
        if (iter != kvMap.end()) {
            if (!StringUtil::fromString(iter->second, stopTs)) {
                BS_LOG(ERROR, "invalid stopTimestamp [%s] from kvMap", iter->second.c_str());
                return false;
            }
            ds["stopTimestamp"] = iter->second;
        }
        iter = kvMap.find("batchMask");
        if (iter != kvMap.end()) {
            int32_t batchMask = -1;
            if (!StringUtil::fromString(iter->second, batchMask)) {
                BS_LOG(INFO, "invalid batchMask [%s] from param, ignore", iter->second.c_str());
            }
            ds[BATCH_MASK] = autil::StringUtil::toString(batchMask);
            BS_LOG(INFO, "create builder with batch mask[%d] for cluster [%s]", batchMask, _clusterName.c_str());
        }
        iter = kvMap.find("useInnerBatchMaskFilter");
        if (iter != kvMap.end() && iter->second == "true") {
            bool enableFastSlowQueue = false;
            if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.enable_fast_slow_queue",
                                                              enableFastSlowQueue)) {
                BS_LOG(ERROR, "parse cluster_config.enable_fast_slow_queue for [%s] failed", _clusterName.c_str());
                return false;
            }
            if (enableFastSlowQueue) {
                BS_LOG(ERROR, "not support enable fast slow queue while useInnerBatchMaskFilter for [%s]",
                       _clusterName.c_str());
                return false;
            }
            ds[USE_INNER_BATCH_MASK_FILTER] = "true";
        }
        iter = kvMap.find("batchId");
        if (iter != kvMap.end()) {
            int32_t batchId = -1;
            if (!StringUtil::fromString(iter->second, batchId)) {
                BS_LOG(INFO, "invalid batchId [%s] from param, ignore", iter->second.c_str());
            }
            _batchId = batchId;
            BS_LOG(INFO, "create builder with batchId [%d] for cluster [%s]", _batchId, _clusterName.c_str());
        }
        iter = kvMap.find("skipMerger");
        if (iter != kvMap.end() && iter->second == "true") {
            BS_LOG(INFO, "create batch builder with skipMerger = true for cluster [%s]", _clusterName.c_str());
            _needSkipMerge = true;
        }
        return true;
    }
    //添加用户指定数据源
    try {
        FromJsonString(ds, iter->second);
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "parse data Description [%s] fail", iter->second.c_str());
        return false;
    }
    if (!util::DataSourceHelper::isRealtime(ds)) {
        BS_LOG(ERROR,
               "invalid data description [%s],"
               "only support swift-like type",
               iter->second.c_str());
        return false;
    }
    iter = kvMap.find(SRC_SIGNATURE);
    if (iter != kvMap.end()) {
        ds[SRC_SIGNATURE] = iter->second;
    }

    auto checkpointSynchronizer = getCheckpointSynchronizer();
    if (checkpointSynchronizer == nullptr) {
        BS_LOG(ERROR, "checkpoint synchronizer is uninitialized, clusterName[%s].", _clusterName.c_str());
        return false;
    }
    const auto& buildId = checkpointSynchronizer->getBuildId();

    config::ControlConfig controlConfig;
    if (!resourceReader->getDataTableConfigWithJsonPath(buildId.datatable(), "control_config", controlConfig)) {
        BS_LOG(ERROR, "get control_config.is_inc_processor_exist failed from dataTable[%s] failed",
               buildId.datatable().c_str());
        return false;
    }

    if (!DataLinkModeUtil::addDataLinkModeParamToBuilderTarget(controlConfig, _clusterName, &ds)) {
        BS_LOG(ERROR, "addDataLinkModeParam failed");
        return false;
    }

    BS_LOG(INFO, "data description for builder, ds [%s].", ToJsonString(ds, true).c_str());
    return true;
}

bool SingleBuilderTaskV2::checkNoNeedBuildDataFinish()
{
    if (_lastAlignSchemaId != _schemaVersion) {
        return false;
    }
    std::shared_ptr<common::IndexTaskRequestQueue> generationLevelRequestQueue;
    _resourceManager->getResource(generationLevelRequestQueue);
    if (generationLevelRequestQueue != nullptr && generationLevelRequestQueue->size() != 0) {
        return false;
    }
    if (_alignVersionId != indexlibv2::INVALID_VERSIONID) {
        return false;
    }
    for (auto& builderGroup : _builders) {
        const auto& indexTaskQueue =
            builderGroup.master.buildCheckpoint.commitedVersion.versionMeta.GetIndexTaskQueue();
        if (indexTaskQueue.size() == 0) {
            continue;
        }
        for (const auto& indexTaskMeta : indexTaskQueue) {
            if (indexTaskMeta.GetState() != indexlibv2::framework::IndexTaskMeta::DONE &&
                indexTaskMeta.GetState() != indexlibv2::framework::IndexTaskMeta::ABORTED) {
                return false;
            }
        }
        if (builderGroup.master.buildCheckpoint.commitedVersion.versionMeta.GetVersionId() !=
            builderGroup.master.buildCheckpoint.lastAlignedVersion.versionMeta.GetVersionId()) {
            return false;
        }
    }
    return true;
}

bool SingleBuilderTaskV2::operate(TaskController::Nodes& nodes)
{
    if (_builderDataDesc.find(config::READ_SRC_TYPE) != _builderDataDesc.end() &&
        _builderDataDesc[config::READ_SRC_TYPE] == SWIFT_PROCESSED_READ_SRC) {
        // for failover recover
        // can not in jsonzie fun
        // because stopped task can be recover back
        registBrokerTopic();
    }

    if (_builders.size() != _partitionCount) {
        _builders.clear();
        prepareBuilders(_builders, false);
        nodes.clear();
    }

    updateBuilderNodes(nodes);
    nodes.clear();

    updateRequestQueue();

    bool isFinished = true;
    for (auto& builder : _builders) {
        builder.master.updateTargetDescription(_builderDataDesc, _specificMergeConfig);
        if (!builder.master.node.reachedTarget) {
            isFinished = false;
        }
        nodes.push_back(builder.master.node);
        for (auto& slave : builder.slaves) {
            slave.updateTargetDescription(_builderDataDesc, "");
            nodes.push_back(slave.node);
        }
    }

    if (isFinished) {
        publishCheckpoint();
        if (_buildStep == proto::BUILD_STEP_FULL || _batchId != -1) {
            cleanBuilderCheckpoint(_builders, /*keepMasterCheckpoint=*/true);
        }
        _swiftResourceGuard.reset();
        BS_LOG(INFO, "[%s] builder finished for cluster [%s]", getBuildStepStr().c_str(), _clusterName.c_str());
    } else {
        triggerAlignVersion();
        if (_alignVersionId != indexlibv2::INVALID_VERSIONID && _buildStep == proto::BUILD_STEP_INC) {
            publishCheckpoint();
        }
    }

    reportBuildFreshness();
    if (!_needBuildData) {
        if (checkNoNeedBuildDataFinish()) {
            return true;
        }
        return false;
    }

    return isFinished;
}

void SingleBuilderTaskV2::updateRequestQueue()
{
    std::shared_ptr<common::IndexTaskRequestQueue> generationLevelRequestQueue;
    _resourceManager->getResource(generationLevelRequestQueue);
    if (generationLevelRequestQueue == nullptr) {
        return;
    }

    for (auto& builderGroup : _builders) {
        auto& master = builderGroup.master;
        auto key = common::IndexTaskRequestQueue::genPartitionLevelKey(_clusterName, master.range);
        auto newRequestQueue = (*generationLevelRequestQueue)[key];
        master.taskInfo.requestQueue.swap(newRequestQueue);
    }
}

void SingleBuilderTaskV2::reportBuildFreshness()
{
    CheckpointMetricReporterPtr reporter;
    _resourceManager->getResource(reporter);
    if (!reporter) {
        return;
    }

    int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
    // report interval 10s
    if (currentTime - _lastReportTimestamp < 10) {
        return;
    }
    _lastReportTimestamp = currentTime;
    for (auto& builderGroup : _builders) {
        kmonitor::MetricsTags tags;
        tags.AddTag("cluster", _clusterName);
        tags.AddTag("role", "master");
        auto& range = builderGroup.master.range;
        tags.AddTag("partition", StringUtil::toString(range.from()) + "_" + StringUtil::toString(range.to()));
        int64_t offset = builderGroup.master.buildCheckpoint.commitedVersion.versionMeta.GetMinLocatorTs();
        reporter->reportSingleBuilderFreshness(offset, tags);
        for (auto& slave : builderGroup.slaves) {
            kmonitor::MetricsTags tags;
            tags.AddTag("cluster", _clusterName);
            tags.AddTag("role", "slave");
            auto& range = slave.range;
            tags.AddTag("partition", StringUtil::toString(range.from()) + "_" + StringUtil::toString(range.to()));
            int64_t offset = slave.buildCheckpoint.commitedVersion.versionMeta.GetMinLocatorTs();
            reporter->reportSingleBuilderFreshness(offset, tags);
        }
    }
}

bool SingleBuilderTaskV2::needTriggerAlignVersion()
{
    if (_alignVersionId != indexlibv2::INVALID_VERSIONID) {
        // when align version failed, need realign version
        for (auto& builder : _builders) {
            if (builder.master.buildCheckpoint.lastAlignFailedVersionId == _alignVersionId) {
                return true;
            }
        }
        return false;
    }

    // when all build finish auto align version
    bool allSlaveFinished = true;
    for (auto& builder : _builders) {
        if (builder.slaves.size() > 0) {
            for (auto& slave : builder.slaves) {
                if (!slave.node.reachedTarget) {
                    allSlaveFinished = false;
                    break;
                }
            }
        } else {
            if (!builder.masterBuildFinished) {
                allSlaveFinished = false;
                break;
            }
        }
        if (!allSlaveFinished) {
            break;
        }
    }

    if (allSlaveFinished) {
        return true;
    }

    // full build only align version when finish
    // batch build for ODL only align versin when finish
    if (_buildStep == proto::BUILD_STEP_FULL || _batchId != -1) {
        return false;
    }
    // if alter schema finished align verison
    bool canAlignSchemaId = true;
    bool alreadyAlignedSchemaId = true;
    for (auto& builder : _builders) {
        if (builder.master.buildCheckpoint.commitedVersion.versionMeta.GetReadSchemaId() != _schemaVersion) {
            canAlignSchemaId = false;
            break;
        }

        if (builder.master.buildCheckpoint.lastAlignedVersion.versionMeta.GetReadSchemaId() != _schemaVersion) {
            alreadyAlignedSchemaId = false;
        }
    }
    if (canAlignSchemaId && !alreadyAlignedSchemaId) {
        return true;
    }
    int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
    int64_t publishInterval = autil::EnvUtil::getEnv("bs_version_publish_interval_sec", 15 * 60);
    if (currentTime - _lastAlignVersionTimestamp >= publishInterval) // 15min
    {
        // when no new commited version, do not publish new version
        bool needAlign = false;
        for (auto& builder : _builders) {
            if (builder.master.buildCheckpoint.commitedVersion.versionMeta.GetVersionId() !=
                builder.master.buildCheckpoint.lastAlignedVersion.versionMeta.GetVersionId()) {
                needAlign = true;
                break;
            }
        }
        if (!needAlign) {
            _lastAlignVersionTimestamp = currentTime;
        }
        return needAlign;
    }
    return false;
}

void SingleBuilderTaskV2::triggerAlignVersion()
{
    if (needTriggerAlignVersion()) {
        for (auto& builder : _builders) {
            _alignVersionId =
                std::max(builder.master.buildCheckpoint.commitedVersion.versionMeta.GetVersionId(), _alignVersionId);
        }
        _alignVersionId += 4;
        _alignVersionId |= indexlibv2::framework::Version::PUBLIC_VERSION_ID_MASK;
        BS_LOG(INFO, "triggerAlignVersion success, set alignVersionId[%d], cluster[%s], setp[%s]", _alignVersionId,
               _clusterName.c_str(), getBuildStepStr().c_str());
        return;
    }
}

bool SingleBuilderTaskV2::needPublishCheckpoint()
{
    if (_alignVersionId == indexlibv2::INVALID_VERSIONID) {
        return false;
    }
    for (const auto& builder : _builders) {
        if (builder.master.buildCheckpoint.lastAlignedVersion.versionMeta.GetVersionId() != _alignVersionId) {
            return false;
        }
    }
    return true;
}

std::shared_ptr<CheckpointSynchronizer> SingleBuilderTaskV2::getCheckpointSynchronizer() const
{
    std::shared_ptr<CheckpointSynchronizer> checkpointSynchronizer;
    if (_resourceManager->getResource(checkpointSynchronizer)) {
        return checkpointSynchronizer;
    }
    return nullptr;
}

void SingleBuilderTaskV2::publishCheckpoint()
{
    if (!needPublishCheckpoint()) {
        return;
    }
    auto checkpointSynchronizer = getCheckpointSynchronizer();
    if (checkpointSynchronizer == nullptr) {
        BS_LOG(ERROR, "Publish checkpoint failed: checkpoint synchronizer is uninitialized, clusterName[%s].",
               _clusterName.c_str());
        return;
    }
    const auto& buildId = checkpointSynchronizer->getBuildId();
    BS_LOG(INFO, "start publishing checkpoint, alignedVersionId[%d], cluster[%s], step[%s] buildId[%s]",
           _alignVersionId, _clusterName.c_str(), getBuildStepStr().c_str(), buildId.ShortDebugString().c_str());
    int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
    ClusterCheckpointSynchronizer::IndexInfoParam indexInfoParam;
    indexInfoParam.startTimestamp = _lastAlignVersionTimestamp;
    indexInfoParam.finishTimestamp = currentTime;
    int64_t alignSchemaId = config::INVALID_SCHEMAVERSION;
    for (const auto& builder : _builders) {
        std::string errMsg;
        const auto& alignedVersion = builder.master.buildCheckpoint.lastAlignedVersion;
        if (alignedVersion.versionMeta.GetVersionId() == indexlib::INVALID_VERSIONID) {
            BS_LOG(DEBUG, "aligned version meta is invalid, buildId[%s]", buildId.ShortDebugString().c_str());
            return;
        }
        if (alignSchemaId == config::INVALID_SCHEMAVERSION) {
            alignSchemaId = alignedVersion.versionMeta.GetReadSchemaId();
        } else {
            auto tmpSchemaId = alignedVersion.versionMeta.GetReadSchemaId();
            alignSchemaId = alignSchemaId > tmpSchemaId ? tmpSchemaId : alignSchemaId;
        }
        if (!checkpointSynchronizer->publishPartitionLevelCheckpoint(
                _clusterName, builder.master.range,
                autil::legacy::ToJsonString(alignedVersion.versionMeta, /*isCompact=*/true), errMsg)) {
            BS_LOG(ERROR, "%s", errMsg.c_str());
            return;
        }
        indexInfoParam.totalRemainIndexSizes[builder.master.range] = alignedVersion.totalRemainIndexSize;
    }
    ClusterCheckpointSynchronizer::SyncOptions syncOptions;
    syncOptions.isDaily = false;
    syncOptions.isInstant = true;
    syncOptions.isOffline = true;
    syncOptions.addIndexInfos = true;
    syncOptions.indexInfoParam = indexInfoParam;
    if (!checkpointSynchronizer->syncCluster(_clusterName, syncOptions)) {
        BS_LOG(ERROR, "Sync cluster [%s] failed. buildId[%s]", _clusterName.c_str(),
               buildId.ShortDebugString().c_str());
        return;
    }
    if (_rollbackInfo.rollbackCheckpointId != INVALID_CHECKPOINT_ID) {
        std::string errMsg;
        if (!checkpointSynchronizer->removeSavepoint(_clusterName, _rollbackInfo.rollbackCheckpointId, errMsg)) {
            BS_LOG(WARN, "Remove savepoint failed: cluster[%s] checkpointId[%ld] errMsg[%s], buildId[%s].",
                   _clusterName.c_str(), _rollbackInfo.rollbackCheckpointId, errMsg.c_str(),
                   buildId.ShortDebugString().c_str());
        }
    }
    _rollbackInfo.rollbackCheckpointId = INVALID_CHECKPOINT_ID;
    _rollbackInfo.rollbackVersionIdMapping.clear();
    BS_LOG(INFO, "Publish checkpoint successfully: current aligned versionId[%d], cluster[%s], step[%s], buildId[%s].",
           _alignVersionId, _clusterName.c_str(), getBuildStepStr().c_str(), buildId.ShortDebugString().c_str());
    _alignVersionId = indexlibv2::INVALID_VERSIONID;
    _lastAlignSchemaId = alignSchemaId;
    _lastAlignVersionTimestamp = currentTime;
}

void SingleBuilderTaskV2::cleanBuilderCheckpoint(const std::vector<BuilderGroup>& builders,
                                                 bool keepMasterCheckpoint) const
{
    for (auto& builder : builders) {
        if (!keepMasterCheckpoint) {
            _builderCkpAccessor->clearMasterCheckpoint(_clusterName, builder.master.range);
        }
        for (auto& slave : builder.slaves) {
            _builderCkpAccessor->clearSlaveCheckpoint(_clusterName, slave.range);
        }
    }
}

SingleBuilderTaskV2::BuilderNode SingleBuilderTaskV2::createBuilderNode(std::string roleName, uint32_t nodeId,
                                                                        uint32_t partitionCount, uint32_t parallelNum,
                                                                        uint32_t instanceIdx, bool isStart) const
{
    BuilderNode builder;
    builder.node.nodeId = nodeId;
    builder.node.roleName = roleName;
    builder.node.instanceIdx = instanceIdx;
    builder.node.reachedTarget = false;
    builder.node.taskTarget.setPartitionCount(partitionCount);
    builder.node.taskTarget.setParallelNum(parallelNum);
    builder.range = util::RangeUtil::getRangeInfo(0, 65535, partitionCount, parallelNum, instanceIdx / parallelNum,
                                                  instanceIdx % parallelNum);
    auto masterBuilderRange = util::RangeUtil::getRangeInfo(0, 65535, partitionCount, /*parallelNum=*/1,
                                                            instanceIdx / parallelNum, /*parallelIdx=*/0);
    if (roleName == "master") {
        if (_parallelNum > 1 || !_needBuildData) {
            builder.taskInfo.buildMode = proto::BuildMode::PUBLISH;
        } else {
            builder.taskInfo.buildMode = proto::BuildMode::BUILD_PUBLISH;
        }
        std::string checkpoint;
        proto::CheckpointInfo checkpointInfo;
        std::string checkpointName;
        if (_builderCkpAccessor->getMasterCheckpoint(_clusterName, builder.range, checkpoint)) {
            common::BuildCheckpoint buildCheckpoint;
            FromJsonString(buildCheckpoint, checkpoint);
            builder.taskInfo.latestPublishedVersion =
                buildCheckpoint.buildInfo.commitedVersion.versionMeta.GetVersionId();
            builder.taskInfo.indexRoot = buildCheckpoint.buildInfo.commitedVersion.indexRoot;
            builder.buildCheckpoint = buildCheckpoint.buildInfo;
            if (buildCheckpoint.buildFinished && buildCheckpoint.buildStep == _buildStep) {
                builder.node.reachedTarget = true;
            }
        } else if (!_rollbackInfo.rollbackVersionIdMapping.empty()) {
            std::string rangeKey = common::PartitionLevelCheckpoint::genRangeKey(masterBuilderRange);
            auto iter = _rollbackInfo.rollbackVersionIdMapping.find(rangeKey);
            if (iter != _rollbackInfo.rollbackVersionIdMapping.end()) {
                builder.taskInfo.latestPublishedVersion = iter->second;
            }
        } else if (_indexCkpAccessor->getLatestIndexCheckpoint(_clusterName, checkpointInfo, checkpointName)) {
            builder.taskInfo.latestPublishedVersion = checkpointInfo.versionid();
        } else if (_importedVersionId != indexlibv2::INVALID_VERSIONID) {
            builder.taskInfo.latestPublishedVersion = _importedVersionId;
        }
        builder.taskInfo.alignVersionId = _alignVersionId;
        builder.taskInfo.skipMerge = _needSkipMerge;
        builder.taskInfo.batchId = _batchId;
        builder.taskInfo.buildStep = getBuildStepStr();
        builder.taskInfo.branchId = _rollbackInfo.branchId;
    } else if (roleName == "slave") {
        builder.taskInfo.buildMode = proto::BuildMode::BUILD;
        builder.taskInfo.buildStep = getBuildStepStr();
        builder.taskInfo.branchId = _rollbackInfo.branchId;
        builder.taskInfo.batchId = _batchId;
        std::string checkpoint;
        if (_builderCkpAccessor->getSlaveCheckpoint(_clusterName, builder.range, checkpoint)) {
            if (!isStart) {
                common::BuildCheckpoint buildCheckpoint;
                FromJsonString(buildCheckpoint, checkpoint);
                updateVersionProgress({buildCheckpoint.buildInfo}, builder);
                builder.buildCheckpoint = buildCheckpoint.buildInfo;
                if (buildCheckpoint.buildFinished && buildCheckpoint.buildStep == _buildStep) {
                    builder.node.reachedTarget = true;
                }
            }
        } else if (!_rollbackInfo.rollbackVersionIdMapping.empty()) {
            std::string rangeKey = common::PartitionLevelCheckpoint::genRangeKey(masterBuilderRange);
            auto iter = _rollbackInfo.rollbackVersionIdMapping.find(rangeKey);
            if (iter != _rollbackInfo.rollbackVersionIdMapping.end()) {
                builder.taskInfo.latestPublishedVersion = iter->second;
            }
        }
    }
    builder.taskInfo.buildStep = getBuildStepStr();
    return builder;
}

std::string SingleBuilderTaskV2::getBuildStepStr() const
{
    if (_buildStep == proto::BUILD_STEP_FULL) {
        return "full";
    }
    return config::BUILD_STEP_INC_STR;
}

void SingleBuilderTaskV2::prepareBuilders(std::vector<BuilderGroup>& builders, bool isStart) const
{
    uint32_t nodeId = 0;
    for (size_t i = 0; i < _partitionCount; i++) {
        BuilderGroup builder;
        builder.master =
            createBuilderNode("master", nodeId, _partitionCount, /*parallelNum=*/1, /*instanceIdx=*/i, isStart);
        nodeId++;
        if (_parallelNum > 1) {
            for (size_t j = 0; j < _parallelNum; j++) {
                builder.slaves.push_back(
                    createBuilderNode("slave", nodeId, _partitionCount, _parallelNum, i * _parallelNum + j, isStart));
                nodeId++;
            }
        }
        updatePublicVersionForSlave(builder);
        updateSlaveProgressForMaster(builder);
        builders.push_back(builder);
    }
}

void SingleBuilderTaskV2::commitBuilderCheckpoint(const proto::BuildTaskCurrentInfo& checkpoint,
                                                  const BuilderNode& builderNode, bool isMaster)
{
    common::BuildCheckpoint buildCheckpoint;
    buildCheckpoint.buildInfo = checkpoint;
    buildCheckpoint.buildFinished = builderNode.node.reachedTarget;
    buildCheckpoint.buildStep = _buildStep;
    versionid_t versionId = checkpoint.commitedVersion.versionMeta.GetVersionId();
    if (isMaster) {
        // inc build will inherit
        _builderCkpAccessor->addMasterCheckpoint(_clusterName, builderNode.range, versionId,
                                                 ToJsonString(buildCheckpoint));
    } else {
        _builderCkpAccessor->addSlaveCheckpoint(_clusterName, builderNode.range, versionId,
                                                ToJsonString(buildCheckpoint));
    }
}

void SingleBuilderTaskV2::updateVersionProgress(const std::vector<proto::BuildTaskCurrentInfo>& checkpoints,
                                                BuilderNode& builderNode) const
{
    std::vector<proto::VersionProgress> versionProgress;
    for (const auto& checkpoint : checkpoints) {
        versionid_t versionId = checkpoint.commitedVersion.versionMeta.GetVersionId();
        if (versionId == indexlibv2::INVALID_VERSIONID) {
            continue;
        }
        proto::VersionProgress progress;
        progress.versionId = versionId;
        progress.fence = checkpoint.commitedVersion.versionMeta.GetFenceName();
        versionProgress.push_back(progress);
    }
    builderNode.taskInfo.slaveVersionProgress.swap(versionProgress);
}

std::pair<bool, TaskController::Node> SingleBuilderTaskV2::getNode(uint32_t nodeId, const TaskController::Nodes& nodes)
{
    for (const auto& node : nodes) {
        if (node.nodeId == nodeId) {
            return make_pair(true, node);
        }
    }
    return make_pair(false, TaskController::Node());
}

bool SingleBuilderTaskV2::parseBuildCheckpoint(const std::string& statusDescription,
                                               proto::BuildTaskCurrentInfo& checkpoint)
{
    if (statusDescription.empty()) {
        return false;
    }
    try {
        FromJsonString(checkpoint, statusDescription);
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "parse status description [%s] fail", statusDescription.c_str());
        return false;
    }
    return true;
}

void SingleBuilderTaskV2::updateBuilderNodes(const TaskController::Nodes& nodes)
{
    if (nodes.size() == 0) {
        return;
    }
    // is all master builder align version success in full build
    bool allMasterFinished = true;
    for (auto& builder : _builders) {
        auto& master = builder.master;
        auto [exist, node] = getNode(master.node.nodeId, nodes);
        if (!exist) {
            allMasterFinished = false;
            continue;
        }
        proto::BuildTaskCurrentInfo masterCheckpoint;
        if (parseBuildCheckpoint(node.statusDescription, masterCheckpoint)) {
            if (node.reachedTarget) {
                builder.masterBuildFinished = true;
            }
            if (!(node.reachedTarget && _alignVersionId != indexlibv2::INVALID_VERSIONID &&
                  masterCheckpoint.lastAlignedVersion.versionMeta.GetVersionId() == _alignVersionId)) {
                allMasterFinished = false;
            }
            if (master.buildCheckpoint != masterCheckpoint || node.reachedTarget) {
                // commit version sync
                versionid_t commitedVersionId = masterCheckpoint.commitedVersion.versionMeta.GetVersionId();
                if (commitedVersionId != indexlibv2::INVALID_VERSIONID) {
                    commitBuilderCheckpoint(masterCheckpoint, master, true); //持久化
                    master.taskInfo.latestPublishedVersion = commitedVersionId;
                    master.taskInfo.indexRoot = masterCheckpoint.commitedVersion.indexRoot;
                    master.buildCheckpoint.commitedVersion = masterCheckpoint.commitedVersion;
                }
                // lastAlignedVersion sync
                if (masterCheckpoint.lastAlignedVersion.versionMeta.GetVersionId() != indexlibv2::INVALID_VERSIONID) {
                    master.buildCheckpoint.lastAlignedVersion = masterCheckpoint.lastAlignedVersion;
                }
                // lastAlignFailedVersion sync
                if (masterCheckpoint.lastAlignFailedVersionId != indexlibv2::INVALID_VERSIONID) {
                    master.buildCheckpoint.lastAlignFailedVersionId = masterCheckpoint.lastAlignFailedVersionId;
                }
            }
        } else {
            allMasterFinished = false;
        }
        master.taskInfo.alignVersionId = _alignVersionId;
        master.taskInfo.skipMerge = _needSkipMerge;
        master.taskInfo.batchId = _batchId;
        for (auto& slave : builder.slaves) {
            proto::BuildTaskCurrentInfo checkpoint;
            auto [exist, node] = getNode(slave.node.nodeId, nodes);
            if (!exist) {
                continue;
            }
            if (slave.node.reachedTarget) {
                continue;
            }
            if (parseBuildCheckpoint(node.statusDescription, checkpoint)) {
                if (node.reachedTarget) {
                    slave.node.reachedTarget = true;
                }
                if (slave.buildCheckpoint != checkpoint || node.reachedTarget) {
                    // sync commited verison
                    if (checkpoint.commitedVersion.versionMeta.GetVersionId() != indexlibv2::INVALID_VERSIONID) {
                        commitBuilderCheckpoint(checkpoint, slave, false);
                        updateVersionProgress({checkpoint}, slave);
                        slave.buildCheckpoint.commitedVersion = checkpoint.commitedVersion;
                    } else {
                        // when slave builder failover, may readchedTarget with invalid_versionid
                        commitBuilderCheckpoint(slave.buildCheckpoint, slave, false);
                    }
                }
            }
        }
        updateSlaveProgressForMaster(builder);
        updatePublicVersionForSlave(builder);
    }
    if (allMasterFinished) {
        for (auto& builder : _builders) {
            auto& master = builder.master;
            master.node.reachedTarget = true;
        }
    }
}

void SingleBuilderTaskV2::updateSlaveProgressForMaster(BuilderGroup& builder) const
{
    std::vector<proto::VersionProgress> slaveVersionProgress;
    bool isAllSlaveFinished = true;
    for (auto slave : builder.slaves) {
        if (!slave.node.reachedTarget) {
            isAllSlaveFinished = false;
        }
        slaveVersionProgress.insert(slaveVersionProgress.end(), slave.taskInfo.slaveVersionProgress.begin(),
                                    slave.taskInfo.slaveVersionProgress.end());
    }
    if (builder.slaves.size() == 0) {
        isAllSlaveFinished = builder.masterBuildFinished;
    }
    builder.master.taskInfo.slaveVersionProgress.swap(slaveVersionProgress);
    // all slave finished && need align version
    builder.master.taskInfo.finished = isAllSlaveFinished && (_alignVersionId != indexlibv2::INVALID_VERSIONID);
}

void SingleBuilderTaskV2::updatePublicVersionForSlave(BuilderGroup& builder) const
{
    versionid_t publicVersion = builder.master.taskInfo.latestPublishedVersion;
    for (auto& slave : builder.slaves) {
        slave.taskInfo.latestPublishedVersion = publicVersion;
    }
}

bool SingleBuilderTaskV2::operator==(const SingleBuilderTaskV2& other) const
{
    return _clusterName == other._clusterName && _partitionCount == other._partitionCount &&
           _parallelNum == other._parallelNum && _configPath == other._configPath &&
           _builderDataDesc == other._builderDataDesc && _buildStep == other._buildStep &&
           _alignVersionId == other._alignVersionId && _lastAlignVersionTimestamp == other._lastAlignVersionTimestamp &&
           _checkpointKeepCount == other._checkpointKeepCount && _needSkipMerge == other._needSkipMerge &&
           _batchId == other._batchId;
}

bool SingleBuilderTaskV2::operator!=(const SingleBuilderTaskV2& other) const { return !(*this == other); }

void SingleBuilderTaskV2::notifyStopped() { _swiftResourceGuard.reset(); }

TaskController* SingleBuilderTaskV2::clone() { return new SingleBuilderTaskV2(*this); }

void SingleBuilderTaskV2::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("cluster_name", _clusterName, _clusterName);
    json.Jsonize("partition_count", _partitionCount, _partitionCount);
    json.Jsonize("parallel_num", _parallelNum, _parallelNum);
    json.Jsonize("config_path", _configPath, _configPath);
    json.Jsonize("build_step", _buildStep, _buildStep);
    json.Jsonize("data_description", _builderDataDesc, _builderDataDesc);
    json.Jsonize("rollback_checkpoint", _rollbackInfo.rollbackCheckpointId, _rollbackInfo.rollbackCheckpointId);
    json.Jsonize("rollback_version_id_mapping", _rollbackInfo.rollbackVersionIdMapping,
                 _rollbackInfo.rollbackVersionIdMapping);
    json.Jsonize("branch_id", _rollbackInfo.branchId, _rollbackInfo.branchId);
    json.Jsonize("aligin_version", _alignVersionId, _alignVersionId);
    json.Jsonize("last_align_version_timestamp", _lastAlignVersionTimestamp, _lastAlignVersionTimestamp);
    json.Jsonize("keep_checkpoint_count", _checkpointKeepCount, _checkpointKeepCount);
    json.Jsonize("need_skip_merge", _needSkipMerge, _needSkipMerge);
    json.Jsonize("batch_id", _batchId, _batchId);
    json.Jsonize("imported_version_id", _importedVersionId, _importedVersionId);
    json.Jsonize("specific_merge_config", _specificMergeConfig, _specificMergeConfig);
    json.Jsonize("schema_version", _schemaVersion, _schemaVersion);
    json.Jsonize("last_align_schema_version", _lastAlignSchemaId, _lastAlignSchemaId);
    json.Jsonize("need_build_data", _needBuildData, _needBuildData);
    if (_schemaVersion == config::INVALID_SCHEMAVERSION) {
        auto configReader = getConfigReader();
        auto tabletSchema = configReader->getTabletSchema(_clusterName);
        if (!tabletSchema) {
            BS_LOG(ERROR, "get schema failed[%s]", _clusterName.c_str());
            throw autil::legacy::ExceptionBase("get schema failed");
        }
        _schemaVersion = tabletSchema->GetSchemaId();
    }
}

void SingleBuilderTaskV2::registBrokerTopic()
{
    if (!_needBuildData) {
        return;
    }
    if (_swiftResourceGuard) {
        return;
    }

    auto reader = getConfigReader();
    _swiftResourceGuard = DefaultBrokerTopicCreator::applyDefaultSwiftResource(_taskId, _clusterName, _buildStep,
                                                                               reader, _resourceManager);
}

void SingleBuilderTaskV2::supplementLableInfo(KeyValueMap& info) const
{
    info["cluster_name"] = _clusterName;
    info["parallel_num"] = StringUtil::toString(_parallelNum);
    info["partition_count"] = StringUtil::toString(_partitionCount);
    info["aligin_version"] = StringUtil::toString(_alignVersionId);
    info["last_align_version_timestamp"] = StringUtil::toString(_lastAlignVersionTimestamp);
    info["need_skip_merge"] = _needSkipMerge ? "true" : "false";
    info["build_step"] =
        (_buildStep == proto::BUILD_STEP_FULL) ? config::BUILD_STEP_FULL_STR : config::BUILD_STEP_INC_STR;
    info["batch_id"] = StringUtil::toString(_batchId);
    info["branch_id"] = StringUtil::toString(_rollbackInfo.branchId);
    info["data_description"] = autil::legacy::ToJsonString(_builderDataDesc, true);
    if (_importedVersionId != indexlibv2::INVALID_VERSIONID) {
        info["imported_version_id"] = StringUtil::toString(_importedVersionId);
    }
    if (_rollbackInfo.rollbackCheckpointId != INVALID_CHECKPOINT_ID) {
        info["rollback_checkpoint_id"] = StringUtil::toString(_rollbackInfo.rollbackCheckpointId);
    }
    if (!_rollbackInfo.rollbackVersionIdMapping.empty()) {
        info["rollback_version_mapping"] = autil::legacy::ToJsonString(_rollbackInfo.rollbackVersionIdMapping, true);
    }
}

std::string SingleBuilderTaskV2::getTaskInfo()
{
    proto::SingleClusterInfo singleClusterInfo;
    singleClusterInfo.set_clustername(_clusterName);
    singleClusterInfo.set_partitioncount(_partitionCount);
    // singleClusterInfo.set_lastversiontimestamp(_lastAlignVersionTimestamp);
    string stepStr = getBuildStepStr() + "_";
    if (_builderDataDesc.find("stopTimestamp") == _builderDataDesc.end()) {
        stepStr += "building";
    } else {
        stepStr += "stopping";
    }
    singleClusterInfo.set_clusterstep(stepStr);
    if (!_specificMergeConfig.empty()) {
        *singleClusterInfo.add_pendingmergetasks() = _specificMergeConfig;
    }
    proto::BuilderInfo* builderInfo = singleClusterInfo.mutable_builderinfo();
    builderInfo->set_buildphrase("build");
    builderInfo->set_parallelnum(_parallelNum);
    google::protobuf::util::JsonPrintOptions options;
    std::string taskInfo;
    if (google::protobuf::util::MessageToJsonString(singleClusterInfo, &taskInfo, options).ok()) {
        return taskInfo;
    }
    return taskInfo;
}

void SingleBuilderTaskV2::convertTaskTargetToBuilderTarget(const proto::TaskTarget& taskTarget,
                                                           proto::BuilderTarget* builderTarget)
{
    builderTarget->set_configpath(taskTarget.configpath());
    builderTarget->set_suspendtask(taskTarget.suspendtask());
    builderTarget->set_targetdescription(taskTarget.targetdescription());
}

bool SingleBuilderTaskV2::convertTaskInfoToSingleClusterInfo(const proto::TaskInfo& taskInfo,
                                                             proto::SingleClusterInfo* singleClusterInfo)
{
    const std::string& taskInfoStr = taskInfo.taskinfo();
    if (!google::protobuf::util::JsonStringToMessage(taskInfoStr, singleClusterInfo).ok()) {
        return false;
    }
    proto::BuilderInfo* builderInfo = singleClusterInfo->mutable_builderinfo();
    builderInfo->set_taskstatus(taskInfo.taskstep());
    for (const auto& taskPartInfo : taskInfo.partitioninfos()) {
        proto::BuilderPartitionInfo partInfo;
        *(partInfo.mutable_pid()) = taskPartInfo.pid();
        convertTaskTargetToBuilderTarget(taskPartInfo.targetstatus(), partInfo.mutable_targetstatus());
        convertTaskTargetToBuilderTarget(taskPartInfo.currentstatus().reachedtarget(),
                                         partInfo.mutable_currentstatus()->mutable_targetstatus());
        if (taskPartInfo.has_currentstatus()) {
            auto currentStatus = taskPartInfo.currentstatus();
            *(partInfo.mutable_currentstatus()->mutable_errorinfos()) = currentStatus.errorinfos();
            if (currentStatus.has_status()) {
                partInfo.mutable_currentstatus()->set_status(currentStatus.status());
            }
            if (currentStatus.has_longaddress()) {
                *(partInfo.mutable_currentstatus()->mutable_longaddress()) = currentStatus.longaddress();
            }
            if (currentStatus.has_progressstatus()) {
                *(partInfo.mutable_currentstatus()->mutable_progressstatus()) = currentStatus.progressstatus();
            }
            if (currentStatus.has_issuspended()) {
                partInfo.mutable_currentstatus()->set_issuspended(currentStatus.issuspended());
            }
            if (currentStatus.has_protocolversion()) {
                partInfo.mutable_currentstatus()->set_protocolversion(currentStatus.protocolversion());
            }
            if (currentStatus.has_configpath()) {
                *(partInfo.mutable_currentstatus()->mutable_configpath()) = currentStatus.configpath();
            }
            if (currentStatus.has_machinestatus()) {
                *(partInfo.mutable_currentstatus()->mutable_machinestatus()) = currentStatus.machinestatus();
            }
        }
        *(partInfo.mutable_slotinfos()) = taskPartInfo.slotinfos();
        *(partInfo.mutable_counteritems()) = taskPartInfo.counteritems();
        *(builderInfo->mutable_partitioninfos()->Add()) = partInfo;
    }
    (*singleClusterInfo->mutable_fatalerrors()) = taskInfo.fatalerrors();
    return true;
}

bool SingleBuilderTaskV2::updateConfig()
{
    ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    ResourceReaderPtr resourceReader = readerAccessor->getConfig(_clusterName, _schemaVersion);
    if (resourceReader->getConfigPath() == _configPath) {
        return true;
    }
    BuildRuleConfig buildRuleConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.builder_rule_config",
                                                      buildRuleConfig)) {
        BS_LOG(ERROR, "parse cluster_config.builder_rule_config for [%s] failed", _clusterName.c_str());
        return false;
    }

    if (buildRuleConfig.partitionCount != _partitionCount) {
        BS_LOG(ERROR, "update config failed, partition count not equal");
        return false;
    }

    if (!updateKeepCheckpointCount(resourceReader)) {
        return false;
    }

    if (_buildStep == proto::BUILD_STEP_INC) {
        if (_parallelNum != buildRuleConfig.incBuildParallelNum) {
            _parallelNum = buildRuleConfig.incBuildParallelNum;
            cleanBuilderCheckpoint(_builders, true);
            _builders.clear();
            _alignVersionId = indexlibv2::INVALID_VERSIONID;
        }
    } else {
        if (_parallelNum != buildRuleConfig.buildParallelNum) {
            _parallelNum = buildRuleConfig.buildParallelNum;
            cleanBuilderCheckpoint(_builders, true);
            _builders.clear();
        }
    }
    _configPath = resourceReader->getOriginalConfigPath();
    return true;
}

string SingleBuilderTaskV2::getUsingConfigPath() const { return _configPath; }

}} // namespace build_service::admin
