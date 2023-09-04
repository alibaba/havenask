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
#include "build_service/admin/GenerationTask.h"

#include <bits/stdint-intn.h>
#include <memory>

#include "autil/EnvUtil.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/admin/AlterFieldCKPAccessor.h"
#include "build_service/admin/BatchBrokerTopicAccessor.h"
#include "build_service/admin/BuildTaskValidator.h"
#include "build_service/admin/CheckpointCreator.h"
#include "build_service/admin/CheckpointSynchronizer.h"
#include "build_service/admin/CheckpointTools.h"
#include "build_service/admin/LegacyCheckpointAdaptor.h"
#include "build_service/admin/ZKCheckpointSynchronizer.h"
#include "build_service/admin/controlflow/ControlDefine.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/admin/taskcontroller/BuildServiceTask.h"
#include "build_service/admin/taskcontroller/MergeCrontabTask.h"
#include "build_service/admin/taskcontroller/RollbackTaskController.h"
#include "build_service/admin/taskcontroller/SingleBuilderTaskV2.h"
#include "build_service/common/IndexCheckpointFormatter.h"
#include "build_service/common/PathDefine.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/config/DataLinkModeUtil.h"
#include "build_service/config/GraphConfig.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/SwiftRootUpgrader.h"
#include "build_service/util/DataSourceHelper.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"
#include "hippo/DriverCommon.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/index_base/version_committer.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace cm_basic;

using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::util;
using namespace build_service::config;

using build_service::common::BrokerTopicAccessor;
using build_service::common::BrokerTopicAccessorPtr;
using build_service::common::CheckpointAccessor;
using build_service::common::CheckpointAccessorPtr;
using build_service::common::IndexCheckpointAccessorPtr;
using build_service::common::IndexCheckpointFormatter;

using namespace indexlib::config;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, GenerationTask);

GenerationTask::GenerationTask(const BuildId& buildId, cm_basic::ZkWrapper* zkWrapper)
    : GenerationTaskBase(buildId, TT_SERVICE, zkWrapper)
    , _currentProcessorStep(proto::BUILD_STEP_FULL)
    , _isFullBuildFinish(false)
    , _lastCheckErrorTs(0)
    , _batchMode(false)
    , _isImportedTask(false)
    , _commitVersionCalled(false)
{
}

GenerationTask::~GenerationTask() {}

void GenerationTask::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    GenerationTaskBase::Jsonize(json);
    json.Jsonize("processor_build_step", _currentProcessorStep, _currentProcessorStep);
    json.Jsonize("full_build_finish", _isFullBuildFinish, _isFullBuildFinish);
    json.Jsonize("data_descriptions_str", _dataDescriptionsStr, _dataDescriptionsStr);
    json.Jsonize("realtime_data_description", _realTimeDataDesc, _realTimeDataDesc);
    json.Jsonize("batch_mode", _batchMode, _batchMode);
    json.Jsonize("is_imported_task", _isImportedTask, _isImportedTask);
    json.Jsonize("imported_version_id", _importedVersionId, _importedVersionId);
    json.Jsonize("commit_version_called", _commitVersionCalled, _commitVersionCalled);
}

bool GenerationTask::publishAndSaveCheckpoint(const config::ResourceReaderPtr& resourceReader,
                                              const std::string& cluster, indexlibv2::versionid_t versionId)
{
    if (versionId == indexlibv2::INVALID_VERSIONID) {
        BS_LOG(ERROR, "can not publish invalid version");
        return false;
    }
    std::string fullBuilderTmpRoot;
    if (!readIndexRoot(resourceReader->getConfigPath(), _indexRoot, fullBuilderTmpRoot)) {
        BS_LOG(ERROR, "readIndexRoot failed");
        return false;
    }
    if (!_checkpointSynchronizer) {
        if (!prepareCheckpointSynchronizer(resourceReader, ClusterCheckpointSynchronizerCreator::Type::DEFAULT)) {
            BS_LOG(ERROR, "commitVersion failed, clusterName[%s], versionId[%d]", cluster.c_str(), versionId);
            return false;
        }
    }

    BS_LOG(INFO, "start publishing checkpoint, alignedVersionId[%d], cluster[%s], importTask[%d], buildId[%s]",
           versionId, cluster.c_str(), (int)_isImportedTask, _buildId.ShortDebugString().c_str());

    std::string errorMsg;
    config::BuildRuleConfig buildRuleConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(cluster, "cluster_config.builder_rule_config", buildRuleConfig)) {
        errorMsg = string("parse cluster_config.builder_rule_config for [") + cluster + string("] failed");
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    uint32_t partitionCount = buildRuleConfig.partitionCount;
    vector<proto::Range> ranges = util::RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, partitionCount);

    errorMsg = "";
    indexlibv2::framework::VersionMeta versionMeta(versionId);
    std::string versionMetaStr = ToJsonString(versionMeta, true);
    for (size_t i = 0; i < ranges.size(); i++) {
        if (!_checkpointSynchronizer->publishPartitionLevelCheckpoint(cluster, ranges[i], versionMetaStr, errorMsg)) {
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    ClusterCheckpointSynchronizer::SyncOptions syncOptions;
    syncOptions.isDaily = false;
    syncOptions.isInstant = true;
    syncOptions.isOffline = true;
    syncOptions.addIndexInfos = false;
    if (!_checkpointSynchronizer->syncCluster(cluster, syncOptions)) {
        BS_LOG(ERROR, "Sync cluster [%s] failed. buildId[%s]", cluster.c_str(), _buildId.ShortDebugString().c_str());
        return false;
    }
    BS_LOG(INFO, "Publish checkpoint successfully: current versionId[%d], cluster[%s], import[%d], buildId[%s].",
           versionId, cluster.c_str(), (int)_isImportedTask, _buildId.ShortDebugString().c_str());

    errorMsg = "";
    std::string savepointStr = "";
    if (!createSavepoint(cluster, INVALID_CHECKPOINT_ID, "import saved", savepointStr, errorMsg)) {
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    BS_LOG(INFO, "save checkpoint successfully, save point [%s]", savepointStr.c_str());
    return true;
}

bool GenerationTask::importBuild(const string& configPath, const string& generationDir,
                                 const string& dataDescriptionKvs, indexlib::versionid_t importedVersionId,
                                 StartBuildResponse* response)
{
    _isImportedTask = true;
    _importedVersionId = importedVersionId;
    assert(_importedVersionId != indexlibv2::INVALID_VERSIONID);
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configPath);
    std::vector<std::string> clusterNames;
    if (!GenerationTaskBase::getAllClusterNames(*resourceReader, clusterNames)) {
        BS_LOG(ERROR, "getAllClusterNames failed");
        return false;
    }

    if (clusterNames.size() != 1) {
        BS_LOG(ERROR, "import build do not support multiple cluster");
    }

    if (!publishAndSaveCheckpoint(resourceReader, clusterNames[0], _importedVersionId)) {
        BS_LOG(ERROR, "publish or save Checkpoint failed");
        return false;
    }

    string errorMsg;
    if (!GenerationTaskBase::loadFromConfig(configPath, generationDir, dataDescriptionKvs, proto::BUILD_STEP_INC)) {
        errorMsg = "loadFromConfig[" + configPath + "] failed";
        response->add_errormessage(errorMsg.c_str());
        return false;
    }

    if (!resourceReader) {
        errorMsg = string("import failed, resourceReader not existed");
        response->add_errormessage(errorMsg.c_str());
        return false;
    }
    config::ControlConfig controlConfig;
    if (!resourceReader->getDataTableConfigWithJsonPath(_buildId.datatable(), "control_config", controlConfig)) {
        errorMsg = string("import failed, get controlConfig for generation[") + _buildId.ShortDebugString() +
                   string("] failed");
        response->add_errormessage(errorMsg.c_str());
        return false;
    }

    auto dataLinkMode = controlConfig.getDataLinkMode();
    // todo: support NPC_MODE
    if (dataLinkMode != ControlConfig::DataLinkMode::NPC_MODE &&
        dataLinkMode != ControlConfig::DataLinkMode::FP_INP_MODE) {
        errorMsg =
            string("import failed, can't import from data_link_mode:") + controlConfig.dataLinkModeToStr(dataLinkMode);
        response->add_errormessage(errorMsg.c_str());
        return false;
    }

    bool validateExistingIndex = true;
    if (!ValidateAndWriteTaskSignature(generationDir, validateExistingIndex, &errorMsg)) {
        BS_LOG(ERROR, "import generation[%s] failed, errorMsg:[%s]", _buildId.ShortDebugString().c_str(),
               errorMsg.c_str());
        response->add_errormessage(errorMsg.c_str());
        return false;
    }
    BS_LOG(INFO, "import generation[%s] done", _buildId.ShortDebugString().c_str());
    return true;
}

bool GenerationTask::doLoadFromConfig(const string& configPath, const string& generationDir,
                                      const DataDescriptions& dataDescriptions, BuildStep buildStep)
{
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));
    resourceReader->init();
    ConfigReaderAccessorPtr configResource;
    _resourceManager->getResource(configResource);
    if (!configResource->addConfig(resourceReader, true)) {
        BS_LOG(ERROR, "GenerationTask addConfig failed, configPath[%s]", configPath.c_str());
        return false;
    }

    vector<string> clusters;
    if (!getAllClusterNames(clusters)) {
        REPORT(SERVICE_ERROR_CONFIG, "get clusters failed for [%s]", _buildId.ShortDebugString().c_str());
        return false;
    }
    KeyValueMap kvMap;
    auto dsVec = dataDescriptions.toVector();
    if (!dataDescriptions.empty()) {
        DataDescription& incDs = const_cast<DataDescription&>(dsVec.back());
        if (DataSourceHelper::isRealtime(incDs)) {
            if (incDs.find(SWIFT_STOP_TIMESTAMP) == incDs.end()) {
                _realTimeDataDesc = incDs;
                _realTimeDataDesc[config::SRC_SIGNATURE] = StringUtil::toString(dataDescriptions.size() - 1);
                BS_LOG(INFO, "find realtime swift-like info for generation task [%s].",
                       _buildId.ShortDebugString().c_str());
            }
            if (incDs.find(SRC_BATCH_MODE) != incDs.end() && incDs[SRC_BATCH_MODE] == "true") {
                _batchMode = true;
            }
        }

        DataDescription& firstDs = const_cast<DataDescription&>(dsVec.front());
        if (firstDs[READ_SRC_TYPE] == INDEX_CLONE_SRC) {
            if (dsVec.size() != 2) {
                BS_LOG(ERROR, "no incremental dataDescription defined in task [%s].",
                       _buildId.ShortDebugString().c_str());
                return false;
            }
            if (!DataSourceHelper::isRealtime(_realTimeDataDesc)) {
                BS_LOG(ERROR, "no swift-like-type incremental dataDescription defined in task [%s].",
                       _buildId.ShortDebugString().c_str());
                return false;
            }
            auto it = firstDs.find(SOURCE_INDEX_ADMIN_ADDRESS);
            if (it == firstDs.end()) {
                BS_LOG(ERROR, "cannot find [%s] for src[%s] in generation task [%s].",
                       SOURCE_INDEX_ADMIN_ADDRESS.c_str(), INDEX_CLONE_SRC.c_str(),
                       _buildId.ShortDebugString().c_str());
                return false;
            }
            kvMap[SOURCE_INDEX_ADMIN_ADDRESS] = it->second;

            it = firstDs.find(SOURCE_INDEX_BUILD_ID);
            if (it == firstDs.end()) {
                BS_LOG(ERROR, "cannot find [%s] for src[%s] in generation task [%s].", SOURCE_INDEX_BUILD_ID.c_str(),
                       INDEX_CLONE_SRC.c_str(), _buildId.ShortDebugString().c_str());
                return false;
            }

            kvMap[SOURCE_INDEX_BUILD_ID] = it->second;

            it = firstDs.find(MADROX_ADMIN_ADDRESS);
            if (it == firstDs.end()) {
                BS_LOG(ERROR, "cannot find [%s] for src[%s] in generation task [%s].", MADROX_ADMIN_ADDRESS.c_str(),
                       INDEX_CLONE_SRC.c_str(), _buildId.ShortDebugString().c_str());
                return false;
            }
            kvMap[MADROX_ADMIN_ADDRESS] = it->second;
            kvMap["needCloneIndex"] = "true";
        }
    }

    KeyValueMap schemaIdMap;
    for (auto cluster : clusters) {
        auto schema = resourceReader->getTabletSchema(cluster);
        if (!schema) {
            REPORT(SERVICE_ERROR_CONFIG, "read [%s] schema failed", cluster.c_str());
            return false;
        }
        schemaIdMap[cluster] = StringUtil::toString(schema->GetSchemaId());
    }

    kvMap["dataDescriptions"] = ToJsonString(dsVec, true);
    kvMap["realtimeDataDescription"] = ToJsonString(_realTimeDataDesc, true);
    kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);
    kvMap["buildStep"] = buildStep == BUILD_STEP_FULL ? "full" : "incremental";
    kvMap["clusterNames"] = ToJsonString(clusters, true);
    kvMap["schemaIdMap"] = ToJsonString(schemaIdMap, true);
    kvMap["configPath"] = configPath;
    if (_isImportedTask) {
        kvMap["useRandomInitialPathVersion"] = "true";
        kvMap["importedVersionId"] = autil::StringUtil::toString(_importedVersionId);
    }
    config::ControlConfig controlConfig;
    if (!resourceReader->getDataTableConfigWithJsonPath(_buildId.datatable(), "control_config", controlConfig)) {
        BS_LOG(ERROR, "get control_config.is_inc_processor_exist failed from dataTable[%s] failed",
               _buildId.datatable().c_str());
        return false;
    }
    if (_batchMode && buildStep == BUILD_STEP_INC) {
        REPORT(SERVICE_ERROR_CONFIG, "batch mode not allow start from inc step, buildId: %s",
               _buildId.ShortDebugString().c_str());
        return false;
    }
    string targetGraph;
    GraphConfig graphConfig;
    if (resourceReader->getGraphConfig(graphConfig)) {
        targetGraph = graphConfig.getGraphName();
        const KeyValueMap& params = graphConfig.getGraphParam();
        vector<string> graphParamKeys;
        auto iter = params.begin();
        for (; iter != params.end(); iter++) {
            kvMap[iter->first] = iter->second;
            graphParamKeys.push_back(iter->first);
        }
        kvMap["user_define_parameter_keys"] = ToJsonString(graphParamKeys, true);
    } else {
        if (controlConfig.getDataLinkMode() == ControlConfig::DataLinkMode::NPC_MODE) {
            targetGraph = "BuildIndexV2.npc_mode.graph";
        } else {
            if (controlConfig.useIndexV2()) {
                targetGraph = _batchMode ? "BatchBuildV2/FullBatchBuild.graph" : "BuildIndexV2.graph";
            } else {
                targetGraph = _batchMode ? "BatchBuild/FullBatchBuild.graph" : "BuildIndex.graph";
            }
        }
    }
    if (!DataLinkModeUtil::addGraphParameters(controlConfig, clusters, dsVec, &kvMap)) {
        REPORT(SERVICE_ERROR_CONFIG, "parse graph parameters failed, buildId: %s", _buildId.ShortDebugString().c_str());
        return false;
    }
    if (!prepareCheckpointSynchronizer(resourceReader)) {
        REPORT(SERVICE_ERROR_CONFIG, "init checkpoint synchronizer [%s] failed", _buildId.ShortDebugString().c_str());
        return false;
    }
    if (!_taskFlowManager->loadSubGraph("", targetGraph, kvMap)) {
        REPORT(SERVICE_ERROR_CONFIG, "init [%s] graph failed", _buildId.ShortDebugString().c_str());
        return false;
    }
    _taskFlowManager->stepRun();

    BuildServiceConfig buildServiceConfig;
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        string errorMsg = "failed to get build service config in config[" + getConfigPath() + "]";
        REPORT(SERVICE_ERROR_CONFIG, "%s", errorMsg.c_str());
        return false;
    }
    _counterConfig = buildServiceConfig.counterConfig;
    _currentProcessorStep = buildStep;
    _dataDescriptionsStr = ToJsonString(dsVec, true);
    return true;
}

bool GenerationTask::loadFromString(const string& statusString, const string& generationDir)
{
    try {
        FromJsonString(*this, statusString);

        // TODO: remove after upgrade swift root done
        proto::SwiftRootUpgrader upgrader;
        if (upgrader.needUpgrade(_buildId.generationid())) {
            upgrader.upgrade(_realTimeDataDesc);
            string originStr = _dataDescriptionsStr;
            _dataDescriptionsStr = upgrader.upgradeDataDescriptions(originStr);
            if (_dataDescriptionsStr != originStr) {
                BS_LOG(INFO, "upgrade swift root in dataDescription for generation [%u], from [%s] to [%s]",
                       _buildId.generationid(), originStr.c_str(), _dataDescriptionsStr.c_str());
            }
        }

    } catch (const autil::legacy::ExceptionBase& e) {
        // log length maybe overflow and only limited content is showed
        // so we use two log record to show exception and status string both
        ZlibCompressor compressor;
        string decompressedStatus;
        if (!compressor.decompress(statusString, decompressedStatus)) {
            // e.what() and statusString may be too long, log may be truncate, split it
            BS_LOG(ERROR, "recover GenerationTask from status failed, exception[%s]", e.what());
            BS_LOG(ERROR, "decompress status string failed [%s]", statusString.c_str());
            return false;
        }
        try {
            FromJsonString(*this, decompressedStatus);
        } catch (const ExceptionBase& e) {
            BS_LOG(ERROR, "recover GenerationTask from status failed, exception[%s]", e.what());
            BS_LOG(ERROR, "recover failed with status [%s]", statusString.c_str());
            return false;
        }
    }
    return true;
}

bool GenerationTask::CleanTaskSignature() const
{
    ResourceReaderPtr resourceReader = GetLatestResourceReader();
    if (!resourceReader) {
        BS_LOG(ERROR, "clean task signature in generation [%s] failed: resource reader is nullptr",
               _buildId.ShortDebugString().c_str());
        return false;
    }
    string errorMsg;
    if (!BuildTaskValidator::CleanTaskSignature(resourceReader, _indexRoot, _buildId, &errorMsg)) {
        BS_LOG(ERROR, "clean taskSignature in Generation[%s] failed, errorMsg:[%s]",
               _buildId.ShortDebugString().c_str(), errorMsg.c_str());
        return false;
    }
    return true;
}

bool GenerationTask::ValidateAndWriteTaskSignature(const std::string& generationZkRoot, bool validateExistingIndex,
                                                   string* errorMsg) const
{
    ResourceReaderPtr resourceReader = GetLatestResourceReader();
    if (!resourceReader) {
        *errorMsg = string("unexpceted: resourceReader not existed");
        return false;
    }
    BuildTaskValidatorPtr validator(
        new BuildTaskValidator(resourceReader, _buildId, _indexRoot, generationZkRoot, _realTimeDataDesc));
    if (!validator->ValidateAndWriteSignature(validateExistingIndex, errorMsg)) {
        BS_LOG(ERROR, "Validate Generation[%s] failed, errorMsg:[%s]", _buildId.ShortDebugString().c_str(),
               errorMsg->c_str());
        return false;
    }
    return true;
}

bool GenerationTask::restart(const string& statusString, const string& generationDir)
{
    if (!loadFromString(statusString, generationDir)) {
        return false;
    }
    if (_indexDeleted) {
        BS_LOG(ERROR, "generation [%s] can not restart because index has been deleted.",
               _buildId.ShortDebugString().c_str());

        string errorMsg =
            "generation [" + _buildId.ShortDebugString() + "] can not restart because index has been deleted.";
        BEEPER_REPORT(GENERATION_ERROR_COLLECTOR_NAME, errorMsg, BuildIdWrapper::getEventTags(_buildId));
        return false;
    }

    bool validateExistingIndex = false;
    string errorMsg;
    if (!ValidateAndWriteTaskSignature(generationDir, validateExistingIndex, &errorMsg)) {
        BS_LOG(INFO, "generation [%s] restart failed, validate task signature failed, errorMsg[%s]",
               _buildId.ShortDebugString().c_str(), errorMsg.c_str());
        return false;
    }
    // if (!_buildTask->recover(generationDir, _configPath)) {
    //     return false;
    // }
    _step = GENERATION_STARTING;
    BS_LOG(INFO, "generation [%s] restarted", _buildId.ShortDebugString().c_str());
    return true;
}

bool GenerationTask::getAllClusterNames(vector<string>& allClusters) const
{
    ResourceReaderPtr reader = GetLatestResourceReader();
    if (!GenerationTaskBase::getAllClusterNames(*(reader.get()), allClusters)) {
        return false;
    }
    return true;
}

bool GenerationTask::doSuspendBuild(const string& clusterName, string& errorMsg)
{
    if (isRollingBack()) {
        errorMsg = "can't suspend when generation is rolling back.";
        return false;
    }

    if (getWorkerProtocolVersion() == UNKNOWN_WORKER_PROTOCOL_VERSION) {
        errorMsg = "workers' protocol version is unknown.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    KeyValueMap kvMap;
    if (!clusterName.empty()) {
        kvMap["clusterNames"] = clusterName;
    } else {
        vector<string> allClusters;
        if (!getAllClusterNames(allClusters)) {
            errorMsg = "failed to read all cluster names from config for [" + _buildId.ShortDebugString() + "]";
            return false;
        }
        kvMap["clusterNames"] = StringUtil::toString(allClusters, ";");
    }
    kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);
    BS_LOG(INFO, "begin suspend for buildId[%s] clusterName[%s].", kvMap["buildId"].c_str(), clusterName.c_str());
    if (!_taskFlowManager->loadSubGraph("", "SuspendBuild.graph", kvMap)) {
        errorMsg = "load SuspendBuild.graph failed [" + _buildId.ShortDebugString() + "]";
        BS_LOG(ERROR, "load SuspendBuild.graph failed [%s]", _buildId.ShortDebugString().c_str());
        return false;
    }
    _taskFlowManager->stepRun();
    BS_LOG(INFO, "end suspend build for buildId[%s] clusterName[%s].", kvMap["buildId"].c_str(), clusterName.c_str());
    return true;
}

bool GenerationTask::doResumeBuild(const string& clusterName, string& errorMsg)
{
    if (isRollingBack()) {
        errorMsg = "can't resume when generation is rolling back.";
        return false;
    }

    auto flows = _taskFlowManager->getAllFlow();
    for (auto flow : flows) {
        if (flow->isFlowSuspending()) {
            errorMsg = "can't resume when generation is suspending.";
            return false;
        }
    }

    if (getWorkerProtocolVersion() == UNKNOWN_WORKER_PROTOCOL_VERSION) {
        errorMsg = "workers' protocol version is unknown.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    KeyValueMap kvMap;
    if (!clusterName.empty()) {
        kvMap["clusterNames"] = clusterName;
    } else {
        vector<string> allClusters;
        if (!getAllClusterNames(allClusters)) {
            errorMsg = "failed to read all cluster names from config for [" + _buildId.ShortDebugString() + "]";
            return false;
        }
        kvMap["clusterNames"] = StringUtil::toString(allClusters, ";");
    }
    kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);
    BS_LOG(INFO, "begin resume for buildId[%s] clusterName[%s].", kvMap["buildId"].c_str(), clusterName.c_str());
    if (!_taskFlowManager->loadSubGraph("", "ResumeBuild.graph", kvMap)) {
        BS_LOG(ERROR, "load ResumeBuild.graph failed [%s]", _buildId.ShortDebugString().c_str());
        return false;
    }
    _taskFlowManager->stepRun();
    BS_LOG(INFO, "end resume build for buildId[%s] clusterName[%s].", kvMap["buildId"].c_str(), clusterName.c_str());
    return true;
}

bool GenerationTask::doRollBack(const string& clusterName, const string& generationZkRoot, versionid_t versionId,
                                int64_t startTimestamp, string& errorMsg)
{
    std::vector<std::string> flowIds;
    _taskFlowManager->getFlowIdByTag("BSBuildV2", flowIds);
    if (!flowIds.empty()) {
        errorMsg = "generation is v2 build, can not use rollback, try rollbackCheckpoint instead.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    auto configReader = RollbackTaskController::getConfigReader(clusterName, _resourceManager);
    if (configReader == nullptr) {
        errorMsg = "cluster [" + clusterName + "] could not get config reader, can not rollback.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (isRollingBack()) {
        errorMsg = "generation is rolling back, can not rollback again.";
        return false;
    }
    if (isSuspending()) {
        errorMsg = "generation is in suspending, can not rollback.";
        return false;
    }
    // not support rollback in full build
    if (_currentProcessorStep == BUILD_STEP_FULL || !_isFullBuildFinish) {
        errorMsg = "can not rollback in full build.";
        return false;
    }
    // only support rollback main index
    if (!RollbackTaskController::checkRollbackLegal(clusterName, versionId, _resourceManager)) {
        return false;
    }

    KeyValueMap kvMap;
    kvMap["buildStep"] = "incremental";
    kvMap["clusterName"] = clusterName;
    kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);
    if (startTimestamp >= 0) {
        kvMap["builderStartTimestamp"] = StringUtil::toString(startTimestamp);
    }
    map<string, string> taskParam;
    taskParam[BS_ROLLBACK_SOURCE_VERSION] = StringUtil::toString(versionId);
    kvMap["taskParam"] = ToJsonString(taskParam, true);
    config::ControlConfig controlConfig;
    if (!configReader->getDataTableConfigWithJsonPath(_buildId.datatable(), "control_config", controlConfig)) {
        errorMsg = "rollback cluster [" + clusterName + "] failed: cannot get is_inc_processor_exist from dataTable[" +
                   _buildId.datatable() + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    kvMap["hasIncProcessor"] = controlConfig.isIncProcessorExist(clusterName) ? "true" : "false";
    kvMap["realtimeDataDescription"] = ToJsonString(_realTimeDataDesc, true);

    BS_LOG(INFO, "begin rollback for buildId[%s] clusterName[%s].", kvMap["buildId"].c_str(), clusterName.c_str());
    if (!_taskFlowManager->loadSubGraph("", "Rollback.graph", kvMap)) {
        BS_LOG(ERROR, "load Rollback.graph failed [%s]", _buildId.ShortDebugString().c_str());
        return false;
    }
    _taskFlowManager->stepRun();
    BS_LOG(INFO, "end rollback for buildId[%s] clusterName[%s], builderStartTs[%ld].", kvMap["buildId"].c_str(),
           clusterName.c_str(), startTimestamp);
    return true;
}

bool GenerationTask::doRollBackCheckpoint(const string& clusterName, const string& generationZkRoot,
                                          checkpointid_t checkpointId,
                                          const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges,
                                          std::string& errorMsg)
{
    auto configReader = RollbackTaskController::getConfigReader(clusterName, _resourceManager);
    if (configReader == nullptr) {
        errorMsg = "cluster [" + clusterName + "] could not get config reader, can not rollback.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    auto schema = configReader->getTabletSchema(clusterName);
    if (!schema) {
        BS_LOG(ERROR, "cluster[%s] rollback to checkpoint[%ld] failed, lack schema", clusterName.c_str(), checkpointId);
        return false;
    }
    if (isRollingBack()) {
        errorMsg = "generation is rolling back, can not rollback again.";
        return false;
    }
    if (isSuspending()) {
        errorMsg = "generation is in suspending, can not rollback.";
        return false;
    }
    // not support rollback in full build
    if (_currentProcessorStep == BUILD_STEP_FULL || !_isFullBuildFinish) {
        errorMsg = "can not rollback in full build.";
        return false;
    }
    if (_checkpointSynchronizer == nullptr) {
        errorMsg = "checkpoint synchronizer is nullptr";
        return false;
    }
    checkpointid_t newCheckpointId;
    if (isLeaderFollowerMode()) {
        BS_LOG(INFO, "rollback by checkpoint synchronizer for cluster [%s], checkpoint id[%ld], buildId[%s]",
               clusterName.c_str(), checkpointId, _buildId.ShortDebugString().c_str());
        return _checkpointSynchronizer->rollback(clusterName, checkpointId, ranges, /*needAddIndexInfo=*/true,
                                                 &newCheckpointId, errorMsg);
    } else if (!ranges.empty()) {
        BS_LOG(WARN,
               "Bad Param: ranges is not empty, "
               "rollback with assigned ranges only support in leader/follower mode, buildId[%s], clusterName[%s]",
               _buildId.ShortDebugString().c_str(), clusterName.c_str());
        return false;
    } else {
        if (!_checkpointSynchronizer->rollback(clusterName, checkpointId, ranges, /*needAddIndexInfo=*/false,
                                               &newCheckpointId, errorMsg)) {
            BS_LOG(ERROR, "rollback for offline failed. buildId[%s], clusterName[%s]",
                   _buildId.ShortDebugString().c_str(), clusterName.c_str());
            return false;
        }
    }
    std::vector<std::string> flowIds;
    _taskFlowManager->getFlowIdByTag("BSBuildV2", flowIds);
    if (flowIds.empty()) {
        errorMsg = "rollbackCheckpoint does not support v1 offline build, try rollback instead.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    std::string savepointStr;
    ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult result;
    if (!_checkpointSynchronizer->createSavepoint(clusterName, newCheckpointId, "", &result, errorMsg)) {
        BS_LOG(ERROR, "cluster[%s] rollback to newCheckpoint[%ld] create savepoint failed.", clusterName.c_str(),
               newCheckpointId);
        return false;
    }

    KeyValueMap kvMap;
    kvMap["buildStep"] = "incremental";
    kvMap["clusterName"] = clusterName;
    kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);
    kvMap[BS_ROLLBACK_SOURCE_CHECKPOINT] = StringUtil::toString(newCheckpointId);
    config::ControlConfig controlConfig;
    if (!configReader->getDataTableConfigWithJsonPath(_buildId.datatable(), "control_config", controlConfig)) {
        errorMsg = "rollback cluster [" + clusterName + "] failed: cannot get is_inc_processor_exist from dataTable[" +
                   _buildId.datatable() + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    kvMap["hasIncProcessor"] = controlConfig.isIncProcessorExist(clusterName) ? "true" : "false";
    kvMap["realtimeDataDescription"] = ToJsonString(_realTimeDataDesc, true);

    BS_LOG(INFO, "begin rollback for checkpointId[%ld] buildId[%s] clusterName[%s], new checkpointId[%ld].",
           checkpointId, kvMap["buildId"].c_str(), clusterName.c_str(), newCheckpointId);
    if (!_taskFlowManager->loadSubGraph("", "RollbackV2.graph", kvMap)) {
        BS_LOG(ERROR, "load Rollback.graph failed [%s]", _buildId.ShortDebugString().c_str());
        return false;
    }
    _taskFlowManager->stepRun();
    BS_LOG(INFO, "end rollback for checkpointId[%ld], buildId[%s] clusterName[%s], new checkpointId[%ld].",
           checkpointId, kvMap["buildId"].c_str(), clusterName.c_str(), newCheckpointId);
    return true;
}

BuildStep GenerationTask::getBuildStep() const
{
    if (_currentProcessorStep == BUILD_STEP_FULL) {
        return _currentProcessorStep;
    }
    assert(_currentProcessorStep == BUILD_STEP_INC);
    return _isFullBuildFinish ? BUILD_STEP_INC : BUILD_STEP_FULL;
}

bool GenerationTask::cleanReservedCheckpoints(const string& clusterName, versionid_t versionId)
{
    IndexCheckpointAccessorPtr checkpointAccessor = CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
    assert(checkpointAccessor);
    string checkpointName;
    proto::CheckpointInfo checkpoint;
    if (!checkpointAccessor->getLatestIndexCheckpoint(clusterName, checkpoint, checkpointName)) {
        // no checkpoint to clean
        return true;
    }

    versionid_t latestVersion = checkpoint.versionid();
    if (latestVersion <= versionId) {
        BS_LOG(ERROR, "can't clean the latest version [%d].", latestVersion);
        return false;
    }
    checkpointAccessor->clearIndexCheckpoint(clusterName, versionId);
    return true;
}

bool GenerationTask::doUpdateConfig(const string& configPath)
{
    if (isSuspending()) {
        REPORT(SERVICE_ERROR_NONE, "can't update config when generation is suspending.");
        return false;
    }
    if (isRollingBack()) {
        REPORT(SERVICE_ERROR_NONE, "can't update config when generation is rolling back.");
        return false;
    }
    vector<string> allClusters;
    ConfigReaderAccessorPtr configReaderAccessor;
    _resourceManager->getResource(configReaderAccessor);
    ResourceReaderPtr oldResourceReader = configReaderAccessor->getLatestConfig();
    if (fslib::util::FileUtil::normalizeDir(configPath) ==
        fslib::util::FileUtil::normalizeDir(oldResourceReader->getConfigPath())) {
        REPORT(SERVICE_ERROR_CONFIG, "can't update config with the same configPath");
        return false;
    }
    if (!getAllClusterNames(allClusters)) {
        REPORT(SERVICE_ERROR_CONFIG, "get clusters failed for [%s]", _buildId.ShortDebugString().c_str());
        return false;
    }
    vector<string> chgSchemaClusters;
    KeyValueMap schemaIdMap;
    // vector<int64_t> chgSchemaVersions;
    KeyValueMap opsIdMap;
    ResourceReaderPtr newResourceReader(new ResourceReader(configPath));
    newResourceReader->init();
    if (!prepareCheckpointSynchronizer(newResourceReader)) {
        BS_LOG(ERROR, "upc init checkpoint synchronizer [%s] failed", _buildId.ShortDebugString().c_str());
        return false;
    }
    bool hasError;
    bool isOldV2BuildMode = isV2Build(oldResourceReader, hasError);
    if (hasError) {
        REPORT(SERVICE_ERROR_CONFIG, "read old config failed, reject update config");
        return false;
    }
    bool isNewV2BuildMode = isV2Build(newResourceReader, hasError);
    if (hasError) {
        REPORT(SERVICE_ERROR_CONFIG, "read new config failed, reject update config");
        return false;
    }

    if (isOldV2BuildMode && !isNewV2BuildMode) {
        REPORT(SERVICE_ERROR_CONFIG, "not support v2 build mode to v1 build mode");
        return false;
    }
    bool isV1BuildModeToV2BuildMode = !isOldV2BuildMode && isNewV2BuildMode;
    bool hasOperations = false;
    for (const auto& clusterName : allClusters) {
        ConfigValidator::SchemaUpdateStatus status = checkUpdateSchema(newResourceReader, clusterName);
        if (status == ConfigValidator::SchemaUpdateStatus::UPDATE_ILLEGAL) {
            REPORT(SERVICE_ERROR_CONFIG, "check update schema failed");
            return false;
        }
        if (status == ConfigValidator::SchemaUpdateStatus::UPDATE_SCHEMA) {
            if (isNewV2BuildMode) {
                REPORT(SERVICE_ERROR_CONFIG, "v2 build mode not support update schema");
                return false;
            }
            chgSchemaClusters.push_back(clusterName);
            auto schema = newResourceReader->getSchema(clusterName);
            if (schema->HasModifyOperations()) {
                hasOperations = true;
            } else {
                if (hasOperations) {
                    REPORT(SERVICE_ERROR_CONFIG, "multi cluster change schema not support"
                                                 " two change schame types together");
                    return false;
                }
            }
            schemaIdMap[clusterName] = StringUtil::toString(schema->GetSchemaVersionId());
            opsIdMap[clusterName] = getOpsInfos(schema, clusterName);
        }
    }
    configReaderAccessor->addConfig(newResourceReader, true);

    if (isV1BuildModeToV2BuildMode) {
        if (!chgSchemaClusters.empty()) {
            BS_LOG(ERROR, "v1 builde mode to v2 build mode not support change schema");
            return false;
        }
        if (getBuildStep() == proto::BUILD_STEP_FULL) {
            BS_LOG(ERROR, "full building not support v1 build mode to v2 build mode");
            return false;
        }
        string updateGraph = "BuildV1ToBuildV2.graph";
        KeyValueMap kvMap;
        kvMap["clusterNames"] = ToJsonString(allClusters, true);
        kvMap["buildStep"] = "incremental";
        kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);
        vector<string> flowIds;
        string incProcessorTag = "BSIncProcessor";
        _taskFlowManager->getFlowIdByTag(incProcessorTag, flowIds);
        if (flowIds.size() == 0) {
            kvMap[DATA_DESCRIPTION_KEY] = ToJsonString(_realTimeDataDesc);
        }
        if (!_taskFlowManager->loadSubGraph("", updateGraph, kvMap)) {
            BS_LOG(ERROR, "load graph [%s] fail", updateGraph.c_str());
            return false;
        }
    }

    if (!chgSchemaClusters.empty() && !doUpdateSchema(hasOperations, chgSchemaClusters, schemaIdMap, opsIdMap)) {
        return false;
    }

    if (!doUpdateConfig(chgSchemaClusters, configPath)) {
        return false;
    }

    return updatePackageInfo(oldResourceReader, newResourceReader);
}

bool GenerationTask::updatePackageInfo(const ResourceReaderPtr& oldResourceReader,
                                       const ResourceReaderPtr& newResourceReader)
{
    std::vector<hippo::PackageInfo> oldPackageInfos;
    std::vector<hippo::PackageInfo> newPackageInfos;

    auto getPackageInfo = [this](const ResourceReaderPtr& resourceReader, vector<hippo::PackageInfo>& packageInfos) {
        bool isConfigExist;
        if (!resourceReader->getConfigWithJsonPath(HIPPO_FILE_NAME, "packages", packageInfos, isConfigExist)) {
            REPORT(SERVICE_ERROR_CONFIG, "get packageInfo failed from path[%s]",
                   resourceReader->getConfigPath().c_str());
            return false;
        }
        if (!isConfigExist) {
            REPORT(SERVICE_ERROR_CONFIG, "no packages find in configPath[%s]", resourceReader->getConfigPath().c_str());
            return false;
        }
        return true;
    };

    if (!getPackageInfo(oldResourceReader, oldPackageInfos) || !getPackageInfo(newResourceReader, newPackageInfos)) {
        return false;
    }

    if (oldPackageInfos.size() != newPackageInfos.size()) {
        return true;
    }

    for (size_t i = 0; i < oldPackageInfos.size(); ++i) {
        if (oldPackageInfos[i].URI != newPackageInfos[i].URI || oldPackageInfos[i].type != newPackageInfos[i].type) {
            return true;
        }
    }
    newResourceReader->updateWorkerProtocolVersion(oldResourceReader->getWorkerProtocolVersion());
    return true;
}

string GenerationTask::getOpsInfos(const IndexPartitionSchemaPtr& newSchema, const string& clusterName)
{
    ConfigReaderAccessorPtr configAccessor;
    _resourceManager->getResource(configAccessor);
    auto oldReader = configAccessor->getLatestConfig();
    auto oldSchema = oldReader->getSchema(clusterName);
    assert(oldSchema);
    vector<string> opsInfos;
    size_t oldOpsIdCount = oldSchema->GetModifyOperationCount();
    size_t newOpsIdCount = newSchema->GetModifyOperationCount();
    opsInfos.reserve(newOpsIdCount - oldOpsIdCount);
    for (size_t i = oldOpsIdCount + 1; i <= newOpsIdCount; i++) {
        KeyValueMap opsInfo;
        opsInfo["operationId"] = StringUtil::toString(i);
        auto operation = newSchema->GetSchemaModifyOperation(i);
        auto params = operation->GetParams();
        opsInfo["alterFieldMode"] = "sync"; // wait aligned IncFlow done, default mode
        auto iter = params.find("execute_mode");
        if (iter != params.end()) {
            if (iter->second == "async") {
                opsInfo["alterFieldMode"] = "async"; // no waiting, multi-round alterField
            }
            if (iter->second == "block") {
                opsInfo["alterFieldMode"] = "block"; // legacy mode, block new IncFlow
            }
        }
        opsInfos.push_back(ToJsonString(opsInfo, true));
    }
    return ToJsonString(opsInfos, true);
}

ConfigValidator::SchemaUpdateStatus GenerationTask::checkUpdateSchema(const ResourceReaderPtr& resourceReader,
                                                                      const string& clusterName)
{
    ConfigReaderAccessorPtr configAccessor;
    _resourceManager->getResource(configAccessor);
    auto tabletSchema = resourceReader->getTabletSchema(clusterName);

    if (!tabletSchema) {
        REPORT(SERVICE_ERROR_CONFIG, "get schema [%s] failed", clusterName.c_str());
        return ConfigValidator::SchemaUpdateStatus::UPDATE_ILLEGAL;
    }

    auto newSchema = tabletSchema->GetLegacySchema();
    if (!newSchema) {
        return ConfigValidator::SchemaUpdateStatus::NO_UPDATE_SCHEMA;
    }

    // ResourceReaderPtr oldReader =  configAccessor->getLatestConfig();
    int64_t newSchemaId = newSchema->GetSchemaVersionId();
    auto maxSchemaId = configAccessor->getMaxSchemaId(clusterName);
    ResourceReaderPtr oldReader = configAccessor->getConfig(clusterName, newSchemaId);
    if (!oldReader) {
        if (newSchemaId <= maxSchemaId) {
            stringstream ss;
            ss << "udpate schema version id [" << newSchemaId << "] should bigger than"
               << "history schema id [" << maxSchemaId << "]";
            string errorMsg = ss.str();
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
            return ConfigValidator::SchemaUpdateStatus::UPDATE_ILLEGAL;
        }
        oldReader = configAccessor->getLatestConfig();
    }
    ConfigValidator configvalidator(_buildId);
    auto ret = configvalidator.checkUpdateSchema(oldReader, resourceReader, clusterName);
    if (ret == ConfigValidator::SchemaUpdateStatus::UPDATE_ILLEGAL) {
        std::vector<proto::ErrorInfo> errorInfos;
        configvalidator.transferErrorInfos(errorInfos);
        addErrorInfos(errorInfos);
        return ret;
    }
    if (ret == ConfigValidator::SchemaUpdateStatus::UPDATE_SCHEMA) {
        if (!_isFullBuildFinish) {
            REPORT(SERVICE_ERROR_CONFIG, "not support update schema in full build");
            return ConfigValidator::SchemaUpdateStatus::UPDATE_ILLEGAL;
        }
        vector<schema_opid_t> disableOpIds;
        newSchema->GetDisableOperations(disableOpIds);
        CheckpointAccessorPtr ckpAccessor;
        _resourceManager->getResource(ckpAccessor);
        CheckpointTools::addDisableOpIds(ckpAccessor, clusterName, disableOpIds);
    } else if (ret == ConfigValidator::SchemaUpdateStatus::UPDATE_STANDARD) {
        if (!_isFullBuildFinish) {
            REPORT(SERVICE_ERROR_CONFIG, "not support update schema standard in full build");
            return ConfigValidator::SchemaUpdateStatus::UPDATE_ILLEGAL;
        }
        CheckpointAccessorPtr ckpAccessor;
        _resourceManager->getResource(ckpAccessor);
        UpdateableSchemaStandardsPtr schemaPatch(new UpdateableSchemaStandards());
        resourceReader->getSchemaPatch(clusterName, schemaPatch);
        string ckpShemaPatchId = IndexCheckpointFormatter::getSchemaPatch(clusterName);
        if (!ckpAccessor->updateCheckpoint(ckpShemaPatchId, BS_CKP_SCHEMA_PATCH, ToJsonString(schemaPatch))) {
            ckpAccessor->addCheckpoint(ckpShemaPatchId, BS_CKP_SCHEMA_PATCH, ToJsonString(schemaPatch));
        }
    }
    return ret;
}

bool GenerationTask::doCreateVersion(const string& clusterName, const string& mergeConfigName)
{
    if (isSuspending()) {
        BS_LOG(WARN, "can't create version when generation is suspending.");
        return false;
    }
    if (isRollingBack()) {
        BS_LOG(WARN, "can't create version when generation is rolling back.");
        return false;
    }

    vector<string> flowIds;
    string bsBuildV2 = "BSBuildV2";
    _taskFlowManager->getFlowIdByTag("BSBuildV2", flowIds);
    if (flowIds.size() > 0) {
        KeyValueMap kvMap;
        kvMap["clusterName"] = clusterName;
        kvMap["buildStep"] = "incremental";
        kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);
        auto mergeConfig = mergeConfigName;
        if (mergeConfig.empty()) {
            mergeConfig = "default_merge";
        }
        kvMap["mergeConfig"] = mergeConfig;
        flowIds.clear();
        string incProcessorTag = clusterName + ":BSIncProcessor";
        _taskFlowManager->getFlowIdByTag(incProcessorTag, flowIds);
        if (flowIds.size() == 0) {
            kvMap[DATA_DESCRIPTION_KEY] = ToJsonString(_realTimeDataDesc);
        }
        string graphName = "CreateVersionV2.graph";
        if (!_taskFlowManager->loadSubGraph("", graphName, kvMap)) {
            REPORT(SERVICE_ERROR_CONFIG, "load sub graph create version failed [%s]",
                   _buildId.ShortDebugString().c_str());
            return false;
        }
        return true;
    }

    string mergeCrontabTag = clusterName + ":MergeCrontab";
    _taskFlowManager->getFlowIdByTag(mergeCrontabTag, flowIds);
    vector<TaskFlowPtr> flows;
    fillFlows(flowIds, true, flows);
    if (flows.size() != 1) {
        BS_LOG(ERROR, "create version for cluster[%s] failed, missing MergeCrontabFlow", clusterName.c_str());
        return false;
    }
    TaskBasePtr task = flows[0]->getTask(BS_TASK_ID_MERGECRONTAB);
    if (!task) {
        BS_LOG(ERROR, "create version for cluster[%s] failed, missing MergeCrontabTask", clusterName.c_str());
        return false;
    }
    MergeCrontabTaskPtr crontab = DYNAMIC_POINTER_CAST(MergeCrontabTask, task);
    assert(crontab);
    return crontab->createVersion(mergeConfigName);
}

bool GenerationTask::getTaskTargetClusterName(string& clusterName, string& errorMsg)
{
    vector<string> allClusters;
    if (!getAllClusterNames(allClusters)) {
        errorMsg = "failed to read all cluster names from config for [" + _buildId.ShortDebugString() + "]";
        return false;
    }

    if (allClusters.size() != 1) {
        errorMsg = "not specify cluster name in multi cluster generation";
        return false;
    }
    clusterName = allClusters[0];
    return true;
}

bool GenerationTask::doStartTask(int64_t taskId, const string& taskName, const string& taskConfigPath,
                                 const string& clusterName, const string& taskParam,
                                 GenerationTaskBase::StartTaskResponse* response)
{
    if (taskId < 0) {
        response->message =
            string("check taskId fail, taskId [") + StringUtil::toString(taskId) + "] should not less than 0 ";
        return false;
    }
    string targetClusterName = clusterName;
    if (targetClusterName.empty()) {
        if (!getTaskTargetClusterName(targetClusterName, response->message)) {
            response->message = "no cluster name:" + clusterName + ", in this generation";
            return false;
        }
    }

    vector<string> clusters;
    if (!getAllClusterNames(clusters)) {
        BS_LOG(ERROR, "get clusters failed for [%s]", _buildId.ShortDebugString().c_str());
        response->message = "read config failed, start task failed";
        return false;
    }

    bool hasTargetCluster = false;
    for (const string& clusterName : clusters) {
        if (clusterName == targetClusterName) {
            hasTargetCluster = true;
        }
    }
    if (!hasTargetCluster) {
        response->message = "no cluster name:" + clusterName + ", in this generation";
        return false;
    }

    KeyValueMap kvMap;
    kvMap["taskParam"] = taskParam;
    kvMap["taskConfigPath"] = taskConfigPath;
    kvMap["clusterName"] = targetClusterName;
    kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);

    // _taskFlowManager->declareTaskParameter(flowId, flowId, kvMap);
    // kvMap.clear();
    string taskIdStr = StringUtil::toString(taskId);
    kvMap["taskId"] = taskIdStr;
    kvMap["kernalType"] = taskName;

    auto taskMaintainer = getUserTask(taskId);
    if (taskMaintainer) {
        BS_LOG(WARN, "task [%ld] already exist", taskId);
        response->message = std::string("start task: ") + taskIdStr + " failed";
        response->duplicated = true;
        response->needRecover = false;
        return false;
    }
    if (!_taskFlowManager->loadSubGraph("", "BuildInTask.graph", kvMap)) {
        response->message = string("start task: ") + taskIdStr + " failed";
        return false;
    }
    _taskFlowManager->stepRun();
    string taskTag = getUserTaskTag(taskId);
    vector<string> flowIds;
    _taskFlowManager->getFlowIdByTag(taskTag, flowIds);
    assert(flowIds.size() == 1);
    TaskFlowPtr taskFlow = _taskFlowManager->getFlow(flowIds[0]);
    if (taskFlow->hasFatalError()) {
        response->message = string("start task: ") + taskIdStr + " failed";
        _taskFlowManager->removeFlow(flowIds[0], true);
        return false;
    }
    taskMaintainer = _taskFlowManager->getTaskMaintainer(taskFlow);
    assert(taskMaintainer);
    proto::TaskInfo taskInfo;
    TaskNodes taskNodes;
    CounterCollector::RoleCounterMap counterMap;
    taskMaintainer->collectTaskInfo(&taskInfo, taskNodes, counterMap);
    response->message = taskInfo.taskinfo();
    return true;
}

bool GenerationTask::doStopTask(int64_t taskId, std::string& errorMsg)
{
    string taskFlowTag = getUserTaskTag(taskId);
    vector<string> flowIds;
    _taskFlowManager->getFlowIdByTag(taskFlowTag, flowIds);
    assert(flowIds.size() <= 1);
    if (flowIds.size() == 0) {
        return true;
    }
    auto flow = _taskFlowManager->getFlow(flowIds[0]);
    if (flow) {
        flow->stopFlow();
    }
    return true;
}

std::vector<ProcessorTaskPtr> GenerationTask::getProcessorTask()
{
    vector<ProcessorTaskPtr> tasks;
    string processorTag = FULL_PROCESS_TAG;
    if (_currentProcessorStep != BUILD_STEP_FULL) {
        processorTag = INC_PROCESS_TAG;
    }
    vector<string> flowIds;
    _taskFlowManager->getFlowIdByTag(processorTag, flowIds);
    vector<TaskFlowPtr> flows;
    fillFlows(flowIds, true, flows);
    for (auto flow : flows) {
        auto tmpTasks = flow->getTasks();
        if (tmpTasks.size() == 0) {
            continue;
        }
        for (auto task : tmpTasks) {
            BuildServiceTaskPtr bsTask;
            bsTask = DYNAMIC_POINTER_CAST(BuildServiceTask, task);
            if (!bsTask) {
                continue;
            }
            ProcessorTaskPtr realTask;
            if (_batchMode) {
                realTask = DYNAMIC_POINTER_CAST(ProcessorTask, bsTask->getTaskImpl());
            } else {
                ProcessorTaskWrapperPtr processorTask =
                    DYNAMIC_POINTER_CAST(ProcessorTaskWrapper, bsTask->getTaskImpl());
                if (!processorTask) {
                    continue;
                }
                realTask = processorTask->getProcessorTask();
            }
            if (!realTask) {
                continue;
            }
            auto iter = find(tasks.begin(), tasks.end(), realTask);
            if (iter == tasks.end()) {
                tasks.push_back(realTask);
            }
        }
    }
    return tasks;
}

bool GenerationTask::doSwitchBuild()
{
    if (isSuspending()) {
        return false;
    }
    auto processorTasks = getProcessorTask();
    for (auto processorTask : processorTasks) {
        if (!processorTask->switchBuild()) {
            return false;
        }
    }
    return true;
}

bool GenerationTask::cleanVersions(const string& clusterName, versionid_t version)
{
    vector<string> clusterNames;
    if (!getAllClusterNames(clusterNames)) {
        BS_LOG(ERROR, "get clusters failed for [%s]", _buildId.ShortDebugString().c_str());
        return false;
    }
    bool found = false;
    for (auto& cn : clusterNames) {
        if (cn == clusterName) {
            found = true;
            break;
        }
    }
    if (!found) {
        BS_LOG(ERROR, "cleanVersions failed for invalid clusterName[%s]", clusterName.c_str());
        return false;
    }

    string generationPath =
        IndexPathConstructor::getGenerationIndexPath(_indexRoot, clusterName, _buildId.generationid());
    try {
        vector<string> fileList;
        if (!fslib::util::FileUtil::listDir(generationPath, fileList)) {
            BS_LOG(WARN, "list generation dir [%s] failed.", generationPath.c_str());
            return false;
        }
        BS_LOG(INFO, "clean version [%d], generation dir [%s] cluster[%s]", version, generationPath.c_str(),
               clusterName.c_str());
        IndexCheckpointAccessorPtr checkpointAccessor =
            CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
        set<versionid_t> reservedVersions = checkpointAccessor->getReservedVersions(clusterName);
        for (auto filePath : fileList) {
            if (!StringUtil::startsWith(filePath, IndexPathConstructor::PARTITION_PREFIX)) {
                continue;
            }
            bool dirExist = false;
            const string& partitionDir = fslib::util::FileUtil::joinFilePath(generationPath, filePath);
            if (!fslib::util::FileUtil::isDir(partitionDir, dirExist)) {
                continue;
            }
            auto directory = indexlib::file_system::Directory::GetPhysicalDirectory(partitionDir);
            indexlib::index_base::VersionCommitter::CleanVersionAndBefore(directory, version, reservedVersions);
        }
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "clean versions before [%d] in cluster[%s] failed", version, clusterName.c_str());
        return false;
    }
    if (_legacyCheckpointSynchronizer) {
        _legacyCheckpointSynchronizer->syncCheckpoints();
    }
    return true;
}

bool GenerationTask::doDeleteIndex() const { return innerDeleteIndex(_indexRoot); }

bool GenerationTask::doDeleteTmpBuilderIndex() const { return innerDeleteIndex(_fullBuilderTmpRoot); }

bool GenerationTask::innerDeleteIndex(const std::string& indexRoot) const
{
    vector<string> clusterNames;
    if (!getAllClusterNames(clusterNames)) {
        BS_LOG(ERROR, "get clusters failed for [%s]", _buildId.ShortDebugString().c_str());
    }
    bool deleteFlag = true;
    for (const auto& clusterName : clusterNames) {
        string generationPath =
            IndexPathConstructor::getGenerationIndexPath(indexRoot, clusterName, _buildId.generationid());
        if (fslib::util::FileUtil::removeIfExist(generationPath)) {
            BS_LOG(INFO, "remove generation [%s]", generationPath.c_str());

            string msg = "remove generation path [" + generationPath + "]";
            BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg, BuildIdWrapper::getEventTags(_buildId));
        } else {
            BS_LOG(WARN, "failed to remove generation [%s]", generationPath.c_str());
            deleteFlag = false;
        }
    }
    return deleteFlag;
}

void GenerationTask::fillIndexInfos(bool isFull, ::google::protobuf::RepeatedPtrField<proto::IndexInfo>* indexInfos)
{
    vector<string> clusterNames;
    if (!getAllClusterNames(clusterNames)) {
        BS_LOG(ERROR, "get clusters failed for [%s]", _buildId.ShortDebugString().c_str());
        return;
    }

    for (auto& clusterName : clusterNames) {
        fillIndexInfosForCluster(clusterName, isFull, indexInfos);
    }
}

void GenerationTask::fillIndexInfosForCluster(const string& clusterName, bool isFull,
                                              ::google::protobuf::RepeatedPtrField<proto::IndexInfo>* indexInfos)
{
    IndexCheckpointAccessorPtr checkpointAccessor = CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
    if (!checkpointAccessor->getIndexInfo(isFull, clusterName, *indexInfos)) {
        return;
    }

    if (!isFull && indexInfos) {
        CheckpointAccessorPtr checkpointAccessor;
        _resourceManager->getResource(checkpointAccessor);
        ResourceReaderPtr latestReader = GetLatestResourceReader();
        auto latestSchema = latestReader->getTabletSchema(clusterName)->GetLegacySchema();
        if (latestSchema && latestSchema->HasModifyOperations()) {
            vector<schema_opid_t> ongoingOpIds;
            size_t maxOpId = latestSchema->GetModifyOperationCount();
            if (!AlterFieldCKPAccessor::getOngoingOpIds(checkpointAccessor, clusterName, maxOpId, ongoingOpIds)) {
                return;
            }
            if (ongoingOpIds.empty()) {
                return;
            }
            string opIds = StringUtil::toString(ongoingOpIds, ",");
            // only has one item
            proto::IndexInfo* indexInfo = indexInfos->Mutable(0);
            indexInfo->set_ongoingmodifyopids(opIds);
        }
    }
}

void GenerationTask::fillGenerationInfo(GenerationInfo* generationInfo, const WorkerTable* workerTable,
                                        bool fillTaskHistory, bool fillFlowInfo, bool fillEffectiveIndexInfo,
                                        const CounterCollector::RoleCounterMap& roleCounterMap)
{
    generationInfo->set_configpath(getConfigPath());
    generationInfo->set_buildstep(stepToString(_step));
    auto processorTasks = getProcessorTask();
    for (auto& processorTask : processorTasks) {
        generationInfo->set_datadescriptions(ToJsonString(processorTask->getInput().dataDescriptions.toVector()));
        if (workerTable) {
            processorTask->fillProcessorInfo(generationInfo->mutable_processorinfo(),
                                             workerTable->getWorkerNodes(processorTask->getTaskIdentifier()),
                                             roleCounterMap);
            processorTask->fillProcessorInfo(generationInfo->mutable_processorinfos()->Add(),
                                             workerTable->getWorkerNodes(processorTask->getTaskIdentifier()),
                                             roleCounterMap);
        }
    }

    generationInfo->set_indexroot(_indexRoot);
    fillIndexInfos(true, generationInfo->mutable_fullindexinfos());
    fillIndexInfos(false, generationInfo->mutable_indexinfos());
    fillIndexInfoSummarys(generationInfo);

    if (workerTable) {
        fillTaskInfosForSingleClusters(generationInfo->mutable_buildinfo(), *workerTable, fillEffectiveIndexInfo,
                                       roleCounterMap);
    }
    generationInfo->set_protocolversion(getWorkerProtocolVersion());
    if (fillFlowInfo && workerTable) {
        _taskFlowManager->fillTaskFlowInfos(generationInfo, *workerTable, roleCounterMap);
    }

    if (_step == GENERATION_STARTED) {
        _fatalErrorChecker.checkFatalErrorInRoles(generationInfo);
    }
    _fatalErrorChecker.fillGenerationFatalErrors(generationInfo);
}

void GenerationTask::makeDecision(WorkerTable& workerTable)
{
    _taskFlowManager->stepRun();
    _flowCleaner->cleanFlows(_taskFlowManager, _generationDir, _batchMode);
    switch (_step) {
    case GENERATION_STARTING:
        prepareGeneration(workerTable);
        break;
    case GENERATION_STARTED:
        doMakeDecision(workerTable);
        break;
    case GENERATION_STOPPING:
        workerTable.clearAllNodes();
        stopGeneration();
        break;
    case GENERATION_STOPPED:
    default:
        break;
    };
}

void GenerationTask::prepareGeneration(WorkerTable& workerTable)
{
    if (!_isImportedTask && !writeRealtimeInfoToIndex()) {
        BS_LOG(ERROR, "write realtimeInfo failed in generation[%s]", _buildId.ShortDebugString().c_str());
        return;
    }
    BrokerTopicAccessorPtr brokerTopicAccessor;
    _resourceManager->getResource(brokerTopicAccessor);
    if (!brokerTopicAccessor->prepareBrokerTopics()) {
        return;
    }

    _step = GENERATION_STARTED;
    BS_LOG(INFO, "prepare generation[%s] end", _buildId.ShortDebugString().c_str());
}

bool GenerationTask::writeRealtimeInfoToIndex()
{
    vector<string> allClusters;
    if (!getAllClusterNames(allClusters)) {
        return false;
    }

    ResourceReaderPtr resourceReader = GetLatestResourceReader();
    bool rtBuildEnableJobMode = false;
    if (!resourceReader->getDataTableConfigWithJsonPath(_buildId.datatable(), "job_mode_realtime_build",
                                                        rtBuildEnableJobMode)) {
        BS_LOG(WARN, "fail to get job_mode_realtime_build param");
        return false;
    }

    for (auto& clusterName : allClusters) {
        if (rtBuildEnableJobMode) {
            if (!doWriteRealtimeInfoForRealtimeJobMode(clusterName, _realTimeDataDesc)) {
                return false;
            }
            continue;
        }

        config::ControlConfig controlConfig;
        if (!resourceReader->getDataTableConfigWithJsonPath(_buildId.datatable(), "control_config", controlConfig)) {
            BS_LOG(ERROR, "get control_config.is_inc_processor_exist failed from dataTable[%s] failed",
                   _buildId.datatable().c_str());
            return false;
        }
        BuildServiceConfig buildServiceConfig;
        if (!resourceReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
            string errorMsg = "failed to get build service config in config[" + getConfigPath() + "]";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return false;
        }
        string realtimeInfoContent = DataLinkModeUtil::generateRealtimeInfoContent(controlConfig, buildServiceConfig,
                                                                                   clusterName, _realTimeDataDesc);
        if (realtimeInfoContent.empty()) {
            BS_LOG(ERROR, "Generate realtime_info failed for buildId[%s], cluster[%s]",
                   _buildId.ShortDebugString().c_str(), clusterName.c_str());
            return false;
        }

        if (!doWriteRealtimeInfoToIndex(clusterName, realtimeInfoContent)) {
            BS_LOG(ERROR, "Serialize realtime_info failed for buildId[%s], cluster[%s]",
                   _buildId.ShortDebugString().c_str(), clusterName.c_str());
            return false;
        }
    }
    return true;
}

void GenerationTask::stopGeneration()
{
    BrokerTopicAccessorPtr brokerTopicAccessor;
    _resourceManager->getResource(brokerTopicAccessor);
    if (!brokerTopicAccessor->clearUselessBrokerTopics(true)) {
        return;
    }
    if (!CleanTaskSignature()) {
        BS_LOG(ERROR, "clean task signature failed in generation[%s]", _buildId.ShortDebugString().c_str());
        return;
    }
    _step = GENERATION_STOPPED;
    BS_LOG(INFO, "stop generation[%s] end", _buildId.ShortDebugString().c_str());
}

void GenerationTask::updateWorkerProtocolVersion(const WorkerNodes& workerNodes)
{
    ResourceReaderPtr resourceReader = GetLatestResourceReader();
    string configPath = resourceReader->getConfigPath();
    int32_t protocolVersion = UNKNOWN_WORKER_PROTOCOL_VERSION;
    for (size_t i = 0; i < workerNodes.size(); ++i) {
        const string& workerConfigPath = fslib::util::FileUtil::normalizeDir(workerNodes[i]->getWorkerConfigPath());

        if (workerConfigPath != configPath) {
            return;
        }
        int32_t currentVersion = workerNodes[i]->getWorkerProtocolVersion();
        if (currentVersion == UNKNOWN_WORKER_PROTOCOL_VERSION) {
            return;
        }
        if (i == 0) {
            protocolVersion = currentVersion;
        } else if (protocolVersion != currentVersion) {
            return;
        }
    }
    resourceReader->updateWorkerProtocolVersion(protocolVersion);
}

bool GenerationTask::createSavepoint(const string& clusterName, checkpointid_t checkpointId, const std::string& comment,
                                     std::string& savepointStr, std::string& errorMsg)
{
    ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult checkpointResult;
    if (!_checkpointSynchronizer) {
        IndexCheckpointAccessorPtr checkpointAccessor =
            CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
        if (!LegacyCheckpointAdaptor(GetLatestResourceReader(), checkpointAccessor, _buildId, clusterName, _indexRoot)
                 .createSavepoint(checkpointId, &checkpointResult)) {
            BS_LOG(ERROR, "create savepoint for cluster [%s] failed", clusterName.c_str());
            return false;
        }
    } else {
        if (!_checkpointSynchronizer->createSavepoint(clusterName, checkpointId, comment, &checkpointResult,
                                                      errorMsg)) {
            BS_LOG(ERROR, "create savepoint for cluster [%s] failed", clusterName.c_str());
            return false;
        }
    }
    savepointStr = autil::legacy::ToJsonString(checkpointResult);
    return true;
}

bool GenerationTask::removeSavepoint(const string& clusterName, checkpointid_t checkpointId, std::string& errorMsg)
{
    if (!_checkpointSynchronizer) {
        std::shared_ptr<common::IndexCheckpointAccessor> indexCheckpointAccessor =
            CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
        return LegacyCheckpointAdaptor(GetLatestResourceReader(), indexCheckpointAccessor, _buildId, clusterName,
                                       _indexRoot)
            .removeSavepoint(checkpointId);
    }
    return _checkpointSynchronizer->removeSavepoint(clusterName, checkpointId, errorMsg);
}

bool GenerationTask::doGetBulkloadInfo(const std::string& clusterName, const std::string& bulkloadId,
                                       const ::google::protobuf::RepeatedPtrField<proto::Range>& rangeVec,
                                       std::string* resultStr, std::string* errorMsg) const
{
    std::map<std::string, common::PartitionLevelBulkloadState> result;
    if (!_checkpointSynchronizer) {
        *errorMsg = "get bulkload info fail, checkpoint synchronizer is nullptr, cluster[ " + clusterName + "].";
        BS_LOG(ERROR, "%s", (*errorMsg).c_str());
        return false;
    } else {
        std::vector<proto::Range> ranges;
        if (rangeVec.empty()) {
            ranges = _checkpointSynchronizer->getRanges(clusterName);
        } else {
            for (const auto& range : rangeVec) {
                ranges.push_back(range);
            }
        }
        for (const auto& range : ranges) {
            std::string rangeKey = common::PartitionLevelCheckpoint::genRangeKey(range);
            common::PartitionLevelBulkloadState bulkloadState;
            result[rangeKey] = bulkloadState;
            auto [ret, partitionCkpt] = _checkpointSynchronizer->getPartitionCheckpoint(clusterName, range);
            if (!ret) {
                *errorMsg =
                    "get partition checkpoint for cluster [" + clusterName + "], range [" + rangeKey + "] failed";
                BS_LOG(WARN, "%s", (*errorMsg).c_str());
                continue;
            }
            auto indexTaskQueue = partitionCkpt.indexTaskQueue;
            for (const auto& indexTask : indexTaskQueue) {
                if (indexTask.taskType == indexlibv2::framework::BULKLOAD_TASK_TYPE &&
                    indexTask.taskName == bulkloadId) {
                    bulkloadState.committedVersionId = indexTask.committedVersionId;
                    bulkloadState.state = indexTask.state;
                    bulkloadState.comment = indexTask.comment;
                    result[rangeKey] = bulkloadState;
                    break;
                }
            }
        }
    }
    *resultStr = autil::legacy::ToJsonString(result);
    return true;
}

bool GenerationTask::getCheckpoint(const std::string& clusterName, checkpointid_t checkpointId,
                                   std::string* resultStr) const
{
    ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult result;
    if (!_checkpointSynchronizer) {
        std::shared_ptr<common::IndexCheckpointAccessor> indexCheckpointAccessor =
            CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
        if (!LegacyCheckpointAdaptor(GetLatestResourceReader(), indexCheckpointAccessor, _buildId, clusterName,
                                     _indexRoot)
                 .getCheckpoint(checkpointId, &result)) {
            BS_LOG(ERROR, "list checkpoint fail, cluster[%s].", clusterName.c_str());
            return false;
        }
    } else if (!_checkpointSynchronizer->getCheckpoint(clusterName, checkpointId, &result)) {
        BS_LOG(ERROR, "get checkpoint fail, cluster[%s].", clusterName.c_str());
        return false;
    }
    *resultStr = autil::legacy::ToJsonString(result);
    return true;
}

bool GenerationTask::listCheckpoint(const std::string& clusterName, bool savepointOnly, uint32_t offset, uint32_t limit,
                                    std::string* resultStr) const
{
    std::vector<ClusterCheckpointSynchronizer::SingleGenerationLevelCheckpointResult> result;
    if (_checkpointSynchronizer == nullptr) {
        std::shared_ptr<common::IndexCheckpointAccessor> indexCheckpointAccessor =
            CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
        if (!LegacyCheckpointAdaptor(GetLatestResourceReader(), indexCheckpointAccessor, _buildId, clusterName,
                                     _indexRoot)
                 .listCheckpoint(savepointOnly, offset, limit, &result)) {
            BS_LOG(ERROR, "list checkpoint fail, cluster[%s].", clusterName.c_str());
            return false;
        }
    } else if (!_checkpointSynchronizer->listCheckpoint(clusterName, savepointOnly, offset, limit, &result)) {
        BS_LOG(ERROR, "list checkpoint fail, cluster[%s].", clusterName.c_str());
        return false;
    }
    *resultStr = autil::legacy::ToJsonString(result);
    return true;
}

bool GenerationTask::getReservedVersions(const std::string& clusterName, const proto::Range& range,
                                         std::set<indexlibv2::framework::VersionCoord>* reservedVersions) const
{
    std::shared_ptr<common::BuilderCheckpointAccessor> builderCheckpointAccessor =
        CheckpointCreator::createBuilderCheckpointAccessor(_resourceManager);
    if (builderCheckpointAccessor) {
        uint32_t incBuildParallelNum = 1;
        if (!getIncBuildParallelNum(clusterName, &incBuildParallelNum)) {
            BS_LOG(ERROR, "fail to get inc build parallel num");
            return false;
        }
        auto subRangeVec = RangeUtil::splitRange(range.from(), range.to(), incBuildParallelNum);
        set<indexlibv2::framework::VersionCoord> slaveReservedVersions =
            builderCheckpointAccessor->getSlaveReservedVersions(clusterName, subRangeVec);
        (*reservedVersions).insert(slaveReservedVersions.begin(), slaveReservedVersions.end());
    }
    std::shared_ptr<common::IndexCheckpointAccessor> indexCheckpointAccessor =
        CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
    if (indexCheckpointAccessor) {
        if (_checkpointSynchronizer == nullptr) {
            // for legacy offline
            // in update scenario, legacy index checkpoint reserved versions will be ignored immediately,
            // this is considered acceptable with keep version count is use.
            auto versions = indexCheckpointAccessor->getReservedVersions(clusterName);
            for (const auto& version : versions) {
                (*reservedVersions).emplace(version);
            }
            BS_LOG(INFO, "clusterName[%s] get reserved version for offline [%s]", clusterName.c_str(),
                   autil::legacy::ToJsonString(*reservedVersions, /*isCompact=*/true).c_str());
            return true;
        } else {
            // in update scenario, legacy snapshot should be reserved
            // legacy checkpoints still be ignored
            std::vector<proto::CheckpointInfo> checkpointInfos;
            if (!indexCheckpointAccessor->listCheckpoint(clusterName, /*savepointOnly=*/true,
                                                         /*limit=*/std::numeric_limits<uint32_t>::max(),
                                                         &checkpointInfos)) {
                BS_LOG(ERROR, "Get snapshot from legacy index checkpoint accessor fail.");
                return false;
            }
            for (const auto& checkpointInfo : checkpointInfos) {
                (*reservedVersions).emplace(checkpointInfo.versionid());
            }
        }
    }
    return _checkpointSynchronizer->getReservedVersions(clusterName, range, reservedVersions);
}

bool GenerationTask::commitVersion(const std::string& clusterName, const proto::Range& range,
                                   const std::string& versionMetaStr, bool instantPublish, std::string& errorMsg)
{
    if (!_checkpointSynchronizer) {
        auto resourceReader = GetLatestResourceReader();
        if (!prepareCheckpointSynchronizer(resourceReader, ClusterCheckpointSynchronizerCreator::Type::DEFAULT)) {
            BS_LOG(ERROR, "commitVersion failed, clusterName[%s], versionMeta[%s]", clusterName.c_str(),
                   versionMetaStr.c_str());
            return false;
        }
    }
    _commitVersionCalled = true;
    _checkpointSynchronizer->setIsLeaderFollowerMode(true);
    // TODO(tianxiao) check error message
    if (!versionMetaStr.empty()) {
        if (!_checkpointSynchronizer->publishPartitionLevelCheckpoint(clusterName, range, versionMetaStr, errorMsg)) {
            BS_LOG(ERROR, "commitVersion failed, clusterName[%s]", clusterName.c_str());
            return false;
        }
    } else if (!instantPublish) {
        BS_LOG(ERROR, "Param error: version meta str and syncInstant are both empty, range [%d_%d]", range.from(),
               range.to());
        errorMsg = "Param error: version meta str and syncInstant are both empty";
        return false;
    }
    ClusterCheckpointSynchronizer::SyncOptions options;
    options.isInstant = instantPublish;
    if (instantPublish && !_checkpointSynchronizer->syncCluster(clusterName, options)) {
        BS_LOG(ERROR, "Sync failed: clusterName %s, versionMeta %s", clusterName.c_str(), versionMetaStr.c_str());
        errorMsg = "Sync failed: clusterName " + clusterName + ", versionMeta " + versionMetaStr;
        return false;
    }
    return true;
}

bool GenerationTask::getCommittedVersions(const std::string& clusterName, const proto::Range& range, uint32_t limit,
                                          std::string& result, std::string& errorMsg)
{
    if (!_checkpointSynchronizer) {
        auto resourceReader = GetLatestResourceReader();
        if (!prepareCheckpointSynchronizer(resourceReader, ClusterCheckpointSynchronizerCreator::Type::DEFAULT)) {
            BS_LOG(ERROR, "get committed version failed, clusterName[%s]", clusterName.c_str());
            return false;
        }
    }
    std::vector<indexlibv2::versionid_t> versions;
    if (!_checkpointSynchronizer->getCommittedVersions(clusterName, range, limit, versions, errorMsg)) {
        BS_LOG(ERROR, "get committed version failed, clusterName[%s], ", clusterName.c_str());
        return false;
    }
    result = autil::legacy::ToJsonString(versions);
    return true;
}

bool GenerationTask::markCheckpoint(const string& clusterName, versionid_t versionId, bool reserveFlag,
                                    string& errorMsg)
{
    // can only mark IndexCheckpoint which is generated by SingleMergerTask
    IndexCheckpointAccessorPtr accessor = CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
    if (!accessor) {
        return false;
    }

    if (reserveFlag) {
        if (!accessor->createIndexSavepoint("user", clusterName, versionId)) {
            errorMsg = "create index snapshot failed.";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        return true;
    }

    if (!accessor->removeIndexSavepoint("user", clusterName, versionId)) {
        errorMsg = "remove index snapshot failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

void GenerationTask::runTasks(WorkerTable& workerTable)
{
    if (_batchMode) {
        auto tasks = _taskFlowManager->getAllTask();
        for (auto& task : tasks) {
            string taskId = task->getNodesIdentifier();
            auto& workerNodes = workerTable.getWorkerNodes(taskId);
            task->syncTaskProperty(workerNodes);
        }
        return;
    }
    GenerationTaskBase::runTasks(workerTable);
}

int32_t GenerationTask::getWorkerProtocolVersion()
{
    ConfigReaderAccessorPtr accessor;
    _resourceManager->getResource(accessor);
    return accessor->getLatestConfig()->getWorkerProtocolVersion();
}

void GenerationTask::doMakeDecision(WorkerTable& workerTable)
{
    BrokerTopicAccessorPtr brokerTopicAccessor;
    _resourceManager->getResource(brokerTopicAccessor);
    brokerTopicAccessor->prepareBrokerTopics();
    brokerTopicAccessor->clearUselessBrokerTopics(false);
    _configCleaner->cleanObsoleteConfig();

    int32_t workerProtocolVersion = getWorkerProtocolVersion();
    if (workerProtocolVersion == UNKNOWN_WORKER_PROTOCOL_VERSION) {
        updateWorkerProtocolVersion(workerTable.getActiveNodes());
    }
    if (_currentProcessorStep == BUILD_STEP_FULL) {
        vector<string> flowIds;
        _taskFlowManager->getFlowIdByTag(FULL_PROCESS_TAG, flowIds);
        bool isFinish = true;
        for (auto& flowId : flowIds) {
            auto flow = _taskFlowManager->getFlow(flowId);
            if (!flow->isFlowFinish()) {
                isFinish = false;
            }
        }

        if (isFinish) {
            _currentProcessorStep = BUILD_STEP_INC;
        }
    }

    if (!_isFullBuildFinish) {
        if (checkFullBuildFinish() && deleteTmpBuilderIndex()) {
            _isFullBuildFinish = true;
        }
    }
    runTasks(workerTable);
    if (_legacyCheckpointSynchronizer) {
        _legacyCheckpointSynchronizer->syncCheckpoints();
    } else {
        vector<string> clusters;
        if (getAllClusterNames(clusters)) {
            _legacyCheckpointSynchronizer.reset(
                new LegacyCheckpointListSyncronizer(_generationDir, _zkWrapper, _resourceManager, clusters));
        }
    }
    if (_checkpointSynchronizer) {
        if (!_checkpointSynchronizer->empty() && _checkpointSynchronizer->isLeaderFollowerMode()) {
            // do not enable auto sync for offline, to avoid unaligned version checkpoint published.
            // _isLeaderFollowerMode will be set true when CommitVersion called.
            _checkpointSynchronizer->sync();
        }
    } else {
        auto resourceReader = std::make_shared<ResourceReader>(getConfigPath());
        resourceReader->init();
        prepareCheckpointSynchronizer(resourceReader);
    }

    checkFatalError(workerTable);
}

bool GenerationTask::prepareCheckpointSynchronizer(const config::ResourceReaderPtr& resourceReader,
                                                   ClusterCheckpointSynchronizerCreator::Type type)
{
    std::shared_ptr<CheckpointSynchronizer> checkpointSynchronizer;
    bool isLeaderFollowerMode = _commitVersionCalled;
    if (type == ClusterCheckpointSynchronizerCreator::Type::DEFAULT) {
        if (!_resourceManager->getResource(checkpointSynchronizer)) {
            BS_LOG(ERROR, "get checkpoint synchronizer failed, buildId[%s]", _buildId.ShortDebugString().c_str());
            return false;
        }
    } else if (type == ClusterCheckpointSynchronizerCreator::Type::ZOOKEEPER) {
        std::shared_ptr<ZKCheckpointSynchronizer> zkCheckpointSynchronizer;
        if (!_resourceManager->getResource(zkCheckpointSynchronizer)) {
            BS_LOG(ERROR, "get zk checkpoint synchronizer failed, buildId[%s]", _buildId.ShortDebugString().c_str());
            return false;
        }
        checkpointSynchronizer = zkCheckpointSynchronizer;
        isLeaderFollowerMode = true;
    } else {
        BS_LOG(ERROR, "invalid checkpoint synchronizer type[%d], buildId[%s]", (int32_t)type,
               _buildId.ShortDebugString().c_str());
        return false;
    }
    std::shared_ptr<CheckpointAccessor> checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    ClusterCheckpointSynchronizerCreator creator(_buildId);
    auto clusterSynchronizers =
        creator.create(checkpointAccessor, resourceReader, _checkpointMetricReporter, _indexRoot, type);
    if (clusterSynchronizers.empty()) {
        BS_LOG(ERROR, "create cluster checkpoint synchronizers failed, buildId[%s].",
               _buildId.ShortDebugString().c_str());
        return false;
    }
    if (!checkpointSynchronizer->init(clusterSynchronizers, isLeaderFollowerMode)) {
        BS_LOG(ERROR, "init checkpoint synchronizer failed, buildId[%s]", _buildId.ShortDebugString().c_str());
        return false;
    }
    _checkpointSynchronizer = checkpointSynchronizer;
    return true;
}

bool GenerationTask::prepareCheckpointSynchronizer(const config::ResourceReaderPtr& resourceReader)
{
    ClusterCheckpointSynchronizerCreator::Type type;
    if (!getCheckpointSynchronizerType(resourceReader, &type)) {
        BS_LOG(ERROR, "get checkpoint synchronizer type failed, buildId[%s].", _buildId.ShortDebugString().c_str());
        return false;
    }
    if (type == ClusterCheckpointSynchronizerCreator::Type::NONE) {
        if (_checkpointSynchronizer) {
            _checkpointSynchronizer->clear();
        }
        return true;
    }
    return prepareCheckpointSynchronizer(resourceReader, type);
}

bool GenerationTask::getCheckpointSynchronizerType(const config::ResourceReaderPtr& resourceReader,
                                                   ClusterCheckpointSynchronizerCreator::Type* type)
{
    if (resourceReader == nullptr) {
        BS_LOG(ERROR, "resource reader is nullptr");
        return false;
    }
    BuildServiceConfig buildServiceConfig;
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        string errorMsg = "failed to get build_app.json in config[" + resourceReader->getConfigPath() + "]";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    if (buildServiceConfig.suezVersionZkRoots.empty()) {
        bool hasError;
        bool isV2BuildMode = isV2Build(resourceReader, hasError);
        if (hasError) {
            BS_LOG(ERROR, "check is v2 build failed");
            return false;
        }
        if (_commitVersionCalled || isV2BuildMode) {
            *type = ClusterCheckpointSynchronizerCreator::Type::DEFAULT;
        } else {
            *type = ClusterCheckpointSynchronizerCreator::Type::NONE;
        }
    } else {
        *type = ClusterCheckpointSynchronizerCreator::Type::ZOOKEEPER;
    }
    return true;
}

void GenerationTask::checkFatalError(const WorkerTable& workerTable)
{
    int64_t now = autil::TimeUtility::currentTimeInSeconds();
    if (_fatalErrorMetricReporter && _fatalErrorChecker.getFatalErrorRole() != FatalErrorChecker::ROLE_NONE) {
        kmonitor::MetricsTags tags;
        tags.AddTag("roleType", _fatalErrorChecker.getFatalErrorRoleTypeString());
        tags.AddTag("roleName", _fatalErrorChecker.getFatalErrorRoleName());
        _fatalErrorMetricReporter->reportFatalError(_fatalErrorChecker.getFatalErrorDuration(), tags);
    }

    if (_isFullBuildFinish && (now - _lastCheckErrorTs) < INNER_CHECK_PERIOD_INTERVAL) {
        // tiny optimize: inc not check fatal error, not support auto stop
        return;
    }
    GenerationInfo generationInfo;
    CounterCollector::RoleCounterMap counterMap;
    fillGenerationInfo(&generationInfo, &workerTable, false, true, false, counterMap);
    _lastCheckErrorTs = now;
}

int64_t GenerationTask::getMaxLastVersionTimestamp() const
{
    vector<string> allClusters;
    if (!getAllClusterNames(allClusters)) {
        BS_LOG(ERROR, "get clusters failed for [%s]", _buildId.ShortDebugString().c_str());
        return -1;
    }
    ::google::protobuf::RepeatedPtrField<proto::IndexInfo> indexInfos;
    int64_t maxVersionTs = -1;
    string checkpointName;
    IndexCheckpointAccessorPtr checkpointAccessor = CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
    // get version finish ts from index checkpoint
    for (auto& clusterName : allClusters) {
        indexInfos.Clear();
        if (checkpointAccessor->getIndexInfo(false, clusterName, indexInfos) && indexInfos.size() > 0 &&
            indexInfos.Get(0).isvalid() && (int64_t)(indexInfos.Get(0).finishtimestamp()) > maxVersionTs) {
            maxVersionTs = indexInfos.Get(0).finishtimestamp();
            continue;
        }
        if (checkpointAccessor->getIndexInfo(true, clusterName, indexInfos) && indexInfos.size() > 0 &&
            indexInfos.Get(0).isvalid() && (int64_t)(indexInfos.Get(0).finishtimestamp()) > maxVersionTs) {
            maxVersionTs = indexInfos.Get(0).finishtimestamp();
            continue;
        }
    }
    return maxVersionTs;
}

void GenerationTask::fillIndexInfoSummarys(GenerationInfo* generationInfo) const
{
    fillIndexInfoSummary(generationInfo->mutable_fullindexinfosummary(), generationInfo->fullindexinfos());
    fillIndexInfoSummary(generationInfo->mutable_indexinfosummary(), generationInfo->indexinfos());
}

void GenerationTask::fillIndexInfoSummary(IndexInfoSummary* indexInfoSummary,
                                          const ::google::protobuf::RepeatedPtrField<IndexInfo>& indexInfos) const
{
    if (indexInfos.size() == 0) {
        return;
    }
    uint64_t totalIndexSize = 0;
    int64_t minIndexVersion = -1;
    int64_t maxIndexVersion = -1;
    int64_t minVersionTimestamp = -1;
    int64_t maxVersionTimestamp = -1;
    int64_t minStartTimestamp = -1;
    int64_t maxStartTimestamp = -1;
    int64_t minFinishTimestamp = -1;
    int64_t maxFinishTimestamp = -1;

    for (int i = 0; i < indexInfos.size(); i++) {
        const IndexInfo& indexInfo = indexInfos.Get(i);
        totalIndexSize += indexInfos.Get(i).indexsize();
        getMinValue(minIndexVersion, indexInfo.indexversion(), -1);
        getMinValue(minVersionTimestamp, indexInfo.versiontimestamp(), -1);
        getMinValue(minStartTimestamp, indexInfo.starttimestamp(), -1);
        getMinValue(minFinishTimestamp, indexInfo.finishtimestamp(), -1);

        getMaxValue(maxIndexVersion, indexInfo.indexversion(), -1);
        getMaxValue(maxVersionTimestamp, indexInfo.versiontimestamp(), -1);
        getMaxValue(maxStartTimestamp, indexInfo.starttimestamp(), -1);
        getMaxValue(maxFinishTimestamp, indexInfo.finishtimestamp(), -1);
    }

    assert(indexInfoSummary);
    indexInfoSummary->set_indexsize(totalIndexSize);
    indexInfoSummary->mutable_indexversionrange()->set_minvalue(minIndexVersion);
    indexInfoSummary->mutable_indexversionrange()->set_maxvalue(maxIndexVersion);
    indexInfoSummary->mutable_versiontimestamprange()->set_minvalue(minVersionTimestamp);
    indexInfoSummary->mutable_versiontimestamprange()->set_maxvalue(maxVersionTimestamp);
    indexInfoSummary->mutable_starttimestamprange()->set_minvalue(minStartTimestamp);
    indexInfoSummary->mutable_starttimestamprange()->set_maxvalue(maxStartTimestamp);
    indexInfoSummary->mutable_finishtimestamprange()->set_minvalue(minFinishTimestamp);
    indexInfoSummary->mutable_finishtimestamprange()->set_maxvalue(maxFinishTimestamp);
}

bool GenerationTask::fatalErrorAutoStop()
{
    if (_step != GENERATION_STARTED) {
        return false;
    }
    // if all cluster have generated fullIndex, no need to stop
    if (_isFullBuildFinish) {
        return false;
    }
    int64_t timeout = EnvUtil::getEnv("fatal_error_wait_time_before_stop", 24 * 3600 * 365 * 10);

    if (_fatalErrorChecker.getFatalErrorRole() != FatalErrorChecker::ROLE_NONE &&
        _fatalErrorChecker.getFatalErrorDuration() >= timeout) {
        return true;
    }
    return false;
}

bool GenerationTask::checkFullBuildFinish() const
{
    if (_isFullBuildFinish) {
        return true;
    }
    vector<string> flowIds;
    _taskFlowManager->getFlowIdByTag(FULL_BUILD_TAG, flowIds);
    for (auto& flowId : flowIds) {
        auto flow = _taskFlowManager->getFlow(flowId);
        assert(flow);
        if (!flow->isFlowFinish()) {
            return false;
        }
    }
    return true;
}

bool GenerationTask::isRollingBack() const
{
    assert(_taskFlowManager);
    vector<string> flowIds;
    _taskFlowManager->getFlowIdByTag(ROLLING_BACK_TAG, flowIds);
    for (const string& flowId : flowIds) {
        TaskFlowPtr flow = _taskFlowManager->getFlow(flowId);
        if (!flow->isFlowFinish() && !flow->isFlowStopped()) {
            BS_LOG(WARN, "flow[%s] is rolling back.", flowId.c_str());
            return true;
        }
    }
    flowIds.clear();
    _taskFlowManager->getFlowIdByTag("BSBuildV2", flowIds);
    for (const string& flowId : flowIds) {
        TaskFlowPtr flow = _taskFlowManager->getFlow(flowId);
        if (flow->getProperty("IsRollingBack") == "true") {
            if (!flow->isFlowFinish() && !flow->isFlowStopped()) {
                BS_LOG(WARN, "flowV2 [%s] is rolling back.", flowId.c_str());
                return true;
            }
        }
    }
    return false;
}

void GenerationTask::fillTaskInfosForSingleClusters(BuildInfo* buildInfo, const WorkerTable& workerTable,
                                                    bool fillEffectiveIndexInfo,
                                                    const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    vector<string> allClusters;
    if (!getAllClusterNames(allClusters)) {
        BS_LOG(ERROR, "get clusters failed for [%s]", _buildId.ShortDebugString().c_str());
        return;
    }

    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    ConfigReaderAccessorPtr configAccessor;
    _resourceManager->getResource(configAccessor);
    ResourceReaderPtr latestReader = configAccessor->getLatestConfig();
    for (auto& cluster : allClusters) {
        vector<TaskFlowPtr> buildFlow;
        vector<TaskFlowPtr> alterFieldFlow;
        vector<TaskFlowPtr> mergeCrontabFlow;
        getTaskFlowsForSingleCluster(cluster, buildFlow, alterFieldFlow, mergeCrontabFlow);

        SingleClusterInfo* singleClusterInfo = buildInfo->add_clusterinfos();
        singleClusterInfo->set_clustername(cluster);
        auto latestSchema = latestReader->getTabletSchema(cluster);
        singleClusterInfo->set_schemaname(latestSchema->GetTableName());
        singleClusterInfo->set_tabletype(convertTableType(latestSchema->GetTableType()));
        // for batch build
        if (_batchMode) {
            vector<TaskFlowPtr> batchBuildFlow;
            vector<TaskFlowPtr> batchMergeFlow;
            getBatchTaskFlowForSingelCluster(cluster, batchBuildFlow, batchMergeFlow);
            fillBatchTaskInfos(singleClusterInfo, batchBuildFlow, batchMergeFlow, workerTable, roleCounterMap);
        } else {
            fillBuildTaskInfos(singleClusterInfo, buildFlow, alterFieldFlow, workerTable, roleCounterMap);
        }
        fillPendingTaskInfos(singleClusterInfo, mergeCrontabFlow);

        fillCheckpointInfos(cluster, singleClusterInfo);
        IndexCheckpointAccessorPtr indexCheckpointAccessor =
            CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
        ::google::protobuf::RepeatedPtrField<proto::IndexInfo> indexInfos;
        if (!indexCheckpointAccessor->getIndexInfo(false, cluster, indexInfos)) {
            ResourceReaderPtr reader = configAccessor->getLatestConfig();
            auto schema = reader->getTabletSchema(cluster);
            singleClusterInfo->set_lastversiontimestamp(-1);
            singleClusterInfo->set_currentschemaversion(schema->GetSchemaId());
        } else {
            const IndexInfo& indexInfo = indexInfos.Get(0);
            singleClusterInfo->set_lastversiontimestamp(indexInfo.finishtimestamp());
            singleClusterInfo->set_currentschemaversion(indexInfo.schemaversion());
            auto legacySchema = latestSchema->GetLegacySchema();
            if (fillEffectiveIndexInfo && legacySchema) {
                IndexPartitionSchemaPtr targetSchema;
                if (legacySchema->HasModifyOperations() && indexInfo.schemaversion() > 0) {
                    targetSchema.reset(legacySchema->CreateSchemaForTargetModifyOperation(indexInfo.schemaversion()));
                    if (!targetSchema) {
                        ResourceReaderPtr reader = configAccessor->getConfig(cluster, indexInfo.schemaversion());
                        if (reader) {
                            targetSchema = reader->getSchema(cluster);
                        }
                    }
                    if (targetSchema) {
                        vector<schema_opid_t> opIds;
                        StringUtil::fromString(indexInfo.ongoingmodifyopids(), opIds, ",");
                        for (auto id : opIds) {
                            targetSchema->MarkOngoingModifyOperation(id);
                        }
                    }
                } else {
                    targetSchema.reset(legacySchema->CreateSchemaWithoutModifyOperations());
                }

                if (targetSchema) {
                    singleClusterInfo->set_lastversioneffectiveindexinfo(targetSchema->GetEffectiveIndexInfo());
                }
            }
        }
    }
}

void GenerationTask::fillCheckpointInfos(const std::string& clusterName,
                                         proto::SingleClusterInfo* singleClusterInfo) const
{
    ConfigReaderAccessorPtr configAccessor;
    _resourceManager->getResource(configAccessor);
    if (!isLeaderFollowerMode() && _checkpointSynchronizer) {
        std::vector<ClusterCheckpointSynchronizer::SingleGenerationLevelCheckpointResult> checkpointResults;
        if (!_checkpointSynchronizer->listCheckpoint(clusterName, /*savepointOnly=*/false, /*offset=*/0,
                                                     /*limit=*/std::numeric_limits<uint32_t>::max(),
                                                     &checkpointResults)) {
            BS_LOG(WARN, "list checkpoint fail, cluster[%s].", clusterName.c_str());
            return;
        }
        for (auto iter = checkpointResults.rbegin(); iter != checkpointResults.rend(); iter++) {
            const auto& checkpointResult = (*iter);
            proto::CheckpointInfo* checkpointInfo = singleClusterInfo->add_checkpointinfos();
            const auto& generationLevelCheckpoint = checkpointResult.generationLevelCheckpoint;
            assert(generationLevelCheckpoint.versionIdMapping.size() > 0);
            checkpointInfo->set_versionid(generationLevelCheckpoint.versionIdMapping.begin()->second);
            checkpointInfo->set_versiontimestamp(generationLevelCheckpoint.minTimestamp);
            checkpointInfo->set_schemaid(generationLevelCheckpoint.readSchemaId);
            checkpointInfo->set_issnapshot(checkpointResult.isSavepoint);
            ResourceReaderPtr reader = configAccessor->getConfig(clusterName, generationLevelCheckpoint.readSchemaId);
            if (reader) {
                checkpointInfo->set_configpath(reader->getConfigPath());
            }
        }
        if (!checkpointResults.empty()) {
            return;
        }
    }

    // legacy index checkpoint
    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    std::string checkpointId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    std::vector<std::pair<std::string, std::string>> checkpoints;
    checkpointAccessor->listCheckpoints(checkpointId, checkpoints);
    for (const auto& checkpoint : checkpoints) {
        proto::CheckpointInfo info;
        IndexCheckpointFormatter::decodeCheckpoint(checkpoint.second, info);
        proto::CheckpointInfo* checkpointInfo = singleClusterInfo->add_checkpointinfos();
        *checkpointInfo = info;
        std::string comment;
        checkpointInfo->set_issnapshot(checkpointAccessor->isSavepoint(checkpointId, checkpoint.first, &comment));
        ResourceReaderPtr reader = configAccessor->getConfig(clusterName, info.schemaid());
        if (reader) {
            checkpointInfo->set_configpath(reader->getConfigPath());
        }
    }
}

void GenerationTask::fillFlows(vector<string>& flowIds, bool onlyActiveFlow, vector<TaskFlowPtr>& flows) const
{
    for (auto flowId : flowIds) {
        auto flow = _taskFlowManager->getFlow(flowId);
        if (onlyActiveFlow) {
            if (flow->getStatus() == TaskFlow::tf_init || flow->getStatus() == TaskFlow::tf_finish ||
                flow->getStatus() == TaskFlow::tf_stopped) {
                continue;
            }
            if (!flow->isUpstreamFlowReady()) {
                continue;
            }
        }
        flows.push_back(flow);
    }
}
void GenerationTask::getTaskFlowsForSingleCluster(const string& cluster, vector<TaskFlowPtr>& buildFlow,
                                                  vector<TaskFlowPtr>& alterFieldFlow,
                                                  vector<TaskFlowPtr>& mergeCrontabFlow) const
{
    vector<string> flowIds;
    if (!_isFullBuildFinish) {
        string fullBuildFlowTag = cluster + ":FullBuild";
        _taskFlowManager->getFlowIdByTag(fullBuildFlowTag, flowIds);
        fillFlows(flowIds, true, buildFlow);
    } else {
        string incBuildFlowTag = cluster + ":IncBuild";
        _taskFlowManager->getFlowIdByTag(incBuildFlowTag, flowIds);
        fillFlows(flowIds, true, buildFlow);
    }

    string alterFieldFlowTag = cluster + ":AlterField";
    _taskFlowManager->getFlowIdByTag(alterFieldFlowTag, flowIds);
    fillFlows(flowIds, true, alterFieldFlow);

    string mergeCrontabFlowTag = cluster + ":MergeCrontab";
    _taskFlowManager->getFlowIdByTag(mergeCrontabFlowTag, flowIds);
    fillFlows(flowIds, true, mergeCrontabFlow);
}

void GenerationTask::getBatchTaskFlowForSingelCluster(const string& cluster, vector<TaskFlowPtr>& batchBuildFlow,
                                                      vector<TaskFlowPtr>& batchMergeFlow) const
{
    vector<string> flowIds;
    if (!_isFullBuildFinish) {
        string fullBuildFlowTag = cluster + ":FullBuild";
        _taskFlowManager->getFlowIdByTag(fullBuildFlowTag, flowIds);
        fillFlows(flowIds, true, batchBuildFlow);
        flowIds.clear();

        string fullMergeFlowTag = cluster + ":BSFullMerger";
        _taskFlowManager->getFlowIdByTag(fullMergeFlowTag, flowIds);
        fillFlows(flowIds, true, batchMergeFlow);
    } else {
        string incBuildFlowTag = cluster + ":IncBuild";
        _taskFlowManager->getFlowIdByTag(incBuildFlowTag, flowIds);
        fillFlows(flowIds, true, batchBuildFlow);
        flowIds.clear();

        string batchMergeFlowTag = cluster + ":BSIncMerger";
        _taskFlowManager->getFlowIdByTag(batchMergeFlowTag, flowIds);
        fillFlows(flowIds, true, batchMergeFlow);
    }
}

void GenerationTask::fillBatchTaskInfos(SingleClusterInfo* singleClusterInfo, const vector<TaskFlowPtr>& batchBuildFlow,
                                        const vector<TaskFlowPtr>& batchMergeFlow, const WorkerTable& workerTable,
                                        const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    TaskFlowPtr flow;
    string step = "";
    for (int32_t i = batchBuildFlow.size() - 1; i >= 0; i--) {
        flow = batchBuildFlow[i];
        string builderName = "builder";
        if (flow->hasTag("BSBuildV2")) {
            builderName = _isFullBuildFinish ? "incBuilder" : "fullBuilder";
        }
        TaskBasePtr task = flow->getTask(builderName);
        if (task) {
            step = "build";
            break;
        }
    }
    if (step.empty()) {
        for (int32_t i = batchMergeFlow.size() - 1; i >= 0; i--) {
            flow = batchMergeFlow[i];
            TaskBasePtr task = flow->getTask(_isFullBuildFinish ? "incMerger" : "merger");
            if (task) {
                step = "merge";
                break;
            }
        }
    }

    if (step.empty() || !flow) {
        singleClusterInfo->set_clusterstep("idle");
        return;
    }
    if (flow->isFlowSuspending()) {
        singleClusterInfo->set_taskstatus("suspending");
    } else if (flow->isFlowSuspended()) {
        singleClusterInfo->set_taskstatus("suspended");
    } else {
        singleClusterInfo->set_taskstatus("normal");
    }

    if (flow->hasTag("BSBuildV2")) {
        FillSingleBuilderInfoV2(singleClusterInfo, flow, _isFullBuildFinish ? "incBuilder" : "fullBuilder", workerTable,
                                roleCounterMap);
    } else {
        if (step == "build") {
            FillSingleBuilderInfo(singleClusterInfo, flow, "builder", workerTable, roleCounterMap);
        } else {
            FillSingleMergerInfo(singleClusterInfo, flow, _isFullBuildFinish ? "incMerger" : "merger", workerTable,
                                 roleCounterMap);
        }
    }
}

void GenerationTask::fillBuildTaskInfos(SingleClusterInfo* singleClusterInfo, const vector<TaskFlowPtr>& buildFlow,
                                        const vector<TaskFlowPtr>& alterFieldFlow, const WorkerTable& workerTable,
                                        const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    TaskFlowPtr flow;
    if (!alterFieldFlow.empty()) {
        flow = *alterFieldFlow.crbegin();
    } else if (!buildFlow.empty()) {
        flow = *buildFlow.crbegin();
    }

    if (!flow) {
        singleClusterInfo->set_clusterstep("unknown");
        return;
    }

    if (flow->isFlowSuspending()) {
        singleClusterInfo->set_taskstatus("suspending");
    } else if (flow->isFlowSuspended()) {
        singleClusterInfo->set_taskstatus("suspended");
    } else {
        singleClusterInfo->set_taskstatus("normal");
    }

    if (flow->hasTag("BSBuildV2")) {
        FillSingleBuilderInfoV2(singleClusterInfo, flow, _isFullBuildFinish ? "incBuilder" : "fullBuilder", workerTable,
                                roleCounterMap);
    } else {
        // for old build
        string flowStatus = flow->getProperty("_flow_status_");
        if (flowStatus.find("Build") != string::npos) {
            FillSingleBuilderInfo(singleClusterInfo, flow, _isFullBuildFinish ? "incBuilder" : "fullBuilder",
                                  workerTable, roleCounterMap);
        } else if (flowStatus.find("Merging") != string::npos) {
            FillSingleMergerInfo(singleClusterInfo, flow, _isFullBuildFinish ? "incMerger" : "fullMerger", workerTable,
                                 roleCounterMap);
        } else if (!flowStatus.empty()) {
            singleClusterInfo->set_clusterstep(flowStatus);
        } else {
            singleClusterInfo->set_clusterstep("idle");
        }
    }
    if (!alterFieldFlow.empty()) {
        singleClusterInfo->set_clusterstep("converting_index");
    }
}

void GenerationTask::FillSingleBuilderInfoV2(SingleClusterInfo* singleClusterInfo, const TaskFlowPtr& flow,
                                             const string& taskId, const WorkerTable& workerTable,
                                             const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    TaskBasePtr task = flow->getTask(taskId);
    if (!task) {
        singleClusterInfo->set_clusterstep("idle");
        return;
    }
    BuildServiceTaskPtr bsTask = DYNAMIC_POINTER_CAST(BuildServiceTask, task);
    if (!bsTask) {
        assert(false);
        return;
    }
    auto taskMaintainer = DYNAMIC_POINTER_CAST(TaskMaintainer, bsTask->getTaskImpl());
    const WorkerNodes& workerNodes = workerTable.getWorkerNodes(task->getIdentifier());
    proto::TaskNodes taskNodes;
    for (const auto& workerNode : workerNodes) {
        auto taskNode = std::dynamic_pointer_cast<TaskNode>(workerNode);
        taskNodes.push_back(taskNode);
    }
    proto::TaskInfo taskInfo;
    taskMaintainer->collectTaskInfo(&taskInfo, taskNodes, roleCounterMap);
    SingleBuilderTaskV2::convertTaskInfoToSingleClusterInfo(taskInfo, singleClusterInfo);
}

void GenerationTask::FillSingleBuilderInfo(SingleClusterInfo* singleClusterInfo, const TaskFlowPtr& flow,
                                           const string& taskId, const WorkerTable& workerTable,
                                           const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    assert(flow);
    TaskBasePtr task = flow->getTask(taskId);
    if (!task) {
        singleClusterInfo->set_clusterstep("idle");
        return;
    }
    BuildServiceTaskPtr bsTask = DYNAMIC_POINTER_CAST(BuildServiceTask, task);
    if (!bsTask) {
        return;
    }

    BuilderTaskWrapperPtr builderTask = DYNAMIC_POINTER_CAST(BuilderTaskWrapper, bsTask->getTaskImpl());
    assert(builderTask);

    const WorkerNodes& workerNodes = workerTable.getWorkerNodes(task->getIdentifier());
    BuilderNodes builderNodes;
    for (auto& workerNode : workerNodes) {
        BuilderNodePtr node = DYNAMIC_POINTER_CAST(BuilderNode, workerNode);
        if (node) {
            builderNodes.push_back(node);
        }
    }
    builderTask->fillClusterInfo(singleClusterInfo, builderNodes, roleCounterMap);
}

void GenerationTask::FillSingleMergerInfo(SingleClusterInfo* singleClusterInfo, const TaskFlowPtr& flow,
                                          const string& taskId, const WorkerTable& workerTable,
                                          const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    assert(flow);
    TaskBasePtr task = flow->getTask(taskId);
    if (!task) {
        assert(false);
        return;
    }

    BuildServiceTaskPtr bsTask = DYNAMIC_POINTER_CAST(BuildServiceTask, task);
    if (!bsTask) {
        return;
    }

    SingleMergerTaskPtr mergerTask = DYNAMIC_POINTER_CAST(SingleMergerTask, bsTask->getTaskImpl());
    assert(mergerTask);

    const WorkerNodes& workerNodes = workerTable.getWorkerNodes(task->getIdentifier());
    MergerNodes mergerNodes;
    for (auto& workerNode : workerNodes) {
        MergerNodePtr node = DYNAMIC_POINTER_CAST(MergerNode, workerNode);
        if (node) {
            mergerNodes.push_back(node);
        }
    }
    mergerTask->fillClusterInfo(singleClusterInfo, mergerNodes, roleCounterMap);
}

void GenerationTask::fillPendingTaskInfos(SingleClusterInfo* singleClusterInfo,
                                          const vector<TaskFlowPtr>& mergeCrontabFlow) const
{
    auto cluster = singleClusterInfo->clustername();
    for (auto rit = mergeCrontabFlow.crbegin(); rit != mergeCrontabFlow.crend(); ++rit) {
        TaskFlowPtr flow = *rit;
        TaskBasePtr task = flow->getTask("mergeCrontab");
        if (!task) {
            continue;
        }

        MergeCrontabTaskPtr crontabTask = DYNAMIC_POINTER_CAST(MergeCrontabTask, task);
        if (!crontabTask) {
            continue;
        }
        vector<string> pendingTasks = crontabTask->getPendingMergeTasks();
        for (size_t i = 0; i < pendingTasks.size(); i++) {
            *singleClusterInfo->add_pendingmergetasks() = pendingTasks[i];
        }

        int64_t now = autil::TimeUtility::currentTimeInSeconds();
        if (now - _lastCheckErrorTs >= INNER_CHECK_PERIOD_INTERVAL && !pendingTasks.empty()) {
            IndexCheckpointAccessorPtr indexCheckpointAccessor =
                CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
            ::google::protobuf::RepeatedPtrField<proto::IndexInfo> indexInfos;
            if (indexCheckpointAccessor->getIndexInfo(false, cluster, indexInfos)) {
                const IndexInfo& indexInfo = indexInfos.Get(0);
                int64_t latestVersionTsInSec = indexInfo.finishtimestamp();
                int64_t gap = now - latestVersionTsInSec;
                if (gap > 7200) // 2h
                {
                    beeper::EventTags tags = BuildIdWrapper::getEventTags(_buildId);
                    tags.AddTag("clusters", cluster);
                    BEEPER_FORMAT_REPORT(GENERATION_ERROR_COLLECTOR_NAME, tags,
                                         "need create version, lastest version timestamp [%ld] "
                                         "for cluster [%s] : time gap [%ld] to now over 2 hours",
                                         latestVersionTsInSec, cluster.c_str(), gap);
                }
            }
            _lastCheckErrorTs = now;
        }

        // TODO: support branch
        break;
    }
}

TaskBasePtr GenerationTask::getTask(const string& flowId, const string& taskId)
{
    auto flow = _taskFlowManager->getFlow(flowId);
    if (!flow) {
        return TaskBasePtr();
    }
    return flow->getTask(taskId);
}

TaskFlowPtr GenerationTask::getFlow(const string& flowId) { return _taskFlowManager->getFlow(flowId); }

bool GenerationTask::getTotalRemainIndexSize(int64_t& totalSize) const
{
    vector<string> allClusters;
    if (!getAllClusterNames(allClusters)) {
        BS_LOG(WARN, "get clusters failed for [%s]", _buildId.ShortDebugString().c_str());
        return false;
    }
    totalSize = 0;
    bool ret = false;
    CheckpointAccessorPtr ckpAccessor;
    _resourceManager->getResource(ckpAccessor);
    for (auto& cluster : allClusters) {
        string totalIndexSizeCkpId = IndexCheckpointFormatter::getIndexTotalSizeCheckpointId(cluster);
        string indexSizeStr;
        int64_t indexSize = -1;
        if (!ckpAccessor->getCheckpoint(totalIndexSizeCkpId, BS_INDEX_TOTAL_SIZE_ID, indexSizeStr, false)) {
            continue;
        }

        if (!StringUtil::fromString(indexSizeStr, indexSize)) {
            BS_LOG(ERROR, "parse index size[%s] failed.", indexSizeStr.c_str());
            continue;
        }
        if (indexSize >= 0) {
            ret = true;
            totalSize += indexSize;
        }
    }
    return ret;
}

std::vector<std::string> GenerationTask::getClusterNames() const
{
    vector<string> clusterName;
    getAllClusterNames(clusterName);
    return clusterName;
}

bool GenerationTask::printGraph(bool fillTaskInfo, string& graphString)
{
    graphString = _taskFlowManager->getDotString(fillTaskInfo);
    return true;
}

bool GenerationTask::printGraphForCluster(const string& clusterName, bool fillTaskInfo, string& graphString)
{
    graphString = _taskFlowManager->getDotStringForMatchingFlows("clusterName", clusterName, fillTaskInfo);
    return true;
}

bool GenerationTask::doWriteRealtimeInfoForRealtimeJobMode(const string& clusterName, const DataDescription& ds)
{
    ResourceReaderPtr resourceReader = GetLatestResourceReader();

    json::JsonMap jsonMap;
    // TODO: how to update config for realtime build
    jsonMap[CONFIG_PATH] = resourceReader->getOriginalConfigPath();
    jsonMap[REALTIME_MODE] = REALTIME_JOB_MODE;
    jsonMap[DATA_TABLE_NAME] = _buildId.datatable();
    for (KeyValueMap::const_iterator it = ds.begin(); it != ds.end(); it++) {
        jsonMap[it->first] = it->second;
    }
    const string& realtimeInfoContent = ToJsonString(jsonMap);
    return doWriteRealtimeInfoToIndex(clusterName, realtimeInfoContent);
}

bool GenerationTask::doUpdateSchema(bool hasOperations, const vector<string>& chgSchemaClusters,
                                    const KeyValueMap& schemaIdMap, const KeyValueMap& opsIdMap)
{
    KeyValueMap kvMap;
    kvMap["clusterNames"] = ToJsonString(chgSchemaClusters, true);
    kvMap["schemaIdMap"] = ToJsonString(schemaIdMap, true);
    kvMap["operationIdMap"] = ToJsonString(opsIdMap, true);
    kvMap["buildStep"] = "incremental";
    kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);
    kvMap["dataDescriptions"] = _dataDescriptionsStr;
    string graphName = "AlterField.graph";
    if (hasOperations) {
        graphName = "ModifySchema.graph";
    }
    if (!_taskFlowManager->loadSubGraph("", graphName, kvMap)) {
        REPORT(SERVICE_ERROR_CONFIG, "load sub graph alterFiled failed [%s]", _buildId.ShortDebugString().c_str());
        return false;
    }
    for (auto& cluster : chgSchemaClusters) {
        string msg = hasOperations ? "ModifySchema" : "AlterField";
        msg += " for cluster [" + cluster + "] : ";
        if (hasOperations) {
            auto iter = opsIdMap.find(cluster);
            msg += (iter != opsIdMap.end()) ? iter->second : "";
        } else {
            auto iter = schemaIdMap.find(cluster);
            msg += "schemaId ";
            msg += (iter != schemaIdMap.end()) ? iter->second : "";
        }
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg);
    }
    _taskFlowManager->stepRun();
    return true;
}

bool GenerationTask::doUpdateConfig(const vector<string>& chgSchemaClusters, const std::string& configPath)
{
    auto allTasks = _taskFlowManager->getAllTask();
    for (auto task : allTasks) {
        if (!task->isTaskFinish() && !task->isTaskStopped() && !task->isTaskStopping()) {
            if (!task->updateConfig()) {
                REPORT(SERVICE_ERROR_CONFIG, "task [%s:%s] update config failed", task->getTaskType().c_str(),
                       task->getIdentifier().c_str());
                return false;
            }
            string msg =
                "task [" + task->getTaskType() + ":" + task->getIdentifier() + "] update config [" + configPath + "]";
            BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg);
        }
    }
    return true;
}

bool GenerationTask::isV2Build(const ResourceReaderPtr& resourceReader, bool& hasError)
{
    GraphConfig graphConfig;
    hasError = false;
    auto status = resourceReader->getGraphConfigReturnStatus(graphConfig);
    if (status == ResourceReader::Status::ERROR) {
        hasError = true;
        BS_LOG(ERROR, "read graph.json failed");
        return false;
    }
    if (status == ResourceReader::Status::OK) {
        string targetGraph = graphConfig.getGraphName();
        return (targetGraph == "BatchBuildV2/FullBatchBuild.graph" || targetGraph == "BuildIndexV2.graph" ||
                targetGraph == "BuildIndexV2.npc_mode.graph");
    }
    config::ControlConfig controlConfig;
    status = resourceReader->getDataTableConfigWithJsonPathReturnStatus(_buildId.datatable(), "control_config",
                                                                        controlConfig);
    if (status == ResourceReader::Status::ERROR) {
        hasError = true;
    }
    if (status == ResourceReader::Status::OK) {
        return controlConfig.useIndexV2() || controlConfig.getDataLinkMode() == ControlConfig::DataLinkMode::NPC_MODE;
    }
    return false;
}

bool GenerationTask::TEST_prepareFakeIndex() const
{
    ResourceReaderPtr resourceReader = GetLatestResourceReader();
    if (!resourceReader) {
        BS_LOG(ERROR, "unexpceted: resourceReader not existed");
        return false;
    }
    BuildTaskValidatorPtr validator(
        new BuildTaskValidator(resourceReader, _buildId, _indexRoot, "", _realTimeDataDesc));
    if (!validator->TEST_prepareFakeIndex()) {
        BS_LOG(ERROR, "prepare fake index failed");
        return false;
    }
    return true;
}

string GenerationTask::stepToString(GenerationStep step) const
{
    if (step == GENERATION_STARTED) {
        return _currentProcessorStep == BUILD_STEP_FULL ? "full" : "incremental";
    }
    return GenerationTaskBase::stepToString(step);
}

bool GenerationTask::getIncBuildParallelNum(const std::string& clusterName, uint32_t* incBuildParallelNum) const
{
    std::shared_ptr<ResourceReader> latestReader = GetLatestResourceReader();
    if (latestReader == nullptr) {
        BS_LOG(ERROR, "resource reader is nullptr");
        return false;
    }
    BuildRuleConfig buildRuleConfig;
    if (!latestReader->getClusterConfigWithJsonPath(clusterName, "cluster_config.builder_rule_config",
                                                    buildRuleConfig)) {
        BS_LOG(ERROR, "get builder_rule_config failed, cluster[%s]", clusterName.c_str());
        return false;
    }
    *incBuildParallelNum = buildRuleConfig.incBuildParallelNum;
    return true;
}
}} // namespace build_service::admin
