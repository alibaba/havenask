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
#include "build_service/admin/GenerationTaskBase.h"

#include <cstdint>
#include <iostream>
#include <memory>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/BatchBrokerTopicAccessor.h"
#include "build_service/admin/CheckpointSynchronizer.h"
#include "build_service/admin/ConfigValidator.h"
#include "build_service/admin/GeneralGenerationTask.h"
#include "build_service/admin/GenerationTask.h"
#include "build_service/admin/GraphGenerationTask.h"
#include "build_service/admin/JobTask.h"
#include "build_service/admin/WorkerTable.h"
#include "build_service/admin/ZKCheckpointSynchronizer.h"
#include "build_service/admin/controlflow/LocalLuaScriptReader.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/BuildServiceTaskFactory.h"
#include "build_service/admin/taskcontroller/TaskOptimizer.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/BrokerTopicAccessor.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/common/IndexTaskRequestQueue.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/misc/common.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::util;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, GenerationTaskBase);

GenerationTaskBase::GenerationTaskBase(const BuildId& buildId, TaskType taskType, cm_basic::ZkWrapper* zkWrapper)
    : _buildId(buildId)
    , _taskType(taskType)
    , _zkWrapper(zkWrapper)
    , _step(GENERATION_UNKNOWN)
    , _debugMode("run")
    , _heartbeatType("rpc")
    , _indexDeleted(false)
    , _startTimeStamp(0)
    , _stopTimeStamp(0)
    , _resourceManager(new TaskResourceManager())
    , _configCleaner(new ConfigCleaner(_resourceManager))
{
    init();
}

GenerationTaskBase::~GenerationTaskBase() {}

void GenerationTaskBase::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("task_type", _taskType, _taskType);
    json.Jsonize("generation_step", _step);
    json.Jsonize("config_path", _configPath);
    json.Jsonize("debug_mode", _debugMode, _debugMode);
    json.Jsonize("heartbeat_type", _heartbeatType, _heartbeatType);
    json.Jsonize("index_deleted", _indexDeleted, _indexDeleted);
    json.Jsonize("index_root", _indexRoot, _indexRoot);
    json.Jsonize("start_time_stamp", _startTimeStamp, _startTimeStamp);
    json.Jsonize("stop_time_stamp", _stopTimeStamp, _stopTimeStamp);
    json.Jsonize("counter_config", _counterConfig, _counterConfig);
    json.Jsonize("full_builder_tmp_root", _fullBuilderTmpRoot, _fullBuilderTmpRoot);

    if (_step != GENERATION_UNKNOWN && _startTimeStamp == 0) {
        _startTimeStamp = TimeUtility::currentTimeInSeconds();
    }

    // legacy code for job task not store _stopTimestamp
    if (_step == GENERATION_STOPPED && _stopTimeStamp == 0) {
        _stopTimeStamp = TimeUtility::currentTimeInSeconds();
    }

    if (_fullBuilderTmpRoot.empty()) {
        _fullBuilderTmpRoot = _indexRoot;
    }
    json.Jsonize("fatal_error_checker", _fatalErrorChecker, _fatalErrorChecker);
    json.Jsonize("generation_dir", _generationDir, _generationDir);
    // no need to serialize _realTimeInfoState

    ConfigReaderAccessorPtr configAccessor;
    _resourceManager->getResource(configAccessor);
    json.Jsonize("config_resource", *configAccessor);
    common::CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    json.Jsonize("checkpoints", *checkpointAccessor);
    json.Jsonize("script_path_id", _luaPathId);
    if (json.GetMode() == TO_JSON) {
        _taskFlowManager->serialize(json);
        string resourceKeeperStr = _resourceManager->serializeResourceKeepers();
        json.Jsonize("resource_keepers", resourceKeeperStr, resourceKeeperStr);
        auto agentRoleMap = getAgentRoleTargetMap();
        if (agentRoleMap) {
            json.Jsonize("agent_role_map", agentRoleMap);
        }
        if (_catalogPartitionIdentifier) {
            json.Jsonize("catalog_partition_identifier", *_catalogPartitionIdentifier);
        }
    } else {
        string resourceKeepers;
        json.Jsonize("resource_keepers", resourceKeepers, resourceKeepers);
        if (!_resourceManager->deserializeResourceKeepers(resourceKeepers)) {
            throw autil::legacy::ExceptionBase("GenerationTask FromJson error, resourceKeeper deserialize fail.");
        }

        string luaPath = _taskFlowManager->getRootPath();
        bool zip = false;
        if (LocalLuaScriptReader::isLocalLuaPathExist(_luaPathId, zip)) {
            luaPath = LocalLuaScriptReader::getLocalLuaPath(_luaPathId);
            if (zip) {
                LocalLuaScriptReader::preloadZipLuaPath(_luaPathId);
            }
        }
        _taskFlowManager.reset(new TaskFlowManager(luaPath, _taskFactory, _resourceManager));
        _taskFlowManager->setLogPrefix(_buildId.ShortDebugString());
        if (!_taskFlowManager->deserialize(json)) {
            throw autil::legacy::ExceptionBase("GenerationTask FromJson error, taskFlowManager deserialize fail.");
        }
        std::shared_ptr<AgentRoleTargetMap> agentRoleMap;
        json.Jsonize("agent_role_map", agentRoleMap, agentRoleMap);
        setAgentRoleTargetMap(agentRoleMap);
        json.Jsonize("catalog_partition_identifier", _catalogPartitionIdentifier, _catalogPartitionIdentifier);
    }
    json.Jsonize("flow_cleaner", _flowCleaner, _flowCleaner);
    json.Jsonize("call_graph_history", _callGraphHistory, _callGraphHistory);
}

std::shared_ptr<config::MultiClusterRealtimeSchemaListKeeper> GenerationTaskBase::getSchemaListKeeper() const
{
    if (_schemaListKeeper) {
        return _schemaListKeeper;
    }
    std::vector<std::string> clusterNames;
    auto resourceReader = GetLatestResourceReader();
    if (!getAllClusterNames(*resourceReader, clusterNames)) {
        return nullptr;
    }
    _schemaListKeeper.reset(new config::MultiClusterRealtimeSchemaListKeeper);
    _schemaListKeeper->init(_indexRoot, clusterNames, _buildId.generationid());
    if (!_schemaListKeeper->updateSchemaCache()) {
        _schemaListKeeper.reset();
        return nullptr;
    }
    return _schemaListKeeper;
}

GenerationTaskBase* GenerationTaskBase::recover(const BuildId& buildId, const string& jsonStr,
                                                cm_basic::ZkWrapper* zkWrapper)
{
    Any json;
    try {
        json = ParseJson(jsonStr);
    } catch (const ExceptionBase& e) {
        // generation_status may be compressed
        ZlibCompressor compressor;
        string decompressedStr;
        if (!compressor.decompress(jsonStr, decompressedStr)) {
            BS_LOG(ERROR, "decompress json string failed [%s]", jsonStr.c_str());
            return NULL;
        }
        try {
            json = ParseJson(decompressedStr);
        } catch (const ExceptionBase& e) {
            BS_LOG(ERROR, "invalid json string %s", jsonStr.c_str());
            return NULL;
        }
    }
    if (!IsJsonMap(json)) {
        BS_LOG(ERROR, "%s is not json map", jsonStr.c_str());
        return NULL;
    }
    JsonMap* jsonMap = AnyCast<JsonMap>(&json);
    assert(jsonMap != NULL);
    JsonMap::const_iterator it = jsonMap->find("task_type");
    GenerationTaskBase::TaskType tt = GenerationTaskBase::TT_SERVICE;
    if (it != jsonMap->end()) {
        const Any& ttJson = it->second;
        if (!IsJsonNumber(ttJson)) {
            BS_LOG(ERROR, "task_type invalid, json string is: %s", jsonStr.c_str());
            return NULL;
        }
        tt = (GenerationTaskBase::TaskType)JsonNumberCast<int>(ttJson);
    }
    GenerationTaskBase* task = NULL;
    switch (tt) {
    case GenerationTaskBase::TT_SERVICE:
        task = new GenerationTask(buildId, zkWrapper);
        break;
    case GenerationTaskBase::TT_JOB:
        task = new JobTask(buildId, zkWrapper);
        break;
    case GenerationTaskBase::TT_CUSTOMIZED:
        task = new GraphGenerationTask(buildId, zkWrapper);
        break;
    case GenerationTaskBase::TT_GENERAL:
        task = new GeneralGenerationTask(buildId, zkWrapper);
        break;
    default:
        break;
    }
    return task;
}

bool GenerationTaskBase::loadFromConfig(const string& configPath, const string& generationDir,
                                        const string& dataDescriptionKvs, BuildStep buildStep)
{
    if (!readHeartbeatType(configPath)) {
        return false;
    }

    if (!readIndexRoot(configPath, _indexRoot, _fullBuilderTmpRoot)) {
        return false;
    }
    DataDescriptions dataDescriptions;
    if (!dataDescriptionKvs.empty()) {
        if (!parseDataDescriptions(dataDescriptionKvs, dataDescriptions)) {
            BS_LOG(ERROR, "no valid DataDescriptions, raw string: %s", dataDescriptionKvs.c_str());
            return false;
        }
    } else {
        if (!loadDataDescriptions(configPath, dataDescriptions)) {
            BS_LOG(ERROR, "[%s] load data descriptions from config failed", _buildId.ShortDebugString().c_str());
            return false;
        }
    }
    BS_LOG(INFO, "dataDescriptions for %s: %s", _buildId.ShortDebugString().c_str(),
           ToJsonString(dataDescriptions.toVector()).c_str());
    if (!doLoadFromConfig(configPath, generationDir, dataDescriptions, buildStep)) {
        return false;
    }
    setConfigPath(configPath);
    _step = GENERATION_STARTING;
    _startTimeStamp = TimeUtility::currentTimeInSeconds();
    _generationDir = generationDir;
    auto schemaListKeeper = getSchemaListKeeper();
    if (!schemaListKeeper) {
        BS_LOG(ERROR, "get schema list keeper failed");
        return false;
    }
    if (!schemaListKeeper->updateConfig(GetLatestResourceReader())) {
        BS_LOG(ERROR, "schema list keeper update config [%s] failed", configPath.c_str());
        return false;
    }

    return true;
}

bool GenerationTaskBase::deleteTmpBuilderIndex() const
{
    if (_fullBuilderTmpRoot.empty() || _fullBuilderTmpRoot == _indexRoot) {
        return true;
    }
    return doDeleteTmpBuilderIndex();
}

void GenerationTaskBase::deleteIndex() { _indexDeleted = doDeleteIndex(); }

bool GenerationTaskBase::checkIndexRoot(const string& configPath)
{
    string indexRoot;
    string fullBuilderTmpRoot;
    if (!readIndexRoot(configPath, indexRoot, fullBuilderTmpRoot)) {
        return false;
    }
    if (_indexRoot == indexRoot && _fullBuilderTmpRoot == fullBuilderTmpRoot) {
        return true;
    } else {
        BS_LOG(ERROR,
               "index root[%s], full builder index root [%s] in new config"
               " is different from old index root[%s], full builder index root[%s]",
               indexRoot.c_str(), fullBuilderTmpRoot.c_str(), _indexRoot.c_str(), _fullBuilderTmpRoot.c_str());
        return false;
    }
}

bool GenerationTaskBase::readIndexRoot(const string& configPath, string& indexRoot, string& fullBuilderTmpRoot)
{
    BuildServiceConfig buildServiceConfig;
    if (!loadBuildServiceConfig(configPath, buildServiceConfig)) {
        return false;
    }
    indexRoot = buildServiceConfig.getIndexRoot();
    fullBuilderTmpRoot = buildServiceConfig.getBuilderIndexRoot(true);

    string indexPathPrefix;
    if (autil::EnvUtil::getEnvWithoutDefault("BUILD_SERVICE_TEST_INDEX_PATH_PREFIX", indexPathPrefix)) {
        size_t pos = indexRoot.find("://");
        if (pos != string::npos) {
            indexRoot = fslib::util::FileUtil::joinFilePath(indexPathPrefix, indexRoot.substr(pos + 3));
        }
        pos = fullBuilderTmpRoot.find("://");
        if (pos != string::npos) {
            fullBuilderTmpRoot =
                fslib::util::FileUtil::joinFilePath(indexPathPrefix, fullBuilderTmpRoot.substr(pos + 3));
        }
    }
    return true;
}

void GenerationTaskBase::stopBuild()
{
    if (!isActive()) {
        return;
    }
    _stopTimeStamp = TimeUtility::currentTimeInSeconds();
    _step = GENERATION_STOPPING;
}

bool GenerationTaskBase::suspendBuild(const string& flowId, string& errorMsg)
{
    if (_step != GENERATION_STARTED) {
        errorMsg = "suspendBuild can only be called on STARTED generation";
        BS_LOG(ERROR,
               "suspendBuild can only be called on STARTED generation"
               ", current step(%d)",
               _step);
        return false;
    }
    if (!doSuspendBuild(flowId, errorMsg)) {
        return false;
    }
    return true;
}

bool GenerationTaskBase::resumeBuild(const string& flowId, string& errorMsg) { return doResumeBuild(flowId, errorMsg); }

bool GenerationTaskBase::getBulkloadInfo(const string& clusterName, const std::string& bulkloadId,
                                         const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges,
                                         std::string* resultStr, std::string* errorMsg) const
{
    return doGetBulkloadInfo(clusterName, bulkloadId, ranges, resultStr, errorMsg);
}

bool GenerationTaskBase::bulkload(const std::string& clusterName, const std::string& bulkloadId,
                                  const ::google::protobuf::RepeatedPtrField<proto::ExternalFiles>& externalFiles,
                                  const std::string& options, const std::string& action, std::string* errorMsg)
{
    return doBulkload(clusterName, bulkloadId, externalFiles, options, action, errorMsg);
}

bool GenerationTaskBase::rollBack(const string& clusterName, const string& generationZkRoot, versionid_t versionId,
                                  int64_t startTimestamp, string& errorMsg)
{
    return doRollBack(clusterName, generationZkRoot, versionId, startTimestamp, errorMsg);
}

bool GenerationTaskBase::rollBackCheckpoint(const string& clusterName, const string& generationZkRoot,
                                            checkpointid_t checkpointId,
                                            const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges,
                                            string& errorMsg)
{
    return doRollBackCheckpoint(clusterName, generationZkRoot, checkpointId, ranges, errorMsg);
}

bool GenerationTaskBase::validateConfig(const std::string& configPath, std::string& errorMsg, bool firstTime)
{
    ConfigValidator configValidator(_buildId);
    if (!configValidator.validate(configPath, firstTime)) {
        string errorInfo = configValidator.getErrorMsg();
        stringstream ss;
        ss << "validate config[" << configPath << "] failed, error msg: " << errorInfo;
        errorMsg = ss.str();
        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        return false;
    }
    return true;
}

bool GenerationTaskBase::updateConfig(const string& configPath)
{
    if (!isActive()) {
        string errorMsg = "generation not active";
        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        return false;
    }

    if (!readHeartbeatType(configPath)) {
        string errorMsg = "read heartbeat type from build_app.json fail";
        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        return false;
    }
    if (!checkCounterConfig(configPath)) {
        string errorMsg = "check counter config fail";
        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        return false;
    }

    if (!checkIndexRoot(configPath)) {
        string errorMsg = "check index root from build_app.json failed";
        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        return false;
    }

    if (!doUpdateConfig(configPath)) {
        return false;
    }

    setConfigPath(configPath);
    auto schemaListKeeper = getSchemaListKeeper();
    if (!schemaListKeeper) {
        BS_LOG(ERROR, "get schema list keeper failed");
        return false;
    }
    if (!schemaListKeeper->updateConfig(GetLatestResourceReader())) {
        BS_LOG(ERROR, "schema list keeper update config [%s] failed", configPath.c_str());
        return false;
    }
    return true;
}

bool GenerationTaskBase::createVersion(const string& clusterName, const string& mergeConfigName)
{
    if (!isActive()) {
        return false;
    }
    return doCreateVersion(clusterName, mergeConfigName);
}

bool GenerationTaskBase::startTask(int64_t taskId, const std::string& taskName, const std::string& taskConfigPath,
                                   const std::string& clusterName, const std::string& taskParam,
                                   StartTaskResponse* response)
{
    if (!isActive()) {
        response->message = "start task failed : gereration not active";
        return false;
    }
    if (_step == GENERATION_STARTING) {
        response->message = "start task failed : gereration is starting";
        return false;
    }
    return doStartTask(taskId, taskName, taskConfigPath, clusterName, taskParam, response);
}

bool GenerationTaskBase::stopTask(int64_t taskId, std::string& errorMsg)
{
    if (!isActive()) {
        errorMsg = "stop task failed : gereration not active";
        return false;
    }
    return doStopTask(taskId, errorMsg);
}

bool GenerationTaskBase::switchBuild()
{
    if (!isActive()) {
        return false;
    }
    return doSwitchBuild();
}

bool GenerationTaskBase::isActive() const
{
    bool ret = _step != GENERATION_STOPPING && _step != GENERATION_STOPPED;
    if (!ret) {
        BS_LOG(INFO, "generation [%s] is not active", _buildId.ShortDebugString().c_str());
    }
    return ret;
}

bool GenerationTaskBase::loadBuildServiceConfig(const string& configPath, BuildServiceConfig& buildServiceConfig)
{
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configPath);
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        string errorMsg = "failed to get build_app.json in config[" + configPath + "]";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool GenerationTaskBase::readHeartbeatType(const string& configPath)
{
    BuildServiceConfig buildServiceConfig;
    if (!loadBuildServiceConfig(configPath, buildServiceConfig)) {
        return false;
    }

    const string& heartbeatType = buildServiceConfig.heartbeatType;
    if (!heartbeatType.empty() && heartbeatType == "zookeeper") {
        _heartbeatType = "zookeeper";
    } else {
        _heartbeatType = "rpc";
    }
    return true;
}

bool GenerationTaskBase::checkCounterConfig(const string& configPath)
{
    BuildServiceConfig buildServiceConfig;
    if (!loadBuildServiceConfig(configPath, buildServiceConfig)) {
        return false;
    }
    if (buildServiceConfig.counterConfig != _counterConfig) {
        BS_LOG(ERROR, "does not support update CounterConfig");
        return false;
    }
    return true;
}

bool GenerationTaskBase::parseDataDescriptions(const string& dataDescriptionKvs, DataDescriptions& dataDescriptions)
{
    try {
        FromJsonString(dataDescriptions.toVector(), dataDescriptionKvs);
    } catch (const autil::legacy::ExceptionBase& e) {
        stringstream ss;
        ss << "parse data descriptions from [" << dataDescriptionKvs << "] failed, exception[" << string(e.what())
           << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    // check dataDescriptions
    if (dataDescriptions.empty() || !dataDescriptions.validate()) {
        string errorMsg = "validate dataDescriptions[" + dataDescriptionKvs + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool GenerationTaskBase::getAllClusterNames(ResourceReader& resourceReader, vector<string>& clusterNames) const
{
    if (!resourceReader.getAllClusters(_buildId.datatable(), clusterNames)) {
        string errorMsg = "getAllClusters for [" + _buildId.datatable() + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool GenerationTaskBase::doWriteRealtimeInfoToIndex(const string& clusterName, json::JsonMap realtimeInfoJsonMap)
{
    if (_realTimeInfoState.find(clusterName) != _realTimeInfoState.end()) {
        return true;
    }

    string generationPath =
        IndexPathConstructor::getGenerationIndexPath(_indexRoot, clusterName, _buildId.generationid());
    string realtimeInfoFile = fslib::util::FileUtil::joinFilePath(generationPath, REALTIME_INFO_FILE_NAME);

    bool dirExist = false;
    if (!fslib::util::FileUtil::isDir(generationPath, dirExist)) {
        BS_LOG(WARN, "check whether [%s] is dir failed.", generationPath.c_str());
        return false;
    }

    if (!dirExist && !fslib::util::FileUtil::mkDir(generationPath, true)) {
        BS_LOG(WARN, "make dir [%s] failed.", generationPath.c_str());
        return false;
    }

    if (!fslib::util::FileUtil::removeIfExist(realtimeInfoFile)) {
        BS_LOG(WARN, "remove realtime info file[%s] failed.", realtimeInfoFile.c_str());
        return false;
    }

    auto resouceReader = GetLatestResourceReader();
    bool enableFastSlowQueue = false;
    if (!resouceReader->getClusterConfigWithJsonPath(clusterName, "cluster_config.enable_fast_slow_queue",
                                                     enableFastSlowQueue)) {
        BS_LOG(ERROR, "parse cluster_config.enable_fast_slow_queue for [%s] failed", clusterName.c_str());
        return false;
    }
    if (enableFastSlowQueue) {
        realtimeInfoJsonMap[ENABLE_FAST_SLOW_QUEUE] = std::string("true");
    }

    const string& realtimeInfoContent = ToJsonString(realtimeInfoJsonMap);
    if (!fslib::util::FileUtil::writeFile(realtimeInfoFile, realtimeInfoContent)) {
        BS_LOG(WARN, "write realtime info file [%s] failed.", realtimeInfoFile.c_str());
        return false;
    }
    BS_LOG(INFO, "write realtime info file [%s], content[%s].", realtimeInfoFile.c_str(), realtimeInfoContent.c_str());
    _realTimeInfoState.insert(clusterName);
    return true;
}

// todo move to graph generation
void GenerationTaskBase::runTasks(WorkerTable& workerTable)
{
    auto tasks = _taskFlowManager->getAllTask();
    for (auto& task : tasks) {
        auto optimizer = _optimizerFactory->getOptimizer(task->getTaskType());
        task->accept(optimizer, TaskOptimizer::COLLECT);
    }

    for (auto& task : tasks) {
        auto optimizer = _optimizerFactory->getOptimizer(task->getTaskType());
        task->accept(optimizer, TaskOptimizer::OPTIMIZE);
        string taskId = task->getNodesIdentifier();
        auto& workerNodes = workerTable.getWorkerNodes(taskId);
        task->syncTaskProperty(workerNodes);
    }
}

void GenerationTaskBase::init()
{
    setBeeperCollector(GENERATION_ERROR_COLLECTOR_NAME);
    initBeeperTag(_buildId);
    _taskFactory.reset(new BuildServiceTaskFactory());

    string luaPath;
    string envLuaPath;
    LocalLuaScriptReader::ConfigInfo luaConfInfo;
    if (LocalLuaScriptReader::getEnvLuaPathInfo(envLuaPath, luaConfInfo)) {
        _luaPathId = luaConfInfo.pathId;
        bool zip = false;
        if (LocalLuaScriptReader::isLocalLuaPathExist(_luaPathId, zip)) {
            luaPath = LocalLuaScriptReader::getLocalLuaPath(_luaPathId);
            if (zip) {
                LocalLuaScriptReader::preloadZipLuaPath(_luaPathId);
            }
        } else {
            luaPath = envLuaPath; // for test
            cerr << "local lua path for pathId [" << _luaPathId << "] not exist" << endl;
        }
    }
    _taskFlowManager.reset(new TaskFlowManager(luaPath, _taskFactory, _resourceManager));
    _taskFlowManager->setLogPrefix(_buildId.ShortDebugString());
    ConfigReaderAccessorPtr configResource(new config::ConfigReaderAccessor(_buildId.datatable()));
    _resourceManager->addResource(configResource);
    common::CheckpointAccessorPtr checkpointAccessor(new common::CheckpointAccessor);
    _resourceManager->addResource(checkpointAccessor);
    auto checkpointSynchronizer = std::make_shared<CheckpointSynchronizer>(_buildId);
    _resourceManager->addResource(checkpointSynchronizer);
    auto zkCheckpointSynchronizer = std::make_shared<ZKCheckpointSynchronizer>(_buildId);
    _resourceManager->addResource(zkCheckpointSynchronizer);
    auto requestQueue = std::make_shared<common::IndexTaskRequestQueue>();
    _resourceManager->addResource(requestQueue);
    common::BrokerTopicAccessorPtr brokerTopicAccessor;
    bool disableBatchTopic = EnvUtil::getEnv(BS_ENV_DISABLE_BATCH_BROKER_TOPIC, false);
    if (disableBatchTopic) {
        brokerTopicAccessor.reset(new common::BrokerTopicAccessor(_buildId));
    } else {
        size_t maxBatchSize =
            EnvUtil::getEnv(BS_ENV_BROKER_TOPIC_MAX_BATCH_SIZE, BatchBrokerTopicAccessor::DEFAULT_MAX_BATCH_SIZE);
        brokerTopicAccessor.reset(new BatchBrokerTopicAccessor(_buildId, maxBatchSize));
    }

    _resourceManager->addResource(brokerTopicAccessor);
    _flowCleaner.reset(new FlowIdMaintainer());
    _optimizerFactory.reset(new TaskOptimizerFactory(_resourceManager));
}

string GenerationTaskBase::stepToString(GenerationStep step) const
{
    switch (step) {
    case GENERATION_STARTING:
        return "starting";
    case GENERATION_STARTED:
        return "started";
    case GENERATION_STOPPING:
        return "stopping";
    case GENERATION_STOPPED:
        return "stopped";
    default:
        return "unknown";
    }
}

bool GenerationTaskBase::isSuspending()
{
    auto flows = _taskFlowManager->getAllFlow();
    for (auto flow : flows) {
        if (flow->isFlowSuspended() || flow->isFlowSuspending()) {
            return true;
        }
    }
    return false;
}

bool GenerationTaskBase::callGraph(const std::string& graphName, const std::string& graphFileName,
                                   const KeyValueMap& graphKVMap)
{
    if (!graphName.empty()) {
        auto iter = _callGraphHistory.find(graphName);
        if (iter != _callGraphHistory.end()) {
            BS_LOG(INFO, "graph[%s][%s] has already been called at %ld", graphName.c_str(), graphFileName.c_str(),
                   iter->second);
            return true;
        }
        _callGraphHistory.insert(make_pair(graphName, TimeUtility::currentTimeInSeconds()));
    }
    KeyValueMap params = graphKVMap;
    params["configPath"] = getConfigPath();
    return _taskFlowManager->loadSubGraph(graphName, graphFileName, params);
}

TaskMaintainerPtr GenerationTaskBase::getUserTask(int64_t taskId)
{
    string taskTag = getUserTaskTag(taskId);
    vector<string> flowIds;
    _taskFlowManager->getFlowIdByTag(taskTag, flowIds);
    if (flowIds.size() == 0) {
        return TaskMaintainerPtr();
    }
    TaskFlowPtr taskFlow = _taskFlowManager->getFlow(flowIds[0]);
    return _taskFlowManager->getTaskMaintainer(taskFlow);
}

void GenerationTaskBase::fillTaskInfo(int64_t taskId, const WorkerTable& workerTable, proto::TaskInfo* taskInfo,
                                      bool& exist)
{
    auto task = getUserTask(taskId);
    if (!task) {
        exist = false;
        return;
    }

    auto& workerNodes = workerTable.getWorkerNodes(task->getTaskIdentifier());
    TaskNodes taskNodes;
    for (auto& workerNode : workerNodes) {
        TaskNodePtr node = DYNAMIC_POINTER_CAST(TaskNode, workerNode);
        if (node) {
            taskNodes.push_back(node);
        }
    }
    CounterCollector::RoleCounterMap roleCounterMap;
    task->collectTaskInfo(taskInfo, taskNodes, roleCounterMap);
    exist = true;
}

void GenerationTaskBase::setCheckpointMetricReporter(const CheckpointMetricReporterPtr& reporter)
{
    _checkpointMetricReporter = reporter;
    _resourceManager->addResource(reporter);
}

void GenerationTaskBase::setTaskStatusMetricReporter(const TaskStatusMetricReporterPtr& reporter)
{
    _taskStatusMetricReporter = reporter;
    _resourceManager->addResource(reporter);
}

void GenerationTaskBase::SetSlowNodeMetricReporter(const SlowNodeMetricReporterPtr& reporter)
{
    _slowNodeMetricReporter = reporter;
    _resourceManager->addResource(reporter);
}

void GenerationTaskBase::SetFatalErrorMetricReporter(const FatalErrorMetricReporterPtr& reporter)
{
    _fatalErrorMetricReporter = reporter;
    _resourceManager->addResource(reporter);
}

ResourceReaderPtr GenerationTaskBase::GetLatestResourceReader() const
{
    assert(_resourceManager);
    ConfigReaderAccessorPtr configResource;
    if (!_resourceManager->getResource(configResource)) {
        BS_LOG(ERROR, "unexpected: configAccessor not existed in generation[%s]", _buildId.ShortDebugString().c_str());
        return nullptr;
    }
    ResourceReaderPtr resourceReader = configResource->getLatestConfig();
    if (!resourceReader) {
        BS_LOG(ERROR, "unexpected: resourceReader not existed in generation[%s]", _buildId.ShortDebugString().c_str());
        return nullptr;
    }
    return resourceReader;
}

void GenerationTaskBase::setCatalogPartitionIdentifier(const std::shared_ptr<CatalogPartitionIdentifier>& identifier)
{
    _catalogPartitionIdentifier = identifier;
}
std::shared_ptr<CatalogPartitionIdentifier> GenerationTaskBase::getCatalogPartitionIdentifier() const
{
    return _catalogPartitionIdentifier;
}

void GenerationTaskBase::setAgentRoleTargetMap(const std::shared_ptr<AgentRoleTargetMap>& map)
{
    _agentRoleTargetMapGuard.set(map);
}

std::shared_ptr<GenerationTaskBase::AgentRoleTargetMap> GenerationTaskBase::getAgentRoleTargetMap() const
{
    std::shared_ptr<AgentRoleTargetMap> map;
    _agentRoleTargetMapGuard.get(map);
    return map;
}

void GenerationTaskBase::fillAgentRoleTargetInfo(proto::GenerationInfo* generationInfo)
{
    auto roleTargetMap = getAgentRoleTargetMap();
    if (!roleTargetMap) {
        return;
    }
    for (auto& item : *roleTargetMap) {
        auto agentNodePlan = generationInfo->mutable_agentnodeplans()->Add();
        agentNodePlan->set_agentrolename(item.first);
        agentNodePlan->set_agentidentifier(item.second.first);
        for (const auto& roleName : item.second.second) {
            // attention: agent role plan will cover target roles which maybe ignored for blacklist or package mismatch
            agentNodePlan->add_targetrolename(roleName);
        }
    }
}

std::string GenerationTaskBase::getConfigPath() const
{
    autil::ScopedLock lock(_configMutex);
    return _configPath;
}

void GenerationTaskBase::setConfigPath(const std::string& path)
{
    autil::ScopedLock lock(_configMutex);
    _configPath = path;
}

bool GenerationTaskBase::loadDataDescriptions(const string& configPath, DataDescriptions& dataDescriptions)
{
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configPath);
    bool exist = false;
    if (!resourceReader->getDataTableConfigWithJsonPath(_buildId.datatable(), "data_descriptions",
                                                        dataDescriptions.toVector(), exist)) {
        BS_LOG(WARN, "failed to get data_descriptions in config[%s] generation[%s]", configPath.c_str(),
               _buildId.ShortDebugString().c_str());
        return false;
    }
    if (!exist) {
        BS_LOG(WARN, "data_descriptions not exist in config[%s] generation[%s]", configPath.c_str(),
               _buildId.ShortDebugString().c_str());
        return false;
    }
    // check dataDescriptions
    if (dataDescriptions.empty() || !dataDescriptions.validate()) {
        BS_LOG(ERROR, "generation [%s] validate dataDescriptions failed, [%s]", _buildId.ShortDebugString().c_str(),
               ToJsonString(dataDescriptions.toVector()).c_str());
        return false;
    }
    return true;
}

std::string GenerationTaskBase::getFatalErrorString() const { return _fatalErrorChecker.toString(); }

}} // namespace build_service::admin
