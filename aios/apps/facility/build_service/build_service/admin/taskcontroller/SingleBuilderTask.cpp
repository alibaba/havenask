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
#include "build_service/admin/taskcontroller/SingleBuilderTask.h"

#include <assert.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "beeper/beeper.h"
#include "build_service/admin/AlterFieldCKPAccessor.h"
#include "build_service/admin/CheckpointCreator.h"
#include "build_service/admin/CheckpointTools.h"
#include "build_service/admin/DefaultBrokerTopicCreator.h"
#include "build_service/admin/ProcessorCheckpointAccessor.h"
#include "build_service/admin/SlowNodeDetector.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/BrokerTopicAccessor.h"
#include "build_service/common/BuilderCheckpointAccessor.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/common/IndexCheckpointFormatter.h"
#include "build_service/common/Locator.h"
#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/RoleNameGenerator.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/proto/WorkerNodeCreator.h"
#include "build_service/util/DataSourceHelper.h"
#include "build_service/util/ErrorLogCollector.h"
#include "indexlib/base/Progress.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_roll_back_util.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::common;

using build_service::admin::ProcessorCheckpointAccessor;
using build_service::admin::ProcessorCheckpointAccessorPtr;
using build_service::common::CheckpointAccessorPtr;
using build_service::common::IndexCheckpointFormatter;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::partition;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, SingleBuilderTask);

const int64_t SingleBuilderTask::DEFAULT_STOP_TIMESTAMP = numeric_limits<int64_t>::max();
const int64_t SingleBuilderTask::DEFAULT_VERSION_TIMESTAMP = numeric_limits<int64_t>::max();

SingleBuilderTask::SingleBuilderTask(const BuildId& buildId, const string& clusterName, const string& taskId,
                                     const TaskResourceManagerPtr& resMgr)
    : AdminTaskBase(buildId, resMgr)
    , _buildStep(BUILD_STEP_FULL)
    , _taskId(taskId)
    , _clusterName(clusterName)
    , _partitionCount(0)
    , _buildParallelNum(1)
    , _startTimestamp(0)
    , _stopTimestamp(DEFAULT_STOP_TIMESTAMP)
    , _versionTimestamp(DEFAULT_VERSION_TIMESTAMP)
    , _workerProtocolVersion(UNKNOWN_WORKER_PROTOCOL_VERSION)
    , _workerPathVersion(0)
    , _schemaVersion(config::INVALID_SCHEMAVERSION)
    , _disableIncParallelInstanceDir(false)
    , _incBuildParallelConfigUpdated(false)
    , _processorCheckpoint(-1)
    , _processorDataSourceIdx(0)
    , _builderStartTimestamp(0)
    , _builderStopTimestamp(0)
    , _minLocatorTimestamp(-1)
    , _lastMinLocatorTsValue(-1)
    , _lastMinLocatorSystemTs(-1)
    , _consumeRate(0)
    , _batchMode(false)
    , _batchMask(-1)
    , _useRandomInitialWorkerPathVersion(false)
    , _versionTimestampAlignSrc(false)
{
    _indexCkpAccessor = CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
    _builderCkpAccessor = CheckpointCreator::createBuilderCheckpointAccessor(_resourceManager);
}

SingleBuilderTask::~SingleBuilderTask() { deregistBrokerTopic(); }

bool SingleBuilderTask::init(proto::BuildStep buildStep)
{
    _buildStep = buildStep;
    return startFromCheckpoint(_buildStep, _schemaVersion);
}

bool SingleBuilderTask::start(const KeyValueMap& kvMap)
{
    _builderStartTimestamp = TimeUtility::currentTimeInSeconds();
    _builderStopTimestamp = 0;
    _minLocatorTimestamp = -1;
    _lastMinLocatorTsValue = -1;
    _lastMinLocatorSystemTs = -1;
    _consumeRate = 0;
    _processorCheckpoint = -1;
    _processorDataSourceIdx = 0;

    assert(_beeperTags);
    if (!prepareBuilderDataDescription(kvMap, _builderDataDesc)) {
        return false;
    }

    int64_t schemaId = config::INVALID_SCHEMAVERSION;
    auto iter = kvMap.find("schemaId");
    if (iter != kvMap.end()) {
        if (!StringUtil::numberFromString(iter->second, schemaId)) {
            BS_LOG(ERROR, "invalid schema id[%s]", iter->second.c_str());
            BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                                 "start builder failed: invalid schema id[%s]", iter->second.c_str());
            return false;
        }
        if (schemaId < _schemaVersion) {
            BS_LOG(ERROR, "invalid schema id[%s], last schema id[%ld] ", iter->second.c_str(), _schemaVersion);
            BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                                 "start builder failed: invalid schema id[%s], last schema id[%ld] ",
                                 iter->second.c_str(), _schemaVersion);
            return false;
        }
    }

    _topicClusterName = getTopicClusterName(kvMap, _builderDataDesc);
    if (!startFromCheckpoint(_buildStep, schemaId)) {
        BS_LOG(ERROR, "start builder[%s] failed.", _clusterName.c_str());
        return false;
    }
    // parse start timestamp
    iter = kvMap.find("startTimestamp");
    if (iter != kvMap.end()) {
        int64_t startTs = 0;
        if (!StringUtil::fromString(iter->second, startTs)) {
            BS_LOG(INFO, "invalid startTimestamp [%s] from kvMap", iter->second.c_str());
            BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                                 "start builder failed: invalid startTimestamp [%s] from kvMap", iter->second.c_str());
            return false;
        }
        BS_LOG(INFO, "set up startTimestamp [%ld] from kvMap", startTs);
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start builder from udf startTimestamp [%ld] from kvMap", startTs);
        _startTimestamp = startTs;
    }
    iter = kvMap.find("buildParallelNum");
    if (iter != kvMap.end()) {
        uint32_t parallelNum = 1;
        if (!StringUtil::fromString(iter->second, parallelNum) || parallelNum == 0) {
            BS_LOG(INFO, "invalid buildParallelNum [%s] from kvMap", iter->second.c_str());
            BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                                 "start builder failed: invalid startTimestamp [%s] from kvMap", iter->second.c_str());
            return false;
        }
        BS_LOG(INFO, "set up buildParallelNum [%u] from kvMap", parallelNum);
        _buildParallelNum = parallelNum;
    }

    iter = kvMap.find("batchMask");
    if (iter != kvMap.end()) {
        int32_t batchMask = -1;
        if (!StringUtil::fromString(iter->second, batchMask)) {
            BS_LOG(INFO, "invalid batchMask [%s] from kvMap, ignore", iter->second.c_str());
        }
        _batchMask = batchMask;
        BS_LOG(INFO, "create builder with batch mask[%d] for [%s]", _batchMask,
               ProtoUtil::buildIdToStr(_buildId).c_str());
    }

    if (!getStopTimestamp(kvMap, _builderDataDesc, _stopTimestamp)) {
        return false;
    }
    BS_LOG(INFO, "stopTimestamp is [%ld]", _stopTimestamp);
    if (_stopTimestamp != DEFAULT_STOP_TIMESTAMP && _stopTimestamp < _startTimestamp) {
        BS_LOG(ERROR, "stopTimestamp [%ld] < startTimestamp [%ld]", _stopTimestamp, _startTimestamp);
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start builder failed: stopTimestamp [%ld] < startTimestamp [%ld]", _stopTimestamp,
                             _startTimestamp);
        return false;
    }
    iter = kvMap.find("useRandomInitialPathVersion");
    if (iter != kvMap.end() && iter->second == string("true")) {
        BS_LOG(ERROR, "init workerpath version with random number in generation[%s]",
               _buildId.ShortDebugString().c_str());
        _useRandomInitialWorkerPathVersion = true;
    }
    string topicName = getTopicName();

    BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                         "start builder success: startTimestamp [%ld], schemaId[%ld], swiftTopic[%s]", _startTimestamp,
                         _schemaVersion, topicName.c_str());

    _workerPathVersion = generateInitialWorkerPathVersion();
    BS_LOG(INFO, "workerPathVersion of generation[%s] is inited with [%d]", _buildId.ShortDebugString().c_str(),
           _workerPathVersion);
    _taskStatus = TASK_NORMAL;
    string versionTimestampAlignSrc = getValueFromKeyValueMap(kvMap, "versionTsAlignSrc");
    if (versionTimestampAlignSrc == string("true")) {
        _versionTimestampAlignSrc = true;
    }

    return true;
}

int32_t SingleBuilderTask::generateInitialWorkerPathVersion() const
{
    if (!_useRandomInitialWorkerPathVersion) {
        return 0;
    }
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937 random(seed);
    return (random() & 0xfffff) | 0x100000;
}

void SingleBuilderTask::updateWorkerPathVersion()
{
    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    if (_buildStep == BUILD_STEP_FULL) {
        return;
    }
    string id = BS_CKP_ID_BUILDER_PATH_VERSION_PREFIX + _clusterName;
    string checkpointName;
    string checkpointValue;
    if (!checkpointAccessor->getLatestCheckpoint(id, checkpointName, checkpointValue)) {
        _workerPathVersion = getNextWorkerPathVersion(_workerPathVersion);
        checkpointAccessor->addCheckpoint(id, BS_CKP_NAME_WORKER_PATH_VERSION,
                                          StringUtil::toString(_workerPathVersion));
        return;
    }

    _workerPathVersion = getNextWorkerPathVersion(StringUtil::numberFromString<int32_t>(checkpointValue));
    if (!checkpointAccessor->updateCheckpoint(id, checkpointName, StringUtil::toString(_workerPathVersion))) {
        BS_LOG(ERROR, "update worker path version for %s failed.", _clusterName.c_str());
        return;
    }
}

bool SingleBuilderTask::startFromCheckpoint(proto::BuildStep buildStep, int64_t schemaId)
{
    _stopTimestamp = DEFAULT_STOP_TIMESTAMP;
    _versionTimestamp = DEFAULT_VERSION_TIMESTAMP;
    config::ConfigReaderAccessorPtr configAccessor;
    _resourceManager->getResource(configAccessor);

    config::ResourceReaderPtr resourceReader;
    string checkpointName;
    proto::CheckpointInfo checkpoint;
    proto::BuilderCheckpoint builderCkp;

    bool hasBuildCkp = _builderCkpAccessor->getLatestCheckpoint(_clusterName, builderCkp);
    bool hasIndexCkp = _indexCkpAccessor->getLatestIndexCheckpoint(_clusterName, checkpoint, checkpointName);
    if (hasIndexCkp && hasBuildCkp) {
        int64_t tmpSchemaId;
        if (builderCkp.has_versionid() && builderCkp.versionid() > checkpoint.versionid()) {
            _startTimestamp = builderCkp.versiontimestamp();
            tmpSchemaId = builderCkp.schemaversion();
        } else {
            _startTimestamp = checkpoint.versiontimestamp();
            tmpSchemaId = checkpoint.schemaid();
        }
        resourceReader = configAccessor->getConfig(_clusterName, tmpSchemaId);
    } else if (hasIndexCkp && !hasBuildCkp) {
        int64_t tmpSchemaId;
        _startTimestamp = checkpoint.versiontimestamp();
        tmpSchemaId = checkpoint.schemaid();
        resourceReader = configAccessor->getConfig(_clusterName, tmpSchemaId);
    } else if (hasBuildCkp && !hasIndexCkp) {
        BS_LOG(ERROR, "inconsistent state, hasBuildCkp[%d] hasIndexCkp[%d]", hasBuildCkp, hasIndexCkp);
        return false;
    } else {
        resourceReader = configAccessor->getLatestConfig();
    }

    // if specified schemaId(in alterField scene), use it as the final config
    if (schemaId != config::INVALID_SCHEMAVERSION) {
        resourceReader = configAccessor->getConfig(_clusterName, schemaId);
    }
    _configPath = resourceReader->getOriginalConfigPath();

    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.builder_rule_config.batch_mode",
                                                      _batchMode)) {
        BS_LOG(ERROR,
               "parse cluster_config.builder_rule_config.batch_mode"
               " for [%s] failed",
               _clusterName.c_str());
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "parse cluster_config.builder_rule_config.batch_mode failed",
                      *_beeperTags);
        return false;
    }
    registBrokerTopic();
    return loadFromConfig(resourceReader);
}

bool SingleBuilderTask::getTopicId(string& topicId)
{
    auto reader = getConfigReader();
    SwiftAdminFacade facade;
    if (!facade.init(_buildId, reader, _topicClusterName)) {
        return false;
    }
    string topicConfigName = _buildStep == proto::BUILD_STEP_FULL ? BS_TOPIC_FULL : BS_TOPIC_INC;
    topicId = facade.getTopicId(topicConfigName, "");
    return true;
}

string SingleBuilderTask::getTopicName() const
{
    string topicConfigName = _buildStep == proto::BUILD_STEP_FULL ? BS_TOPIC_FULL : BS_TOPIC_INC;
    BrokerTopicAccessorPtr topicAccessor;
    _resourceManager->getResource(topicAccessor);
    auto reader = getConfigReader();
    return topicAccessor->getTopicName(reader, _topicClusterName, topicConfigName, "");
}

void SingleBuilderTask::registBrokerTopic()
{
    if (_taskStatus == TASK_STOPPED || _taskStatus == TASK_FINISHED) {
        return;
    }
    if (_topicClusterName.empty()) {
        return;
    }
    if (_brokerTopicKeeper) {
        return;
    }
    auto reader = getConfigReader();
    _brokerTopicKeeper = DefaultBrokerTopicCreator::applyDefaultSwiftResource(_taskId, _topicClusterName, _buildStep,
                                                                              reader, _resourceManager);
}

void SingleBuilderTask::deregistBrokerTopic() { _brokerTopicKeeper.reset(); }

void SingleBuilderTask::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("build_step", _buildStep);
    json.Jsonize("config_path", _configPath);
    json.Jsonize("partition_count", _partitionCount);
    json.Jsonize("build_parallel_num", _buildParallelNum);
    json.Jsonize("start_timestamp", _startTimestamp);
    json.Jsonize("stop_timestamp", _stopTimestamp);
    json.Jsonize("version_timestamp", _versionTimestamp, _stopTimestamp); // for legacy
    json.Jsonize("slow_node_detect", _slowNodeDetect, false);
    json.Jsonize("nodes_start_timestamp", _nodesStartTimestamp, -1L);
    json.Jsonize("worker_protocol_version", _workerProtocolVersion, _workerProtocolVersion);
    json.Jsonize("has_create_nodes", _hasCreateNodes, _hasCreateNodes);
    json.Jsonize("worker_path_version", _workerPathVersion, _workerPathVersion);
    json.Jsonize("schema_version", _schemaVersion, _schemaVersion);
    json.Jsonize("disable_inc_parallel_instance_path", _disableIncParallelInstanceDir, _disableIncParallelInstanceDir);
    json.Jsonize("inc_build_parallel_config_updated", _incBuildParallelConfigUpdated, _incBuildParallelConfigUpdated);
    json.Jsonize("processor_checkpoint", _processorCheckpoint, _processorCheckpoint);
    json.Jsonize("processor_datasource_idx", _processorDataSourceIdx, _processorDataSourceIdx);
    json.Jsonize("builder_start_timestamp", _builderStartTimestamp, _builderStartTimestamp);
    json.Jsonize("builder_stop_timestamp", _builderStopTimestamp, _builderStopTimestamp);
    json.Jsonize("batch_mode", _batchMode, _batchMode);
    json.Jsonize("topic_cluster_name", _topicClusterName, _topicClusterName);
    json.Jsonize("data_description", _builderDataDesc, _builderDataDesc);
    json.Jsonize("task_status", _taskStatus, _taskStatus);
    json.Jsonize("op_ids", _opIds, _opIds);
    json.Jsonize(BATCH_MASK, _batchMask, _batchMask);
    json.Jsonize("use_random_initial_path_version", _useRandomInitialWorkerPathVersion,
                 _useRandomInitialWorkerPathVersion);
    json.Jsonize("version_timestamp_align_src", _versionTimestampAlignSrc, _versionTimestampAlignSrc);
    if (json.GetMode() == FROM_JSON) {
        registBrokerTopic();
    }
}

string SingleBuilderTask::getTaskPhaseIdentifier() const { return string("builder"); }

void SingleBuilderTask::doSupplementLableInfo(KeyValueMap& info) const
{
    info["partitionCount"] = StringUtil::toString(_partitionCount);
    info["parallelNum"] = StringUtil::toString(_buildParallelNum);
    info["schemaVersion"] = StringUtil::toString(_schemaVersion);
    info["processorCheckpoint"] = AdminTaskBase::getDateFormatString(_processorCheckpoint);
    info["buildDataRange"] = string("from:") + AdminTaskBase::getDateFormatString(_startTimestamp) +
                             ",to:" + AdminTaskBase::getDateFormatString(_stopTimestamp);
    info["topicClusterName"] = _topicClusterName;
    info["batchMode"] = _batchMode ? "true" : "false";
    info["topicName"] = getTopicName();
    doSupplementLableInfoForLocatorInfo(info);
    if (_fatalErrorDiscoverer) {
        _fatalErrorDiscoverer->supplementFatalErrorInfo(info);
    }
}

void SingleBuilderTask::doSupplementLableInfoForLocatorInfo(KeyValueMap& info) const
{
    if (_minLocatorTimestamp <= 0 || _minLocatorTimestamp <= _startTimestamp) {
        return;
    }

    info["minBuilderLocator"] = AdminTaskBase::getDateFormatString(_minLocatorTimestamp);
    int64_t gapTs = (TimeUtility::currentTime() - _minLocatorTimestamp) / 1000000;
    info["incEnd2endLatency"] = StringUtil::toString(gapTs / 60) + "m " + StringUtil::toString(gapTs % 60) + "s";
    info["consumeRate"] = StringUtil::toString(_consumeRate);

    int64_t targetTs = (_stopTimestamp != DEFAULT_STOP_TIMESTAMP) ? _stopTimestamp : TimeUtility::currentTime();
    int64_t expectRunTsInSec = 0;
    if (_consumeRate > 0) {
        expectRunTsInSec = (int64_t)((targetTs - _minLocatorTimestamp) / _consumeRate / 1000000);
    } else {
        expectRunTsInSec = (TimeUtility::currentTimeInSeconds() - _builderStartTimestamp) *
                           (targetTs - _minLocatorTimestamp) / (_minLocatorTimestamp - _startTimestamp);
    }

    if (expectRunTsInSec > 0) {
        info["estimateRemainTime"] =
            StringUtil::toString(expectRunTsInSec / 60) + "m " + StringUtil::toString(expectRunTsInSec % 60) + "s";
    }
}

bool SingleBuilderTask::loadFromConfig(const config::ResourceReaderPtr& resourceReader)
{
    BuildRuleConfig buildRuleConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.builder_rule_config",
                                                      buildRuleConfig)) {
        string errorMsg = "parse cluster_config.builder_rule_config for [" + _clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _configPath = resourceReader->getOriginalConfigPath();

    indexlib::config::IndexPartitionSchemaPtr schema = resourceReader->getSchema(_clusterName);
    if (!schema) {
        string errorMsg = "fail to get Schema for cluster[" + _clusterName + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _schemaVersion = schema->GetSchemaVersionId();
    uint32_t maxOpId = schema->GetModifyOperationCount();
    if (!initOpIds(maxOpId)) {
        return false;
    }
    _partitionCount = buildRuleConfig.partitionCount;
    if (_buildStep == BUILD_STEP_FULL) {
        _buildParallelNum = buildRuleConfig.buildParallelNum;
    } else {
        _buildParallelNum = buildRuleConfig.incBuildParallelNum;
    }
    _disableIncParallelInstanceDir = buildRuleConfig.disableIncParallelInstanceDir;
    return initSlowNodeDetect(resourceReader, _clusterName);
}

bool SingleBuilderTask::initOpIds(uint32_t maxOpId)
{
    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    vector<schema_opid_t> ongoingOpIds;
    if (!AlterFieldCKPAccessor::getOngoingOpIds(checkpointAccessor, _clusterName, maxOpId, ongoingOpIds)) {
        return false;
    }
    set<schema_opid_t> allOpIds(ongoingOpIds.begin(), ongoingOpIds.end());
    vector<schema_opid_t> disableOpIds;
    if (!CheckpointTools::getDisableOpIds(checkpointAccessor, _clusterName, disableOpIds)) {
        return false;
    }
    for (schema_opid_t opId : disableOpIds) {
        if (allOpIds.find(opId) == allOpIds.end() && opId <= maxOpId) {
            ongoingOpIds.push_back(opId);
        }
    }

    if (getStep() == BUILD_STEP_FULL && ongoingOpIds.size() > 0) {
        BS_LOG(INFO, "clear full build ongoing opsids in schema");
        for (size_t i = 0; i < ongoingOpIds.size(); i++) {
            AlterFieldCKPAccessor::updateCheckpoint(_clusterName, ongoingOpIds[i], checkpointAccessor);
        }
        ongoingOpIds.clear();
    }

    _opIds = StringUtil::toString(ongoingOpIds, ",");
    BS_LOG(INFO, "init opid[%s] for builder[%s]", _opIds.c_str(), _clusterName.c_str());
    return true;
}

void SingleBuilderTask::waitSuspended(WorkerNodes& workerNodes)
{
    if (_taskStatus == TASK_SUSPENDED) {
        return;
    }
    BuilderNodes builderNodes;
    builderNodes.reserve(workerNodes.size());
    for (auto& workerNode : workerNodes) {
        builderNodes.push_back(DYNAMIC_POINTER_CAST(BuilderNode, workerNode));
    }
    if (doWaitSuspended(builderNodes)) {
        _taskStatus = TASK_SUSPENDED;
    }
}

bool SingleBuilderTask::determineBuilderCountPerPartition(size_t currentBuilderCount, int32_t currentWorkerVersion,
                                                          int32_t& builderCountPerPartition)
{
    if (_buildStep == BUILD_STEP_FULL) {
        builderCountPerPartition = _buildParallelNum;
        _workerProtocolVersion = currentWorkerVersion;
        return false;
    }

    bool resetBuilders = false;
    if (currentWorkerVersion != _workerProtocolVersion) {
        resetBuilders = true;
    }
    if (_disableIncParallelInstanceDir) {
        builderCountPerPartition = 1;
    } else {
        builderCountPerPartition = _buildParallelNum;
    }

    if (_incBuildParallelConfigUpdated) {
        resetBuilders = true;
        _incBuildParallelConfigUpdated = false;
    }
    _workerProtocolVersion = currentWorkerVersion;
    return resetBuilders;
}

bool SingleBuilderTask::run(WorkerNodes& workerNodes)
{
    if (_stopTimestamp <= _startTimestamp) {
        BS_LOG(INFO,
               "builder [%s] stoptime [%ld] is "
               "less than starttime[%ld], auto finish",
               _clusterName.c_str(), _stopTimestamp, _startTimestamp);
        workerNodes.clear();
        deregistBrokerTopic();
        _taskStatus = TASK_FINISHED;
        return true;
    }
    registBrokerTopic();
    BuilderNodes builderNodes;
    builderNodes.reserve(workerNodes.size());
    for (auto& workerNode : workerNodes) {
        builderNodes.push_back(DYNAMIC_POINTER_CAST(BuilderNode, workerNode));
    }
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    int32_t workerProtocolVersion = readerAccessor->getLatestConfig()->getWorkerProtocolVersion();
    bool ret = run(workerProtocolVersion, builderNodes);
    workerNodes.clear();
    workerNodes.reserve(builderNodes.size());
    for (auto& builderNode : builderNodes) {
        workerNodes.push_back(builderNode);
    }
    if (ret) {
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "builder has been finished", *_beeperTags);
        _taskStatus = TASK_FINISHED;
        if (_batchMask != -1) {
            string msg = "batch mode builder finish batchMask " + StringUtil::toString(_batchMask);
            BEEPER_REPORT(BATCHMODE_INFO_COLLECTOR_NAME, msg, *_beeperTags);
        }
    }
    return ret;
}

bool SingleBuilderTask::run(int32_t workerProtocolVersion, BuilderNodes& activeNodes)
{
    int builderCountPerPartition;
    bool needResetBuilders =
        determineBuilderCountPerPartition(activeNodes.size(), workerProtocolVersion, builderCountPerPartition);

    if (needResetBuilders) {
        /*
            no need to suspend all builder before clear worker nodes
            because new builder would work on diffrent path
        */
        activeNodes.clear();
        clearBackupInfo();
        updateWorkerPathVersion();
        int32_t actualWorkerPathVersion = getWorkerPathVersion();
        BS_LOG(INFO,
               "generation[%s] cluster[%s], _workerPathVersion updated to [%d], "
               "actual workerPathVerion is [%d]",
               _buildId.ShortDebugString().c_str(), _clusterName.c_str(), _workerPathVersion, actualWorkerPathVersion);
    }

    if (activeNodes.empty()) {
        _nodeStatusManager->Update(activeNodes);
        _slowNodeDetector.reset();
        if (_nodesStartTimestamp == -1) {
            _nodesStartTimestamp = autil::TimeUtility::currentTimeInMicroSeconds();
        }
        activeNodes = WorkerNodeCreator<ROLE_BUILDER>::createNodes(_partitionCount, builderCountPerPartition,
                                                                   _buildStep, _buildId, _clusterName);
        recoverFromBackInfo(activeNodes);
        _hasCreateNodes = true;
        generateAndSetTarget(activeNodes);
        return false;
    }
    if (!_fatalErrorDiscoverer) {
        _fatalErrorDiscoverer.reset(new FatalErrorDiscoverer());
    }
    _fatalErrorDiscoverer->collectNodeFatalErrors(activeNodes);

    bool isFinished = checkAndSetFinished(_nodeStatusManager, activeNodes,
                                          _slowNodeDetect ? &(_slowNodeDetector->getMetric()) : nullptr);
    _nodeStatusManager->Update(activeNodes);
    detectSlowNodes<BuilderNodes>(activeNodes, _clusterName);
    updateMinLocatorAndConsumeRate(_nodeStatusManager->GetMinLocator());
    saveBackupInfo();
    if (isFinished) {
        /*
            wait all role to be suspend or finished
            If a role belongs to a finished roleGroup that has another finished role in the same role group is not
           finished, it should receive a suspend command to make sure this role is drastically stopped, otherwise it may
           affect the build processes in the next phase.
        */
        if (doWaitSuspended(activeNodes)) {
            activeNodes.clear();
            _nodeStatusManager->Update(activeNodes);
            endBuild();
            return true;
        }
        BS_INTERVAL_LOG(10, INFO, "wait all role to be suspended to enter next build phase...");
    } else {
        generateAndSetTarget(activeNodes);
    }
    return false;
}

void SingleBuilderTask::updateMinLocatorAndConsumeRate(int64_t minLocatorTs)
{
    _minLocatorTimestamp = minLocatorTs;
    if (_minLocatorTimestamp > 0) {
        int64_t currentTime = TimeUtility::currentTime();
        if (_lastMinLocatorSystemTs < 0) {
            _lastMinLocatorTsValue = _minLocatorTimestamp;
            _lastMinLocatorSystemTs = currentTime;
        } else if (currentTime - _lastMinLocatorSystemTs > 30 * 1000000) {
            double gapTsInSecond = (double)(currentTime - _lastMinLocatorSystemTs);
            _consumeRate = (_minLocatorTimestamp - _lastMinLocatorTsValue) / gapTsInSecond;
            if (_consumeRate < 0) {
                _consumeRate = 0;
            }
            _lastMinLocatorTsValue = _minLocatorTimestamp;
            _lastMinLocatorSystemTs = currentTime;
        }
    }
}

bool SingleBuilderTask::getBuilderCheckpoint(proto::BuilderCheckpoint& buildCheckpoint) const
{
    if (_stopTimestamp <= _startTimestamp) {
        return false;
    }
    buildCheckpoint.set_clustername(_clusterName);
    buildCheckpoint.set_versiontimestamp(_versionTimestamp);
    buildCheckpoint.set_processorcheckpoint(_processorCheckpoint);
    buildCheckpoint.set_processordatasourceidx(_processorDataSourceIdx);
    buildCheckpoint.set_builderstarttime(_builderStartTimestamp);
    buildCheckpoint.set_builderstoptime(_builderStopTimestamp);
    buildCheckpoint.set_schemaversion(_schemaVersion);
    buildCheckpoint.set_workerpathversion(_workerPathVersion);
    return true;
}

void SingleBuilderTask::endBuild()
{
    clearBackupInfo();
    deregistBrokerTopic();
    _nodesStartTimestamp = -1;
    _minLocatorTimestamp = -1;
    _lastMinLocatorTsValue = -1;
    _lastMinLocatorSystemTs = -1;
    _consumeRate = 0;
    _slowNodeDetector.reset();
    _builderStopTimestamp = TimeUtility::currentTimeInSeconds();
}

void SingleBuilderTask::notifyStopped()
{
    _taskStatus = TASK_STOPPED;
    deregistBrokerTopic();
}

bool SingleBuilderTask::updateConfig()
{
    ConfigReaderAccessorPtr configReaderAccessor;
    _resourceManager->getResource(configReaderAccessor);
    ResourceReaderPtr resourceReader = configReaderAccessor->getConfig(_clusterName, _schemaVersion);
    if (_configPath != resourceReader->getOriginalConfigPath()) {
        return updateConfig(resourceReader->getOriginalConfigPath());
    }
    return true;
}

bool SingleBuilderTask::updateConfig(const string& configPath)
{
    _configPath = configPath;
    ResourceReaderPtr resourceReader = getConfigReader();
    BuildRuleConfig buildRuleConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, "cluster_config.builder_rule_config",
                                                      buildRuleConfig)) {
        string errorMsg = "parse cluster_config.builder_rule_config for [" + _clusterName + "] failed";
        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    auto schema = resourceReader->getTabletSchema(_clusterName);
    if (!schema) {
        string errorMsg = "fail to get Schema for cluster[" + _clusterName + "]";
        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _schemaVersion = schema->GetSchemaId();

    if (_buildStep == BUILD_STEP_INC) {
        if (buildRuleConfig.incBuildParallelNum != _buildParallelNum) {
            _incBuildParallelConfigUpdated = true;
        }
        if (buildRuleConfig.disableIncParallelInstanceDir != _disableIncParallelInstanceDir) {
            _incBuildParallelConfigUpdated = true;
        }
        _buildParallelNum = buildRuleConfig.incBuildParallelNum;
    }
    _disableIncParallelInstanceDir = buildRuleConfig.disableIncParallelInstanceDir;

    _fatalErrorDiscoverer.reset();
    if (!initSlowNodeDetect(resourceReader, _clusterName)) {
        string errorMsg = "init slowNode detector failed for cluster " + _clusterName;
        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

void SingleBuilderTask::stopBuild(int64_t finishTimeInSeconds, uint64_t readAfterFinishTsInSeconds)
{
    _versionTimestamp = finishTimeInSeconds * 1000 * 1000;
    _stopTimestamp = (finishTimeInSeconds + (int64_t)readAfterFinishTsInSeconds) * 1000 * 1000;
}

void SingleBuilderTask::fillClusterInfo(SingleClusterInfo* clusterInfo, const BuilderNodes& nodes,
                                        const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    clusterInfo->set_partitioncount(_partitionCount);
    string stepStr = _buildStep == BUILD_STEP_FULL ? "full_" : "incremental_";
    if (_stopTimestamp == DEFAULT_STOP_TIMESTAMP) {
        stepStr += "building";
    } else {
        stepStr += "stopping";
    }
    clusterInfo->set_clusterstep(stepStr);
    BuilderInfo* builderInfo = clusterInfo->mutable_builderinfo();
    builderInfo->set_parallelnum(_buildParallelNum);
    fillPartitionInfos(clusterInfo->mutable_builderinfo(), nodes, roleCounterMap);
    if (_fatalErrorDiscoverer) {
        _fatalErrorDiscoverer->fillFatalErrors(clusterInfo->mutable_fatalerrors());
    }
    builderInfo->set_taskstatus(getTaskStatusString());
    if (_schemaVersion != config::INVALID_SCHEMAVERSION) {
        builderInfo->set_schemaversion(_schemaVersion);
    }
}

void SingleBuilderTask::generateAndSetTarget(BuilderNodes& nodes) const
{
    BuilderTarget target = generateTargetStatus();
    BuilderTarget suspendTarget = target;
    suspendTarget.set_suspendtask(true);
    for (BuilderNodes::iterator it = nodes.begin(); it != nodes.end(); it++) {
        const proto::PartitionId& pid = (*it)->getPartitionId();
        std::string roleGroupName = proto::RoleNameGenerator::generateRoleGroupName(pid);
        if (_nodeStatusManager->IsFinishedGroup(roleGroupName) && !(*it)->isFinished()) {
            /*
                 suspend the node that belong to the finished roleGroups
                 to prevent unexpected situation
            */
            (*it)->setTargetStatus(suspendTarget);
        } else {
            (*it)->setTargetStatus(target);
        }
    }
}

BuilderTarget SingleBuilderTask::generateTargetStatus() const
{
    BuilderTarget target;
    target.Clear();
    target.set_configpath(_configPath);
    target.set_starttimestamp(_startTimestamp);
    int32_t pathVersion = getWorkerPathVersion();
    target.set_workerpathversion(pathVersion);
    KeyValueMap kvMap;
    if (_stopTimestamp != DEFAULT_STOP_TIMESTAMP) {
        target.set_stoptimestamp(_stopTimestamp);
        if (_processorCheckpoint != -1 && _versionTimestampAlignSrc) {
            kvMap[VERSION_TIMESTAMP] = StringUtil::toString(_processorCheckpoint);
        } else {
            assert(_versionTimestamp != DEFAULT_VERSION_TIMESTAMP);
            kvMap[VERSION_TIMESTAMP] = StringUtil::toString(_versionTimestamp);
        }
    }

    if (needBuildFromDataSource(_builderDataDesc)) {
        kvMap[DATA_DESCRIPTION_KEY] = ToJsonString(_builderDataDesc, true);
    }
    if (!_topicClusterName.empty() && _topicClusterName != _clusterName) {
        kvMap[SHARED_TOPIC_CLUSTER_NAME] = _topicClusterName;
    }
    if (!_opIds.empty()) {
        kvMap[OPERATION_IDS] = _opIds;
    }
    if (_batchMask != -1) {
        kvMap[BATCH_MASK] = StringUtil::toString(_batchMask);
    }

    string schemaChangeStandard;
    if (_indexCkpAccessor->getSchemaChangedStandard(_clusterName, schemaChangeStandard)) {
        kvMap[SCHEMA_PATCH] = schemaChangeStandard;
    }

    if (!kvMap.empty()) {
        target.set_targetdescription(ToJsonString(kvMap, true));
    }

    set<versionid_t> versions =
        IndexCheckpointAccessor::getReservedVersions(_clusterName, _indexCkpAccessor, _builderCkpAccessor);
    string versionStr = StringUtil::toString(versions.begin(), versions.end());
    target.set_reservedversions(versionStr);

    return target;
}

proto::BuildStep SingleBuilderTask::getStep() const { return _buildStep; }

void SingleBuilderTask::clearFullWorkerZkNode(const std::string& generationDir) const
{
    doClearFullWorkerZkNode(generationDir, ROLE_BUILDER, _clusterName);
}

bool SingleBuilderTask::finish(const KeyValueMap& kvMap)
{
    int64_t stopBuildRequiredLatencyInSecond = -1, stopBuildTimeoutInSecond = -1;
    string latencyStr = getValueFromKeyValueMap(kvMap, "stop_build_required_latency_in_second", "-1");
    if (!StringUtil::numberFromString(latencyStr, stopBuildRequiredLatencyInSecond)) {
        BS_LOG(ERROR, "invalid stop build latency [%s]", latencyStr.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start builder failed: invalid stop build latency[%s]", latencyStr.c_str());
        return false;
    }
    string timeoutStr = getValueFromKeyValueMap(kvMap, "stop_build_timeout_in_second", "-1");
    if (!StringUtil::numberFromString(timeoutStr, stopBuildTimeoutInSecond)) {
        BS_LOG(ERROR, "invalid stop build timeout [%s]", timeoutStr.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start builder failed: invalid stop build timeout[%s]", timeoutStr.c_str());
        return false;
    }
    BS_LOG(INFO, "will use stop build latency [%ld]", stopBuildRequiredLatencyInSecond);
    int64_t stopTimeInterval = autil::EnvUtil::getEnv("builder_sync_stoptime_interval", int64_t(5));
    BS_LOG(INFO, "builder finishing...param[%s]", ToJsonString(kvMap).c_str());
    bool userDefineStopTs = false;
    int64_t finishTimeInSeconds = autil::TimeUtility::currentTimeInSeconds() + stopTimeInterval;
    uint64_t readAfterFinishTsInSeconds = 0;

    auto iter = kvMap.find("finishTimeInSecond");
    if (iter != kvMap.end()) {
        // "finishTimeInSecond" tells finishTs (version stopTS)
        // "readAfterFinishTsInSeconds" tells reader read time after finishedTs, default 0
        userDefineStopTs = true;
        if (!StringUtil::fromString(iter->second, finishTimeInSeconds)) {
            BS_LOG(ERROR, "invalid stopTimestampInSecond [%s] from kvMap", iter->second.c_str());
            return false;
        }

        string readAfterFinishTsInSecondsStr = getValueFromKeyValueMap(kvMap, "readAfterFinishTsInSeconds", "0");
        if (!StringUtil::fromString(readAfterFinishTsInSecondsStr, readAfterFinishTsInSeconds)) {
            BS_LOG(ERROR, "invaild readAfterFinishTsInSeconds [%s] from kvMap", readAfterFinishTsInSecondsStr.c_str());
            return false;
        }
    }
    if (needBuildFromDataSource(_builderDataDesc)) {
        BS_LOG(INFO, "generation[%s], cluster[%s] will finish building on ts: %ld", _buildId.ShortDebugString().c_str(),
               _clusterName.c_str(), finishTimeInSeconds);
        stopBuild(finishTimeInSeconds, readAfterFinishTsInSeconds);
        return true;
    }
    ProcessorCheckpointAccessorPtr processorCheckpoint;
    _resourceManager->getResource(processorCheckpoint);
    if (processorCheckpoint) {
        string topicId;
        if (!getTopicId(topicId)) {
            BS_LOG(INFO, "builder finish failed can not get topic id");
            return false;
        }
        Locator locator;
        if (processorCheckpoint->getCommitedCheckpoint(_clusterName, topicId, &locator)) {
            if (!userDefineStopTs && locator.GetOffset().first == _startTimestamp && _versionTimestampAlignSrc &&
                _buildStep == BUILD_STEP_INC) {
                BS_LOG(ERROR,
                       "finish [%s] builder fail, processor checkpoint not change, processorcheckpoint is [%ld], "
                       "startTs is [%ld]",
                       _buildId.ShortDebugString().c_str(), locator.GetOffset().first, _startTimestamp);
                return false;
            }
            auto processorCheckpoint = locator.GetOffset().first;
            if (!canStopBuild(processorCheckpoint, stopBuildRequiredLatencyInSecond, stopBuildTimeoutInSecond)) {
                return false;
            }
            _processorCheckpoint = processorCheckpoint;
            _processorDataSourceIdx = locator.GetSrc();
        }
    }
    if (_processorCheckpoint == -1 && _versionTimestampAlignSrc) {
        BS_LOG(ERROR, "finish [%s] builder fail, processor checkpoint is -1", _buildId.ShortDebugString().c_str());
        return false;
    }
    stopBuild(finishTimeInSeconds, readAfterFinishTsInSeconds);
    return true;
}

bool SingleBuilderTask::canStopBuild(int64_t processorCheckpoint, int64_t stopBuildRequiredLatencyInSecond,
                                     int64_t stopBuildTimeoutInSecond)
{
    if (stopBuildRequiredLatencyInSecond == -1) {
        return true;
    }
    int64_t currentTs = autil::TimeUtility::currentTimeInSeconds();
    if (processorCheckpoint >= (currentTs - stopBuildRequiredLatencyInSecond) * 1000 * 1000) {
        return true;
    }
    if (stopBuildTimeoutInSecond != -1 && currentTs - _builderStartTimestamp >= stopBuildTimeoutInSecond) {
        return true;
    }
    BS_LOG(DEBUG,
           "generation[%s], cluster[%s],  unable to stop build "
           "checkpoint [%ld] startTs [%ld], requiredLatency [%ld] timeout [%ld]",
           _buildId.ShortDebugString().c_str(), _clusterName.c_str(), processorCheckpoint, _builderStartTimestamp,
           stopBuildRequiredLatencyInSecond, stopBuildTimeoutInSecond);
    return false;
}

bool SingleBuilderTask::prepareBuilderDataDescription(const KeyValueMap& kvMap, DataDescription& ds)
{
    auto iter = kvMap.find(DATA_DESCRIPTION_KEY);
    if (iter == kvMap.end()) {
        BS_LOG(INFO, "no data description for builder.");
        ds = DataDescription();
        return true;
    }

    try {
        FromJsonString(ds, iter->second);
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "parse data Description [%s] fail", iter->second.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start builder failed: parse data Description [%s] fail", iter->second.c_str());
        return false;
    }
    if (!DataSourceHelper::isRealtime(ds)) {
        BS_LOG(ERROR,
               "invalid data description [%s],"
               "only support swift-like type",
               iter->second.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start builder failed: invalid data description [%s], only support swift-like type",
                             iter->second.c_str());
        return false;
    }

    iter = kvMap.find(SRC_SIGNATURE);
    if (iter != kvMap.end()) {
        ds[SRC_SIGNATURE] = iter->second;
    }
    BS_LOG(INFO, "data description for builder, ds [%s].", ToJsonString(ds, true).c_str());
    return true;
}

string SingleBuilderTask::getTopicClusterName(const KeyValueMap& kvMap, const DataDescription& ds)
{
    if (needBuildFromDataSource(ds)) {
        // need build from data source, no need broker topic
        return string("");
    }

    string topicClusterName = _clusterName;
    auto iter = kvMap.find(SHARED_TOPIC_CLUSTER_NAME);
    if (iter != kvMap.end()) {
        topicClusterName = iter->second;
    }
    return topicClusterName;
}

bool SingleBuilderTask::needBuildFromDataSource(const DataDescription& ds) const
{
    return DataSourceHelper::isRealtime(ds);
}

bool SingleBuilderTask::getStopTimestamp(const KeyValueMap& kvMap, const proto::DataDescription& ds, int64_t& stopTs)
{
    stopTs = DEFAULT_STOP_TIMESTAMP;
    auto iter = kvMap.find("stopTimestamp");
    if (iter != kvMap.end()) {
        if (!StringUtil::fromString(iter->second, stopTs)) {
            BS_LOG(INFO, "invalid stopTimestamp [%s] from kvMap", iter->second.c_str());
            BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                                 "start builder failed: invalid stopTimestamp [%s] from kvMap", iter->second.c_str());
            return false;
        }
    }
    iter = ds.find(SWIFT_STOP_TIMESTAMP);
    if (iter != ds.end()) {
        if (!StringUtil::fromString(iter->second, stopTs)) {
            BS_LOG(INFO, "invalid %s [%s] from builderDataSource [%s]", SWIFT_STOP_TIMESTAMP.c_str(),
                   iter->second.c_str(), ToJsonString(ds).c_str());
            BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                                 "start builder failed: invalid %s [%s] from builderDataSource [%s]",
                                 SWIFT_STOP_TIMESTAMP.c_str(), iter->second.c_str(), ToJsonString(ds).c_str());
            return false;
        }
    }
    return true;
}

string SingleBuilderTask::getClusterName() const { return _clusterName; }
proto::BuildStep SingleBuilderTask::getBuildStep() const { return _buildStep; }

bool SingleBuilderTask::doWaitSuspended(proto::BuilderNodes& builderNodes)
{
    bool allSuspended = true;
    for (const auto& builderNode : builderNodes) {
        if (builderNode->isSuspended() || builderNode->isFinished()) {
            continue;
        }
        if (isSuspended(builderNode)) {
            builderNode->setSuspended(true);
        } else {
            allSuspended = false;
            BuilderTarget suspendTarget;
            suspendTarget.set_suspendtask(true);
            builderNode->setTargetStatus(suspendTarget);
        }
    }
    return allSuspended;
}

}} // namespace build_service::admin
