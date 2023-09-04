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
#include "build_service/admin/taskcontroller/ProcessorTask.h"

#include "autil/HashUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/admin/DefaultBrokerTopicCreator.h"
#include "build_service/admin/GenerationTaskBase.h"
#include "build_service/admin/controlflow/ListParamParser.h"
#include "build_service/admin/taskcontroller/ProcessorNodesUpdater.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/PathDefine.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/config/ProcessorConfigReader.h"
#include "build_service/config/ProcessorConfigurator.h"
#include "build_service/proto/ProcessorTaskIdentifier.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/SwiftRootUpgrader.h"
#include "build_service/proto/WorkerNodeCreator.h"
#include "build_service/util/DataSourceHelper.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace build_service::proto;
using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::common;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ProcessorTask);

const int64_t ProcessorTask::DEFAULT_SWITCH_TIMESTAMP = numeric_limits<int64_t>::max();
const int64_t ProcessorTask::DEFAULT_SUSPEND_TIMESTAMP = numeric_limits<int64_t>::max();

const int64_t ProcessorTask::READER_SYNC_TIME_INTERVAL = 5;                          // seconds
const int64_t ProcessorTask::DEFAULT_SWIFT_SRC_FULL_BUILD_TIMEOUT = 3600 * 24 * 365; // 1 year

const std::string ProcessorTask::PROCESSOR_WRITER_VERSION = "processor_writer_version";

ProcessorTask::ProcessorTask(const BuildId& buildId, proto::BuildStep step, const TaskResourceManagerPtr& resMgr)
    : AdminTaskBase(buildId, resMgr)
    , _partitionCount(0)
    , _parallelNum(0)
    , _step(step)
    , _switchTimestamp(DEFAULT_SWITCH_TIMESTAMP)
    , _useCustomizeProcessorCount(false)
    , _id(0)
    , _fullBuildProcessLastSwiftSrc(true)
    , _hasIncDatasource(true)
    , _alreadyRun(false)
    , _beeperReportTs(0)
    , _reportCheckpointTs(0)
    , _batchMode(false)
    , _isLastStage(false)
    , _isTablet(false)
    , _safeWrite(false)
    , _needUpdateTopicVersionControl(false)
    , _runningPartitionCount(0)
    , _runningParallelNum(0)
    , _adaptiveScalingFixedMax(false)
{
    ProtoUtil::buildIdToBeeperTags(_buildId, *_beeperTags);
    _beeperTags->AddTag("buildStep", ProtoUtil::toStepString(_step));
    _adaptiveScalingFixedMax = autil::EnvUtil::getEnv("bs_processor_adaptive_scaling_fix_max_count", false);
}

ProcessorTask::~ProcessorTask() { deregistBrokerTopics(); }

bool ProcessorTask::init(const KeyValueMap& kvMap)
{
    std::string isTabletStr = getValueFromKeyValueMap(kvMap, "isTablet", "false");
    bool isTablet = false;
    if (isTabletStr == "true") {
        isTablet = true;
    }
    ProcessorParamParser parser;
    if (!parser.parse(kvMap, _resourceManager)) {
        return false;
    }

    if (!init(parser.getInput(), parser.getOutput(), parser.getClusterNames(), parser.getConfigInfo(), isTablet)) {
        BS_LOG(ERROR, "init processor task failed");
        return false;
    }
    return true;
}

/**
 * for batchMode, targetCheckpoint=batch beginTime, startDataDescriptionId=last data id
 **/
bool ProcessorTask::init(const ProcessorInput& input, const ProcessorOutput& output,
                         const std::vector<std::string>& clusterNames, const ProcessorConfigInfo& configInfo,
                         bool isTablet)
{
    _beeperTags->AddTag("clusters", StringUtil::toString(clusterNames, " "));
    _isTablet = isTablet;
    return doInit(input, output, clusterNames, configInfo, true);
}

bool ProcessorTask::doInit(const ProcessorInput& input, const ProcessorOutput& output,
                           const std::vector<std::string>& clusterNames, const ProcessorConfigInfo& configInfo,
                           bool changeTaskId)
{
    _input = input;
    _output = output;
    _configPath = configInfo.configPath;
    _preStage = configInfo.preStage;
    _stage = configInfo.stage;
    _isLastStage = configInfo.isLastStage;
    _configName = configInfo.configName;
    _clusterNames = clusterNames;
    sort(_clusterNames.begin(), _clusterNames.end());
    auto configReader = getConfigReader();
    if (!configReader) {
        BS_LOG(ERROR, "init processor failed, config path[%s] not exist", _configPath.c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "init processor failed: config path[%s] not exist", _configPath.c_str());
        return false;
    }

    if (!loadFromConfig(configReader)) {
        return false;
    }
    if (!getSyncedWriterVersionsIfEmpty()) {
        BS_LOG(ERROR, "inhert writer versions failed");
        return false;
    }

    if (changeTaskId) {
        createProcessorTaskId();
    }

    if (!registCheckpointInfo()) {
        BS_LOG(ERROR, "processor regist checkpoint info fail");
        return false;
    }

    common::Locator locator;
    if (!prepareStartLocator(locator)) {
        return false;
    }
    startFromLocator(locator);
    registBrokerTopics();
    _partitionCount = getPartitionCount(_processorRuleConfig);
    _parallelNum = getParallelNum(_processorRuleConfig);

    stringstream ss;
    ss << "start " << ProtoUtil::toStepString(_step) << " processor for[";
    for (size_t i = 0; i < _clusterNames.size(); i++) {
        if (i != 0) {
            ss << ",";
        }
        ss << _clusterNames[i] << ":" << _clusterToSchemaId[_clusterNames[i]] << ", taskId: " << getTaskIdentifier();
    }
    ss << "] success";
    BS_LOG(INFO, "%s", ss.str().c_str());
    BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, ss.str(), *_beeperTags);
    if (_batchMode) {
        BS_LOG(INFO, "Create processor with batch mask[%d] for [%s]", _input.batchMask,
               ProtoUtil::buildIdToStr(_buildId).c_str());
    }
    auto accessor = getProcessorCheckpointAccessor();
    return true;
}

bool ProcessorTask::isTablet() const { return _isTablet; }

bool ProcessorTask::prepareStartLocator(common::Locator& locator)
{
    if (!_input.startCheckpointName.empty()) {
        auto accessor = getProcessorCheckpointAccessor();
        if (accessor->getCheckpoint(_input.startCheckpointName, &locator)) {
            return true;
        }
        BS_LOG(ERROR, "no start Locator for processor checkpoint name [%s]", _input.startCheckpointName.c_str());
        return false;
    }
    getStartLocator(locator);
    if (_input.src != -1) {
        BS_LOG(INFO, "Locator use user-define source [%ld]", _input.src);
        locator.SetSrc(static_cast<uint32_t>(_input.src));
    }

    if (_input.offset != -1) {
        BS_LOG(INFO, "Locator use user-define checkpoint [%ld]", _input.offset);
        locator.SetOffset({_input.offset, 0});
    } else {
        _input.offset = 0;
        size_t locatorIdx = (size_t)locator.GetSrc();
        if (locatorIdx < _input.dataDescriptions.size() &&
            DataSourceHelper::isRealtime(_input.dataDescriptions[locatorIdx]) &&
            _processorRuleConfig.incProcessorStartTs >= 0) {
            locator.SetOffset({max(locator.GetOffset().first, _processorRuleConfig.incProcessorStartTs), 0});
        }
    }
    return true;
}

bool ProcessorTask::registCheckpointInfo()
{
    auto accessor = getProcessorCheckpointAccessor();
    for (auto& cluster : _clusterNames) {
        string topicId;
        getBrokerTopicId(cluster, topicId);
        if (!accessor->registCheckpointInfo(cluster, topicId, _preStage, _stage, _isLastStage)) {
            return false;
        }
    }
    return true;
}

void ProcessorTask::getStartLocator(common::Locator& locator)
{
    for (size_t i = 0; i < _clusterNames.size(); i++) {
        common::Locator tmpLocator;
        string clusterName = _clusterNames[i];
        string topicId;
        getBrokerTopicId(clusterName, topicId);
        int64_t schemaId = -1;
        getSchemaId(clusterName, schemaId);
        getStartLocator(clusterName, schemaId, topicId, tmpLocator);
        if (i == 0) {
            locator = tmpLocator;
            continue;
        }
        if (tmpLocator.GetSrc() > locator.GetSrc()) {
            continue;
        }
        if (tmpLocator.GetSrc() < locator.GetSrc()) {
            locator = tmpLocator;
            continue;
        }
        if (tmpLocator.GetOffset() < locator.GetOffset()) {
            BS_LOG(INFO, "Locator debug cluster:%s locator[%ld]:[%ld]", clusterName.c_str(),
                   tmpLocator.GetOffset().first, locator.GetOffset().first);
            locator = tmpLocator;
        }
    }
}

bool ProcessorTask::updateConfig()
{
    string config;
    if (!checkAllClusterConfigEqual(config)) {
        return false;
    }

    if (config == _configPath) {
        return true;
    }

    if (!checkUpdateConfigLegal(config)) {
        return false;
    }

    auto locator = getLocator();
    int64_t lastIncProcessorStartTs = _processorRuleConfig.incProcessorStartTs;
    ProcessorConfigInfo configInfo = getConfigInfo();
    configInfo.configPath = config;
    bool oldEnableSafeWrite = _safeWrite;
    uint32_t oldPartitionCount = _partitionCount;
    uint32_t oldParallelNum = _parallelNum;
    if (!doInit(getInput(), getOutput(), _clusterNames, configInfo, false)) {
        return false;
    }

    if (oldParallelNum != _parallelNum || oldPartitionCount != _partitionCount) {
        // TODO: remove after upgrade swift root done
        SwiftRootUpgrader upgrader;
        if (upgrader.needUpgrade(_buildId.generationid())) {
            string oldStr = ToJsonString(_input.dataDescriptions.toVector(), true);
            upgrader.upgrade(_input.dataDescriptions);
            string newStr = ToJsonString(_input.dataDescriptions.toVector(), true);
            if (oldStr != newStr) {
                BS_LOG(INFO, "dataDescriptions in processor input upgrade from [%s] to [%s]", oldStr.c_str(),
                       newStr.c_str());
            }
        }
    }

    if (_step == BUILD_STEP_INC && _hasIncDatasource && _processorRuleConfig.incProcessorStartTs >= 0 &&
        _processorRuleConfig.incProcessorStartTs != lastIncProcessorStartTs) {
        if (_input.src >= _input.dataDescriptions.size()) {
            BS_LOG(INFO, "force switch to last data source for update inc_processor_start_timestamp!");
            BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME,
                          "force switch to last data source for update inc_processor_start_timestamp!", *_beeperTags);
            locator.SetSrc(_input.dataDescriptions.size() - 1);
        }
        locator.SetOffset({_processorRuleConfig.incProcessorStartTs, 0});
        stringstream ss;
        ss << "update inc_processor_start_timestamp to [" << _processorRuleConfig.incProcessorStartTs
           << "], rewrite to locator [" << locator.GetSrc() << ":" << locator.GetOffset().first << "].";
        BS_LOG(INFO, "%s", ss.str().c_str());
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, ss.str(), *_beeperTags);
    }
    if (_safeWrite != oldEnableSafeWrite) {
        _needUpdateTopicVersionControl = true;
    }
    startFromLocator(locator);
    return true;
}

bool ProcessorTask::checkAllClusterConfigEqual(string& config)
{
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    for (size_t i = 0; i < _clusterNames.size(); i++) {
        int64_t schemaId = config::INVALID_SCHEMAVERSION;
        getSchemaId(_clusterNames[i], schemaId);
        string tmpConfig = readerAccessor->getConfig(_clusterNames[i], schemaId)->getOriginalConfigPath();
        if (i == 0) {
            config = tmpConfig;
            continue;
        }
        if (config != tmpConfig) {
            return false;
        }
    }
    return true;
}

void ProcessorTask::getStartLocator(std::string& clusterName, int64_t schemaId, const string& topicId,
                                    common::Locator& locator)
{
    auto accessor = getProcessorCheckpointAccessor();
    if (accessor->getCheckpoint(clusterName, topicId, &locator)) {
        return;
    }
    if (accessor->getCheckpointFromIndex(clusterName, schemaId, &locator)) {
        return;
    }

    if (accessor->getCheckpointFromIndex(clusterName, &locator)) {
        return;
    }

    locator.SetSrc(0);
    locator.SetOffset({-1, 0});
}

string ProcessorTask::getTaskPhaseIdentifier() const
{
    return string("processor_phase_") + autil::StringUtil::toString(_input.src);
}

void ProcessorTask::doSupplementLableInfo(KeyValueMap& info) const
{
    info["current_data_description_id"] = StringUtil::toString(_input.src);
    info["data_description_size"] = StringUtil::toString(_input.dataDescriptions.size());
    if (_input.src < _input.dataDescriptions.size()) {
        info["current_data_description"] = autil::legacy::ToJsonString(_input.dataDescriptions[_input.src], true);
    } else {
        info["current_data_description"] = "current_data_description_id [" + StringUtil::toString(_input.src) +
                                           "] has reached data_description_size [" +
                                           StringUtil::toString(_input.dataDescriptions.size()) + "]";
    }
    info["partitionCount"] = StringUtil::toString(_partitionCount);
    info["parallelNum"] = StringUtil::toString(_parallelNum);
    info["checkpoint"] = AdminTaskBase::getDateFormatString(_input.offset);
    info["processor_task_id"] = StringUtil::toString(_id);
    info["process_clusters"] = ListParamParser::toListString(_clusterNames);
    info["is_tablet"] = _isTablet ? "true" : "false";
    info["safe_write"] = _safeWrite ? "true" : "false";
    if (_fatalErrorDiscoverer) {
        _fatalErrorDiscoverer->supplementFatalErrorInfo(info);
    }
}

void ProcessorTask::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("config_path", _configPath);
    json.Jsonize("pre_stage", _preStage, _preStage);
    json.Jsonize("stage", _stage, _stage);
    json.Jsonize("last_stage", _isLastStage, _isLastStage);
    json.Jsonize("config_name", _configName, _configName);
    json.Jsonize("data_descriptions", _input.dataDescriptions.toVector());
    json.Jsonize("partition_count", _partitionCount);
    json.Jsonize("parallel_num", _parallelNum);
    json.Jsonize("running_partition_count", _runningPartitionCount, _runningPartitionCount);
    json.Jsonize("running_parallel_num", _runningParallelNum, _runningParallelNum);
    json.Jsonize("build_step", _step);
    json.Jsonize("current_data_description_id", _input.src);
    json.Jsonize("switch_timestamp", _switchTimestamp);
    json.Jsonize("checkpoint", _input.offset);
    json.Jsonize("use_customize_processor_count", _useCustomizeProcessorCount, false);
    json.Jsonize("slow_node_detect", _slowNodeDetect, false);
    json.Jsonize("nodes_start_timestamp", _nodesStartTimestamp, -1L);
    json.Jsonize("is_inc_processor_existed", _fullBuildProcessLastSwiftSrc, _fullBuildProcessLastSwiftSrc);
    json.Jsonize("has_inc_datasource", _hasIncDatasource, _hasIncDatasource);
    json.Jsonize("has_create_nodes", _hasCreateNodes, _hasCreateNodes);
    json.Jsonize("batch_mode", _batchMode, _batchMode);
    json.Jsonize(BATCH_MASK, _input.batchMask, _input.batchMask);
    json.Jsonize("is_tablet", _isTablet, _isTablet);
    json.Jsonize("safe_write", _safeWrite, _safeWrite);
    json.Jsonize("need_update_topic_safe_write", _needUpdateTopicVersionControl, _needUpdateTopicVersionControl);

    json.Jsonize("update_schema_count", _id, _id);
    json.Jsonize("processor_task_id", _id, _id);
    json.Jsonize("cluster_names", _clusterNames, _clusterNames);
    json.Jsonize("cluster_schema_id", _clusterToSchemaId, _clusterToSchemaId);
    json.Jsonize("task_status", _taskStatus, _taskStatus);
    json.Jsonize("suspend_reason", _suspendReason, _suspendReason);
    json.Jsonize("output", _output, _output);
    json.Jsonize("last_target_infos", _lastTargetInfos, _lastTargetInfos);
    json.Jsonize("processor_writer_infos", _writerVersion, _writerVersion);

    // for compatibility
    if (json.GetMode() == TO_JSON) {
        json.Jsonize("processor_rule_config", _processorRuleConfig);
        if (_toUpdateWriterInfo) {
            json.Jsonize("to_update_writer_info", _toUpdateWriterInfo.value());
        }
    } else {
        auto jsonMap = json.GetMap();
        map<string, Any>::const_iterator it = jsonMap.find("processor_rule_config");
        if (it == jsonMap.end()) {
            _processorRuleConfig.partitionCount = _partitionCount;
            _processorRuleConfig.parallelNum = _parallelNum;
        } else {
            FromJson(_processorRuleConfig, it->second);
        }
        it = jsonMap.find("to_update_writer_info");
        if (it != jsonMap.end()) {
            WriterUpdateInfo writerInfo;
            FromJson(writerInfo, it->second);
            _toUpdateWriterInfo = writerInfo;
        }
        registBrokerTopics();
        _beeperTags.reset(new beeper::EventTags);
        ProtoUtil::buildIdToBeeperTags(_buildId, *_beeperTags);
        _beeperTags->AddTag("buildStep", ProtoUtil::toStepString(_step));
        _beeperTags->AddTag("clusters", StringUtil::toString(_clusterNames, " "));
        registCheckpointInfo();
    }
}

bool ProcessorTask::loadFromConfig(const ResourceReaderPtr& reader)
{
    bool safeWrite = false;
    if (!reader->getDataTableConfigWithJsonPath(_buildId.datatable(), config::SAFE_WRITE, safeWrite)) {
        BS_LOG(ERROR, "get safe write config failed");
        return false;
    }
    _safeWrite = safeWrite;
    BS_LOG(INFO, "safe write enabled [%d]", _safeWrite);

    ProcessorRuleConfig processorRuleConfig;
    ProcessorConfigReader processorConfigReader(reader, _buildId.datatable(), _configName);
    if (!processorConfigReader.validateConfig(_clusterNames)) {
        BS_LOG(ERROR, "processor config validate fail");
        return false;
    }

    if (!processorConfigReader.readRuleConfig(&processorRuleConfig)) {
        string errorMsg = "read processor config failed, config path [" + reader->getOriginalConfigPath() + "]";
        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, errorMsg, *_beeperTags);
        return false;
    }
    bool hasIncDataSource = true;
    if (reader->getDataTableConfigWithJsonPath(_buildId.datatable(), "has_inc_datasource", hasIncDataSource)) {
        _hasIncDatasource = hasIncDataSource;
    }
    config::ControlConfig controlConfig;
    if (!reader->getDataTableConfigWithJsonPath(_buildId.datatable(), "control_config", controlConfig)) {
        string errorMsg = "read control config failed, config path [" + reader->getOriginalConfigPath() + "]";

        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, errorMsg, *_beeperTags);
        return false;
    }
    _fullBuildProcessLastSwiftSrc = controlConfig.isAllClusterNeedIncProcessor(_clusterNames);
    _processorRuleConfig = processorRuleConfig;
    _useCustomizeProcessorCount = false;
    if (!_input.dataDescriptions.empty()) {
        size_t i = 0;
        for (; i < _input.dataDescriptions.size(); ++i) {
            if (_input.dataDescriptions[i].find(CUSTOMIZE_FULL_PROCESSOR_COUNT) != _input.dataDescriptions[i].end()) {
                _useCustomizeProcessorCount = true;
                break;
            }
        }

        // last ds is swift && is batch_mode, set for _batchMode
        if (ProcessorInput::isBatchMode(_input.dataDescriptions)) {
            _batchMode = true;
            if (_step == BUILD_STEP_FULL) {
                _fullBuildProcessLastSwiftSrc = false;
            }
        }
        if (!_input.fullBuildProcessLastSwiftSrc) {
            _fullBuildProcessLastSwiftSrc = false;
        }
    }

    if (_input.fullBuildProcessLastSwiftSrc != _fullBuildProcessLastSwiftSrc) {
        BS_LOG(INFO, "Generation[%s] sync ProcessorInput parameter:fullBuildProcessLastSwiftSrc to [%s] ",
               _buildId.ShortDebugString().c_str(), _fullBuildProcessLastSwiftSrc ? "true" : "false");
        _input.fullBuildProcessLastSwiftSrc = _fullBuildProcessLastSwiftSrc;
    }
    if (!_fullBuildProcessLastSwiftSrc) {
        BS_LOG(INFO, "Generation[%s] full build skip last swift dataDescription", _buildId.ShortDebugString().c_str());
        if (_input.dataDescriptions.size() < 2) {
            BS_LOG(ERROR, "Generation[%s] need at least 2 data description to skip last src",
                   _buildId.ShortDebugString().c_str());
            return false;
        }
    }

    if (!fillSchemaIdInfo(reader)) {
        return false;
    }
    config::SlowNodeDetectConfig slowNodeDetectConfig;
    if (!getSlowNodeDetectConfig(reader, /*clusterName*/ "", slowNodeDetectConfig)) {
        BS_LOG(ERROR, "get slowNodeDetectConfig from %s failed", reader->getOriginalConfigPath().c_str());
        return false;
    }
    _slowNodeDetect = slowNodeDetectConfig.enableSlowDetect;
    return true;
}

bool ProcessorTask::prepareSlowNodeDetector(const std::string& clusterName)
{
    if (_slowNodeDetect) {
        config::SlowNodeDetectConfig slowNodeDetectConfig;
        auto reader = getConfigReader();
        if (!getSlowNodeDetectConfig(reader, /*clusterName*/ "", slowNodeDetectConfig)) {
            BS_LOG(ERROR, "get slowNodeDetectConfig from %s failed", reader->getOriginalConfigPath().c_str());
            return false;
        }
        if (!rewriteDetectSlowNodeByLocatorConfig(slowNodeDetectConfig)) {
            BS_LOG(INFO, "rewrite  failed [%s] for cluster[%s]", ToJsonString(slowNodeDetectConfig, true).c_str(),
                   clusterName.c_str());
            return false;
        }
        auto detector = std::make_shared<SlowNodeDetector>();
        if (detector->Init<proto::ProcessorNodes>(slowNodeDetectConfig, _nodesStartTimestamp)) {
            _slowNodeDetector = detector;
            BS_LOG(INFO, "init slowNodeDetectConfig[%s] for cluster[%s]",
                   ToJsonString(slowNodeDetectConfig, true).c_str(), clusterName.c_str());
        }
    }
    return true;
}

bool ProcessorTask::checkUpdateConfigLegal(const string& configPath)
{
    ProcessorTask other(_buildId, _step, _resourceManager);
    ProcessorConfigInfo configInfo = getConfigInfo();
    configInfo.configPath = configPath;
    if (!other.doInit(getInput(), getOutput(), _clusterNames, configInfo, true)) {
        return false;
    }
    other.startFromLocator(getLocator());
    if (_partitionCount != other._partitionCount && _input.src < _input.dataDescriptions.size() &&
        !_input.supportUpdatePartitionCount()) {
        stringstream ss;
        ss << "update partitionCount failed, support swift src only, "
           << "partitionCount: origin [" << _partitionCount << "], update [" << other._partitionCount << "]";
        REPORT_ERROR(SERVICE_ADMIN_ERROR, ss.str());
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "update processor failed: " + ss.str(), *_beeperTags);
        return false;
    }
    if (other._processorRuleConfig.incProcessorStartTs >= 0 &&
        _processorRuleConfig.incProcessorStartTs != other._processorRuleConfig.incProcessorStartTs &&
        _partitionCount == other._partitionCount && _parallelNum == other._parallelNum) {
        stringstream ss;
        ss << "update inc_processor_start_timestamp failed, partitionCount [" << _partitionCount << "] or parallelNum ["
           << _parallelNum << "] should also be changed.";
        REPORT_ERROR(SERVICE_ADMIN_ERROR, ss.str());
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, "update processor failed: " + ss.str(), *_beeperTags);
        return false;
    }
    return true;
}

bool ProcessorTask::fillSchemaIdInfo(const ResourceReaderPtr& resourceReader)
{
    _clusterToSchemaId.clear();
    for (auto clusterName : _clusterNames) {
        auto schema = resourceReader->getTabletSchema(clusterName);
        if (!schema) {
            string errorMsg = "read cluster [" + clusterName + "]schema failed, config path [" +
                              resourceReader->getConfigPath() + "]";
            REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
            BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, errorMsg, *_beeperTags);
            return false;
        }
        _clusterToSchemaId[clusterName] = schema->GetSchemaId();
    }
    return true;
}

Locator ProcessorTask::getLocator() const { return Locator(_input.src, _input.offset); }

void ProcessorTask::startFromLocator(const common::Locator& locator)
{
    BS_LOG(INFO, "start from locator [%s]", locator.DebugString().c_str());
    _input.src = locator.GetSrc();
    _input.offset = locator.GetOffset().first;
    _nodesUpdater = createProcessorNodesUpdater();
    _fatalErrorDiscoverer.reset();
    _slowNodeDetector.reset();
    _adaptiveScaler.reset();
    updateCheckpoint();
}

bool ProcessorTask::suspendTask(bool forceSuspend) { return suspendTask(forceSuspend, "for_suspend"); }

bool ProcessorTask::suspendTask(bool forceSuspend, const std::string& suspendReason)
{
    _suspendReason = suspendReason;
    AdminTaskBase::suspendTask(forceSuspend);
    return true;
}

SwiftAdminFacadePtr ProcessorTask::getSwiftAdminFacade(const std::string& clusterName) const
{
    auto reader = getConfigReader();
    SwiftAdminFacadePtr facade(new SwiftAdminFacade);
    if (!facade->init(_buildId, reader, clusterName)) {
        BS_LOG(ERROR, "facade init failed, cluster name [%s]", clusterName.c_str());
        return nullptr;
    }
    return facade;
}

bool ProcessorTask::updateSwiftWriterVersion(const ProcessorNodes& processorNodes, uint32_t majorVersion,
                                             const std::vector<std::pair<size_t, uint32_t>>& updatedMinorVersions) const
{
    std::vector<std::pair<std::string, uint32_t>> minorVersions;
    for (auto [idx, version] : updatedMinorVersions) {
        auto pid = processorNodes[idx]->getPartitionId();
        std::string writerName = config::SwiftConfig::getProcessorWriterName(pid.range().from(), pid.range().to());
        minorVersions.emplace_back(writerName, version);
    }
    for (const auto& clusterName : _clusterNames) {
        auto facade = getSwiftAdminFacade(clusterName);
        if (!facade) {
            BS_LOG(ERROR, "failed to get facade for cluster [%s]", clusterName.c_str());
            return false;
        }
        string topicConfigName = _step == proto::BUILD_STEP_FULL ? BS_TOPIC_FULL : BS_TOPIC_INC;
        string topicName = facade->getTopicName(topicConfigName);
        if (!facade->updateWriterVersion(topicConfigName, topicName, majorVersion, minorVersions)) {
            BS_LOG(ERROR, "update swift writer version failed, cluster name [%s], step [%s], topic name [%s]",
                   clusterName.c_str(), topicConfigName.c_str(), topicName.c_str());
            return false;
        }
    }
    return true;
}

bool ProcessorTask::executeWriterTodo(const ProcessorNodes& processorNodes)
{
    if (_needUpdateTopicVersionControl) {
        for (const auto& clusterName : _clusterNames) {
            auto facade = getSwiftAdminFacade(clusterName);
            if (!facade) {
                BS_LOG(ERROR, "failed to get facade for cluster [%s]", clusterName.c_str());
                return false;
            }
            string topicConfigName = _step == proto::BUILD_STEP_FULL ? BS_TOPIC_FULL : BS_TOPIC_INC;
            string topicName = facade->getTopicName(topicConfigName);
            if (!facade->updateTopicVersionControl(topicConfigName, topicName, _safeWrite)) {
                BS_LOG(ERROR, "update version control [%d] for cluster name [%s], step [%s], topic name [%s] failed",
                       _safeWrite, clusterName.c_str(), topicConfigName.c_str(), topicName.c_str());
                return false;
            }
        }
        _toUpdateWriterInfo = _writerVersion.forceUpdateMajorVersion(processorNodes.size());
        _needUpdateTopicVersionControl = false;
    }

    if (_toUpdateWriterInfo && _safeWrite &&
        !updateSwiftWriterVersion(processorNodes, _toUpdateWriterInfo.value().updatedMajorVersion,
                                  _toUpdateWriterInfo.value().updatedMinorVersions)) {
        BS_LOG(ERROR, "update swift writer version failed");
        return false;
    }
    _toUpdateWriterInfo = std::nullopt;
    return true;
}

void ProcessorTask::AdaptiveScaling()
{
    if (!_processorRuleConfig.adaptiveScalingConfig.enableAdaptiveScaling || _step == BUILD_STEP_FULL) {
        return;
    }
    if (!_adaptiveScaler) {
        if (_input.src < 0 || _input.src >= _input.dataDescriptions.size()) {
            BS_LOG(INFO, "deal src [%ld] not begin or is finished, processor [%s]", _input.src,
                   _buildId.ShortDebugString().c_str());
            return;
        }
        auto dataDescription = _input.dataDescriptions[_input.src];
        auto iterator = dataDescription.find(READ_SRC_TYPE);
        if (iterator == dataDescription.end()) {
            BS_LOG(INFO, "not need adaptive scaling by locator for empty src, processor [%s]",
                   _buildId.ShortDebugString().c_str());
            return;
        }
        _adaptiveScaler.reset(new ProcessorAdaptiveScaling(_processorRuleConfig.adaptiveScalingConfig, iterator->second,
                                                           _adaptiveScalingFixedMax));
    }

    auto ret = _adaptiveScaler->handle(_nodeStatusManager, _partitionCount, _parallelNum);
    if (ret.has_value()) {
        auto [partitionCount, parallelNum] = ret.value();
        assert(partitionCount != _partitionCount || parallelNum != _parallelNum);
        BS_LOG(INFO, "need scaling, partition count [%u] -> [%u], parallelNum [%u] -> [%u]", _partitionCount,
               partitionCount, _parallelNum, parallelNum);
        _partitionCount = partitionCount;
        _parallelNum = parallelNum;
    }
}

// TODO: refactor when data source only one
bool ProcessorTask::run(ProcessorNodes& processorNodes)
{
    if (_taskStatus == TASK_FINISHED) {
        return true;
    }
    // batch mode not use optimize
    if (_alreadyRun && !_batchMode) {
        return false;
    }
    _alreadyRun = true;
    registBrokerTopics();
    if (_taskStatus == TASK_SUSPENDED) {
        return false;
    }
    if (_taskStatus == TASK_SUSPENDING) {
        waitSuspended(processorNodes);
        return false;
    }
    if (_step == BUILD_STEP_INC && !_hasIncDatasource) {
        // do nothing when has no inc dataSource
        return false;
    }
    if (_input.src >= _input.dataDescriptions.size()) {
        return false;
    }

    // use newNodes to ensure target have been generated before worker start
    if (!_nodesUpdater) {
        _nodesUpdater = createProcessorNodesUpdater();
    }
    _nodeStatusManager->Update(processorNodes);
    AdaptiveScaling();

    if (_partitionCount * _parallelNum != _nodeStatusManager->GetAllNodeGroups().size() ||
        _partitionCount != _runningPartitionCount || _parallelNum != _runningParallelNum) {
        _slowNodeDetector.reset();
        _adaptiveScaler.reset();
        if (_nodesStartTimestamp == -1) {
            _nodesStartTimestamp = autil::TimeUtility::currentTimeInMicroSeconds();
        }
        BS_LOG(INFO,
               "release processor nodes [%s], whose partition count change from [%u, %u] to [%u, %u], current nodes "
               "size [%lu]",
               getTaskIdentifier().c_str(), _runningPartitionCount, _runningParallelNum, _partitionCount, _parallelNum,
               _nodeStatusManager->GetAllNodeGroups().size());
        processorNodes.clear();
        startWorkers(processorNodes);
        if (_nodeStatusManager->GetAllNodeGroups().size() == 0) {
            recoverFromBackInfo(processorNodes);
        }
    }
    if (!executeWriterTodo(processorNodes)) {
        return false;
    }
    _toUpdateWriterInfo = _writerVersion.update(processorNodes, _runningPartitionCount, _runningParallelNum);
    if (_toUpdateWriterInfo && _safeWrite) {
        return false;
    }
    detectSlowNodes(processorNodes);
    _nodesUpdater->update(processorNodes, _writerVersion,
                          /*PorcessorBasicInfo*/ {_input.src, _input.offset, _partitionCount, _parallelNum},
                          _lastTargetInfos);

    if (!_fatalErrorDiscoverer) {
        _fatalErrorDiscoverer.reset(new FatalErrorDiscoverer());
    }

    _fatalErrorDiscoverer->collectNodeFatalErrors(processorNodes);
    if (_input.offset != _nodesUpdater->getCheckPoint()) {
        _input.offset = _nodesUpdater->getCheckPoint();
        updateCheckpoint();
    }
    ReportCheckpointFreshness(processorNodes);

    if (!_nodesUpdater->isAllProcessorFinished()) {
        // can not switch when not all workers have been finished
        return false;
    }
    syncWriterVersions();

    _nodesStartTimestamp = -1;
    BS_LOG(INFO, "release processor nodes [%s], which have finished", getTaskIdentifier().c_str());
    processorNodes.clear();

    bool isFullFinished =
        _step == BUILD_STEP_FULL && isInLastDataSource() && _switchTimestamp != DEFAULT_SWITCH_TIMESTAMP;
    if (!isFullFinished) {
        deregistBrokerTopics();
        switchDataSource();
        registBrokerTopics();
        // return true;
    }

    bool ret = false;
    if (isFullFinished || _input.src >= _input.dataDescriptions.size()) {
        if (_switchTimestamp != DEFAULT_SWITCH_TIMESTAMP) {
            _input.offset = _switchTimestamp;
            _switchTimestamp = DEFAULT_SWITCH_TIMESTAMP;
        }
        ret = _step == BUILD_STEP_FULL || _batchMode;
    }

    bool needSkipLastSwiftDataSource = !_fullBuildProcessLastSwiftSrc && _step == BUILD_STEP_FULL &&
                                       isInLastDataSource() &&
                                       DataSourceHelper::isRealtime(_input.dataDescriptions[_input.src]);
    if (needSkipLastSwiftDataSource) {
        ret = true;
    }

    if (ret) {
        updateCheckpoint();
        deregistBrokerTopics();
        _taskStatus = TASK_FINISHED;

        string ss = ProtoUtil::toStepString(_step) + " processor task for[";
        for (size_t i = 0; i < _clusterNames.size(); i++) {
            if (i != 0) {
                ss = ss + ",";
            }
            ss = ss + _clusterNames[i] + ":" + _clusterToSchemaId[_clusterNames[i]];
        }
        ss = ss + "] finished!";
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, ss, *_beeperTags);
        if (_batchMode) {
            string msg = "batch mode processor finish batchMask " + StringUtil::toString(_input.batchMask);
            BEEPER_REPORT(BATCHMODE_INFO_COLLECTOR_NAME, msg, *_beeperTags);
        }
    }
    return ret;
}

void ProcessorTask::syncWriterVersions()
{
    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    checkpointAccessor->addOrUpdateCheckpoint(PROCESSOR_WRITER_VERSION, PROCESSOR_WRITER_VERSION,
                                              _writerVersion.serilize());
}

bool ProcessorTask::getSyncedWriterVersionsIfEmpty()
{
    if (_writerVersion.size() != 0) {
        return true;
    }
    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    string writerVersionStr;
    if (!checkpointAccessor->getCheckpoint(PROCESSOR_WRITER_VERSION, PROCESSOR_WRITER_VERSION, writerVersionStr,
                                           false)) {
        return true;
    }
    return _writerVersion.deserilize(writerVersionStr);
}

bool ProcessorTask::getBrokerTopicId(const std::string& clusterName, string& topicId)
{
    auto iter = _clusterToTopicId.find(clusterName);
    if (iter != _clusterToTopicId.end()) {
        topicId = iter->second;
        return true;
    }
    if (!_output.isEmpty()) {
        if (!_output.getOutputId(clusterName, &topicId)) {
            return false;
        }
        _clusterToTopicId[clusterName] = topicId;
        return true;
    }

    auto reader = getConfigReader();
    SwiftAdminFacade facade;
    if (!facade.init(_buildId, reader, clusterName)) {
        return false;
    }

    string topicConfigName = _step == proto::BUILD_STEP_FULL ? BS_TOPIC_FULL : BS_TOPIC_INC;
    topicId = facade.getTopicId(topicConfigName, "");
    _clusterToTopicId[clusterName] = topicId;
    return true;
}

void ProcessorTask::registBrokerTopics()
{
    if (_taskStatus == TASK_STOPPED || _taskStatus == TASK_FINISHED) {
        return;
    }
    // if output not empty, then topic is prepared outside
    if (!_output.isEmpty()) {
        return;
    }
    // prepareDefaultTopic
    auto reader = getConfigReader();
    for (auto& clusterName : _clusterNames) {
        prepareBrokerTopicForCluster(clusterName, _step, reader, _resourceManager);
    }
}

void ProcessorTask::prepareBrokerTopicForCluster(const std::string& clusterName, const proto::BuildStep& step,
                                                 const ResourceReaderPtr& configReader,
                                                 TaskResourceManagerPtr& resourceManager)
{
    string topicId;
    getBrokerTopicId(clusterName, topicId);
    string topicConfig = step == proto::BUILD_STEP_FULL ? BS_TOPIC_FULL : BS_TOPIC_INC;
    string resourceName = DefaultBrokerTopicCreator::getDefaultTopicResourceName(clusterName, topicConfig, topicId);

    auto iter = _brokerTopicKeepers.find(resourceName);
    if (iter != _brokerTopicKeepers.end()) {
        return;
    }
    auto topicKeeper = DefaultBrokerTopicCreator::applyDefaultSwiftResource(getTaskIdentifier(), clusterName, step,
                                                                            configReader, resourceManager);
    if (topicKeeper) {
        _brokerTopicKeepers[resourceName] = topicKeeper;
    }
}

void ProcessorTask::deregistBrokerTopics() { _brokerTopicKeepers.clear(); }

void ProcessorTask::notifyStopped()
{
    _taskStatus = TASK_STOPPED;
    deregistBrokerTopics();
}

ProcessorCheckpointAccessorPtr ProcessorTask::getProcessorCheckpointAccessor()
{
    if (_processorCheckpointAccessor) {
        return _processorCheckpointAccessor;
    }
    _resourceManager->getResource(_processorCheckpointAccessor);
    if (_processorCheckpointAccessor) {
        return _processorCheckpointAccessor;
    }
    CheckpointAccessorPtr checkpointAccessor;
    _resourceManager->getResource(checkpointAccessor);
    _processorCheckpointAccessor.reset(new ProcessorCheckpointAccessor(checkpointAccessor));
    _resourceManager->addResource(_processorCheckpointAccessor);
    return _processorCheckpointAccessor;
}

void ProcessorTask::ReportCheckpointFreshness(const proto::ProcessorNodes& processorNodes)
{
    if (_step != BUILD_STEP_INC) {
        return;
    }

    if (!_hasIncDatasource) {
        return;
    }

    if (_input.offset <= 0) {
        return;
    }

    if (!_resourceManager) {
        return;
    }

    CheckpointMetricReporterPtr reporter;
    _resourceManager->getResource(reporter);
    if (!reporter) {
        return;
    }

    int64_t now = TimeUtility::currentTimeInSeconds();
    if ((now - _reportCheckpointTs) <= 5) { // avoid frequently report
        return;
    }
    _reportCheckpointTs = now;

    for (size_t i = 0; i < processorNodes.size(); i++) {
        if (processorNodes[i]->isFinished()) {
            continue;
        }
        int64_t checkpoint = _input.offset == -1 ? 0 : _input.offset;
        const ProcessorCurrent& current = processorNodes[i]->getCurrentStatus();
        if (current.has_currentlocator()) {
            checkpoint = current.currentlocator().checkpoint();
        }
        kmonitor::MetricsTags tags;
        std::string clusterName;
        if (_clusterNames.size() > 1) {
            clusterName = "ALL";
        } else {
            clusterName = _clusterNames[0];
        }
        tags.AddTag("cluster", clusterName);
        auto range = processorNodes[i]->getPartitionId().range();
        tags.AddTag("partition", StringUtil::toString(range.from()) + "_" + StringUtil::toString(range.to()));
        reporter->reportSingleProcessorFreshness(checkpoint, tags);
    }

    for (auto& cluster : _clusterNames) {
        string topicId;
        getBrokerTopicId(cluster, topicId);
        kmonitor::MetricsTags tags;
        tags.AddTag("cluster", cluster);
        tags.AddTag("buildStep", ProtoUtil::toStepString(_step));
        reporter->calculateAndReportProcessorFreshness(_input.offset, tags);
    }

    // report beeper if needed
    int64_t minLocatorSwiftTs = _input.offset / 1000000; // us to s
    int64_t gap = now - minLocatorSwiftTs;
    if (gap > 1800 && now - _beeperReportTs > 600) {
        BEEPER_FORMAT_REPORT(GENERATION_ERROR_COLLECTOR_NAME, *_beeperTags,
                             "inc processor min checkpoint [%ld], time gap [%ld] over half an hour", _input.offset,
                             gap);
        _beeperReportTs = now;
    }
}

void ProcessorTask::updateCheckpoint()
{
    auto accessor = getProcessorCheckpointAccessor();
    Locator locator = getLocator();

    for (auto& cluster : _clusterNames) {
        string topicId;
        getBrokerTopicId(cluster, topicId);
        accessor->addCheckpoint(cluster, topicId, _stage, locator);
    }
}

void ProcessorTask::createProcessorTaskId()
{
    auto accessor = getProcessorCheckpointAccessor();
    _id = accessor->createMaxProcessorTaskId();
}

void ProcessorTask::waitSuspended(WorkerNodes& workerNodes)
{
    if (_taskStatus == TASK_SUSPENDED || _taskStatus == TASK_STOPPED) {
        return;
    }
    ProcessorNodes nodes;
    nodes.reserve(workerNodes.size());
    for (auto& workerNode : workerNodes) {
        nodes.push_back(DYNAMIC_POINTER_CAST(ProcessorNode, workerNode));
    }
    waitSuspended(nodes);
    workerNodes.clear();
    workerNodes.reserve(nodes.size());
    for (auto& processorNode : nodes) {
        workerNodes.push_back(processorNode);
    }
}

void ProcessorTask::waitSuspended(ProcessorNodes& nodes)
{
    // avoid to suspend success when nodes.size() = 0
    if (nodes.size() == 0 && _hasCreateNodes) {
        startWorkers(nodes);
        return;
    }

    bool allSuspended = true;
    for (const auto& processorNode : nodes) {
        if (processorNode->isSuspended() || processorNode->isFinished()) {
            continue;
        }
        if (isSuspended(processorNode)) {
            processorNode->setSuspended(true);
        } else {
            allSuspended = false;
            ProcessorTarget suspendTarget;
            suspendTarget.set_suspendtask(true);
            processorNode->setTargetStatus(suspendTarget);
        }
    }
    if (allSuspended) {
        nodes.clear();
        _taskStatus = TASK_SUSPENDED;
    }
    return;
}

void ProcessorTask::startWorkers(ProcessorNodes& nodes)
{
    nodes = WorkerNodeCreator<ROLE_PROCESSOR>::createNodes(_partitionCount, _parallelNum, _step, _buildId, "", "",
                                                           getTaskIdentifier());
    if (_safeWrite) {
        for (auto& node : nodes) {
            node->setIsReady(false);
        }
    }
    if (_runningPartitionCount != _partitionCount || _runningParallelNum != _parallelNum) {
        _toUpdateWriterInfo = std::nullopt;
    }
    _hasCreateNodes = true;
    _runningPartitionCount = _partitionCount;
    _runningParallelNum = _parallelNum;
}

void ProcessorTask::switchDataSource()
{
    string action = "current data source[" + ToJsonString(_input.dataDescriptions[_input.src], true) +
                    "] has been processed, switch to next automatically";
    BS_LOG(INFO, "%s", action.c_str());

    BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags, "processor: %s", action.c_str());
    _input.src++;
    _input.offset = 0;

    if (isInLastDataSource() && DataSourceHelper::isRealtime(_input.dataDescriptions[_input.src]) &&
        _step == BUILD_STEP_FULL) {
        _input.offset = _processorRuleConfig.incProcessorStartTs >= 0 ? _processorRuleConfig.incProcessorStartTs : 0;
        _switchTimestamp = (TimeUtility::currentTimeInSeconds() + READER_SYNC_TIME_INTERVAL) * 1000 * 1000;
        BS_LOG(INFO, "processor switch time at %ld", _switchTimestamp);
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "last data source, processor switch time at %ld", _switchTimestamp);
    }
    _nodesUpdater = createProcessorNodesUpdater();

    _partitionCount = getPartitionCount(_processorRuleConfig);
    _parallelNum = getParallelNum(_processorRuleConfig);
    _fatalErrorDiscoverer.reset();

    _slowNodeDetector.reset();
    _adaptiveScaler.reset();
}

ProcessorNodesUpdaterPtr ProcessorTask::createProcessorNodesUpdater()
{
    if (_input.src >= _input.dataDescriptions.size()) {
        return ProcessorNodesUpdaterPtr();
    }
    BS_LOG(INFO, "create processor node updater input src [%ld] offset [%ld]", _input.src, _input.offset);
    return ProcessorNodesUpdaterPtr(new ProcessorNodesUpdater(getConfigReader(), _nodeStatusManager, _clusterNames,
                                                              {_input, _output}, _switchTimestamp, _configName,
                                                              _isTablet, _safeWrite));
}

int64_t ProcessorTask::getCurrentTime() const { return TimeUtility::currentTimeInSeconds(); }

bool ProcessorTask::isInLastDataSource() const { return _input.src + 1 == _input.dataDescriptions.size(); }

bool ProcessorTask::switchBuild()
{
    if (_step == BUILD_STEP_INC || _input.src >= _input.dataDescriptions.size()) {
        return false;
    }
    if (!isInLastDataSource()) {
        string errorMsg = "force switch is allowed only in last data source";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    DataDescription ds = _input.dataDescriptions[_input.src];
    if (!DataSourceHelper::isRealtime(ds)) {
        string errorMsg = "force switch is allowed only in swift-like data source";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    if (_switchTimestamp != DEFAULT_SWITCH_TIMESTAMP) {
        string errorMsg = "can not switch twice";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    _switchTimestamp = (TimeUtility::currentTimeInSeconds() + READER_SYNC_TIME_INTERVAL) * 1000 * 1000;
    if (_nodesUpdater) {
        _nodesUpdater->resetStopTimestamp(_switchTimestamp);
    }
    BS_LOG(INFO, "force switch to [%ld]", _switchTimestamp);
    BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags, "processor force switch to [%ld]",
                         _switchTimestamp);
    return true;
}

void ProcessorTask::fillProcessorInfo(ProcessorInfo* processorInfo, const WorkerNodes& workerNodes,
                                      const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    ProcessorNodes nodes;
    nodes.reserve(workerNodes.size());
    for (auto& workerNode : workerNodes) {
        nodes.push_back(DYNAMIC_POINTER_CAST(ProcessorNode, workerNode));
    }
    processorInfo->set_partitioncount(_partitionCount);
    processorInfo->set_parallelnum(_parallelNum);
    if (_input.src >= _input.dataDescriptions.size()) {
        processorInfo->set_processorstep("finished");
        return;
    }
    if (_step == BUILD_STEP_INC && !_hasIncDatasource) {
        processorInfo->set_processorstep("finished");
        return;
    }

    processorInfo->set_datadescription(ToJsonString(_input.dataDescriptions[_input.src]));
    processorInfo->set_processorstep(BuildStep_Name(_step));
    fillPartitionInfos(processorInfo, nodes, roleCounterMap);
    if (_fatalErrorDiscoverer) {
        _fatalErrorDiscoverer->fillFatalErrors(processorInfo->mutable_fatalerrors());
    }
    processorInfo->set_taskstatus(getTaskStatusString());
}

uint32_t ProcessorTask::getPartitionCount(const ProcessorRuleConfig& processorRuleConfig) const
{
    // for BUILD_STEP_INC
    if (_step == BUILD_STEP_INC) {
        if (processorRuleConfig.adaptiveScalingConfig.enableAdaptiveScaling && _runningPartitionCount != 0) {
            return _runningPartitionCount;
        }
        return processorRuleConfig.partitionCount;
    }

    // for BUILD_STEP_FULL
    if (!_useCustomizeProcessorCount || _input.src >= _input.dataDescriptions.size()) {
        return processorRuleConfig.partitionCount;
    }

    string fullProcessorCountStr =
        getValueFromKeyValueMap(_input.dataDescriptions[_input.src].kvMap, CUSTOMIZE_FULL_PROCESSOR_COUNT);

    if (!fullProcessorCountStr.empty()) {
        return StringUtil::fromString<uint32_t>(fullProcessorCountStr);
    }
    return processorRuleConfig.partitionCount;
}

uint32_t ProcessorTask::getParallelNum(const ProcessorRuleConfig& processorRuleConfig) const
{
    if (_input.src >= _input.dataDescriptions.size()) {
        return 1;
    }

    if (_step == BUILD_STEP_INC) {
        if (processorRuleConfig.adaptiveScalingConfig.enableAdaptiveScaling && _runningParallelNum != 0) {
            return _runningParallelNum;
        }
        return processorRuleConfig.incParallelNum;
    }

    if (!_useCustomizeProcessorCount) {
        return processorRuleConfig.parallelNum;
    }

    string fullProcessorCountStr =
        getValueFromKeyValueMap(_input.dataDescriptions[_input.src].kvMap, CUSTOMIZE_FULL_PROCESSOR_COUNT);

    if (!fullProcessorCountStr.empty()) {
        return 1u;
    }

    return processorRuleConfig.parallelNum;
}

void ProcessorTask::clearFullWorkerZkNode(const std::string& generationDir) const
{
    doClearFullWorkerZkNode(generationDir, ROLE_PROCESSOR);
}

// now only support batch mode to clear zk nodes
void ProcessorTask::clearCounterAndIncZkNode(const std::string& generationDir)
{
    if (!_batchMode) {
        return;
    }
    // batch build clear processor ds is 0
    ProcessorTaskIdentifier identifier;
    identifier.setDataDescriptionId(0);
    identifier.setTaskId(StringUtil::toString(_id));
    string clusterName = calculateRoleClusterName();
    if (!clusterName.empty()) {
        identifier.setClusterName(clusterName);
    }

    auto nodes = WorkerNodeCreator<ROLE_PROCESSOR>::createNodes(_partitionCount, _parallelNum, _step, _buildId, "", "",
                                                                identifier.toString());

    for (const auto& node : nodes) {
        auto pid = node->getPartitionId();
        string workerZkRoot = common::PathDefine::getRoleZkPath(generationDir, pid, true /*ignoreBackupId*/);
        if (workerZkRoot.empty() || !fslib::util::FileUtil::remove(workerZkRoot)) {
            BS_LOG(WARN, "remove worker zk [%s] failed", workerZkRoot.c_str());
        }
        CounterSynchronizer::removeRoleZkCounter(generationDir, pid, true /*ignoreBackupId*/);
    }
}

bool ProcessorTask::run(WorkerNodes& workerNodes)
{
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    auto reader = readerAccessor->getConfig(_configPath);
    if (!reader) {
        // when config deleted processor auto stopped
        workerNodes.clear();
        notifyStopped();
        string clusterNames = StringUtil::toString(_clusterNames, ",");
        BS_LOG(INFO,
               "no config path, "
               "processor task auto finished, "
               "config path [%s], cluster names[%s]",
               _configPath.c_str(), clusterNames.c_str());
        return true;
    }
    ProcessorNodes processorNodes;
    processorNodes.reserve(workerNodes.size());
    for (auto& workerNode : workerNodes) {
        processorNodes.push_back(DYNAMIC_POINTER_CAST(ProcessorNode, workerNode));
    }
    bool ret = run(processorNodes);
    workerNodes.clear();
    workerNodes.reserve(processorNodes.size());
    for (auto& processorNode : processorNodes) {
        workerNodes.push_back(processorNode);
    }
    return ret;
}

string ProcessorTask::getTaskIdentifier()
{
    ProcessorTaskIdentifier identifier;
    identifier.setDataDescriptionId(_input.src);
    identifier.setTaskId(StringUtil::toString(_id));
    string clusterName = calculateRoleClusterName();
    if (!clusterName.empty()) {
        identifier.setClusterName(clusterName);
    }

    return identifier.toString();
}

string ProcessorTask::calculateRoleClusterName()
{
    if (_clusterNames.size() > 1) {
        return "";
    } else {
        return _clusterNames[0];
    }
}

bool ProcessorTask::getSchemaId(const std::string& clusterName, int64_t& schemaId) const
{
    auto iter = _clusterToSchemaId.find(clusterName);
    if (iter == _clusterToSchemaId.end()) {
        return false;
    }
    schemaId = iter->second;
    return true;
}

void ProcessorTask::stop(int64_t stopTimestamp)
{
    _switchTimestamp = stopTimestamp;
    _nodesUpdater.reset();
}

bool ProcessorTask::finish(const KeyValueMap& kvMap)
{
    auto endTime = kvMap.find("endTime");
    if (endTime == kvMap.end()) {
        BS_LOG(ERROR, "failed to stop inc processor[%s], lack of endTime", _buildId.ShortDebugString().c_str());
        return false;
    }
    int64_t stopTs = -1;
    if (!StringUtil::fromString(endTime->second, stopTs)) {
        BS_LOG(ERROR, "failed to stop inc processor[%s], invalid endTime[%s]", _buildId.ShortDebugString().c_str(),
               endTime->second.c_str());
        return false;
    }
    stop(stopTs);
    return true;
}

void ProcessorTask::setBeeperTags(const beeper::EventTagsPtr beeperTags)
{
    assert(beeperTags);
    AdminTaskBase::setBeeperTags(beeperTags);
}

bool ProcessorTask::rewriteDetectSlowNodeByLocatorConfig(config::SlowNodeDetectConfig& slowNodeDetectConfig)
{
    if (_step == proto::BuildStep::BUILD_STEP_FULL) {
        BS_LOG(INFO, "not need detect slow node by locator for full build, processor [%s]",
               _buildId.ShortDebugString().c_str());
        return true;
    }
    if (_input.src < 0 || _input.src >= _input.dataDescriptions.size()) {
        BS_LOG(INFO, "deal src [%ld] not begin or is finished, processor [%s]", _input.src,
               _buildId.ShortDebugString().c_str());
        return true;
    }
    auto detectSlowConfigs = _processorRuleConfig.detectSlowConfigs;
    auto dataDescription = _input.dataDescriptions[_input.src];
    auto it_type = dataDescription.find(READ_SRC_TYPE);
    if (it_type == dataDescription.end()) {
        BS_LOG(INFO, "not need detect slow node by locator for empty src, processor [%s]",
               _buildId.ShortDebugString().c_str());
        return true;
    }
    for (const auto& detectByLocatorConfig : detectSlowConfigs) {
        if (detectByLocatorConfig.src == it_type->second) {
            BS_LOG(INFO, "need detect slow node by locator processor [%s], src [%s], gap [%ld]",
                   _buildId.ShortDebugString().c_str(), detectByLocatorConfig.src.c_str(), detectByLocatorConfig.gap);
            slowNodeDetectConfig.locatorCheckCondition = detectByLocatorConfig.condition;
            slowNodeDetectConfig.locatorCheckGap = detectByLocatorConfig.gap;
            slowNodeDetectConfig.enableDetectSlowByLocator = true;
            if (!slowNodeDetectConfig.enableSlowDetect) {
                BS_LOG(INFO, "not need detect slow node by locator for not enable slow node detect, processor [%s]",
                       _buildId.ShortDebugString().c_str());
                return false;
            }
            return true;
        }
    }
    BS_LOG(INFO, "not need detect slow node by locator for not match, processor [%s]",
           _buildId.ShortDebugString().c_str());
    return true;
    // TODO(tianxiao) 处理全量切增量的情况下，locator都会比较小
}
}} // namespace build_service::admin
