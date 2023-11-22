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
#include "build_service/worker/ProcessorServiceImpl.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <map>
#include <strings.h>
#include <unordered_map>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "beeper/beeper.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/common/ExceedTsAction.h"
#include "build_service/common/NetworkTrafficEstimater.h"
#include "build_service/common/ProcessorOutput.h"
#include "build_service/common/ResourceKeeperCreator.h"
#include "build_service/common/SwiftResourceKeeper.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/ProcessorConfigReader.h"
#include "build_service/config/ProcessorConfigurator.h"
#include "build_service/config/ProcessorRuleConfig.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/config/SrcNodeConfig.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/config/TaskInputConfig.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/ParserConfig.h"
#include "build_service/proto/ProcessorTaskIdentifier.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/task_base/RestartIntervalController.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/SwiftClientCreator.h"
#include "build_service/workflow/AsyncStarter.h"
#include "build_service/workflow/BuildFlowMode.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "build_service/workflow/SrcDataNode.h"
#include "build_service/workflow/StopOption.h"
#include "build_service/workflow/SwiftProcessedDocConsumer.h"
#include "build_service/workflow/WorkflowItem.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::reader;
using namespace build_service::util;
using namespace build_service::workflow;
using namespace build_service::common;

using namespace indexlib::common;
using namespace indexlib::util;

namespace build_service { namespace worker {
BS_LOG_SETUP(worker, ProcessorServiceImpl);

ProcessorServiceImpl::ProcessorServiceImpl(const PartitionId& pid, indexlib::util::MetricProviderPtr metricProvider,
                                           const LongAddress& address, const string& appZkRoot,
                                           const string& adminServiceName)
    // TODO : set valid epochId, fencing old message
    : WorkerStateHandler(pid, metricProvider, appZkRoot, adminServiceName, "")
    , _buildFlow(NULL)
    , _brokerFactory(NULL)
    , _startTimestamp(-1)
    , _baseDocCount(-1)
{
    *_current.mutable_longaddress() = address;
}

ProcessorServiceImpl::~ProcessorServiceImpl()
{
    DELETE_AND_SET_NULL(_buildFlow);
    DELETE_AND_SET_NULL(_brokerFactory);
}

bool ProcessorServiceImpl::init()
{
    bool ret = true;
    ret = _cpuEstimater.Start(/*sampleCountLimit=*/5, /*checkInterval=*/60);
    if (!ret) {
        return false;
    }
    ret = _networkEstimater.Start();
    return ret;
}

void ProcessorServiceImpl::doHandleTargetState(const string& state, bool hasResourceUpdated)
{
    ProcessorTarget target;
    if (!target.ParseFromString(state)) {
        string errorMsg = "failed to deserialize processor target state[" + state + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return;
    }
    if (hasResourceUpdated) {
        bool needRestart = false;
        updateResourceKeeperMap(needRestart);
        if (needRestart) {
            BS_LOG(INFO, "update resource need restart");
            BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, "processor update resource restart.");
            ProcessorTarget target = _current.targetstatus();
            if (restartProcessor(target)) {
                ScopedLock lock(_lock);
                _restartIntervalController.reset();
            } else {
                string errorMsg = "update resource restart failed";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, "processor update config fail.");
            }
        }
    }
    BS_INTERVAL_LOG(60, INFO, "target status: %s", target.ShortDebugString().c_str());
    ProcessorServiceImpl::CommandType commandType = getCommandType(target, _current);

    {
        ScopedLock lock(_lock);
        if (target.has_configpath()) {
            if (_configPath != target.configpath()) {
                _restartIntervalController.reset();
            }
            _configPath = target.configpath();
        }
    }
    switch (commandType) {
    case ProcessorServiceImpl::CT_START: {
        if (startProcessor(target)) {
            // processor start
            ScopedLock lock(_lock);
            *_current.mutable_targetstatus() = target;
            _current.mutable_targetstatus()->clear_stoptimestamp();
            _restartIntervalController.reset();
        }
        break;
    }
    case ProcessorServiceImpl::CT_UPDATE_CONFIG: {
        updateConfig(target.configpath());
        break;
    }
    case ProcessorServiceImpl::CT_STOP: {
        stopProcessor(target);
        // processor stop
        break;
    }
    case ProcessorServiceImpl::CT_SUSPEND: {
        suspendProcessor(target);
    }
    default:
        break;
    }
}

void ProcessorServiceImpl::fillSuspendStatus(BuildFlow* buildFlow, ProcessorCurrent& current)
{
    if (!buildFlow) {
        return;
    }

    if (!buildFlow->isStarted()) {
        return;
    }

    if (buildFlow->isFinished()) {
        return;
    }

    RawDocumentReader* reader = buildFlow->getReader();
    if (!reader) {
        return;
    }
    if (reader->isExceedSuspendTimestamp()) {
        current.set_status(proto::WS_SUSPEND_READ);
    }
}

void ProcessorServiceImpl::fillProgressStatus(BuildFlow* buildFlow, ProcessorCurrent& current)
{
    current.mutable_progressstatus()->set_reporttimestamp(TimeUtility::currentTime());
    if (!buildFlow) {
        return;
    }
    if (!buildFlow->isStarted()) {
        return;
    }
    current.mutable_progressstatus()->set_starttimestamp(_startTimestamp);
    if (!_consumedDocCountCounter) {
        current.mutable_progressstatus()->set_startpoint(INVALID_PROGRESS);
        current.mutable_progressstatus()->set_progress(INVALID_PROGRESS);
        return;
    }

    current.mutable_progressstatus()->set_startpoint(_baseDocCount);
    current.mutable_progressstatus()->set_progress(_consumedDocCountCounter->Get());
}

void ProcessorServiceImpl::fillCpuRatio(ProcessorCurrent& current)
{
    if (_cpuRatioSampler) {
        current.mutable_machinestatus()->set_cpuratioinwindow(_cpuRatioSampler->getAvgCpuRatio());
    }
}

void ProcessorServiceImpl::fillWorkflowError(BuildFlow* buildFlow, ProcessorCurrent& current)
{
    if (_workflowErrorSampler) {
        _workflowErrorSampler->workLoop(buildFlow);
        current.set_workflowerrorexceedthreadhold(_workflowErrorSampler->workFlowErrorExceedThreadhold());
    }
}

void ProcessorServiceImpl::getCurrentState(string& content)
{
    ScopedLock lock(_lock);
    if (isSuspended()) {
        _current.set_issuspended(true);
    }

    fillCurrentState(_buildFlow, _current);
    fillLocator(_buildFlow, _current);
    if (_buildFlow) {
        _current.set_datasourcefinish(_buildFlow->isFinished());
    }
    fillSuspendStatus(_buildFlow, _current);
    fillProgressStatus(_buildFlow, _current);
    fillProtocolVersion(_current);
    fillCpuSpeed(_current);
    fillCpuRatio(_current);
    fillWorkflowError(_buildFlow, _current);
    fillNetworkTraffic(_current);
    saveCurrent(_current, content);
}

bool ProcessorServiceImpl::hasFatalError()
{
    ScopedLock lock(_lock);
    return _hasFatalError || (_buildFlow && _buildFlow->hasFatalError());
}

bool ProcessorServiceImpl::fillInput(const string dataDescriptionStr, const proto::PartitionId& pid, KeyValueMap& kvMap)
{
    // for rawDoc producer
    DataDescription dataDescription;
    if (!getDataDescription(dataDescriptionStr, dataDescription)) {
        return false;
    }
    for (auto it = dataDescription.begin(); it != dataDescription.end(); ++it) {
        kvMap[it->first] = it->second;
    }
    kvMap[DATA_DESCRIPTION_KEY] = dataDescriptionStr;
    if (getValueFromKeyValueMap(kvMap, READ_SRC) == "InputConfig" &&
        getValueFromKeyValueMap(kvMap, READ_SRC_TYPE) == "InputConfig") {
        string readSrcConfig = getValueFromKeyValueMap(kvMap, READ_SRC_CONFIG);
        if (pid.clusternames().size() != 1) {
            BS_LOG(ERROR, "clustername is not unique");
            return false;
        }
        config::TaskInputConfig inputConfig;
        try {
            FromJsonString(inputConfig, readSrcConfig);
        } catch (const autil::legacy::ExceptionBase& e) {
            string errorMsg =
                "Invalid json string for input info[" + readSrcConfig + "] : error [" + string(e.what()) + "]";
            return false;
        }
        string inputFormat;
        auto iter = inputConfig.getParameters().find("documentFormat");
        if (iter == inputConfig.getParameters().end()) {
            BS_LOG(ERROR, "input config do not have format");
            return false;
        }
        inputFormat = iter->second;
        if (inputFormat == "raw") {
            kvMap[INPUT_DOC_TYPE] = INPUT_DOC_RAW;
        } else {
            assert(inputFormat == "processed");
            kvMap[INPUT_DOC_TYPE] = INPUT_DOC_PROCESSED;
        }
        string clusterName = pid.clusternames(0);
        BS_LOG(INFO, "cluster %s use read config %s", clusterName.c_str(), readSrcConfig.c_str());
        kvMap[FlowFactory::clusterInputKey(clusterName)] = readSrcConfig;
    }
    return true;
}

bool ProcessorServiceImpl::startProcessor(const ProcessorTarget& target)
{
    if (_buildFlow) {
        return true;
    }
    if (!_restartIntervalController) {
        _restartIntervalController.reset(new task_base::RestartIntervalController());
    }
    _restartIntervalController->wait();

    BS_LOG(INFO, "start process, target[%s]", target.ShortDebugString().c_str());
    string msg = "start process, target[" + target.ShortDebugString() + "]";
    BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, msg);

    string localConfigPath = downloadConfig(target.configpath());
    if (localConfigPath.empty()) {
        return false;
    }
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(localConfigPath);
    BuildServiceConfig serviceConfig;
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", serviceConfig)) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "failed to parse build_app.json", BS_STOP);
        return false;
    }

    if (!loadGenerationMeta(resourceReader, serviceConfig.getIndexRoot())) {
        _hasFatalError = true;
        return false;
    }

    // overwrite CounterConfig
    CounterConfig& counterConfig = serviceConfig.counterConfig;
    if (!overWriteCounterConfig(_pid, counterConfig)) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "rewrite CounterConfig failed", BS_STOP);
        return false;
    }

    // fill kvMap
    KeyValueMap kvMap;
    kvMap[CONFIG_PATH] = localConfigPath;

    // for processedDoc consumer

    kvMap[CHECKPOINT] = StringUtil::toString(target.startlocator().checkpoint());
    kvMap[CONSUMER_CHECKPOINT] = StringUtil::toString(target.startlocator().checkpoint());
    kvMap[LOCATOR_USER_DATA] = target.startlocator().userdata();
    kvMap[SRC_SIGNATURE] = StringUtil::toString(target.startlocator().sourcesignature());
    kvMap[COUNTER_CONFIG_JSON_STR] = ToJsonString(counterConfig);

    // for src processedDoc producer
    kvMap[ALLOW_SEEK_CROSS_SRC] = KV_TRUE;
    string readThroughDcache = autil::EnvUtil::getEnv(READ_THROUGH_DCACHE.c_str());
    if (!readThroughDcache.empty() && 0 == strcasecmp(readThroughDcache.c_str(), "true")) {
        kvMap[READ_THROUGH_DCACHE] = "true";
    }

    int64_t stopTimestamp;
    DataDescription dataDescription;
    string dataDescriptionStr = target.datadescription();
    if (!getDataDescription(dataDescriptionStr, dataDescription)) {
        return false;
    }
    if (getStopTimestamp(target, dataDescription, stopTimestamp)) {
        kvMap[SWIFT_STOP_TIMESTAMP] = StringUtil::toString(stopTimestamp);
    }

    if (target.has_suspendtimestamp()) {
        kvMap[SWIFT_SUSPEND_TIMESTAMP] = StringUtil::toString(target.suspendtimestamp());
    }

    PartitionId newPid;
    // this pid will add target clusters, it can not be used by ProtoUtil::partionIdToRoleId
    if (!parseTargetDesc(resourceReader, _pid, target, newPid, kvMap)) {
        return false;
    }
    kvMap[PROCESSED_DOC_SWIFT_ROOT] = serviceConfig.getSwiftRoot(newPid.step() == BUILD_STEP_FULL);
    if (!fillInput(target.datadescription(), newPid, kvMap)) {
        BS_LOG(ERROR, "fill input with data  description [%s] failed", target.datadescription().c_str());
        return false;
    }

    ProcessorConfigReaderPtr reader(new ProcessorConfigReader(
        resourceReader, _pid.buildid().datatable(), getValueFromKeyValueMap(kvMap, config::PROCESSOR_CONFIG_NODE)));
    SrcNodeConfig srcConfig;
    bool isExist = false;
    if (!SrcDataNode::readSrcConfig(reader, srcConfig, &isExist)) {
        return false;
    }

    ProcessorRuleConfig processorRuleConfig;
    if (!reader->readRuleConfig(&processorRuleConfig)) {
        BS_LOG(ERROR, "read processor rule config failed for [%s]", _pid.buildid().DebugString().c_str());
        return false;
    }
    if (processorRuleConfig.adaptiveScalingConfig.enableAdaptiveScaling) {
        if (!startSamplersForAdaptiveScaling(processorRuleConfig.adaptiveScalingConfig)) {
            BS_LOG(ERROR, "start samplers failed [%s]", _pid.buildid().DebugString().c_str());
            return false;
        }
    }

    if (isExist && _metricProvider) {
        _metricProvider->setMetricPrefix("src");
    }
    return startProcessFlow(resourceReader, newPid, kvMap);
}

bool ProcessorServiceImpl::startSamplersForAdaptiveScaling(const ProcessorAdaptiveScalingConfig& adaptiveScalingConfig)
{
    size_t sampleWindow = adaptiveScalingConfig.cpuStrategy.sampleWindow;
    _cpuRatioSampler.reset(new CpuRatioSampler(sampleWindow));
    if (!_cpuRatioSampler->start()) {
        BS_LOG(ERROR, "cpu ratio sampler start failed [%s]", _pid.buildid().DebugString().c_str());
        return false;
    }
    _workflowErrorSampler.reset(new ProcessorWorkflowErrorSampler());
    return true;
}

bool ProcessorServiceImpl::loadGenerationMeta(const ResourceReaderPtr resourceReader, const string& indexRoot)
{
    // all clusters in one data_table should have the same generation meta
    if (!resourceReader->initGenerationMeta(_pid.buildid().generationid(), _pid.buildid().datatable(), indexRoot)) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "init generation meta failed", BS_STOP);
        return false;
    }
    return true;
}

common::ResourceKeeperMap ProcessorServiceImpl::createResources(const ResourceReaderPtr& resourceReader,
                                                                const PartitionId& pid, KeyValueMap& kvMap)
{
    common::ResourceKeeperMap ret;
    ret = getCurrentResourceKeeperMap();
    for (int32_t i = 0; i < pid.clusternames_size(); ++i) {
        const string& clusterName = pid.clusternames(i);
        string key = FlowFactory::clusterOutputKey(clusterName);
        if (kvMap.find(key) != kvMap.end()) {
            continue;
        }
        config::TaskOutputConfig config;
        config.setType("dependResource");
        config.addParameters("resourceName", clusterName);
        ResourceKeeperPtr keeper = ResourceKeeperCreator::create(clusterName, "swift", nullptr);
        BS_LOG(INFO, "cluster [%s] create legacy reader", clusterName.c_str());
        auto swiftKeeper = DYNAMIC_POINTER_CAST(SwiftResourceKeeper, keeper);
        swiftKeeper->initLegacy(clusterName, pid, resourceReader, kvMap);
        ret[clusterName] = keeper;
        kvMap[key] = ToJsonString(config);
    }
    return ret;
}

bool ProcessorServiceImpl::startProcessFlow(const ResourceReaderPtr& resourceReader, const PartitionId& pid,
                                            KeyValueMap& kvMap)
{
    ScopedLock lock(_lock);
    assert(!_brokerFactory);
    assert(!_buildFlow);
    _buildFlow = createBuildFlow(resourceReader);
    _brokerFactory = new FlowFactory(
        createResources(resourceReader, pid, kvMap),
        SwiftClientCreatorPtr(new SwiftClientCreator(_metricProvider ? _metricProvider->GetReporter() : nullptr)));
    BuildFlowMode mode = BuildFlow::getBuildFlowMode(pid.role());
    if (!_buildFlow->startBuildFlow(resourceReader, pid, kvMap, _brokerFactory, mode, SERVICE, _metricProvider)) {
        fillErrorInfo(_buildFlow);
        fillErrorInfo(_brokerFactory);
        DELETE_AND_SET_NULL(_buildFlow);
        DELETE_AND_SET_NULL(_brokerFactory);
        BS_LOG(ERROR, "start buildflow failed");
        return false;
    }
    _startTimestamp = TimeUtility::currentTime();
    CounterMapPtr counterMap = _buildFlow->getCounterMap();
    if (!counterMap) {
        BS_LOG(ERROR, "get counterMap failed");
    }
    _consumedDocCountCounter = GET_ACC_COUNTER(counterMap, consumedDocCount);

    if (!_consumedDocCountCounter) {
        BS_LOG(ERROR, "get consumedDocCount counter failed");
    } else {
        _baseDocCount = _consumedDocCountCounter->Get();
        BS_LOG(INFO, "baseDocCount : %lu, currentTime: %lu", _baseDocCount, _startTimestamp);
    }

    // string msg = "start process end, current[" + _current.ShortDebugString() + "]";
    // BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, msg);
    return true;
}

workflow::BuildFlow* ProcessorServiceImpl::createBuildFlow(const ResourceReaderPtr& resourceReader)
{
    if (_isTablet) {
        return new BuildFlow(std::shared_ptr<indexlibv2::config::ITabletSchema>(), workflow::BuildFlowThreadResource());
    }
    return new BuildFlow();
}

bool ProcessorServiceImpl::updateConfig(const string& configPath)
{
    BS_LOG(INFO, "change config from [%s] to [%s]", _current.targetstatus().configpath().c_str(), configPath.c_str());
    BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, "processor update config.");
    ProcessorTarget target = _current.targetstatus();
    target.set_configpath(configPath);
    if (restartProcessor(target)) {
        ScopedLock lock(_lock);
        _current.mutable_targetstatus()->set_configpath(configPath);
        _restartIntervalController.reset();
        return true;
    } else {
        string errorMsg = "update config failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, "processor update config fail.");
        return false;
    }
}

bool ProcessorServiceImpl::restartProcessor(const ProcessorTarget& target)
{
    {
        ScopedLock lock(_lock);
        if (_buildFlow) {
            _buildFlow->stop(StopOption::SO_INSTANT);
        }
        DELETE_AND_SET_NULL(_buildFlow);
        DELETE_AND_SET_NULL(_brokerFactory);
    }
    return startProcessor(target);
}

void ProcessorServiceImpl::suspendProcessor(const ProcessorTarget& target)
{
    BS_LOG(INFO, "suspend processor, target[%s]", target.ShortDebugString().c_str());

    string msg = "suspend processor, target[" + target.ShortDebugString() + "]";
    BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, msg);

    assert(_buildFlow);
    assert(target.has_suspendtimestamp());
    if (!_buildFlow->suspendReadAtTimestamp(target.suspendtimestamp(), target.suspendtimestamp(),
                                            common::ETA_SUSPEND)) {
        string errorMsg = "suspend reader failed";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, errorMsg);
        return;
    }
    ScopedLock lock(_lock);
    *_current.mutable_targetstatus() = target;
}

bool ProcessorServiceImpl::getDataDescription(const string& dataDescriptionStr, DataDescription& dataDescription)
{
    try {
        autil::legacy::FromJsonString(dataDescription, dataDescriptionStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        string errorMsg =
            "Invalid json string for dataDescription[" + dataDescriptionStr + "] : error [" + string(e.what()) + "]";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg.c_str(), BS_STOP);
        return false;
    }
    return true;
}

bool ProcessorServiceImpl::getStopTimestamp(const ProcessorTarget& target, const DataDescription& dataDescription,
                                            int64_t& stopTimestamp)
{
    stopTimestamp = -1;
    if (target.has_stoptimestamp()) {
        stopTimestamp = target.stoptimestamp();
    }

    const auto& iter = dataDescription.find(SWIFT_STOP_TIMESTAMP);
    if (iter != dataDescription.end()) {
        int64_t timestamp;
        if (StringUtil::fromString(iter->second, timestamp) && timestamp < stopTimestamp) {
            stopTimestamp = timestamp;
        }
    }
    return stopTimestamp != -1;
}

void ProcessorServiceImpl::stopProcessor(const ProcessorTarget& target)
{
    BS_LOG(INFO, "stopping processor, target[%s]", target.ShortDebugString().c_str());

    // string msg = "stopping processor, target[" + target.ShortDebugString() + "]";
    // BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);

    assert(_buildFlow);
    string dataDescriptionStr = target.datadescription();
    DataDescription dataDescription;
    if (!getDataDescription(dataDescriptionStr, dataDescription)) {
        return;
    }
    int64_t stopTimestamp;
    if (!getStopTimestamp(target, dataDescription, stopTimestamp)) {
        return;
    }
    if (!_buildFlow->suspendReadAtTimestamp(stopTimestamp, stopTimestamp, common::ETA_STOP)) {
        string errorMsg = "stop reader failed";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, errorMsg);
        return;
    }

    if (!_buildFlow->waitFinish()) {
        string errorMsg = "Stop processor failed!";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, errorMsg);
        return;
    }
    ScopedLock lock(_lock);
    *_current.mutable_targetstatus() = target;
    _current.set_status(WS_STOPPED);
    _current.set_datasourcefinish(_buildFlow->getReader()->isEof());

    DELETE_AND_SET_NULL(_buildFlow);
    DELETE_AND_SET_NULL(_brokerFactory);

    string msg = "processor has been stopped, current status is: [" + _current.ShortDebugString() + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    BS_LOG(INFO, "processor has been stopped, current status is: [%s]", _current.ShortDebugString().c_str());
}

ProcessorServiceImpl::CommandType ProcessorServiceImpl::getCommandType(const ProcessorTarget& target,
                                                                       const ProcessorCurrent& current) const
{
    if (!target.has_configpath()) {
        BS_LOG(INFO, "target is none, do nothing");
        return ProcessorServiceImpl::CT_NONE;
    }
    if (!current.has_targetstatus()) {
        return ProcessorServiceImpl::CT_START;
    }
    const ProcessorTarget& currentTarget = current.targetstatus();
    if (currentTarget.configpath() != target.configpath()) {
        return ProcessorServiceImpl::CT_UPDATE_CONFIG;
    }
    if (currentTarget.datadescription() != target.datadescription()) {
        return ProcessorServiceImpl::CT_NONE;
    }

    if (target.has_suspendtimestamp()) {
        if (currentTarget.has_stoptimestamp()) {
            return target.suspendtimestamp() >= currentTarget.stoptimestamp() ? ProcessorServiceImpl::CT_START
                                                                              : ProcessorServiceImpl::CT_NONE;
        }

        if (currentTarget.has_suspendtimestamp()) {
            return currentTarget.suspendtimestamp() < target.suspendtimestamp() ? ProcessorServiceImpl::CT_SUSPEND
                                                                                : ProcessorServiceImpl::CT_NONE;
        }
        BS_LOG(ERROR, "target has suspend time when current.target has no suspend time; drop it!");
        return ProcessorServiceImpl::CT_NONE;
    }

    if (target.has_stoptimestamp()) {
        if (currentTarget.has_stoptimestamp()) {
            return currentTarget.stoptimestamp() >= target.stoptimestamp() ? ProcessorServiceImpl::CT_NONE
                                                                           : ProcessorServiceImpl::CT_STOP;
        }

        if (currentTarget.has_suspendtimestamp()) {
            return currentTarget.suspendtimestamp() == target.stoptimestamp() ? ProcessorServiceImpl::CT_STOP
                                                                              : ProcessorServiceImpl::CT_NONE;
        }

        return ProcessorServiceImpl::CT_STOP;
    }

    return ProcessorServiceImpl::CT_START;
}

bool ProcessorServiceImpl::needSuspendTask(const std::string& state)
{
    ProcessorTarget target;
    if (!target.ParseFromString(state)) {
        string errorMsg = "failed to deserialize builder target state[" + state + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (target.has_suspendtask() && target.suspendtask() == true) {
        BS_LOG(WARN, "suspend task cmd received!");
        return true;
    }
    return false;
}

bool ProcessorServiceImpl::getTargetClusterNames(const ResourceReaderPtr& resourceReader, const PartitionId& pid,
                                                 vector<string>& targetClusterNames)
{
    // fill partitionId
    ProcessorTaskIdentifier identifier;
    if (!pid.has_taskid()) {
        string errorMsg = "lack of taskid for ProcessWorker";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg.c_str(), BS_STOP);
        return false;
    }
    if (!identifier.fromString(pid.taskid())) {
        string errorMsg = "invalid taskid[" + pid.taskid() + "] for ProcessWorker";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg.c_str(), BS_STOP);
        return false;
    }

    string clusterName;
    identifier.getClusterName(clusterName);
    if (!clusterName.empty()) {
        // set clusterName for single updatable processor task
        if (!checkSingleUpdatableClusterName(resourceReader, pid, clusterName)) {
            return false;
        }
        targetClusterNames.push_back(clusterName);
        return true;
    }

    vector<string> clusterNames;
    if (!resourceReader->getAllClusters(pid.buildid().datatable(), clusterNames)) {
        string errorMsg = "getClusterNames for " + resourceReader->getConfigPath() + " failed";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg.c_str(), BS_STOP);
        return false;
    }
    if (pid.step() == BUILD_STEP_FULL || clusterNames.size() == 1) {
        // full processor & single cluster senario
        targetClusterNames.insert(targetClusterNames.end(), clusterNames.begin(), clusterNames.end());
        return true;
    }

    // multi cluster default processor group for BUILD_STEP_INC
    return getTargetClustersForDefaultProcessorGroup(resourceReader, pid, targetClusterNames);
}

bool ProcessorServiceImpl::checkSingleUpdatableClusterName(const ResourceReaderPtr& resourceReader,
                                                           const PartitionId& pid, const string& clusterName)
{
    if (pid.step() == BUILD_STEP_FULL) {
        string errorMsg = "clusterName [" + clusterName + "]should not be set in taskId for full build step!";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg.c_str(), BS_STOP);
        return false;
    }

    vector<string> clusterNames;
    if (!resourceReader->getAllClusters(pid.buildid().datatable(), clusterNames)) {
        string errorMsg = "getClusterNames for " + resourceReader->getConfigPath() + " failed";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg.c_str(), BS_STOP);
        return false;
    }

    if (find(clusterNames.begin(), clusterNames.end(), clusterName) == clusterNames.end()) {
        string errorMsg = "target clusterName [" + clusterName + "] not found in dataTable!";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg.c_str(), BS_STOP);
        return false;
    }

    vector<string> updatableSchemaClusters;
    if (!ProcessorConfigurator::getSchemaUpdatableClusters(resourceReader, pid.buildid().datatable(),
                                                           updatableSchemaClusters)) {
        return false;
    }

    if (find(updatableSchemaClusters.begin(), updatableSchemaClusters.end(), clusterName) ==
        updatableSchemaClusters.end()) {
        string errorMsg = "target clusterName [" + clusterName + "] not schema updatable!";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg.c_str(), BS_STOP);
        return false;
    }
    return true;
}

bool ProcessorServiceImpl::getTargetClustersForDefaultProcessorGroup(const ResourceReaderPtr& resourceReader,
                                                                     const PartitionId& pid,
                                                                     vector<string>& clusterNames)
{
    if (!ProcessorConfigurator::getSchemaNotUpdatableClusters(resourceReader, pid.buildid().datatable(),
                                                              clusterNames)) {
        string errorMsg = "get clusterNames for default processor group fail!";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg.c_str(), BS_STOP);
        return false;
    }

    if (clusterNames.empty()) {
        string errorMsg = "no cluster found for default processor group!";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg.c_str(), BS_STOP);
        return false;
    }
    return true;
}

void ProcessorServiceImpl::doGetResourceKeeperMap(common::ResourceKeeperMap& usingKeepers)
{
    if (_brokerFactory) {
        _brokerFactory->collectUsingResources(usingKeepers);
    }
}

void ProcessorServiceImpl::updateResourceKeeperMap(bool& needRestart)
{
    if (_brokerFactory) {
        auto resources = getCurrentResourceKeeperMap();
        _brokerFactory->updateUsingResources(resources, needRestart);
    }
}

bool ProcessorServiceImpl::parseTargetDesc(const ResourceReaderPtr& resourceReader, const PartitionId& pid,
                                           const ProcessorTarget& target, PartitionId& newPid, KeyValueMap& output)
{
    // fill partitionId
    vector<string> clusterNames;
    if (target.has_targetdescription()) {
        KeyValueMap kvMap;
        try {
            FromJsonString(kvMap, target.targetdescription());
        } catch (const autil::legacy::ExceptionBase& e) {
            BS_LOG(ERROR, "INVALID target json str[%s]", target.targetdescription().c_str());
            return false;
        }
        auto iter = kvMap.find("output");
        if (iter != kvMap.end()) {
            ProcessorOutput outputInfo;
            try {
                FromJsonString(outputInfo, iter->second);
                auto outputMap = outputInfo.getOutput();
                for (auto it = outputMap.begin(); it != outputMap.end(); ++it) {
                    output[FlowFactory::clusterOutputKey(it->first)] = ToJsonString(it->second);
                    BS_LOG(INFO, "get output for cluster [%s], config [%s]", it->first.c_str(),
                           ToJsonString(it->second).c_str());
                }
            } catch (const autil::legacy::ExceptionBase& e) {
                BS_LOG(ERROR, "INVALID target json str[%s]", target.targetdescription().c_str());
                return false;
            }
        }
        iter = kvMap.find("clusterNames");
        if (iter == kvMap.end()) {
            BS_LOG(ERROR, "INVALID target json str[%s]", target.targetdescription().c_str());
            return false;
        }
        StringUtil::fromString(iter->second, clusterNames, ";");
        if (clusterNames.size() == 0) {
            BS_LOG(ERROR, "INVALID target json str[%s]", target.targetdescription().c_str());
            return false;
        }

        auto fillFunction = [&](const std::string& oldKey, const std::string& newKey) {
            auto tmpIter = kvMap.find(oldKey);
            if (tmpIter != kvMap.end()) {
                output[newKey] = tmpIter->second;
            }
        };

        fillFunction("configName", PROCESSOR_CONFIG_NODE);
        fillFunction("rawDocQuery", RAW_DOCUMENT_QUERY_STRING);
        fillFunction(BATCH_MASK, BATCH_MASK);
        fillFunction(SYNC_LOCATOR_FROM_COUNTER, SYNC_LOCATOR_FROM_COUNTER);
        fillFunction(SWIFT_MAJOR_VERSION, SWIFT_MAJOR_VERSION);
        fillFunction(SWIFT_MINOR_VERSION, SWIFT_MINOR_VERSION);
        if (kvMap.find(SWIFT_MAJOR_VERSION) != kvMap.end() && kvMap.find(SWIFT_MINOR_VERSION) != kvMap.end()) {
            output[SWIFT_PROCESSOR_WRITER_NAME] =
                config::SwiftConfig::getProcessorWriterName(pid.range().from(), pid.range().to());
        }

        iter = kvMap.find("isTablet");
        if (iter != kvMap.end()) {
            if (!autil::StringUtil::parseTrueFalse(iter->second, _isTablet)) {
                BS_LOG(ERROR, "parse is tablet [%s] failed", iter->second.c_str());
                return false;
            }
        }
    } else {
        if (!getTargetClusterNames(resourceReader, pid, clusterNames)) {
            return false;
        }
    }

    newPid = pid;
    newPid.clear_clusternames();
    for (size_t i = 0; i < clusterNames.size(); i++) {
        BS_LOG(INFO, "add cluster [%s] to current processor", clusterNames[i].c_str());
        *newPid.add_clusternames() = clusterNames[i];
    }

    BEEPER_ADD_GLOBAL_TAG("clusters", StringUtil::toString(clusterNames, " "));
    return true;
}
}} // namespace build_service::worker
