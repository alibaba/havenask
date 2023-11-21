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
#include "build_service/worker/BuilderServiceImpl.h"

#include <assert.h>
#include <iostream>
#include <map>
#include <memory>
#include <stddef.h>
#include <unordered_map>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "beeper/beeper.h"
#include "build_service/builder/Builder.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/CounterSynchronizerCreator.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/common/ExceedTsAction.h"
#include "build_service/common/Locator.h"
#include "build_service/common/NetworkTrafficEstimater.h"
#include "build_service/common/ResourceKeeperCreator.h"
#include "build_service/common/SwiftResourceKeeper.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/config/TaskInputConfig.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/task_base/RestartIntervalController.h"
#include "build_service/task_base/UpdateLocatorTaskItem.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/SwiftClientCreator.h"
#include "build_service/workflow/AsyncStarter.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "build_service/workflow/Workflow.h"
#include "build_service/workflow/WorkflowItem.h"
#include "indexlib/base/Progress.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/TaskItem.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"

using namespace indexlib::common;
using namespace indexlib::util;

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::reader;
using namespace build_service::workflow;
using namespace build_service::common;

namespace build_service { namespace worker {
BS_LOG_SETUP(worker, BuilderServiceImpl);

BuilderServiceImpl::BuilderServiceImpl(const PartitionId& pid, indexlib::util::MetricProviderPtr metricProvider,
                                       const LongAddress& address, const string& appZkRoot,
                                       const string& adminServiceName, const std::string& epochId)
    : WorkerStateHandler(pid, metricProvider, appZkRoot, adminServiceName, epochId)
    , _buildFlow(NULL)
    , _brokerFactory(NULL)
    , _updateLocatorTaskId(TaskScheduler::INVALID_TASK_ID)
    , _startTimestamp(-1)
    , _baseDocCount(-1)
    , _workerPathVersion(-1)
{
    *_current.mutable_longaddress() = address;
}

BuilderServiceImpl::~BuilderServiceImpl()
{
    _taskScheduler.reset();
    DELETE_AND_SET_NULL(_buildFlow);
    DELETE_AND_SET_NULL(_brokerFactory);
}

bool BuilderServiceImpl::init()
{
    bool ret = true;
    ret = _cpuEstimater.Start(/*sampleCountLimit=*/5, /*checkInterval=*/60);
    if (!ret) {
        return false;
    }
    ret = _networkEstimater.Start();
    return ret;
}

void BuilderServiceImpl::doHandleTargetState(const string& state, bool hasResourceUpdated)
{
    BuilderTarget target;
    if (!target.ParseFromString(state)) {
        string errorMsg = "failed to deserialize builder target state[" + state + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return;
    }

    {
        ScopedLock lock(_lock);
        if (target.has_configpath()) {
            if (_configPath != target.configpath()) {
                _restartIntervalController.reset();
            }
            _configPath = target.configpath();
        }
    }
    BS_INTERVAL_LOG(300, INFO, "target status: %s", target.ShortDebugString().c_str());
    BuilderServiceImpl::CommandType cmdType = getCommandType(target, _current);

    switch (cmdType) {
    case BuilderServiceImpl::CT_START_BUILD: {
        if (startBuild(target)) {
            _restartIntervalController.reset();
        }
        break;
    }
    case BuilderServiceImpl::CT_STOP_BUILD: {
        stopBuild(target);
    }
    default:
        break;
    }
}

bool BuilderServiceImpl::needRestartProcess(const string& state)
{
    BuilderTarget target;
    if (!target.ParseFromString(state)) {
        string errorMsg = "failed to deserialize builder target state[" + state + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (checkUpdateConfig(target)) {
        return true;
    }

    if (!_buildFlow) {
        // not finish start build
        return false;
    }

    if (_pid.step() == BUILD_STEP_FULL) {
        // full build no need restart for checking path version fail
        return false;
    }
    return !checkWorkerPathVersion(target);
}

bool BuilderServiceImpl::needSuspendTask(const string& state)
{
    BuilderTarget target;
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
void BuilderServiceImpl::fillProgressStatus(BuildFlow* buildFlow, BuilderCurrent& current)
{
    current.mutable_progressstatus()->set_reporttimestamp(TimeUtility::currentTime());
    if (!buildFlow) {
        return;
    }
    if (!buildFlow->isStarted()) {
        return;
    }
    current.mutable_progressstatus()->set_starttimestamp(_startTimestamp);
    if (!_totalDocCountCounter) {
        current.mutable_progressstatus()->set_startpoint(INVALID_PROGRESS);
        current.mutable_progressstatus()->set_progress(INVALID_PROGRESS);
        return;
    }
    current.mutable_progressstatus()->set_startpoint(_baseDocCount);
    current.mutable_progressstatus()->set_progress(_totalDocCountCounter->Get());

    builder::Builder* builder = buildFlow->getBuilder();
    if (builder) {
        common::Locator locator;
        if (builder->getLastLocator(locator)) {
            current.mutable_progressstatus()->set_locatortimestamp(locator.GetOffset().first);
        }
    }
}

void BuilderServiceImpl::getCurrentState(string& content)
{
    ScopedLock lock(_lock);
    if (isSuspended()) {
        _current.set_issuspended(true);
    }
    fillCurrentState(_buildFlow, _current);
    fillProgressStatus(_buildFlow, _current);
    fillProtocolVersion(_current);
    fillCpuSpeed(_current);
    fillNetworkTraffic(_current);
    saveCurrent(_current, content);
}

bool BuilderServiceImpl::hasFatalError()
{
    ScopedLock lock(_lock);
    return _hasFatalError || (_buildFlow && _buildFlow->hasFatalError());
}

bool BuilderServiceImpl::parseReservedVersions(const BuilderTarget& target, KeyValueMap& kvMap)
{
    // read reserved_cluster_checkpoints
    string reservedCCP = "";
    if (target.has_reservedversions()) {
        kvMap[RESERVED_VERSION_LIST] = target.reservedversions();
        BS_LOG(INFO, "BuilderTarget.reservedversions: [%s]", target.reservedversions().c_str());
    } else if (readReservedClusterCheckpoints(_pid, reservedCCP)) {
        kvMap[RESERVED_CLUSTER_CHECKPOINT_LIST] = reservedCCP;
        BS_LOG(INFO, "BuilderTarget has no reservedversions. read it from zk. reserved_version_list: [%s]",
               reservedCCP.c_str());
    } else {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "read reserved_checkpoints failed", BS_RETRY);
        return false;
    }
    return true;
}

bool BuilderServiceImpl::startBuild(const BuilderTarget& target)
{
    if (_buildFlow) {
        return true;
    }
    if (!_restartIntervalController) {
        _restartIntervalController.reset(new task_base::RestartIntervalController());
    }
    _restartIntervalController->wait();
    BS_LOG(INFO, "start build begin, target[%s]", target.ShortDebugString().c_str());

    string msg = "start build begin, target[" + target.ShortDebugString() + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);

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

    config::ControlConfig controlConfig;
    if (!resourceReader->getDataTableConfigWithJsonPath(_pid.buildid().datatable(), "control_config", controlConfig)) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "get ControlConfig failed", BS_STOP);
        return false;
    }

    bool isCompatibleWithBs18 = false;
    KeyValueMap kvMap;
    if (!parseTargetDescription(target, kvMap)) {
        BS_LOG(WARN, "parse BuilderTarget.targetDescription failed. ReTry with version(1.8) parsing.");
        kvMap.clear();
        if (!parseTargetDescriptionCompatibleWithBs_1_8(target, kvMap)) {
            BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, "parse target description fail");
            return false;
        }
        isCompatibleWithBs18 = true;
    }
    // read reserved_cluster_checkpoints
    if (!parseReservedVersions(target, kvMap)) {
        return false;
    }

    // overwrite CounterConfig
    _counterConfig.reset(new CounterConfig());
    *_counterConfig = serviceConfig.counterConfig;
    if (!overWriteCounterConfig(_pid, *_counterConfig)) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "rewrite CounterConfig failed", BS_STOP);
        return false;
    }

    kvMap[CONFIG_PATH] = localConfigPath;
    kvMap[INDEX_ROOT_PATH] = serviceConfig.getBuilderIndexRoot(_pid.step() == BUILD_STEP_FULL);

    auto it = kvMap.find(DATA_DESCRIPTION_KEY);

    if (isCompatibleWithBs18) {
        BS_LOG(INFO, "parse data description with CompatibleWithBs1.8");
        assert(_pid.clusternames_size() == 1);
        const std::string& clusterName = _pid.clusternames(0);
        if (!controlConfig.isIncProcessorExist(clusterName) && _pid.step() == BUILD_STEP_INC && it == kvMap.end()) {
            FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "admin target conflicts with ControlConfig", BS_STOP);
            return false;
        }
    }

    if (it == kvMap.end()) {
        kvMap[PROCESSED_DOC_SWIFT_ROOT] = serviceConfig.getSwiftRoot(_pid.step() == BUILD_STEP_FULL);
        if (target.has_stoptimestamp()) {
            kvMap[PROCESSED_DOC_SWIFT_STOP_TIMESTAMP] = StringUtil::toString(target.stoptimestamp());
        }
    } else {
        if (target.has_stoptimestamp()) {
            kvMap[SWIFT_STOP_TIMESTAMP] = StringUtil::toString(target.stoptimestamp());
        }
    }

    int64_t buildVersionTimestamp = 0;
    if (getBuildVersionTimestamp(target, buildVersionTimestamp)) {
        kvMap[BUILD_VERSION_TIMESTAMP] = StringUtil::toString(buildVersionTimestamp);
    }

    kvMap[CHECKPOINT] = StringUtil::toString(target.starttimestamp());
    if (target.has_workerpathversion()) {
        _workerPathVersion = target.workerpathversion();
        kvMap[WORKER_PATH_VERSION] = StringUtil::toString(target.workerpathversion());
    }

    if (!_epochId.empty()) {
        kvMap[BUILDER_EPOCH_ID] = _epochId;
    }

    ScopedLock lock(_lock);
    startBuildFlow(resourceReader, _pid, kvMap);

    *_current.mutable_targetstatus() = target;
    _current.mutable_targetstatus()->clear_stoptimestamp();
    BS_LOG(INFO, "start build end, current[%s]", _current.ShortDebugString().c_str());
    msg = "start build end, current[" + _current.ShortDebugString() + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    return true;
}

void BuilderServiceImpl::startBuildFlow(const ResourceReaderPtr& resourceReader, const PartitionId& pid,
                                        KeyValueMap& kvMap)
{
    DELETE_AND_SET_NULL(_buildFlow);
    DELETE_AND_SET_NULL(_brokerFactory);

    _brokerFactory = createFlowFactory(resourceReader, pid, kvMap);
    _buildFlow = createBuildFlow();

    BuildFlowMode mode = BuildFlow::getBuildFlowMode(pid.role());
    if (kvMap.find(DATA_DESCRIPTION_KEY) != kvMap.end()) {
        mode = BuildFlowMode::ALL;
    }

    if (!_buildFlow->startBuildFlow(resourceReader, pid, kvMap, _brokerFactory, mode, SERVICE, _metricProvider)) {
        fillErrorInfo(_buildFlow);
        fillErrorInfo(_brokerFactory);
        // if can't connect to swift, retry without exit
        ServiceErrorCode errorCode = _brokerFactory->getServiceErrorCode();
        if (errorCode == BUILDFLOW_ERROR_CONNECT_SWIFT || errorCode == BUILDFLOW_ERROR_SEEK ||
            errorCode == BUILDFLOW_ERROR_SWIFT_CREATE_READER || errorCode == BUILDFLOW_ERROR_SWIFT_CREATE_WRITER) {
            DELETE_AND_SET_NULL(_buildFlow);
            DELETE_AND_SET_NULL(_brokerFactory);
            return;
        }
        BS_LOG(ERROR, "start build failed, builder exit");
        setFatalError();
        return;
    }

    CounterMapPtr counterMap = _buildFlow->getCounterMap();
    if (!counterMap) {
        BS_LOG(ERROR, "counter map is NULL");
    }

    _totalDocCountCounter = GET_ACC_COUNTER(counterMap, totalDocCount);
    if (!_totalDocCountCounter) {
        BS_LOG(ERROR, "get totalDocCountCounter failed");
    } else {
        _baseDocCount = _totalDocCountCounter->Get();
    }

    _startTimestamp = TimeUtility::currentTime();

    if (_counterConfig && counterMap) {
        _counterSynchronizer.reset(common::CounterSynchronizerCreator::create(counterMap, _counterConfig));
        if (!_counterSynchronizer) {
            BS_LOG(ERROR, "fail to init counterSynchronizer");
        } else {
            if (!_counterSynchronizer->beginSync()) {
                BS_LOG(ERROR, "fail to start sync counter thread");
                _counterSynchronizer.reset();
            }
        }
    }

    if (!prepareIntervalTask(resourceReader, mode, _buildFlow, pid)) {
        FILL_ERRORINFO(BUILDER_ERROR_UNKNOWN, "prepare interval task failed, builder exit", BS_STOP);
        setFatalError();
        return;
    }
}

common::ResourceKeeperMap BuilderServiceImpl::createResources(const ResourceReaderPtr& resourceReader,
                                                              const PartitionId& pid, KeyValueMap& kvMap)
{
    common::ResourceKeeperMap ret;
    for (int32_t i = 0; i < pid.clusternames_size(); ++i) {
        const string& clusterName = pid.clusternames(i);
        config::TaskInputConfig config;
        config.setType("dependResource");
        config.addParameters("resourceName", clusterName);
        ResourceKeeperPtr keeper = ResourceKeeperCreator::create(clusterName, "swift", nullptr);
        auto swiftKeeper = DYNAMIC_POINTER_CAST(SwiftResourceKeeper, keeper);
        swiftKeeper->initLegacy(clusterName, pid, resourceReader, kvMap);
        ret[clusterName] = DYNAMIC_POINTER_CAST(ResourceKeeper, keeper);
        kvMap[FlowFactory::clusterInputKey(clusterName)] = ToJsonString(config);
    }
    return ret;
}

workflow::FlowFactory* BuilderServiceImpl::createFlowFactory(const ResourceReaderPtr& resourceReader,
                                                             const PartitionId& pid, KeyValueMap& kvMap)
{
    return new FlowFactory(
        createResources(resourceReader, pid, kvMap),
        SwiftClientCreatorPtr(new SwiftClientCreator(_metricProvider ? _metricProvider->GetReporter() : nullptr)));
}

workflow::BuildFlow* BuilderServiceImpl::createBuildFlow() { return new BuildFlow(); }

bool BuilderServiceImpl::getBuildVersionTimestamp(const proto::BuilderTarget& target, int64_t& versionStopTimestamp)
{
    if (!target.has_stoptimestamp()) {
        return false;
    }
    versionStopTimestamp = target.stoptimestamp();
    KeyValueMap kvMap;
    if (!parseTargetDescription(target, kvMap)) {
        BS_LOG(WARN, "parse BuilderTarget.targetDescription failed. ReTry with version(1.8) parsing.");
        kvMap.clear();
        if (!parseTargetDescriptionCompatibleWithBs_1_8(target, kvMap)) {
            return false;
        }
    }
    auto iter = kvMap.find(VERSION_TIMESTAMP);
    if (iter != kvMap.end()) {
        if (!StringUtil::fromString(iter->second, versionStopTimestamp)) {
            return false;
        }
    }
    return true;
}

void BuilderServiceImpl::stopBuild(const BuilderTarget& target)
{
#define SET_FATAL_AND_RETURN                                                                                           \
    do {                                                                                                               \
        ScopedLock lock(_lock);                                                                                        \
        fillErrorInfo(_buildFlow);                                                                                     \
        fillErrorInfo(_brokerFactory);                                                                                 \
        if (_taskScheduler && _updateLocatorTaskId != TaskScheduler::INVALID_TASK_ID) {                                \
            _taskScheduler->DeleteTask(_updateLocatorTaskId);                                                          \
        }                                                                                                              \
        _taskScheduler.reset();                                                                                        \
        _hasFatalError = true;                                                                                         \
        DELETE_AND_SET_NULL(_buildFlow);                                                                               \
        DELETE_AND_SET_NULL(_brokerFactory);                                                                           \
        AUTIL_LOG(ERROR, "stop builder failed, exit!");                                                                \
        setFatalError();                                                                                               \
        return;                                                                                                        \
    } while (0)

    if (!_buildFlow) {
        if (startBuild(target)) {
            _restartIntervalController.reset();
        }
        return;
    }

    int64_t versionTimestamp = 0;
    if (!getBuildVersionTimestamp(target, versionTimestamp)) {
        SET_FATAL_AND_RETURN;
    }

    if (!_buildFlow->suspendReadAtTimestamp(target.stoptimestamp(), versionTimestamp, common::ETA_STOP)) {
        FILL_ERRORINFO(BUILDER_ERROR_UNKNOWN, "suspend builder failed", BS_RETRY);
        return;
    }

    string msg = "waiting builder stop, target [" + target.ShortDebugString() + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);

    // BuildFlow will retry itself when SERVICE mode
    // delete updateLocatorTask before buildFlow, because buildFlow hold raw pointers of builder & producer
    if (!_buildFlow->waitFinish()) {
        SET_FATAL_AND_RETURN;
    }

    ScopedLock lock(_lock);
    if (_taskScheduler && _updateLocatorTaskId != TaskScheduler::INVALID_TASK_ID) {
        _taskScheduler->DeleteTask(_updateLocatorTaskId);
    }
    _taskScheduler.reset();
    DELETE_AND_SET_NULL(_buildFlow);
    DELETE_AND_SET_NULL(_brokerFactory);

    *_current.mutable_targetstatus() = target;
    _current.set_status(WS_STOPPED);

    msg = "builder has been stopped, current status is: [" + _current.ShortDebugString() + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);

    BS_LOG(INFO, "builder has been stopped, current status is: [%s]", _current.ShortDebugString().c_str());
}

BuilderServiceImpl::CommandType BuilderServiceImpl::getCommandType(const BuilderTarget& target,
                                                                   const BuilderCurrent& current) const
{
    if (!target.has_configpath()) {
        BS_LOG(INFO, "target is none, do nothing");
        return BuilderServiceImpl::CT_NONE;
    }
    if (!target.has_stoptimestamp()) {
        return BuilderServiceImpl::CT_START_BUILD;
    } else {
        const BuilderTarget& currentTarget = current.targetstatus();
        if (target == currentTarget) {
            return BuilderServiceImpl::CT_NONE;
        } else {
            return BuilderServiceImpl::CT_STOP_BUILD;
        }
    }
}

bool BuilderServiceImpl::prepareIntervalTask(const ResourceReaderPtr& resourceReader, BuildFlowMode mode,
                                             workflow::BuildFlow* buildFlow, const proto::PartitionId& pid)
{
    if (!buildFlow) {
        BS_LOG(ERROR, "prepare interval task for builder pid [%s] has no buildFlow", pid.ShortDebugString().c_str());
        return false;
    }
    if (!(mode & BuildFlowMode::BUILDER)) {
        return true;
    }
    if (pid.clusternames_size() != 1) {
        BS_LOG(ERROR, "pid [%s] has more than one clustername", pid.ShortDebugString().c_str());
        return false;
    }
    if (!pid.has_step()) {
        BS_LOG(ERROR, "pid[%s] has no build step", pid.ShortDebugString().c_str());
        return false;
    }

    string clusterName = pid.clusternames(0);
    SwiftConfig swiftConfig;
    if (!resourceReader->getConfigWithJsonPath(ResourceReader::getClusterConfRelativePath(clusterName), "swift_config",
                                               swiftConfig)) {
        string errorMsg = "failed to parse swift_config for cluster: " + clusterName;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    SwiftTopicConfigPtr swiftTopicConfig;
    if (pid.step() == BUILD_STEP_FULL) {
        swiftTopicConfig = swiftConfig.getFullBrokerTopicConfig();
    } else {
        swiftTopicConfig = swiftConfig.getIncBrokerTopicConfig();
    }

    if (!swiftTopicConfig) {
        BS_LOG(ERROR, "pid[%s] has no swift topic config", pid.ShortDebugString().c_str());
        return false;
    }

    auto topicMode = swiftTopicConfig->get().topicmode();
    if (topicMode == swift::protocol::TOPIC_MODE_MEMORY_PREFER) {
        auto workflow = buildFlow->getInputFlow();
        if (!workflow) {
            BS_LOG(ERROR, "no ProcessedDocWorkflow in build flow");
            return false;
        }
        SwiftProcessedDocProducer* producer = dynamic_cast<SwiftProcessedDocProducer*>(workflow->getProducer());
        builder::Builder* builder = buildFlow->getBuilder();
        if (!producer) {
            BS_LOG(ERROR, "producer is NULL in buildFlow");
            return false;
        }
        if (!builder) {
            BS_LOG(ERROR, "builder is NULL in buildFlow");
            return false;
        }

        _taskScheduler.reset(new indexlib::util::TaskScheduler());
        if (!_taskScheduler->DeclareTaskGroup("update_locator", UPDATE_LOCATOR_INTERVAL)) {
            return false;
        }
        TaskItemPtr taskItem(new task_base::UpdateLocatorTaskItem(producer, builder, _metricProvider));
        _updateLocatorTaskId = _taskScheduler->AddTask("update_locator", taskItem);
        if (_updateLocatorTaskId == TaskScheduler::INVALID_TASK_ID) {
            return false;
        }
        BS_LOG(INFO, "update locator thread started");
    }
    return true;
}

bool BuilderServiceImpl::checkWorkerPathVersion(const BuilderTarget& target)
{
    int32_t targetWorkerPathVersion = -1;
    if (target.has_workerpathversion()) {
        targetWorkerPathVersion = target.workerpathversion();
    }
    if (targetWorkerPathVersion != _workerPathVersion) {
        string errorMsg = "target workerPathVersion [" + StringUtil::toString(targetWorkerPathVersion) +
                          "] not match with current [" + StringUtil::toString(_workerPathVersion) + "].";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        cerr << errorMsg.c_str() << endl;
        return false;
    }
    return true;
}

bool BuilderServiceImpl::checkUpdateConfig(const BuilderTarget& target)
{
    if (!target.has_configpath() || _configPath.empty()) {
        return false;
    }
    string targetConfigPath = target.configpath();
    if (targetConfigPath != _configPath) {
        BS_LOG(INFO, "worker need restart for update config, old configPath[%s], new configPath[%s]",
               _configPath.c_str(), targetConfigPath.c_str());
        return true;
    }
    return false;
}

bool BuilderServiceImpl::parseTargetDescription(const BuilderTarget& target, KeyValueMap& kvMap)
{
    if (!target.has_targetdescription()) {
        return true;
    }
    KeyValueMap tdKVMap;
    try {
        FromJsonString(tdKVMap, target.targetdescription());
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "parse targetDescription [%s] fail", target.targetdescription().c_str());
        return false;
    }

    for (auto it = tdKVMap.begin(); it != tdKVMap.end(); ++it) {
        auto oldValue = kvMap.find(it->first);
        if (oldValue != kvMap.end()) {
            BS_LOG(INFO, "[%s:%s] is already exits, override it with [%s]", oldValue->first.c_str(),
                   oldValue->second.c_str(), it->second.c_str());
        }
        kvMap[it->first] = it->second;
    }
    auto iter = tdKVMap.find(DATA_DESCRIPTION_KEY);
    if (iter != tdKVMap.end()) {
        DataDescription ds;
        try {
            FromJsonString(ds, iter->second);
        } catch (const ExceptionBase& e) {
            BS_LOG(ERROR, "parse dataDescription [%s] fail", iter->second.c_str());
            return false;
        }
        for (auto it = ds.begin(); it != ds.end(); ++it) {
            kvMap[it->first] = it->second;
        }
    }
    return true;
}

bool BuilderServiceImpl::parseTargetDescriptionCompatibleWithBs_1_8(const BuilderTarget& target, KeyValueMap& kvMap)
{
    if (!target.has_targetdescription()) {
        BS_LOG(WARN, "no datadescription defined in BuilderTarget");
        return true;
    }

    DataDescription dataDescription;
    string dataDescriptionStr = target.targetdescription();
    if (dataDescriptionStr.empty()) {
        return true;
    }
    try {
        autil::legacy::FromJsonString(dataDescription, dataDescriptionStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        string errorMsg = "Invalid json string for version(1.8) dataDescription[" + dataDescriptionStr + "] : error [" +
                          string(e.what()) + "]";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg.c_str(), BS_STOP);
        return false;
    }

    if (_pid.step() == BUILD_STEP_INC) {
        for (auto it = dataDescription.begin(); it != dataDescription.end(); ++it) {
            kvMap[it->first] = it->second;
        }
        kvMap[DATA_DESCRIPTION_KEY] = dataDescriptionStr;
    }
    return true;
}

}} // namespace build_service::worker
