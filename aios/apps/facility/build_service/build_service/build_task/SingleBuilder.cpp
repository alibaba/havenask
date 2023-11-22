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
#include "build_service/build_task/SingleBuilder.h"

#include <map>
#include <unordered_map>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "beeper/beeper.h"
#include "build_service/build_task/UpdateLocatorTaskItemV2.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/ExceedTsAction.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common/ResourceKeeperCreator.h"
#include "build_service/common/SwiftResourceKeeper.h"
#include "build_service/common_define.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/DataLinkModeUtil.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/config/TaskInputConfig.h"
#include "build_service/proto/BuildTaskTargetInfo.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RangeUtil.h"
#include "build_service/util/SwiftClientCreator.h"
#include "build_service/workflow/BuildFlowMode.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "build_service/workflow/Workflow.h"
#include "build_service/workflow/WorkflowItem.h"
#include "indexlib/file_system/IDirectory.h"

namespace build_service::build_task {

BS_LOG_SETUP(task_base, SingleBuilder);

SingleBuilder::~SingleBuilder() { reset(); }

bool SingleBuilder::parseTargetDescription(const config::TaskTarget& target, KeyValueMap& kvMap)
{
    std::string content;
    if (!target.getTargetDescription(config::DATA_DESCRIPTION_KEY, content)) {
        BS_LOG(ERROR, "get data description key failed.");
        return false;
    }
    std::string taskTargetInfoStr;
    if (!target.getTargetDescription(config::BS_BUILD_TASK_TARGET, taskTargetInfoStr)) {
        BS_LOG(ERROR, "get build task target failed.");
        return false;
    }

    proto::BuildTaskTargetInfo targetInfo;
    proto::DataDescription ds;
    try {
        autil::legacy::FromJsonString(ds, content);
        autil::legacy::FromJsonString(targetInfo, taskTargetInfoStr);
        _buildStep = targetInfo.buildStep;
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "deserialize data description or task target info failed.");
        return false;
    }
    for (auto it = ds.begin(); it != ds.end(); ++it) {
        kvMap[it->first] = it->second;
    }

    if (isProcessedDocDataDesc(kvMap)) {
        if (_buildStep == "full") {
            kvMap["topicConfigName"] = config::FULL_SWIFT_BROKER_TOPIC_CONFIG;
        } else {
            kvMap["topicConfigName"] = config::INC_SWIFT_BROKER_TOPIC_CONFIG;
        }
        // for start build with stopTimestamp, avoid swift actual stop timestamp > expected
        int64_t stopTimestamp = 0;
        if (getTimestamp(kvMap, "stopTimestamp", stopTimestamp)) {
            kvMap[config::PROCESSED_DOC_SWIFT_STOP_TIMESTAMP] = autil::StringUtil::toString(stopTimestamp);
        }
    } else if (config::DataLinkModeUtil::isDataLinkNPCMode(kvMap)) {
        kvMap["topicConfigName"] = config::RAW_SWIFT_TOPIC_CONFIG;
        // for start build with stopTimestamp, avoid swift actual stop timestamp > expected
        int64_t stopTimestamp = 0;
        if (getTimestamp(kvMap, "stopTimestamp", stopTimestamp)) {
            kvMap[config::SWIFT_STOP_TIMESTAMP] = autil::StringUtil::toString(stopTimestamp);
        }
    }
    kvMap["clusterName"] = _taskInitParam.clusterName;
    kvMap[config::CONFIG_PATH] = _taskInitParam.resourceReader->getConfigPath();
    config::BuildServiceConfig serviceConfig;
    if (!_taskInitParam.resourceReader->getConfigWithJsonPath("build_app.json", "", serviceConfig)) {
        REPORT_ERROR_WITH_ADVICE(proto::SERVICE_ERROR_CONFIG, std::string("failed to parse build_app.json"),
                                 proto::BS_STOP);
        return false;
    }
    kvMap[config::INDEX_ROOT_PATH] = serviceConfig.getBuilderIndexRoot(_buildStep == "full");
    return true;
}

bool SingleBuilder::startBuild(const config::TaskTarget& target)
{
    std::lock_guard<std::mutex> lock(_lock);
    if (_buildFlow != nullptr) {
        return true;
    }

    if (!_restartIntervalController) {
        _restartIntervalController.reset(new task_base::RestartIntervalController());
    }
    _restartIntervalController->wait();

    std::string msg = "start build begin, target[" + autil::legacy::ToJsonString(target) + "]";
    BS_LOG(INFO, "%s", msg.c_str());
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);

    KeyValueMap kvMap;
    if (!parseTargetDescription(target, kvMap)) {
        BS_LOG(ERROR, "parse TaskTarget.targetDescription failed.");
        return false;
    }

    if (!startBuildFlow(kvMap)) {
        BS_LOG(ERROR, "start build failed.");
        return false;
    }

    msg = "start build end, current[" + autil::legacy::ToJsonString(_currentFinishTarget) + "]";
    BS_LOG(INFO, "%s", msg.c_str());
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    return true;
}

// TODO: delele create resource, create reader from dataDescription directly
common::ResourceKeeperMap SingleBuilder::createResources(KeyValueMap& kvMap) const
{
    common::ResourceKeeperMap ret;
    if (!isProcessedDocDataDesc(kvMap) && !config::DataLinkModeUtil::isDataLinkNPCMode(kvMap)) {
        return ret;
    }
    const auto& clusterName = getValueFromKeyValueMap(kvMap, "clusterName");
    config::TaskInputConfig config;
    config.setType("dependResource");
    config.addParameters("resourceName", clusterName);
    std::shared_ptr<common::ResourceKeeper> keeper =
        common::ResourceKeeperCreator::create(clusterName, "swift", /*resourceContainer*/ nullptr);
    auto swiftKeeper = std::dynamic_pointer_cast<common::SwiftResourceKeeper>(keeper);
    swiftKeeper->init(kvMap);
    ret[clusterName] = std::dynamic_pointer_cast<common::ResourceKeeper>(keeper);
    kvMap[workflow::FlowFactory::clusterInputKey(clusterName)] = autil::legacy::ToJsonString(config);
    return ret;
}

void SingleBuilder::reset()
{
    if (_taskScheduler && _updateLocatorTaskId != indexlib::util::TaskScheduler::INVALID_TASK_ID) {
        _taskScheduler->DeleteTask(_updateLocatorTaskId);
        _updateLocatorTaskId = indexlib::util::TaskScheduler::INVALID_TASK_ID;
    }
    _taskScheduler.reset();
    _buildFlow.reset();
    _brokerFactory.reset();
    _latestOpenedPublishedVersion = indexlibv2::INVALID_VERSIONID;
}

bool SingleBuilder::prepareIntervalTask(const KeyValueMap& kvMap)
{
    if (_buildFlow == nullptr) {
        BS_LOG(ERROR, "prepare interval task for builder pid [%s] has no buildFlow",
               _taskInitParam.pid.ShortDebugString().c_str());
        return false;
    }

    if (!isProcessedDocDataDesc(kvMap) || _buildStep != "full") {
        // only update commit locator for full build relay topic
        return true;
    }
    const std::string clusterName = getValueFromKeyValueMap(kvMap, "clusterName");
    config::SwiftConfig swiftConfig;
    if (!_taskInitParam.resourceReader->getConfigWithJsonPath(
            config::ResourceReader::getClusterConfRelativePath(clusterName), "swift_config", swiftConfig)) {
        BS_LOG(ERROR, "failed to parse swift_config, cluster:%s", clusterName.c_str());
        return false;
    }
    const std::string swiftTopicConfigName = getValueFromKeyValueMap(kvMap, "topicConfigName");
    std::shared_ptr<config::SwiftTopicConfig> swiftTopicConfig = swiftConfig.getTopicConfig(swiftTopicConfigName);
    if (swiftTopicConfig == nullptr) {
        BS_LOG(ERROR, "pid[%s] has no swift topic config", _taskInitParam.pid.ShortDebugString().c_str());
        return false;
    }

    auto workflow = _buildFlow->getInputFlow();
    if (workflow == nullptr) {
        BS_LOG(ERROR, "no ProcessedDocWorkflow in build flow");
        return false;
    }
    workflow::SwiftProcessedDocProducer* producer =
        dynamic_cast<workflow::SwiftProcessedDocProducer*>(workflow->getProducer());
    if (producer == nullptr) {
        BS_LOG(ERROR, "producer is nullptr in buildFlow");
        return false;
    }

    _taskScheduler = std::make_shared<indexlib::util::TaskScheduler>();
    if (!_taskScheduler->DeclareTaskGroup("update_locator", UPDATE_LOCATOR_INTERVAL)) {
        return false;
    }
    std::string indexRoot;
    if (!getValueFromKVMap(kvMap, config::INDEX_ROOT_PATH, indexRoot)) {
        BS_LOG(ERROR, "get index root path from kvmap failed");
        return false;
    }

    indexRoot = util::IndexPathConstructor::constructIndexPath(indexRoot, getPartitionRange());
    auto rootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(indexRoot);
    if (rootDir == nullptr) {
        BS_LOG(ERROR, "root dir is nullptr");
        return false;
    }
    auto taskItem = std::make_shared<UpdateLocatorTaskItemV2>(rootDir, producer, this);
    _updateLocatorTaskId = _taskScheduler->AddTask("update_locator", taskItem);
    if (_updateLocatorTaskId == indexlib::util::TaskScheduler::INVALID_TASK_ID) {
        return false;
    }
    BS_LOG(INFO, "update locator thread started, indexRoot[%s]", indexRoot.c_str());
    return true;
}

bool SingleBuilder::isProcessedDocDataDesc(const KeyValueMap& kvMap) const
{
    std::string srcType;
    if (!getValueFromKVMap(kvMap, config::READ_SRC_TYPE, srcType)) {
        return false;
    }
    return srcType == config::SWIFT_PROCESSED_READ_SRC;
}

bool SingleBuilder::startBuildFlow(KeyValueMap& kvMap)
{
    const auto& resourceReader = _taskInitParam.resourceReader;
    const auto& pid = _taskInitParam.pid;

    workflow::BuildFlowMode mode = workflow::BuildFlowMode::ALL;
    if (isProcessedDocDataDesc(kvMap) || config::DataLinkModeUtil::isDataLinkNPCMode(kvMap)) {
        mode = workflow::BuildFlowMode::BUILDER;
    }
    _buildFlow = std::make_unique<workflow::BuildFlow>(
        _taskInitParam.resourceReader->getTabletSchema(_taskInitParam.clusterName),
        workflow::BuildFlowThreadResource());

    _brokerFactory = std::make_unique<workflow::FlowFactory>(
        createResources(kvMap), std::make_shared<util::SwiftClientCreator>(), _tablet, _taskScheduler);
    if (!_buildFlow->startBuildFlow(resourceReader, pid, kvMap, _brokerFactory.get(), mode, workflow::SERVICE,
                                    _taskInitParam.metricProvider)) {
        collectErrorInfos();
        // if can't connect to swift, retry without exit
        proto::ServiceErrorCode errorCode = _brokerFactory->getServiceErrorCode();
        if (errorCode == proto::BUILDFLOW_ERROR_CONNECT_SWIFT || errorCode == proto::BUILDFLOW_ERROR_SEEK ||
            errorCode == proto::BUILDFLOW_ERROR_SWIFT_CREATE_READER ||
            errorCode == proto::BUILDFLOW_ERROR_SWIFT_CREATE_WRITER) {
            reset();
            return false;
        }
        BS_LOG(ERROR, "start build failed, single builder task exit");
        // TODO(xc & lc) return fatal error to build task? and update TaskStateHandle's hasFatalError
        _hasFatalError = true;
        return false;
    }

    _counterMap = _buildFlow->getCounterMap();
    if (_counterMap == nullptr) {
        BS_LOG(ERROR, "counter map is nullptr");
    }

    _startTimestamp = autil::TimeUtility::currentTime();
    if (!prepareIntervalTask(kvMap)) {
        REPORT_ERROR_WITH_ADVICE(proto::BUILDER_ERROR_UNKNOWN,
                                 std::string("prepare interval task failed, builder exit"), proto::BS_STOP);
        _hasFatalError = true;
        return false;
    }
    return true;
}

SingleBuilder::CommandType SingleBuilder::getCommandType(const config::TaskTarget& target)
{
    KeyValueMap kvMap;
    if (!parseTargetDescription(target, kvMap)) {
        BS_LOG(ERROR, "parse TaskTarget.targetDescription failed.");
        return SingleBuilder::CommandType::INVALID_PARAM;
    }

    auto it = kvMap.find("stopTimestamp");
    if (it == kvMap.end()) {
        return SingleBuilder::CommandType::START_BUILD;
    } else {
        int64_t stopTs = 0;
        if (!autil::StringUtil::fromString(it->second, stopTs)) {
            BS_LOG(INFO, "invalid stopTimestamp [%s] from kvMap", it->second.c_str());
            return SingleBuilder::CommandType::INVALID_PARAM;
        }
        if (target == _currentFinishTarget) {
            return SingleBuilder::CommandType::NONE;
        } else {
            return SingleBuilder::CommandType::STOP_BUILD;
        }
    }
}

bool SingleBuilder::handleTarget(const config::TaskTarget& target)
{
    if (_isDone) {
        return true;
    }
    bool ret = false;
    SingleBuilder::CommandType cmdType = getCommandType(target);
    switch (cmdType) {
    case SingleBuilder::CommandType::START_BUILD: {
        ret = startBuild(target);
        if (ret) {
            //_restartIntervalController.reset();
            _currentFinishTarget = target;
        }
        break;
    }
    case SingleBuilder::CommandType::STOP_BUILD: {
        ret = stopBuild(target);
        if (ret) {
            _currentFinishTarget = target;
            _isDone = true;
        }
        break;
    }
    case CommandType::NONE:
        ret = true;
    default:
        break;
    }

    return ret;
}

bool SingleBuilder::getTimestamp(const KeyValueMap& kvMap, const std::string& key, int64_t& timestamp) const
{
    auto iter = kvMap.find(key);
    if (iter == kvMap.end()) {
        BS_LOG(WARN, "get timestamp failed, key[%s].", key.c_str());
        return false;
    }
    if (!autil::StringUtil::fromString(iter->second, timestamp)) {
        BS_LOG(WARN, "invalid timestamp [%s] from kvMap", iter->second.c_str());
        return false;
    }
    return true;
}

bool SingleBuilder::stopBuild(const config::TaskTarget& target)
{
#define SET_FATAL_AND_RETURN                                                                                           \
    do {                                                                                                               \
        std::lock_guard<std::mutex> lock(_lock);                                                                       \
        collectErrorInfos();                                                                                           \
        _hasFatalError = true;                                                                                         \
        reset();                                                                                                       \
        BS_LOG(ERROR, "stop builder failed, exit!");                                                                   \
        return false;                                                                                                  \
    } while (0)

    if (_buildFlow == nullptr) {
        if (!startBuild(target)) {
            BS_LOG(ERROR, "start build failed.");
            return false;
        }
    }

    KeyValueMap kvMap;
    if (!parseTargetDescription(target, kvMap)) {
        BS_LOG(WARN, "parse TasTarget.targetDescription failed.");
        return false;
    }

    int64_t stopTimestamp = 0;
    if (!getTimestamp(kvMap, "stopTimestamp", stopTimestamp)) {
        BS_LOG(ERROR, "no stop timestamp in kvmap.");
        SET_FATAL_AND_RETURN;
    }
    if (!_buildFlow->suspendReadAtTimestamp(stopTimestamp, stopTimestamp, common::ETA_STOP)) {
        REPORT_ERROR_WITH_ADVICE(proto::BUILDER_ERROR_UNKNOWN, std::string("suspend builder failed"), proto::BS_RETRY);
        return false;
    }

    std::string msg = std::string("waiting builder stop, target [") + autil::legacy::ToJsonString(target).c_str() + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);

    // BuildFlow will retry itself when SERVICE mode
    // delete updateLocatorTask before buildFlow, because buildFlow hold raw pointers of builder & producer
    if (!_buildFlow->waitFinish()) {
        BS_LOG(ERROR, "wait build flow finish failed.");
        SET_FATAL_AND_RETURN;
    }

    std::lock_guard<std::mutex> lock(_lock);
    reset();

    msg = std::string("builder has been stopped, current status is: [") +
          autil::legacy::ToJsonString(_currentFinishTarget).c_str() + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    BS_LOG(INFO, "builder has been stopped, current status is: [%s]", msg.c_str());
    return true;
}

proto::PartitionId SingleBuilder::getPartitionRange(const task_base::Task::TaskInitParam& initParam)
{
    auto partitionCount = initParam.instanceInfo.partitionCount;
    if (partitionCount <= 0) {
        return initParam.pid;
    }
    auto ranges = util::RangeUtil::splitRange(0, 65535, partitionCount);
    auto tmpPartitionId = initParam.pid;
    for (const auto& range : ranges) {
        if (range.from() <= initParam.pid.range().from() && initParam.pid.range().to() <= range.to()) {
            tmpPartitionId.mutable_range()->set_from(range.from());
            tmpPartitionId.mutable_range()->set_to(range.to());
            return tmpPartitionId;
        }
    }
    return tmpPartitionId;
}

proto::PartitionId SingleBuilder::getPartitionRange() const { return getPartitionRange(_taskInitParam); }

void SingleBuilder::collectErrorInfos() const
{
    std::vector<proto::ErrorInfo> errorInfos;
    if (_buildFlow != nullptr) {
        _buildFlow->proto::ErrorCollector::fillErrorInfos(errorInfos);
    }
    if (_brokerFactory != nullptr) {
        _brokerFactory->fillErrorInfos(errorInfos);
    }
    addErrorInfos(errorInfos);
}

bool SingleBuilder::hasFatalError() const
{
    std::lock_guard<std::mutex> lock(_lock);
    if (_hasFatalError) {
        return true;
    }
    if (_buildFlow) {
        return _buildFlow->hasFatalError() || _buildFlow->needReconstruct();
    }
    return false;
}

bool SingleBuilder::isDone() const { return _isDone; }

} // namespace build_service::build_task
