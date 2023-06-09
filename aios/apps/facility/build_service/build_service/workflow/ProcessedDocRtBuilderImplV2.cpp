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
#include "build_service/workflow/ProcessedDocRtBuilderImplV2.h"

#include "autil/TimeUtility.h"
#include "build_service/builder/Builder.h"
#include "build_service/common/Locator.h"
#include "build_service/common/ResourceKeeperCreator.h"
#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/common/SwiftResourceKeeper.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/config/TaskInputConfig.h"
#include "build_service/util/Monitor.h"
#include "indexlib/framework/ITablet.h"

namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, ProcessedDocRtBuilderImplV2);

ProcessedDocRtBuilderImplV2::ProcessedDocRtBuilderImplV2(const std::string& configPath,
                                                         std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                                                         const RealtimeBuilderResource& builderResource,
                                                         future_lite::NamedTaskScheduler* tasker)
    : RealtimeBuilderImplV2(configPath, std::move(tablet), builderResource, tasker)
    , _producer(NULL)
    , _startSkipCalibrateDone(false)
{
}

ProcessedDocRtBuilderImplV2::~ProcessedDocRtBuilderImplV2()
{
    stop(SO_NORMAL);
    _buildFlow.reset();
    _brokerFactory.reset();
}

bool ProcessedDocRtBuilderImplV2::doStart(const proto::PartitionId& partitionId, KeyValueMap& kvMap)
{
    proto::PartitionId rtPartitionId;
    rtPartitionId.CopyFrom(partitionId);
    rtPartitionId.set_step(proto::BUILD_STEP_INC);
    config::ResourceReaderPtr resourceReader = config::ResourceReaderManager::getResourceReader(_configPath);
    BuildFlowMode buildFlowMode = BuildFlowMode::BUILDER;
    // in this mode, inc job is not allowed, inc index and rt index are built from
    // the same swift, we can use inc index's locator to seek rt build
    WorkflowMode workflowMode = build_service::workflow::REALTIME;
    kvMap[config::SRC_SIGNATURE] = std::to_string(config::SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE);
    // set step to BUILD_STEP_INC so that RtBuilder could find corresponding broker topic

    // resetStartSkipTimestamp(kvMap);

    const std::string& clusterName = partitionId.clusternames(0);
    std::string topicPrefix = getValueFromKeyValueMap(kvMap, config::PROCESSED_DOC_SWIFT_TOPIC_PREFIX);
    std::string realtimeTopicName =
        common::SwiftAdminFacade::getRealtimeTopicName(topicPrefix, partitionId.buildid(), clusterName);
    kvMap[config::PROCESSED_DOC_SWIFT_TOPIC_NAME] = realtimeTopicName;
    _brokerFactory.reset(createFlowFactory(resourceReader, rtPartitionId, kvMap));
    _buildFlow->startWorkLoop(resourceReader, rtPartitionId, kvMap, _brokerFactory.get(), buildFlowMode, workflowMode,
                              _metricProvider);

    _starter.setName("BsDocRtBuild");
    _starter.asyncStart([this]() { return getBuilderAndProducer(); });
    return true;
}

void ProcessedDocRtBuilderImplV2::externalActions()
{
    setProducerRecovered();
    reportFreshnessWhenSuspendBuild();
}

void ProcessedDocRtBuilderImplV2::setProducerRecovered()
{
    if (!_isProducerRecovered) {
        if (_producer && _isRecovered) {
            _producer->setRecovered(true);
            _isProducerRecovered = true;
        }
    }
}

bool ProcessedDocRtBuilderImplV2::producerAndBuilderPrepared() const
{
    if (!_builder || !_producer) {
        return false;
    }
    return true;
}

bool ProcessedDocRtBuilderImplV2::getBuilderAndProducer()
{
    auto workflow = _buildFlow->getInputFlow();
    if (!workflow) {
        return false;
    }
    SwiftProcessedDocProducer* producer = dynamic_cast<SwiftProcessedDocProducer*>(workflow->getProducer());
    builder::BuilderV2* builder = _buildFlow->getBuilderV2();
    {
        std::lock_guard<std::mutex> lock(_realtimeMutex);
        _builder = builder;
        _producer = producer;
        if (!_producer || !_builder) {
            return false;
        }
    }
    return true;
}

bool ProcessedDocRtBuilderImplV2::getLastTimestampInProducer(int64_t& timestamp)
{
    if (_producer) {
        return _producer->getMaxTimestamp(timestamp);
    }
    return false;
}

bool ProcessedDocRtBuilderImplV2::getLastReadTimestampInProducer(int64_t& timestamp)
{
    if (_producer) {
        return _producer->getLastReadTimestamp(timestamp);
    }
    return false;
}

bool ProcessedDocRtBuilderImplV2::producerSeek(const common::Locator& locator)
{
    auto targetLocator = locator;
    if (!locator.IsSameSrc(common::Locator(config::SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE, 0), false)) {
        BS_INTERVAL_LOG2(120, INFO, "[%s] reset target skip locator [%s] source to [%ld]",
                         _partitionId.buildid().ShortDebugString().c_str(), targetLocator.DebugString().c_str(),
                         config::SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE);
        targetLocator.SetSrc(config::SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE);
    }

    if (targetLocator == _lastSkipedLocator) {
        BS_INTERVAL_LOG2(120, INFO, "[%s] already skip to locator [%s] a moment ago!",
                         _partitionId.buildid().ShortDebugString().c_str(), targetLocator.DebugString().c_str());
        return true;
    }

    if (!_producer->seek(targetLocator)) {
        std::stringstream ss;
        ss << "seek to locator[" << targetLocator.DebugString() << "] failed";
        std::string errorMsg = ss.str();
        BS_LOG(WARN, "[%s] %s", _partitionId.buildid().ShortDebugString().c_str(), errorMsg.c_str());
        setErrorInfoUnsafe(ERROR_SWIFT_SEEK, errorMsg);
        return false;
    }

    BS_LOG(INFO, "[%s] Skip to latestLocator [%s]", _partitionId.buildid().ShortDebugString().c_str(),
           targetLocator.DebugString().c_str());
    _lastSkipedLocator = targetLocator;
    return true;
}

common::ResourceKeeperMap ProcessedDocRtBuilderImplV2::createResources(const config::ResourceReaderPtr& resourceReader,
                                                                       const proto::PartitionId& pid,
                                                                       KeyValueMap& kvMap) const
{
    common::ResourceKeeperMap ret;
    assert(pid.clusternames_size() == 1);
    const std::string& clusterName = pid.clusternames(0);
    common::ResourceKeeperPtr keeper = common::ResourceKeeperCreator::create(clusterName, "swift", nullptr);
    config::TaskInputConfig config;
    config.setType("dependResource");
    config.addParameters("resourceName", clusterName);
    auto swiftKeeper = DYNAMIC_POINTER_CAST(common::SwiftResourceKeeper, keeper);
    swiftKeeper->initLegacy(clusterName, pid, resourceReader, kvMap);
    ret[clusterName] = keeper;
    kvMap[FlowFactory::clusterInputKey(clusterName)] = ToJsonString(config);
    return ret;
}

FlowFactory* ProcessedDocRtBuilderImplV2::createFlowFactory(const config::ResourceReaderPtr& resourceReader,
                                                            const proto::PartitionId& pid, KeyValueMap& kvMap) const
{
    // TODO(tianxiao.ttx): may need taskScheduler
    return new FlowFactory(createResources(resourceReader, pid, kvMap), _swiftClientCreator, _tablet,
                           /*taskScheduler*/ nullptr);
}

void ProcessedDocRtBuilderImplV2::reportFreshnessWhenSuspendBuild()
{
    if (!_autoSuspend && !_adminSuspend) {
        return;
    }

    auto [tmpLocator, _] = getLatestLocator();

    tmpLocator.DebugString();
    // TODO(tianxiao.ttx) conver locator to common::Locator
    common::Locator latestLocator;
    if (!latestLocator.IsValid()) {
        return;
    }

    assert(_producer);
    static const std::string emptyDocSource("");
    _producer->reportFreshnessMetrics(latestLocator.GetOffset(), /* no more msg */ false, emptyDocSource,
                                      /* report fast queue delay */ true);
}

// int64_t ProcessedDocRtBuilderImplV2::getStartSkipTimestamp() const
// {
//     // TODO(tianxiao.ttx) get skip timestamp
//     // return _indexPartition->GetRtSeekTimestampFromIncVersion();
//     return 0;
// }

// void ProcessedDocRtBuilderImplV2::resetStartSkipTimestamp(KeyValueMap& kvMap)
// {
//     KeyValueMap::const_iterator it = kvMap.find(config::CHECKPOINT);
//     if (it != kvMap.end()) {
//         BS_LOG(INFO, "[%s] in kvMap already set to [%s]", config::CHECKPOINT.c_str(), it->second.c_str());
//         return;
//     }

//     int64_t startSkipTs = getStartSkipTimestamp();
//     if (startSkipTs <= 0) {
//         return;
//     }
//     kvMap[config::CHECKPOINT] = std::to_string(startSkipTs);
//     BS_LOG(INFO, "[%s] in kvMap is set to [%ld]", config::CHECKPOINT.c_str(), startSkipTs);
// }

}} // namespace build_service::workflow
