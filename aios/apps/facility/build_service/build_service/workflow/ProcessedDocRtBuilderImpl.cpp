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
#include "build_service/workflow/ProcessedDocRtBuilderImpl.h"

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
#include "indexlib/base/Progress.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace std;
using namespace indexlib::index_base;
using namespace indexlib::partition;
using namespace autil;

using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::config;

namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, ProcessedDocRtBuilderImpl);

ProcessedDocRtBuilderImpl::ProcessedDocRtBuilderImpl(const std::string& configPath, const IndexPartitionPtr& indexPart,
                                                     const RealtimeBuilderResource& builderResource)
    : RealtimeBuilderImplBase(configPath, indexPart, builderResource, false)
    , _producer(NULL)
    , _startSkipCalibrateDone(false)
{
}

ProcessedDocRtBuilderImpl::~ProcessedDocRtBuilderImpl()
{
    stop();
    _buildFlow.reset();
    _brokerFactory.reset();
}

bool ProcessedDocRtBuilderImpl::doStart(const PartitionId& partitionId, KeyValueMap& kvMap)
{
    PartitionId rtPartitionId;
    rtPartitionId.CopyFrom(partitionId);
    rtPartitionId.set_step(BUILD_STEP_INC);
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
    BuildFlowMode buildFlowMode = BuildFlowMode::BUILDER;
    // in this mode, inc job is not allowed, inc index and rt index are built from
    // the same swift, we can use inc index's locator to seek rt build
    WorkflowMode workflowMode = build_service::workflow::REALTIME;

    // set step to BUILD_STEP_INC so that RtBuilder could find corresponding broker topic

    resetStartSkipTimestamp(kvMap);

    const string& clusterName = partitionId.clusternames(0);
    string topicPrefix = getValueFromKeyValueMap(kvMap, PROCESSED_DOC_SWIFT_TOPIC_PREFIX);
    string realtimeTopicName = SwiftAdminFacade::getRealtimeTopicName(topicPrefix, partitionId.buildid(), clusterName,
                                                                      _indexPartition->GetSchema());
    kvMap[PROCESSED_DOC_SWIFT_TOPIC_NAME] = realtimeTopicName;
    _brokerFactory.reset(createFlowFactory(resourceReader, rtPartitionId, kvMap));
    _buildFlow->startWorkLoop(resourceReader, rtPartitionId, kvMap, _brokerFactory.get(), buildFlowMode, workflowMode,
                              _metricProvider);

    _starter.setName("BsDocRtBuild");
    _starter.asyncStart(bind(&ProcessedDocRtBuilderImpl::getBuilderAndProducer, this));
    return true;
}

void ProcessedDocRtBuilderImpl::externalActions()
{
    setProducerRecovered();
    skipToLatestLocator();
    reportFreshnessWhenSuspendBuild();
}

void ProcessedDocRtBuilderImpl::setProducerRecovered()
{
    if (!_isProducerRecovered) {
        if (_producer && _isRecovered) {
            _producer->setRecovered(true);
            _isProducerRecovered = true;
        }
    }
}

bool ProcessedDocRtBuilderImpl::producerAndBuilderPrepared() const
{
    if (!_builder || !_producer) {
        return false;
    }
    return true;
}

bool ProcessedDocRtBuilderImpl::getBuilderAndProducer()
{
    auto workflow = _buildFlow->getInputFlow();
    if (!workflow) {
        return false;
    }
    SwiftProcessedDocProducer* producer = dynamic_cast<SwiftProcessedDocProducer*>(workflow->getProducer());
    builder::Builder* builder = _buildFlow->getBuilder();
    {
        autil::ScopedLock lock(_realtimeMutex);
        _builder = builder;
        _producer = producer;
        if (!_producer || !_builder) {
            return false;
        }
    }
    return true;
}

bool ProcessedDocRtBuilderImpl::getLastTimestampInProducer(int64_t& timestamp)
{
    if (_producer) {
        return _producer->getMaxTimestamp(timestamp);
    }
    return false;
}

bool ProcessedDocRtBuilderImpl::getLastReadTimestampInProducer(int64_t& timestamp)
{
    if (_producer) {
        return _producer->getLastReadTimestamp(timestamp);
    }
    return false;
}

bool ProcessedDocRtBuilderImpl::producerSeek(const Locator& locator)
{
    assert(_producer != NULL);
    Locator targetLocator = locator;
    if (targetLocator.GetSrc() != (uint64_t)_producer->getStartTimestamp()) {
        BS_INTERVAL_LOG2(120, INFO, "[%s] reset target skip locator [%s] source to [%ld]",
                         _partitionId.buildid().ShortDebugString().c_str(), targetLocator.DebugString().c_str(),
                         _producer->getStartTimestamp());
        targetLocator.SetSrc(_producer->getStartTimestamp());
    }

    // 兼容老locator的range逻辑
    indexlibv2::base::Progress progress(_partitionId.range().from(), _partitionId.range().to(),
                                        targetLocator.GetOffset());
    targetLocator.SetProgress({progress});

    if (targetLocator == _lastSkipedLocator) {
        BS_INTERVAL_LOG2(120, INFO, "[%s] already skip to locator [%s] a moment ago!",
                         _partitionId.buildid().ShortDebugString().c_str(), targetLocator.DebugString().c_str());
        return true;
    }

    if (!_producer->seek(targetLocator)) {
        stringstream ss;
        ss << "seek to locator[" << targetLocator.DebugString() << "] failed";
        string errorMsg = ss.str();
        BS_LOG(WARN, "[%s] %s", _partitionId.buildid().ShortDebugString().c_str(), errorMsg.c_str());
        setErrorInfoUnsafe(ERROR_SWIFT_SEEK, errorMsg);
        return false;
    }

    BS_LOG(INFO, "[%s] Skip to latestLocator [%s]", _partitionId.buildid().ShortDebugString().c_str(),
           targetLocator.DebugString().c_str());
    _lastSkipedLocator = targetLocator;
    return true;
}

void ProcessedDocRtBuilderImpl::skipToLatestLocator()
{
    Locator latestLocator;
    bool fromInc = false;
    if (!getLatestLocator(latestLocator, fromInc)) {
        return;
    }

    if (!fromInc && _startSkipCalibrateDone) {
        return;
    }

    if (!latestLocator.IsValid()) {
        BS_INTERVAL_LOG2(120, INFO, "[%s] get null latestLocator [%s]",
                         _partitionId.buildid().ShortDebugString().c_str(), latestLocator.DebugString().c_str());
        return;
    }

    if (producerSeek(latestLocator)) {
        _startSkipCalibrateDone = true;
    } else {
        BS_LOG(ERROR, "[%s] Skip to latestLocator [%s] fail!", _partitionId.buildid().ShortDebugString().c_str(),
               latestLocator.DebugString().c_str());
    }
}

common::ResourceKeeperMap ProcessedDocRtBuilderImpl::createResources(const ResourceReaderPtr& resourceReader,
                                                                     const PartitionId& pid, KeyValueMap& kvMap) const
{
    common::ResourceKeeperMap ret;
    assert(pid.clusternames_size() == 1);
    const string& clusterName = pid.clusternames(0);
    ResourceKeeperPtr keeper = ResourceKeeperCreator::create(clusterName, "swift", nullptr);
    config::TaskInputConfig config;
    config.setType("dependResource");
    config.addParameters("resourceName", clusterName);
    auto swiftKeeper = DYNAMIC_POINTER_CAST(SwiftResourceKeeper, keeper);
    swiftKeeper->initLegacy(clusterName, pid, resourceReader, kvMap);
    ret[clusterName] = keeper;
    kvMap[FlowFactory::clusterInputKey(clusterName)] = ToJsonString(config);
    return ret;
}

FlowFactory* ProcessedDocRtBuilderImpl::createFlowFactory(const ResourceReaderPtr& resourceReader,
                                                          const PartitionId& pid, KeyValueMap& kvMap) const
{
    return new FlowFactory(createResources(resourceReader, pid, kvMap), _swiftClientCreator, _indexPartition,
                           _taskScheduler);
}

void ProcessedDocRtBuilderImpl::reportFreshnessWhenSuspendBuild()
{
    if (!_autoSuspend && !_adminSuspend) {
        return;
    }

    Locator latestLocator;
    getLatestLocator(latestLocator);

    if (!latestLocator.IsValid()) {
        return;
    }

    assert(_producer);
    static const std::string emptyDocSource("");
    _producer->reportFreshnessMetrics(latestLocator.GetOffset(), /* no more msg */ false, emptyDocSource,
                                      /* report fast queue delay */ true);
}

int64_t ProcessedDocRtBuilderImpl::getStartSkipTimestamp() const
{
    return _indexPartition->GetRtSeekTimestampFromIncVersion();
}

void ProcessedDocRtBuilderImpl::resetStartSkipTimestamp(KeyValueMap& kvMap)
{
    KeyValueMap::const_iterator it = kvMap.find(CHECKPOINT);
    if (it != kvMap.end()) {
        BS_LOG(INFO, "[%s] in kvMap already set to [%s]", CHECKPOINT.c_str(), it->second.c_str());
        return;
    }

    int64_t startSkipTs = getStartSkipTimestamp();
    if (startSkipTs <= 0) {
        return;
    }
    kvMap[CHECKPOINT] = StringUtil::toString(startSkipTs);
    BS_LOG(INFO, "[%s] in kvMap is set to [%ld]", CHECKPOINT.c_str(), startSkipTs);
}

}} // namespace build_service::workflow
