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
#include "build_service/workflow/DocumentBatchRtBuilderImplV2.h"

#include <algorithm>
#include <assert.h>
#include <atomic>
#include <map>
#include <mutex>
#include <ostream>
#include <stddef.h>
#include <unordered_map>
#include <utility>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/builder/BuilderV2.h"
#include "build_service/common/Locator.h"
#include "build_service/common/ResourceKeeperCreator.h"
#include "build_service/common/SwiftResourceKeeper.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/config/TaskInputConfig.h"
#include "build_service/util/Monitor.h"
#include "build_service/workflow/AsyncStarter.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/BuildFlowMode.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/RealtimeErrorDefine.h"
#include "build_service/workflow/StopOption.h"
#include "build_service/workflow/Workflow.h"
#include "build_service/workflow/WorkflowItem.h"
#include "indexlib/base/Progress.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/misc/common.h"
#include "kmonitor/client/MetricType.h"

namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, DocumentBatchRtBuilderImplV2);

class DocumentBatchRtBuilderImplV2::ForceSeekStatus
{
public:
    ForceSeekStatus() : _done(false), _skipBegin(-1), _skipEnd(-1) {}
    ~ForceSeekStatus() = default;

public:
    bool isDone() const { return _done; }
    void markDone(int64_t skipBeginTs, int64_t skipEndTs)
    {
        _skipBegin = skipBeginTs;
        _skipEnd = skipEndTs;
    }
    int64_t getLostTimeInterval(int64_t versionTs)
    {
        int64_t lost = _skipEnd - std::max(versionTs, _skipBegin);
        if (lost <= 0) {
            return 0;
        }
        return lost;
    }

private:
    bool _done;
    int64_t _skipBegin;
    int64_t _skipEnd;
};

DocumentBatchRtBuilderImplV2::DocumentBatchRtBuilderImplV2(const std::string& configPath,
                                                           std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                                                           const RealtimeBuilderResource& builderResource,
                                                           future_lite::NamedTaskScheduler* tasker)
    : RealtimeBuilderImplV2(configPath, std::move(tablet), builderResource, tasker)
    , _producer(NULL)
    , _seekToLatestInForceRecover(false)
{
}
DocumentBatchRtBuilderImplV2::~DocumentBatchRtBuilderImplV2()
{
    stop(SO_NORMAL);
    _buildFlow.reset();
    _brokerFactory.reset();
}

bool DocumentBatchRtBuilderImplV2::doStart(const proto::PartitionId& partitionId, KeyValueMap& kvMap)
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
    if (!prepareForNPCMode(resourceReader, partitionId, kvMap)) {
        return false;
    }
    _brokerFactory.reset(createFlowFactory(resourceReader, rtPartitionId, kvMap));
    _buildFlow->startWorkLoop(resourceReader, rtPartitionId, kvMap, _brokerFactory.get(), buildFlowMode, workflowMode,
                              _metricProvider);
    _starter.setName("BsDocRtBuild");
    _starter.asyncStart([this]() { return getBuilderAndProducer(); });
    return true;
}

bool DocumentBatchRtBuilderImplV2::prepareForNPCMode(const config::ResourceReaderPtr& resourceReader,
                                                     const proto::PartitionId& partitionId, KeyValueMap& kvMap)
{
    auto iter = kvMap.find(config::REALTIME_MODE);
    if (iter == kvMap.end()) {
        BS_LOG(ERROR, "no realtime_mode defined in kvMap");
        return false;
    }
    if (iter->second != config::REALTIME_SERVICE_NPC_MODE) {
        BS_LOG(ERROR, "realtime_mode[%s] not supported, supported type {npc_mode}", iter->second.c_str());
        return false;
    }
    const std::string& clusterName = partitionId.clusternames(0);
    std::string relativePath = config::ResourceReader::getClusterConfRelativePath(clusterName);
    if (!resourceReader->getConfigWithJsonPath(relativePath, "build_option_config.seek_to_latest_when_recover_fail",
                                               _seekToLatestInForceRecover)) {
        std::string errorMsg = "fail to read seek_to_latest_when_recover_fail from config path " + relativePath;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (_seekToLatestInForceRecover) {
        BS_LOG(INFO, "set seek_to_latest_when_recover_fail = true in cluster[%s]", clusterName.c_str());
        _lostRtMetric = DECLARE_METRIC(_metricProvider, "basic/lostRealtime", kmonitor::GAUGE, "us");
    }
    return true;
}

bool DocumentBatchRtBuilderImplV2::seekProducerToLatest()
{
    int64_t maxTimestamp;
    if (!getLastTimestampInProducer(maxTimestamp)) {
        std::string errorMsg = "get last timestamp in producer failed";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    int64_t lastReadTs = -1;
    if (!getLastReadTimestampInProducer(lastReadTs)) {
        std::string errorMsg = "get last read timestamp in producer failed";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    if (_builder == nullptr) {
        BS_LOG(ERROR, "UNEXPECTED: builder is NULL");
        return false;
    }
    auto latestLocator = getLatestLocator();
    if (latestLocator.GetOffset().first >= maxTimestamp) {
        BS_LOG(INFO, "LOCATOR offset[%zu] exceeds maxTimestamp[%zu], no need to skip", latestLocator.GetOffset().first,
               maxTimestamp);
        return true;
    }
    common::Locator locatorToJump(latestLocator.GetSrc(), maxTimestamp);
    producerSeek(locatorToJump);
    _forceSeekStatus->markDone(lastReadTs, maxTimestamp);
    return true;
}

void DocumentBatchRtBuilderImplV2::handleRecoverTimeout()
{
    if (!_isRecovered) {
        BS_LOG(INFO, "force recover for partition [%s]", _partitionId.ShortDebugString().c_str());
        if (_seekToLatestInForceRecover) {
            _forceSeekStatus.reset(new ForceSeekStatus());
        }
    }
}

void DocumentBatchRtBuilderImplV2::reportProducerSkipInterval()
{
    if (_forceSeekStatus == nullptr || _builder == nullptr) {
        return;
    }
    auto incLocator = _builder->getLatestVersionLocator();
    if (!incLocator.IsValid()) {
        return;
    }
    int64_t lostTime = _forceSeekStatus->getLostTimeInterval(incLocator.GetOffset().first);
    REPORT_METRIC(_lostRtMetric, (double)lostTime);
}

void DocumentBatchRtBuilderImplV2::externalActions()
{
    setProducerRecovered();
    reportFreshnessWhenSuspendBuild();
    reportProducerSkipInterval();
}

void DocumentBatchRtBuilderImplV2::setProducerRecovered()
{
    if (!_isProducerRecovered) {
        if (_producer && _isRecovered) {
            if (_forceSeekStatus != nullptr && !_forceSeekStatus->isDone()) {
                if (false == seekProducerToLatest()) {
                    BS_LOG(INFO, "force seek failed for partition [%s]", _partitionId.ShortDebugString().c_str());
                }
            }
            _producer->setRecovered(true);
            _isProducerRecovered = true;
        }
    }
}

bool DocumentBatchRtBuilderImplV2::producerAndBuilderPrepared() const
{
    if (!_builder || !_producer) {
        return false;
    }
    return true;
}

bool DocumentBatchRtBuilderImplV2::getBuilderAndProducer()
{
    auto workflow = _buildFlow->getInputFlow();
    if (!workflow) {
        return false;
    }
    SwiftDocumentBatchProducer* producer = dynamic_cast<SwiftDocumentBatchProducer*>(workflow->getProducer());
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

bool DocumentBatchRtBuilderImplV2::getLastTimestampInProducer(int64_t& timestamp)
{
    if (_producer) {
        return _producer->getMaxTimestamp(timestamp);
    }
    return false;
}

bool DocumentBatchRtBuilderImplV2::getLastReadTimestampInProducer(int64_t& timestamp)
{
    if (_producer) {
        return _producer->getLastReadTimestamp(timestamp);
    }
    return false;
}

bool DocumentBatchRtBuilderImplV2::producerSeek(const common::Locator& locator)
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

common::ResourceKeeperMap DocumentBatchRtBuilderImplV2::createResources(const config::ResourceReaderPtr& resourceReader,
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
    kvMap["clusterName"] = clusterName;
    kvMap["topicConfigName"] = config::RAW_SWIFT_TOPIC_CONFIG;
    swiftKeeper->init(kvMap);
    ret[clusterName] = keeper;
    kvMap[FlowFactory::clusterInputKey(clusterName)] = ToJsonString(config);
    return ret;
}

FlowFactory* DocumentBatchRtBuilderImplV2::createFlowFactory(const config::ResourceReaderPtr& resourceReader,
                                                             const proto::PartitionId& pid, KeyValueMap& kvMap) const
{
    // TODO(tianxiao.ttx): may need taskScheduler
    return new FlowFactory(createResources(resourceReader, pid, kvMap), _swiftClientCreator, _tablet,
                           /*taskScheduler*/ nullptr);
}

void DocumentBatchRtBuilderImplV2::reportFreshnessWhenSuspendBuild()
{
    if (!_autoSuspend && !_adminSuspend) {
        return;
    }

    auto tmpLocator = getLatestLocator();

    tmpLocator.DebugString();
    // TODO(tianxiao.ttx) conver locator to common::Locator
    common::Locator latestLocator;
    if (!latestLocator.IsValid()) {
        return;
    }

    assert(_producer);
    _producer->reportFreshnessMetrics(latestLocator.GetOffset().first, /* no more msg */ false);
}

}} // namespace build_service::workflow
