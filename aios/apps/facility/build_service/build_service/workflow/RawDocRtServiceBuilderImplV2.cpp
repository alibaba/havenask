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
#include "build_service/workflow/RawDocRtServiceBuilderImplV2.h"

#include "autil/TimeUtility.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/DocReaderProducer.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/Version.h"
#include "indexlib/index_base/online_join_policy.h"

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, RawDocRtServiceBuilderImplV2);

RawDocRtServiceBuilderImplV2::RawDocRtServiceBuilderImplV2(const std::string& configPath,
                                                           std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                                                           const RealtimeBuilderResource& builderResource,
                                                           future_lite::NamedTaskScheduler* tasker)

    : RealtimeBuilderImplV2(configPath, std::move(tablet), builderResource, tasker)
    , _producer(NULL)
    , _startSkipCalibrateDone(false)
    , _isProducerRecovered(false)
    , _rtSrcSignature(builderResource.realtimeInfo.getSrcSignature())
{
}

RawDocRtServiceBuilderImplV2::~RawDocRtServiceBuilderImplV2()
{
    stop(SO_NORMAL);
    _buildFlow.reset();
    _factory.reset();
}

bool RawDocRtServiceBuilderImplV2::doStart(const proto::PartitionId& partitionId, KeyValueMap& kvMap)
{
    config::ResourceReaderPtr resourceReader =
        config::ResourceReaderManager::getResourceReader(kvMap[config::CONFIG_PATH]);
    BuildFlowMode buildFlowMode = BuildFlowMode::ALL; // TODO(hanyao): add only builder mode
    WorkflowMode workflowMode = build_service::workflow::REALTIME;
    proto::PartitionId pIdAddDataTable;
    pIdAddDataTable.CopyFrom(partitionId);
    auto iter = kvMap.find(config::DATA_TABLE_NAME);
    if (iter != kvMap.end()) {
        pIdAddDataTable.mutable_buildid()->set_datatable(iter->second);
    }
    _factory.reset(createFlowFactory());
    _buildFlow->startWorkLoop(resourceReader, pIdAddDataTable, kvMap, _factory.get(), buildFlowMode, workflowMode,
                              _metricProvider);

    _starter.setName("BsRawDocRtServiceBuild");
    _starter.asyncStart([this]() { return getBuilderAndProducer(); });
    return true;
}

FlowFactory* RawDocRtServiceBuilderImplV2::createFlowFactory()
{
    return new FlowFactory(/*resourceMap*/ {}, _swiftClientCreator, _tablet, /*taskScheduler*/ nullptr);
}

bool RawDocRtServiceBuilderImplV2::getBuilderAndProducer()
{
    auto workflow = _buildFlow->getInputFlow();
    if (!workflow) {
        return false;
    }
    DocReaderProducer* producer = dynamic_cast<DocReaderProducer*>(workflow->getProducer());
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

bool RawDocRtServiceBuilderImplV2::producerSeek(const common::Locator& locator)
{
    assert(_producer != NULL);
    if (!_producer->seek(locator)) {
        std::stringstream ss;
        ss << "seek to locator[" << locator.DebugString() << "] failed";
        std::string errorMsg = ss.str();
        BS_LOG(WARN, "[%s] %s", _partitionId.buildid().ShortDebugString().c_str(), errorMsg.c_str());
        setErrorInfoUnsafe(ERROR_SWIFT_SEEK, errorMsg);
        return false;
    }
    return true;
}

bool RawDocRtServiceBuilderImplV2::producerAndBuilderPrepared() const
{
    if (!_builder || !_producer) {
        return false;
    }
    return true;
}

bool RawDocRtServiceBuilderImplV2::getLastTimestampInProducer(int64_t& timestamp)
{
    if (_producer) {
        return _producer->getMaxTimestamp(timestamp);
    }
    return false;
}

bool RawDocRtServiceBuilderImplV2::getLastReadTimestampInProducer(int64_t& timestamp)
{
    if (_producer) {
        return _producer->getLastReadTimestamp(timestamp);
    }
    return false;
}

bool RawDocRtServiceBuilderImplV2::seekProducerToLatest()
{
    assert(false);
    // TODO(hanyao): support force seek
    return false;
}

void RawDocRtServiceBuilderImplV2::externalActions()
{
    if (!_isProducerRecovered) {
        if (_producer && _isRecovered) {
            _producer->setRecovered(true);
            _isProducerRecovered = true;
        }
    }
}

}} // namespace build_service::workflow
