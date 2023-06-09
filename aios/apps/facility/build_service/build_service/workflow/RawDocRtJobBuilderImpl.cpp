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
#include "build_service/workflow/RawDocRtJobBuilderImpl.h"

#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/DocReaderProducer.h"
#include "indexlib/partition/index_partition.h"

using namespace std;
using namespace indexlib::index_base;
using namespace indexlib::partition;

using namespace build_service::config;
using namespace build_service::proto;

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, RawDocRtJobBuilderImpl);

RawDocRtJobBuilderImpl::RawDocRtJobBuilderImpl(const std::string& configPath, const IndexPartitionPtr& indexPart,
                                               const RealtimeBuilderResource& builderResource)
    : RealtimeBuilderImplBase(configPath, indexPart, builderResource, false)
    , _producer(NULL)
{
}

RawDocRtJobBuilderImpl::~RawDocRtJobBuilderImpl()
{
    stop();
    _buildFlow.reset();
    _factory.reset();
}

bool RawDocRtJobBuilderImpl::doStart(const PartitionId& partitionId, KeyValueMap& kvMap)
{
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(kvMap[CONFIG_PATH]);
    BuildFlowMode buildFlowMode = BuildFlowMode::ALL;
    WorkflowMode workflowMode = build_service::workflow::REALTIME;
    PartitionId pIdAddDataTable;
    pIdAddDataTable.CopyFrom(partitionId);
    pIdAddDataTable.mutable_buildid()->set_datatable(kvMap[DATA_TABLE_NAME]);
    _factory.reset(new FlowFactory({}, _swiftClientCreator, _indexPartition));
    _buildFlow->startWorkLoop(resourceReader, pIdAddDataTable, kvMap, _factory.get(), buildFlowMode, workflowMode,
                              _metricProvider);

    _starter.setName("BsRawDocRtBuild");
    _starter.asyncStart(bind(&RawDocRtJobBuilderImpl::getBuilderAndProducer, this));
    return true;
}

bool RawDocRtJobBuilderImpl::getBuilderAndProducer()
{
    auto workflow = _buildFlow->getInputFlow();
    if (!workflow) {
        return false;
    }
    DocReaderProducer* producer = dynamic_cast<DocReaderProducer*>(workflow->getProducer());
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

bool RawDocRtJobBuilderImpl::producerSeek(const common::Locator& locator)
{
    assert(_producer != NULL);
    if (!_producer->seek(locator)) {
        stringstream ss;
        ss << "seek to locator[" << locator.DebugString() << "] failed";
        string errorMsg = ss.str();
        BS_LOG(WARN, "[%s] %s", _partitionId.buildid().ShortDebugString().c_str(), errorMsg.c_str());
        setErrorInfoUnsafe(ERROR_SWIFT_SEEK, errorMsg);
        return false;
    }
    return true;
}

bool RawDocRtJobBuilderImpl::producerAndBuilderPrepared() const
{
    if (!_builder || !_producer) {
        return false;
    }
    return true;
}

bool RawDocRtJobBuilderImpl::getLastTimestampInProducer(int64_t& timestamp)
{
    if (_producer) {
        return _producer->getMaxTimestamp(timestamp);
    }
    return false;
}

bool RawDocRtJobBuilderImpl::getLastReadTimestampInProducer(int64_t& timestamp)
{
    if (_producer) {
        return _producer->getLastReadTimestamp(timestamp);
    }
    return false;
}

}} // namespace build_service::workflow
