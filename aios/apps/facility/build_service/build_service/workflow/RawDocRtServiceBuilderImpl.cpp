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
#include "build_service/workflow/RawDocRtServiceBuilderImpl.h"

#include <assert.h>
#include <atomic>
#include <functional>
#include <ostream>
#include <stddef.h>

#include "alog/Logger.h"
#include "autil/Lock.h"
#include "autil/Span.h"
#include "build_service/builder/Builder.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/workflow/AsyncStarter.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/BuildFlowMode.h"
#include "build_service/workflow/DocReaderProducer.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/RealtimeErrorDefine.h"
#include "build_service/workflow/Workflow.h"
#include "build_service/workflow/WorkflowItem.h"
#include "indexlib/base/Progress.h"
#include "indexlib/document/locator.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/builder_branch_hinter.h"
#include "indexlib/partition/index_partition.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::partition;

using namespace build_service::config;
using namespace build_service::proto;

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, RawDocRtServiceBuilderImpl);

RawDocRtServiceBuilderImpl::RawDocRtServiceBuilderImpl(const std::string& configPath,
                                                       const IndexPartitionPtr& indexPart,
                                                       const RealtimeBuilderResource& builderResource)
    : RealtimeBuilderImplBase(configPath, indexPart, builderResource, true)
    , _producer(NULL)
    , _startSkipCalibrateDone(false)
    , _isProducerRecovered(false)
    , _rtSrcSignature(builderResource.realtimeInfo.getSrcSignature())
{
}

RawDocRtServiceBuilderImpl::~RawDocRtServiceBuilderImpl()
{
    stop();
    _buildFlow.reset();
    _factory.reset();
}

bool RawDocRtServiceBuilderImpl::doStart(const PartitionId& partitionId, KeyValueMap& kvMap)
{
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(kvMap[CONFIG_PATH]);
    BuildFlowMode buildFlowMode = BuildFlowMode::BUILDER;
    WorkflowMode workflowMode = build_service::workflow::REALTIME;
    PartitionId pIdAddDataTable;
    pIdAddDataTable.CopyFrom(partitionId);
    pIdAddDataTable.mutable_buildid()->set_datatable(kvMap[DATA_TABLE_NAME]);
    _factory.reset(new FlowFactory({}, _swiftClientCreator, _indexPartition));
    _buildFlow->startWorkLoop(resourceReader, pIdAddDataTable, kvMap, _factory.get(), buildFlowMode, workflowMode,
                              _metricProvider);

    _starter.setName("BsRawDocRtServiceBuild");
    _starter.asyncStart(bind(&RawDocRtServiceBuilderImpl::getBuilderAndProducer, this));
    return true;
}

bool RawDocRtServiceBuilderImpl::getBuilderAndProducer()
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

bool RawDocRtServiceBuilderImpl::producerSeek(const common::Locator& locator)
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

bool RawDocRtServiceBuilderImpl::producerAndBuilderPrepared() const
{
    if (!_builder || !_producer) {
        return false;
    }
    return true;
}

bool RawDocRtServiceBuilderImpl::getLastTimestampInProducer(int64_t& timestamp)
{
    if (_producer) {
        return _producer->getMaxTimestamp(timestamp);
    }
    return false;
}

bool RawDocRtServiceBuilderImpl::getLastReadTimestampInProducer(int64_t& timestamp)
{
    if (_producer) {
        return _producer->getLastReadTimestamp(timestamp);
    }
    return false;
}

bool RawDocRtServiceBuilderImpl::seekProducerToLatest(std::pair<int64_t, int64_t>& forceSeekInfo)
{
    if (!_producer) {
        return false;
    }
    int64_t lastReadTs = -1;
    if (!_producer->getLastReadTimestamp(lastReadTs)) {
        BS_LOG(WARN, "get producer last read timestamp failed");
        return false;
    }
    int64_t maxTs = -1;
    if (!_producer->getMaxTimestamp(maxTs)) {
        BS_LOG(WARN, "get producer max timestamp failed");
        return false;
    }
    if (maxTs == -1) {
        BS_LOG(WARN, "maxTs is -1, regard as seek success");
        return true;
    }
    common::Locator targetLocator;
    targetLocator.SetSrc(_producer->getLocatorSrc());
    targetLocator.SetOffset({maxTs, 0});
    bool ret = producerSeek(targetLocator);
    if (ret) {
        forceSeekInfo.first = lastReadTs;
        forceSeekInfo.second = maxTs;
        _indexPartition->SetForceSeekInfo(forceSeekInfo);
    }
    BS_LOG(WARN, "producer seek from [%ld] to latest locator[%s] %s", lastReadTs, targetLocator.DebugString().c_str(),
           ret ? "sucuess" : "failed");
    return ret;
}

void RawDocRtServiceBuilderImpl::externalActions()
{
    if (!_isProducerRecovered) {
        if (_producer && _isRecovered) {
            _producer->setRecovered(true);
            _isProducerRecovered = true;
        }
    }

    common::Locator rtLocator;
    common::Locator versionLocator;

    if (!_builder->getLastLocator(rtLocator)) {
        return;
    }

    // full version timestamp and rt timestamp are not comparable if inc processor not exist.
    //  full version timestamp came from full-processed swift topic, but rt builder is reading from raw swift topic.
    // if inc processor exist(e.g. mainse), rt builer will consume from inc-processed topic,
    //  whose creation is after full-processed topic destroyed, i.e. full version timestamp always smaller than
    //  inc-processed.
    uint64_t rtSrc = rtLocator.GetSrc();
    _rtSrcSignature.Get(&rtSrc);
    indexlib::index_base::Version incVersion = _indexPartition->GetPartitionData()->GetOnDiskVersion();
    versionLocator.Deserialize(incVersion.GetLocator().GetLocator());
    if (versionLocator.GetSrc() != rtSrc) {
        BS_INTERVAL_LOG(120, WARN, "version.locator.src[%lu] not match rtSrc[%lu], rtLocatr.src[%lu], do not seek",
                        versionLocator.GetSrc(), rtSrc, rtLocator.GetSrc());
        return;
    }
    bool fromInc = false;
    int64_t seekTs = indexlib::index_base::OnlineJoinPolicy::GetRtSeekTimestamp(incVersion.GetTimestamp(),
                                                                                rtLocator.GetOffset().first, fromInc);
    if (!fromInc && _startSkipCalibrateDone) {
        return;
    }
    common::Locator targetLocator(rtSrc, seekTs);

    if (!targetLocator.IsValid()) {
        BS_INTERVAL_LOG2(120, INFO, "[%s] get null latestLocator [%s]",
                         _partitionId.buildid().ShortDebugString().c_str(), targetLocator.DebugString().c_str());
        return;
    }

    if (producerSeek(targetLocator)) {
        _startSkipCalibrateDone = true;
    } else {
        BS_LOG(ERROR, "[%s] Skip to latestLocator [%s] fail!", _partitionId.buildid().ShortDebugString().c_str(),
               targetLocator.DebugString().c_str());
    }
}

}} // namespace build_service::workflow
