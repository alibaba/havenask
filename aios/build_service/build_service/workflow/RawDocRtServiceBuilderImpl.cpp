#include "build_service/workflow/RawDocRtServiceBuilderImpl.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/DocReaderProducer.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include <indexlib/partition/index_partition.h>
#include <indexlib/index/online_join_policy.h>
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(partition);

using namespace build_service::config;
using namespace build_service::proto;

namespace build_service {
namespace workflow {
BS_LOG_SETUP(workflow, RawDocRtServiceBuilderImpl);


RawDocRtServiceBuilderImpl::RawDocRtServiceBuilderImpl(
        const std::string &configPath,
        const IndexPartitionPtr &indexPart,
        const RealtimeBuilderResource& builderResource)
    : RealtimeBuilderImplBase(configPath, indexPart, builderResource, true)
    , _producer(NULL)
    , _startSkipCalibrateDone(false)      
{
}

RawDocRtServiceBuilderImpl::~RawDocRtServiceBuilderImpl() {
    stop();
}

bool RawDocRtServiceBuilderImpl::doStart(const PartitionId &partitionId, KeyValueMap &kvMap) {
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(kvMap[CONFIG_PATH]);
    BuildFlow::BuildFlowMode buildFlowMode = BuildFlow::ALL;
    WorkflowMode workflowMode = build_service::workflow::REALTIME;
    PartitionId pIdAddDataTable;
    pIdAddDataTable.CopyFrom(partitionId);
    pIdAddDataTable.mutable_buildid()->set_datatable(kvMap[DATA_TABLE_NAME]);
    _buildFlow->startWorkLoop(resourceReader, kvMap, pIdAddDataTable,
                              NULL, buildFlowMode, workflowMode, _metricProvider);

    _starter.setName("BsRawDocRtServiceBuild");
    _starter.asyncStart(tr1::bind(&RawDocRtServiceBuilderImpl::getBuilderAndProducer,
                                  this));
    return true;
}

bool RawDocRtServiceBuilderImpl::getBuilderAndProducer() {
    RawDocWorkflow *workflow = _buildFlow->getRawDocWorkflow();
    if (!workflow) {
        return false;
    }
    DocReaderProducer *producer =
        dynamic_cast<DocReaderProducer*>(workflow->getProducer());
    builder::Builder *builder = _buildFlow->getBuilder();
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

bool RawDocRtServiceBuilderImpl::producerSeek(const common::Locator &locator) {
    assert(_producer != NULL);
    if (!_producer->seek(locator)) {
        stringstream ss;
        ss << "seek to locator[" << locator.toDebugString() << "] failed";
        string errorMsg = ss.str();
        BS_LOG(WARN, "[%s] %s",
               _partitionId.buildid().ShortDebugString().c_str(), errorMsg.c_str());
        setErrorInfoUnsafe(ERROR_SWIFT_SEEK, errorMsg);
        return false;
    }
    return true;
}

bool RawDocRtServiceBuilderImpl::producerAndBuilderPrepared() const {
    if (!_builder || !_producer) {
        return false;
    }
    return true;
}

bool RawDocRtServiceBuilderImpl::getLastTimestampInProducer(int64_t &timestamp) {
    if (_producer) {
        return _producer->getMaxTimestamp(timestamp);
    }
    return false;
}

bool RawDocRtServiceBuilderImpl::getLastReadTimestampInProducer(int64_t &timestamp) {
    if (_producer) {
        return _producer->getLastReadTimestamp(timestamp);
    }
    return false;
}

bool RawDocRtServiceBuilderImpl::seekProducerToLatest(std::pair<int64_t, int64_t>& forceSeekInfo) {
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
    targetLocator.setSrc(_producer->getLocatorSrc());
    targetLocator.setOffset(maxTs);
    bool ret = producerSeek(targetLocator);
    if (ret) {
        forceSeekInfo.first = lastReadTs;
        forceSeekInfo.second = maxTs;
        _indexPartition->SetForceSeekInfo(forceSeekInfo);
    }
    BS_LOG(WARN, "producer seek from [%ld] to latest locator[%lu, %lu] %s", lastReadTs,
           targetLocator.getSrc(), targetLocator.getOffset(), ret ? "sucuess" : "failed");
    return ret;
}

void RawDocRtServiceBuilderImpl::externalActions() {
    common::Locator rtLocator;
    common::Locator versionLocator;

    if (!_builder->getLastLocator(rtLocator))
    {
        return;
    }
    IE_NAMESPACE(index_base)::Version incVersion
        = _indexPartition->GetPartitionData()->GetOnDiskVersion();
    
    versionLocator.fromString(incVersion.GetLocator().GetLocator());
    if (versionLocator.getSrc() != rtLocator.getSrc())
    {
        BS_INTERVAL_LOG(120, WARN, "version.locator.src[%ld] not match rtLocatr.src[%ld], do not seek",
                        versionLocator.getSrc(), rtLocator.getSrc());
        return;
    }
    bool fromInc = false;
    int64_t seekTs = IE_NAMESPACE(index)::OnlineJoinPolicy::GetRtSeekTimestamp(
        incVersion.GetTimestamp(), rtLocator.getOffset(), fromInc);

    if (!fromInc && _startSkipCalibrateDone) {
        return;
    }
    
    common::Locator targetLocator;
    targetLocator.setSrc(rtLocator.getSrc());
    targetLocator.setOffset(seekTs);
    
    if (targetLocator == common::INVALID_LOCATOR)
    {
        BS_INTERVAL_LOG2(120, INFO, "[%s] get null latestLocator [%s]",
                         _partitionId.buildid().ShortDebugString().c_str(),
                         targetLocator.toDebugString().c_str());
        return;
    }

    if (producerSeek(targetLocator))
    {
        _startSkipCalibrateDone = true;
    }
    else {
        BS_LOG(ERROR, "[%s] Skip to latestLocator [%s] fail!",
               _partitionId.buildid().ShortDebugString().c_str(),
               targetLocator.toDebugString().c_str());
    }
}

}
}
