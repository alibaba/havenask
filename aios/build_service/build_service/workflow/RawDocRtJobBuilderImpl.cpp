#include "build_service/workflow/RawDocRtJobBuilderImpl.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/DocReaderProducer.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include <indexlib/partition/index_partition.h>

using namespace std;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(partition);

using namespace build_service::config;
using namespace build_service::proto;

namespace build_service {
namespace workflow {
BS_LOG_SETUP(workflow, RawDocRtJobBuilderImpl);


RawDocRtJobBuilderImpl::RawDocRtJobBuilderImpl(
        const std::string &configPath,
        const IndexPartitionPtr &indexPart,
        const RealtimeBuilderResource& builderResource)
    : RealtimeBuilderImplBase(configPath, indexPart, builderResource, false)
    , _producer(NULL)
{
}

RawDocRtJobBuilderImpl::~RawDocRtJobBuilderImpl() {
    stop();
}

bool RawDocRtJobBuilderImpl::doStart(const PartitionId &partitionId, KeyValueMap &kvMap) {
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(kvMap[CONFIG_PATH]);
    BuildFlow::BuildFlowMode buildFlowMode = BuildFlow::ALL;
    WorkflowMode workflowMode = build_service::workflow::REALTIME;
    PartitionId pIdAddDataTable;
    pIdAddDataTable.CopyFrom(partitionId);
    pIdAddDataTable.mutable_buildid()->set_datatable(kvMap[DATA_TABLE_NAME]);
    _buildFlow->startWorkLoop(resourceReader, kvMap, pIdAddDataTable,
                              NULL, buildFlowMode, workflowMode, _metricProvider);

    _starter.setName("BsRawDocRtBuild");
    _starter.asyncStart(tr1::bind(&RawDocRtJobBuilderImpl::getBuilderAndProducer,
                                  this));
    return true;
}

bool RawDocRtJobBuilderImpl::getBuilderAndProducer() {
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

bool RawDocRtJobBuilderImpl::producerSeek(const common::Locator &locator) {
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

bool RawDocRtJobBuilderImpl::producerAndBuilderPrepared() const {
    if (!_builder || !_producer) {
        return false;
    }
    return true;
}

bool RawDocRtJobBuilderImpl::getLastTimestampInProducer(int64_t &timestamp) {
    if (_producer) {
        return _producer->getMaxTimestamp(timestamp);
    }
    return false;
}

bool RawDocRtJobBuilderImpl::getLastReadTimestampInProducer(int64_t &timestamp) {
    if (_producer) {
        return _producer->getLastReadTimestamp(timestamp);
    }
    return false;
}

}
}
