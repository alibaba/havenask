#include "build_service/workflow/ProcessedDocRtBuilderImpl.h"
#include "build_service/workflow/SwiftBrokerFactory.h"
#include "build_service/builder/Builder.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/common/Locator.h"
#include "build_service/util/Monitor.h"
#include <indexlib/misc/metric_provider.h>
#include <indexlib/partition/online_partition.h>
#include <autil/TimeUtility.h>

using namespace std;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(partition);
using namespace autil;

using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::config;

namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, ProcessedDocRtBuilderImpl);

ProcessedDocRtBuilderImpl::ProcessedDocRtBuilderImpl(
        const std::string &configPath,
        const IndexPartitionPtr &indexPart,
        const RealtimeBuilderResource& builderResource)
    : RealtimeBuilderImplBase(configPath, indexPart, builderResource, false)
    , _producer(NULL)
    , _startSkipCalibrateDone(false)
    , _lastSkipedLocator(INVALID_LOCATOR)
{
}

ProcessedDocRtBuilderImpl::~ProcessedDocRtBuilderImpl() {
    stop();
}

bool ProcessedDocRtBuilderImpl::doStart(const PartitionId &partitionId, KeyValueMap &kvMap) {
    _brokerFactory.reset(createBrokerFactory());
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
    BuildFlow::BuildFlowMode buildFlowMode = BuildFlow::BUILDER;
    // in this mode, inc job is not allowed, inc index and rt index are built from
    // the same swift, we can use inc index's locator to seek rt build
    WorkflowMode workflowMode = build_service::workflow::REALTIME;

    // set step to BUILD_STEP_INC so that RtBuilder could find corresponding broker topic
    PartitionId rtPartitionId;
    rtPartitionId.CopyFrom(partitionId);
    rtPartitionId.set_step(BUILD_STEP_INC);

    resetStartSkipTimestamp(kvMap);
    _buildFlow->startWorkLoop(resourceReader, kvMap, rtPartitionId,
                              _brokerFactory.get(), buildFlowMode,
                              workflowMode, _metricProvider);

    _starter.setName("BsDocRtBuild");
    _starter.asyncStart(tr1::bind(&ProcessedDocRtBuilderImpl::getBuilderAndProducer,
                                  this));
    return true;
}

void ProcessedDocRtBuilderImpl::externalActions() {
    skipToLatestLocator();
    reportFreshnessWhenSuspendBuild();
}

bool ProcessedDocRtBuilderImpl::producerAndBuilderPrepared() const {
    if (!_builder || !_producer) {
        return false;
    }
    return true;
}

bool ProcessedDocRtBuilderImpl::getBuilderAndProducer() {
    ProcessedDocWorkflow *workflow = _buildFlow->getProcessedDocWorkflow();
    if (!workflow) {
        return false;
    }
    SwiftProcessedDocProducer *producer =
        dynamic_cast<SwiftProcessedDocProducer*>(workflow->getProducer());
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

bool ProcessedDocRtBuilderImpl::getLastTimestampInProducer(int64_t &timestamp) {
    if (_producer) {
        return _producer->getMaxTimestamp(timestamp);
    }
    return false;
}

bool ProcessedDocRtBuilderImpl::getLastReadTimestampInProducer(int64_t &timestamp) {
    if (_producer) {
        return _producer->getLastReadTimestamp(timestamp);
    }
    return false;
}

bool ProcessedDocRtBuilderImpl::producerSeek(const Locator &locator) {
    assert(_producer != NULL);
    Locator targetLocator = locator;
    if (targetLocator.getSrc() != (uint64_t)_producer->getStartTimestamp())
    {
        BS_INTERVAL_LOG2(120, INFO, "[%s] reset target skip locator [%s] source to [%ld]",
                         _partitionId.buildid().ShortDebugString().c_str(),
                         targetLocator.toDebugString().c_str(),
                         _producer->getStartTimestamp());
        targetLocator.setSrc(_producer->getStartTimestamp());
    }
    
    if (targetLocator == _lastSkipedLocator) {
        BS_INTERVAL_LOG2(120, INFO, "[%s] already skip to locator [%s] a moment ago!",
                         _partitionId.buildid().ShortDebugString().c_str(),
                         targetLocator.toDebugString().c_str());
        return true;
    }

    if (!_producer->seek(targetLocator)) {
        stringstream ss;
        ss << "seek to locator[" << targetLocator.toDebugString() << "] failed";
        string errorMsg = ss.str();
        BS_LOG(WARN, "[%s] %s",
               _partitionId.buildid().ShortDebugString().c_str(), errorMsg.c_str());
        setErrorInfoUnsafe(ERROR_SWIFT_SEEK, errorMsg);
        return false;
    }

    BS_LOG(INFO, "[%s] Skip to latestLocator [%s]",
           _partitionId.buildid().ShortDebugString().c_str(),
           targetLocator.toDebugString().c_str());
    _lastSkipedLocator = targetLocator;
    return true;
}

void ProcessedDocRtBuilderImpl::skipToLatestLocator() {
    Locator latestLocator;
    bool fromInc = false;
    if (!getLatestLocator(latestLocator, fromInc)) {
        return;
    }
    
    if (!fromInc && _startSkipCalibrateDone) {
        return;
    }

    if (latestLocator == common::INVALID_LOCATOR)
    {
        BS_INTERVAL_LOG2(120, INFO, "[%s] get null latestLocator [%s]",
                         _partitionId.buildid().ShortDebugString().c_str(),
                         latestLocator.toDebugString().c_str());
        return;
    }

    if (producerSeek(latestLocator))
    {
        _startSkipCalibrateDone = true;
    } else {
        BS_LOG(ERROR, "[%s] Skip to latestLocator [%s] fail!",
               _partitionId.buildid().ShortDebugString().c_str(),
               latestLocator.toDebugString().c_str());
    }
}

BrokerFactory *ProcessedDocRtBuilderImpl::createBrokerFactory() const {
    return new SwiftBrokerFactory(_swiftClientCreator);
}

void ProcessedDocRtBuilderImpl::reportFreshnessWhenSuspendBuild() {
    if (!_autoSuspend && !_adminSuspend) {
        return;
    }

    Locator latestLocator;
    getLatestLocator(latestLocator);

    if (latestLocator == INVALID_LOCATOR) {
        return;
    }

    assert(_producer);
    _producer->reportFreshnessMetrics(latestLocator.getOffset());
}

int64_t ProcessedDocRtBuilderImpl::getStartSkipTimestamp() const {
    return _indexPartition->GetRtSeekTimestampFromIncVersion();
}

void ProcessedDocRtBuilderImpl::resetStartSkipTimestamp(KeyValueMap &kvMap) {

    KeyValueMap::const_iterator it = kvMap.find(CHECKPOINT);
    if (it != kvMap.end()) {
        BS_LOG(INFO, "[%s] in kvMap already set to [%s]",
               CHECKPOINT.c_str(), it->second.c_str());
        return;
    }

    int64_t startSkipTs = getStartSkipTimestamp();
    if (startSkipTs <= 0) {
        return;
    }
    kvMap[CHECKPOINT] = StringUtil::toString(startSkipTs);
    BS_LOG(INFO, "[%s] in kvMap is set to [%ld]", CHECKPOINT.c_str(), startSkipTs);
}

}
}
