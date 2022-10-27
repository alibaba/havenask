#include "build_service/workflow/BuildFlow.h"
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include "build_service/config/ResourceReader.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/workflow/FlowFactory.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "build_service/util/SwiftClientCreator.h"
#include "build_service/workflow/DocBuilderConsumer.h"
#include "build_service/util/SwiftAdminFacade.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::proto;
using namespace build_service::document;
using namespace build_service::reader;
using build_service::common::Locator;
using build_service::common::INVALID_LOCATOR;

namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, BuildFlow);

BuildFlow::BuildFlow(const util::SwiftClientCreatorPtr &swiftClientCreator,
                     const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPartition,
                     const BuildFlowThreadResource &threadResource)
    : _indexPartition(indexPartition)
    , _swiftClientCreator(swiftClientCreator)
    , _brokerFactory(NULL)
    , _mode(NONE)
    , _workflowMode(SERVICE)
    , _buildFlowFatalError(false)
    , _isSuspend(false)
    , _isReadToBuildFlow(false)
    , _buildFlowThreadResource(threadResource)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
    if (!_swiftClientCreator)
    {
        _swiftClientCreator.reset(new util::SwiftClientCreator);
    }
}

BuildFlow::~BuildFlow() {
    stop();
    _readToBuildFlow.reset();
    _readToProcessorFlow.reset();
    _processorToBuildFlow.reset();
    _flowFactory.reset();
    _brokerFactory = NULL;
}

void BuildFlow::startWorkLoop(const ResourceReaderPtr &resourceReader,
                              const KeyValueMap &kvMap,
                              const proto::PartitionId &partitionId,
                              BrokerFactory *brokerFactory,
                              BuildFlowMode buildFlowMode,
                              WorkflowMode workflowMode,
                              IE_NAMESPACE(misc)::MetricProviderPtr metricProvider)
{
    initBeeperTag(partitionId);
    _starter.setName("BsBuildFlow");
    _starter.asyncStart(tr1::bind(&BuildFlow::buildFlowWorkLoop,
                                  this, resourceReader, kvMap, partitionId,
                                  brokerFactory, buildFlowMode, workflowMode, metricProvider));
}

bool BuildFlow::buildFlowWorkLoop(const ResourceReaderPtr &resourceReader,
                                  const KeyValueMap &kvMap,
                                  const proto::PartitionId &partitionId,
                                  BrokerFactory *brokerFactory,
                                  BuildFlowMode buildFlowMode,
                                  WorkflowMode workflowMode,
                                  IE_NAMESPACE(misc)::MetricProviderPtr metricProvider)
{
    if (!isStarted()) {
        BS_LOG(INFO, "[%s] start BuildFlow",
               partitionId.buildid().ShortDebugString().c_str());
        if (!startBuildFlow(resourceReader, kvMap, partitionId,
                            brokerFactory, buildFlowMode, workflowMode,
                            metricProvider))
        {
            BS_LOG(INFO, "[%s] BuildFlow failed, retry later",
                   partitionId.buildid().ShortDebugString().c_str());
            return false;
        }
    }

    return true;
}

bool BuildFlow::initCounterMap(BuildFlowMode mode,
                               BrokerFactory *brokerFactory,
                               BrokerFactory *flowFactory,
                               BrokerFactory::RoleInitParam& param)
{
    if (mode & BUILDER) {
        return flowFactory->initCounterMap(param);
    } else {
        return brokerFactory->initCounterMap(param);
    }
}


bool BuildFlow::startBuildFlow(const ResourceReaderPtr &resourceReader,
                               const KeyValueMap &kvMap,
                               const proto::PartitionId &partitionId,
                               BrokerFactory *brokerFactory,
                               BuildFlowMode buildFlowMode,
                               WorkflowMode workflowMode,
                               IE_NAMESPACE(misc)::MetricProviderPtr metricProvider)
{
    autil::ScopedLock lock(_mutex);

    // clear members
    clear();

    _mode = buildFlowMode;
    _workflowMode = workflowMode;
    _brokerFactory = brokerFactory;
    _buildFlowFatalError = false;

    bool hasReader = _mode & READER;
    bool hasProcessor = _mode & PROCESSOR;
    bool hasBuilder = _mode & BUILDER;

    // todo: determine workflow mode from BuildFlowMode: JOB or SERVICE
    _flowFactory.reset(createFlowFactory());
    BrokerFactory::RoleInitParam param;
    if (!initRoleInitParam(resourceReader, kvMap, partitionId, workflowMode, 
                           metricProvider, param)) {
        return false;
    }

    string realTimeMode = getValueFromKeyValueMap(kvMap, REALTIME_MODE);
    if (realTimeMode == REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE)
    {
        param.isReadToBuildFlow = true;
    }
    if (hasBuilder && partitionId.step() == BUILD_STEP_INC)
    {
        config::ControlConfig controlConfig;
        if (resourceReader->getDataTableConfigWithJsonPath(partitionId.buildid().datatable(),
                                                           "control_config", controlConfig))
        {
            param.isReadToBuildFlow = !controlConfig.isIncProcessorExist;
        }
    }
    _counterMap = param.counterMap;
    _isReadToBuildFlow = param.isReadToBuildFlow;;
    if (_isReadToBuildFlow) {
        unique_ptr<RawDocProducer> producer(_flowFactory->createRawDocProducer(param));
        if (!producer.get())
        {
            BS_LOG(INFO, "create RawDocProducer failed"); 
            clear();
            return false;
        }
        unique_ptr<RawDocConsumer> consumer(_flowFactory->createRawDocConsumer(param));
        if (!consumer.get())
        {
            BS_LOG(INFO, "create RawDocConsumer failed"); 
            clear();
            return false;
        }
        _readToBuildFlow.reset(new RawDocWorkflow(producer.release(), consumer.release()));
    } else {
        if (hasReader || hasProcessor) {
            unique_ptr<RawDocProducer> producer(hasReader ?
                                                _flowFactory->createRawDocProducer(param)
                                                : brokerFactory->createRawDocProducer(param));
            unique_ptr<RawDocConsumer> consumer (hasProcessor ?
                                                 _flowFactory->createRawDocConsumer(param)
                                                 : brokerFactory->createRawDocConsumer(param));
            if (!producer.get() || !consumer.get()) {
                clear();
                return false;
            }
            _readToProcessorFlow.reset(new RawDocWorkflow(producer.release(), consumer.release()));
        }

        if (hasProcessor || hasBuilder) {
            unique_ptr<ProcessedDocProducer> producer(hasProcessor ?
                                                      _flowFactory->createProcessedDocProducer(param)
                                                      : brokerFactory->createProcessedDocProducer(param));
            unique_ptr<ProcessedDocConsumer> consumer(hasBuilder ?
                                                      _flowFactory->createProcessedDocConsumer(param)
                                                      : brokerFactory->createProcessedDocConsumer(param));
            if (!producer.get() || !consumer.get()) {
                clear();
                return false;
            }
            _processorToBuildFlow.reset(new ProcessedDocWorkflow(producer.release(), consumer.release()));
        }
    }
    
    if (_isSuspend) {
        if (_readToProcessorFlow != nullptr) {
            _readToProcessorFlow->suspend();
        }
        if (_processorToBuildFlow != nullptr) {
            _processorToBuildFlow->suspend();
        }
        if (_readToBuildFlow != nullptr) {
            _readToBuildFlow->suspend();
        }
    }

    if (!seek(_readToProcessorFlow.get(), _processorToBuildFlow.get(), _readToBuildFlow.get(), partitionId)) {
        clear();
        return false;
    }

    if (!startWorkflow(_readToProcessorFlow.get(), workflowMode,
                       _buildFlowThreadResource.readerToProcessorThreadPool) ||
        !startWorkflow(_processorToBuildFlow.get(), workflowMode,
                       _buildFlowThreadResource.processorToBuilderThreadPool) ||
        !startWorkflow(_readToBuildFlow.get(), workflowMode,
                       _buildFlowThreadResource.readerToProcessorThreadPool))  // threadpool is null in job and service
    {
        clear();
        return false;
    }
    return true;
}

template <typename T>
bool BuildFlow::startWorkflow(Workflow<T> *flow, WorkflowMode workflowMode,
                              const WorkflowThreadPoolPtr& workflowThreadPool) {
    if (!flow) {
        return true;
    }
    if (!flow->start(workflowMode, workflowThreadPool)) {
        BS_LOG(ERROR, "start workflow[%s] failed.", typeid(*flow).name());

        string msg = "start workflow[" + string(typeid(*flow).name()) + "] failed.";
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, msg);
        return false;
    }
    return true;
}

void BuildFlow::stop() {
    _starter.stop();

    autil::ScopedLock lock(_mutex);
    if (_readToProcessorFlow != nullptr) {
        _readToProcessorFlow->stop();
    }
    if (_processorToBuildFlow != nullptr) {
        _processorToBuildFlow->stop();
    }
    if (_readToBuildFlow != nullptr) {
        _readToBuildFlow->stop();
    }
}

bool BuildFlow::waitFinish() {
    while (!isFinished()) {
        if (hasFatalError()) {
            string errorMsg = "Wait finish failed, has fatal error.";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);            
            return false;
        }
        usleep(WAIT_FINISH_INTERVAL);
    }
    return true;
}

BrokerFactory *BuildFlow::createFlowFactory() const {
    return new FlowFactory(_swiftClientCreator, _indexPartition);
}

BuildFlow::BuildFlowMode BuildFlow::getBuildFlowMode(proto::RoleType role) {
    if (role == proto::ROLE_BUILDER) {
        return BUILDER;
    } else if (role == proto::ROLE_PROCESSOR) {
        return READER_AND_PROCESSOR;
    } else if (role == proto::ROLE_JOB) {
        return ALL;
    }
    assert(false);
    return ALL;
}

builder::Builder* BuildFlow::getBuilderUnsafe() const {
    FlowFactory *flowFactory = dynamic_cast<FlowFactory *>(_flowFactory.get());
    return flowFactory ? flowFactory->getBuilder() : NULL;
}

builder::Builder* BuildFlow::getBuilder() const {
    autil::ScopedLock lock(_mutex);
    return getBuilderUnsafe();
}

processor::Processor* BuildFlow::getProcessorUnsafe() const {
    FlowFactory *flowFactory = dynamic_cast<FlowFactory *>(_flowFactory.get());
    return flowFactory ? flowFactory->getProcessor() : NULL;
}

processor::Processor* BuildFlow::getProcessor() const {
    autil::ScopedLock lock(_mutex);
    return getProcessorUnsafe();
}

reader::RawDocumentReader* BuildFlow::getReaderUnsafe() const {
    FlowFactory *flowFactory = dynamic_cast<FlowFactory *>(_flowFactory.get());
    return flowFactory ? flowFactory->getReader() : NULL;
}

reader::RawDocumentReader* BuildFlow::getReader() const {
    autil::ScopedLock lock(_mutex);
    return getReaderUnsafe();
}

bool BuildFlow::suspendReadAtTimestamp(int64_t timestamp, RawDocumentReader::ExceedTsAction action) {
    if ((_mode & READER)) {
        reader::RawDocumentReader* reader = getReader();
        if (!reader) {
            BS_LOG(ERROR, "reader is not exist, "
                   "suspend buildFlow at %ld, action[%d] failed",
                   timestamp, action);
            return false;
        }
        reader->suspendReadAtTimestamp(timestamp, action);
        BS_LOG(INFO, "reader will stop at swift timestamp[%ld]", timestamp);

        string msg = "reader will stop at swift timestamp[" + StringUtil::toString(timestamp) + "]";
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
        return true;
    } else if (_mode & BUILDER) {
        autil::ScopedLock lock(_mutex);
        if (_readToBuildFlow) {
            reader::RawDocumentReader* reader = getReader();
            if (!reader) {
                BS_LOG(ERROR, "reader is not exist, "
                       "suspend buildFlow at %ld, action[%d] failed",
                       timestamp, action);
                return false;
            }
            reader->suspendReadAtTimestamp(timestamp, action);
            BS_LOG(INFO, "reader will stop at swift timestamp[%ld]", timestamp);
            RawDocBuilderConsumer* rawDocBuilderConsumer = dynamic_cast<RawDocBuilderConsumer*>(_readToBuildFlow->getConsumer());
            rawDocBuilderConsumer->SetEndBuildTimestamp(timestamp);
            BS_LOG(INFO, "builder will stop at swift timestamp[%ld]", timestamp);
            return true;
        }
        if (!_processorToBuildFlow.get()) {
            BS_LOG(ERROR, "processorToBuildFlow is not exist, "
                   "suspend buildFlow at %ld, action[%d] failed",
                   timestamp, action);
            return false;
        }
        ProcessedDocProducer *producer = _processorToBuildFlow->getProducer();
        SwiftProcessedDocProducer *swiftProducer =
            dynamic_cast<SwiftProcessedDocProducer *>(producer);
        if (!swiftProducer) {
            BS_LOG(ERROR, "SwiftProcessedDocProducer is not exist, "
                   "suspend buildFlow at %ld, action[%d] failed",
                   timestamp, action);
            return false;
        }
        swiftProducer->suspendReadAtTimestamp(timestamp);

        ProcessedDocConsumer *consumer = _processorToBuildFlow->getConsumer();
        DocBuilderConsumer *docBuilderConsumer =
            dynamic_cast<DocBuilderConsumer *>(consumer);
        if (!docBuilderConsumer) {
            BS_LOG(ERROR, "cast consumer to DocBuilderConsumer failed, "
                   "suspend buildFlow at %ld, action[%d] failed",
                   timestamp, action);

            return false;
        }
        docBuilderConsumer->SetEndBuildTimestamp(timestamp);
        BS_LOG(INFO, "builder will stop at swift timestamp[%ld]", timestamp);
        string msg = "builder will stop at swift timestamp[" + StringUtil::toString(timestamp) + "]";
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
        return true;
    } else {
        assert(false);
        return false;
    }
}

bool BuildFlow::isFinished() const {
    autil::ScopedLock lock(_mutex);
    if (_readToProcessorFlow == nullptr && _processorToBuildFlow == nullptr && _readToBuildFlow == nullptr) {
        return false;
    }

    if (_readToProcessorFlow != nullptr && !_readToProcessorFlow->isFinished()) {
        return false;
    }

    if (_processorToBuildFlow != nullptr && !_processorToBuildFlow->isFinished()) {
        return false;
    }
    if (_readToBuildFlow != nullptr && !_readToBuildFlow->isFinished()) {
        return false;
    }

    return true;
}

bool BuildFlow::isStarted() const {
    autil::ScopedLock lock(_mutex);

    if (_mode == NONE) {
        return false;
    }

    if (_isReadToBuildFlow) {
        return _readToBuildFlow != nullptr;
    }
    
    if ((_mode & READER) && _readToProcessorFlow == nullptr) {
        return false;
    }

    if ((_mode & PROCESSOR) && _processorToBuildFlow == nullptr) {
        return false;
    }

    if ((_mode & BUILDER) && _processorToBuildFlow == nullptr) {
        return false;
    }

    return true;
}

void BuildFlow::resume() {
    autil::ScopedLock lock(_mutex);
    _isSuspend = false;
    if (_processorToBuildFlow != nullptr) {
        _processorToBuildFlow->resume();
    }
    if (_readToProcessorFlow != nullptr) {
        _readToProcessorFlow->resume();
    }
    if (_readToBuildFlow != nullptr) {
        _readToBuildFlow->resume();
    }
}

void BuildFlow::suspend() {
    autil::ScopedLock lock(_mutex);
    _isSuspend = true;
    if (_readToProcessorFlow != nullptr) {
        _readToProcessorFlow->suspend();
    }
    if (_processorToBuildFlow != nullptr) {
        _processorToBuildFlow->suspend();
    }
    if (_readToBuildFlow != nullptr) {
        _readToBuildFlow->suspend();
    }
}

void BuildFlow::collectSubModuleErrorInfos() const {
    vector<proto::ErrorInfo> errorInfos;
    if (_flowFactory.get()) {
        _flowFactory->collectErrorInfos(errorInfos);
    }

    if (_brokerFactory) {
        _brokerFactory->fillErrorInfos(errorInfos);
    }

    reader::RawDocumentReader *reader = getReaderUnsafe();
    if (reader) {
        reader->fillErrorInfos(errorInfos);
    }

    processor::Processor *processor = getProcessorUnsafe();
    if (processor) {
        processor->fillErrorInfos(errorInfos);
    }

    builder::Builder *builder = getBuilderUnsafe();
    if (builder) {
        builder->fillErrorInfos(errorInfos);
    }

    addErrorInfos(errorInfos);
}

void BuildFlow::fillErrorInfos(deque<proto::ErrorInfo> &errorInfos) const {
    collectSubModuleErrorInfos();
    ErrorCollector::fillErrorInfos(errorInfos);
}

bool BuildFlow::hasFatalError() const {
    ScopedLock lock(_mutex);
    if (_buildFlowFatalError) {
        return true;
    }

    if (_readToProcessorFlow != nullptr && _readToProcessorFlow->hasFatalError()) {
        return true;
    }

    if (_processorToBuildFlow != nullptr && _processorToBuildFlow->hasFatalError()) {
        return true;
    }
    
    if (_readToBuildFlow != nullptr && _readToBuildFlow->hasFatalError()) {
        return true;
    }

    return false;
}

bool BuildFlow::initRoleInitParam(const ResourceReaderPtr &resourceReader,
                                  const KeyValueMap &kvMap,
                                  const PartitionId &partitionId,
                                  WorkflowMode workflowMode,
                                  IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
                                  BrokerFactory::RoleInitParam& param)
{

    config::BuilderClusterConfig clusterConfig;
    const string &clusterName = partitionId.clusternames(0);
    string mergeConfigName = getValueFromKeyValueMap(kvMap, MERGE_CONFIG_NAME);
    config::BuilderConfig builderConfig;
    if (clusterConfig.init(clusterName, *resourceReader, mergeConfigName)) {
        builderConfig = clusterConfig.builderConfig;
    }
    param.resourceReader = resourceReader;
    param.kvMap = kvMap;
    if (workflowMode == build_service::workflow::REALTIME) {
        if (builderConfig.documentFilter) {
            param.swiftFilterMask = ProcessedDocument::SWIFT_FILTER_BIT_REALTIME;
        }
        else {
            param.swiftFilterMask = ProcessedDocument::SWIFT_FILTER_BIT_NONE;
        }
        param.swiftFilterResult = 0;
    } else {
        param.swiftFilterMask = ProcessedDocument::SWIFT_FILTER_BIT_NONE;
        param.swiftFilterResult = 0;
    }
    param.partitionId = partitionId;
    param.metricProvider = metricProvider;
    if (!initCounterMap(_mode, _brokerFactory, _flowFactory.get(), param)) {
        BS_LOG(ERROR, "init counter map failed");
    }

    if (_indexPartition) {
        param.schema = _indexPartition->GetSchema();
        SwiftAdminFacade::getTopicId(param.schema, param.specifiedTopicId);
    }

    if (!param.schema) {
        // if can not get schema from indexPartition, get it from resource reader
        param.schema = resourceReader->getSchema(clusterName); 
    }
    return true;
}

void BuildFlow::clear() {
    collectSubModuleErrorInfos();
    _readToBuildFlow.reset();
    _readToProcessorFlow.reset();
    _processorToBuildFlow.reset();
    _flowFactory.reset();
    BS_LOG(INFO, "clear build flow");
}

ProcessedDocWorkflow* BuildFlow::getProcessedDocWorkflow() const {
    autil::ScopedLock lock(_mutex);
    return _processorToBuildFlow.get();
}

RawDocWorkflow *BuildFlow::getRawDocWorkflow() const {
    autil::ScopedLock lock(_mutex);
    if (_isReadToBuildFlow) {
        return _readToBuildFlow.get();
    }
    return _readToProcessorFlow.get();
}

bool BuildFlow::getLocator(Locator &locator) const {
    if (_readToBuildFlow != nullptr) {
        assert(_readToBuildFlow->getConsumer());
        return _readToBuildFlow->getConsumer()->getLocator(locator);
    } else if (_processorToBuildFlow.get()) {
        assert(_processorToBuildFlow->getConsumer());
        return _processorToBuildFlow->getConsumer()->getLocator(locator);
    } else if (_readToProcessorFlow.get()) {
        assert(_readToProcessorFlow->getConsumer());
        return _readToProcessorFlow->getConsumer()->getLocator(locator);
    }
    return false;
}

}
}
