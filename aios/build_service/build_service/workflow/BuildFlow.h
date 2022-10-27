#ifndef ISEARCH_BS_BUILDFLOW_H
#define ISEARCH_BS_BUILDFLOW_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/Builder.h"
#include "build_service/processor/Processor.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/workflow/Workflow.h"
#include "build_service/workflow/BrokerFactory.h"
#include "build_service/workflow/AsyncStarter.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "indexlib/util/counter/counter_map.h"
#include <autil/Thread.h>
#include <autil/Lock.h>

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr; 
IE_NAMESPACE_END(misc);

namespace build_service {
namespace workflow {

class BuildFlow : public proto::ErrorCollector
{
public:
    BuildFlow(const util::SwiftClientCreatorPtr &swiftClientCreator =
              util::SwiftClientCreatorPtr(),
              const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPartition =
              IE_NAMESPACE(partition)::IndexPartitionPtr(),
              const BuildFlowThreadResource &threadResource =
              BuildFlowThreadResource());
    
    virtual ~BuildFlow();
private:
    BuildFlow(const BuildFlow &);
    BuildFlow& operator=(const BuildFlow &);
public:
    enum BuildFlowMode {
        NONE = 0,
        READER = 0x1,
        PROCESSOR = 0x2,   // not exist now.
        READER_AND_PROCESSOR = READER | PROCESSOR,
        BUILDER = 0x4,
        PROCESSOR_AND_BUILDER = PROCESSOR | BUILDER, // not exist now.
        ALL = READER | PROCESSOR | BUILDER
    };
public:
    // REALTIME
    void startWorkLoop(const config::ResourceReaderPtr &resourceReader,
                       const KeyValueMap &kvMap,
                       const proto::PartitionId &partitionId,
                       BrokerFactory *brokerFactory,
                       BuildFlow::BuildFlowMode buildFlowMode,
                       WorkflowMode workflowMode,
                       IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);

    // SERVICE or JOB
    // virtual for test
    virtual bool startBuildFlow(const config::ResourceReaderPtr &resourceReader,
                        const KeyValueMap &kvMap,
                        const proto::PartitionId &partitionId,
                        BrokerFactory *brokerFactory,
                        BuildFlow::BuildFlowMode buildFlowMode,
                        WorkflowMode workflowMode,
                        IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);
    bool waitFinish();
    void stop();
    bool hasFatalError() const;
    void resume();
    void suspend();
    virtual void fillErrorInfos(std::deque<proto::ErrorInfo> &errorInfos) const;
public:
    static BuildFlowMode getBuildFlowMode(proto::RoleType role);
public:
    virtual bool isStarted() const;
    virtual bool isFinished() const;
    virtual builder::Builder* getBuilder() const;
    virtual processor::Processor* getProcessor() const;
    virtual reader::RawDocumentReader* getReader() const;

    virtual bool suspendReadAtTimestamp(int64_t timestamp,
                                        reader::RawDocumentReader::ExceedTsAction action);

    ProcessedDocWorkflow *getProcessedDocWorkflow() const;
    RawDocWorkflow *getRawDocWorkflow() const;
    virtual bool getLocator(common::Locator &locator) const;
    IE_NAMESPACE(util)::CounterMapPtr getCounterMap() const { return _counterMap; }
    
private:
    bool buildFlowWorkLoop(const config::ResourceReaderPtr &resourceReader,
                           const KeyValueMap &kvMap,
                           const proto::PartitionId &partitionId,
                           BrokerFactory *brokerFactory,
                           BuildFlow::BuildFlowMode buildFlowMode,
                           WorkflowMode workflowMode,
                           IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);
    bool initWorkflows(BrokerFactory *brokerFactory,
                       const KeyValueMap &kvMap);

    virtual bool initRoleInitParam(const config::ResourceReaderPtr &resourceReader,
                                   const KeyValueMap &kvMap,
                                   const proto::PartitionId &partitionId,
                                   WorkflowMode workflowMode,
                                   IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
                                   BrokerFactory::RoleInitParam& param);

    bool initCounterMap(BuildFlowMode mode,
                        BrokerFactory *brokerFactory,
                        BrokerFactory *flowFactory,
                        BrokerFactory::RoleInitParam& param); 

    template <typename T>
    bool startWorkflow(Workflow<T> *flow, WorkflowMode workflowMode,
                       const WorkflowThreadPoolPtr& workflowThreadPool);

    template <typename T1, typename T2, typename T3>
    bool seek(Workflow<T1> *readToProcessorFlow, Workflow<T2> *processorToBuildFlow,
              Workflow<T3> *readToBuildFlow, const proto::PartitionId &partitionId);    

    builder::Builder* getBuilderUnsafe() const;
    processor::Processor* getProcessorUnsafe() const;
    reader::RawDocumentReader* getReaderUnsafe() const;

    void collectSubModuleErrorInfos() const;
    void clear();
private:
    virtual BrokerFactory *createFlowFactory() const;
private:
    static const uint32_t WAIT_FINISH_INTERVAL = 1000 * 1000; // 1s
private:
    AsyncStarter _starter;
    IE_NAMESPACE(partition)::IndexPartitionPtr _indexPartition;
    util::SwiftClientCreatorPtr _swiftClientCreator;
    std::unique_ptr<RawDocWorkflow> _readToProcessorFlow;
    std::unique_ptr<RawDocWorkflow> _readToBuildFlow;    
    std::unique_ptr<ProcessedDocWorkflow> _processorToBuildFlow;
    std::unique_ptr<BrokerFactory> _flowFactory;
    BrokerFactory *_brokerFactory;
    BuildFlowMode _mode;
    volatile WorkflowMode _workflowMode;
    volatile bool _buildFlowFatalError;
    volatile bool _isSuspend;
    bool _isReadToBuildFlow;
    mutable autil::ThreadMutex _mutex;
    IE_NAMESPACE(util)::CounterMapPtr _counterMap;
    BuildFlowThreadResource _buildFlowThreadResource;
private:
    friend class BuildFlowTest;
    BS_LOG_DECLARE();
};

template <typename T1, typename T2, typename T3>
bool BuildFlow::seek(Workflow<T1> *readToProcessorFlow, Workflow<T2> *processorToBuildFlow,
                     Workflow<T3> *readToBuildFlow, const proto::PartitionId &partitionId)
{
    common::Locator locator;
    if (readToProcessorFlow) {
        Consumer<T1> *consumer = readToProcessorFlow->getConsumer();
        if (!consumer->getLocator(locator)) {
            std::string errorMsg = std::string("consumer get locator failed");
            REPORT_ERROR_WITH_ADVICE(proto::BUILDFLOW_ERROR_SEEK, errorMsg, proto::BS_RETRY);
            return false;
        }
    }

    if (processorToBuildFlow) {
        Consumer<T2> *consumer = processorToBuildFlow->getConsumer();
        if (!consumer->getLocator(locator)) {
            std::string errorMsg = std::string("consumer get locator failed");
            REPORT_ERROR_WITH_ADVICE(proto::BUILDFLOW_ERROR_SEEK, errorMsg, proto::BS_RETRY);
            return false;
        }
    }

    if (readToBuildFlow) {
        Consumer<T3> *consumer = readToBuildFlow->getConsumer();
        if (!consumer->getLocator(locator)) {
            std::string errorMsg = std::string("consumer get locator failed");
            REPORT_ERROR_WITH_ADVICE(proto::BUILDFLOW_ERROR_SEEK, errorMsg, proto::BS_RETRY);
            return false;
        }
    }

    if (readToProcessorFlow) {
        Producer<T1> *producer = readToProcessorFlow->getProducer();
        BS_LOG(INFO, "[%s] seek locator [%s]",
               partitionId.buildid().ShortDebugString().c_str(),
               locator.toDebugString().c_str());
        if (locator != common::INVALID_LOCATOR && !producer->seek(locator)) {
            std::string errorMsg = std::string("producer seek locator: ") + locator.toDebugString() + " failed.";
            REPORT_ERROR_WITH_ADVICE(proto::BUILDFLOW_ERROR_SEEK, errorMsg, proto::BS_RETRY);
            return false;
        }
    } else if (processorToBuildFlow) {
        BS_LOG(INFO, "[%s] seek locator [%s]",
               partitionId.buildid().ShortDebugString().c_str(),
               locator.toDebugString().c_str());
        Producer<T2> *producer = processorToBuildFlow->getProducer();
        if (locator != common::INVALID_LOCATOR && !producer->seek(locator)) {
            std::string errorMsg = std::string("producer seek locator: ") + locator.toDebugString() + " failed.";
            REPORT_ERROR_WITH_ADVICE(proto::BUILDFLOW_ERROR_SEEK, errorMsg, proto::BS_RETRY);
            return false;
        }
    }

    if (readToBuildFlow) {
        BS_LOG(INFO, "[%s] seek locator [%s]",
               partitionId.buildid().ShortDebugString().c_str(),
               locator.toDebugString().c_str());
        Producer<T3> *producer = readToBuildFlow->getProducer();
        if (locator != common::INVALID_LOCATOR && !producer->seek(locator)) {
            std::string errorMsg = std::string("producer seek locator: ") + locator.toDebugString() + " failed.";
            REPORT_ERROR_WITH_ADVICE(proto::BUILDFLOW_ERROR_SEEK, errorMsg, proto::BS_RETRY);
            return false;
        }
    }
    return true;
}

}
}

#endif //ISEARCH_BS_BUILDFLOW_H
