#ifndef ISEARCH_BS_PROCESSOR_H
#define ISEARCH_BS_PROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/DocProcessorChainConfig.h"
#include "build_service/processor/DocumentProcessorChain.h"
#include "build_service/processor/ProcessorMetricReporter.h"
#include "build_service/processor/ProcessorWorkItemExecutor.h"
#include "build_service/processor/ProcessorChainSelector.h"
#include <autil/Lock.h>
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include <indexlib/config/index_partition_schema.h>

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);

namespace build_service {
namespace config {
class DocProcessorChainConfig;
}
}

namespace build_service {
namespace processor {

class Processor : public proto::ErrorCollector
{
public:
    Processor(const std::string& strategyParam = "");
    virtual ~Processor();
private:
    Processor(const Processor &);
    Processor& operator=(const Processor &);
public:
    // virtual for mock
    virtual bool start(const config::ResourceReaderPtr &resourceReaderPtr,
                       const proto::PartitionId &partitionId,
                       IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
                       const IE_NAMESPACE(util)::CounterMapPtr& counterMap);
    virtual void stop();
public:
    virtual void processDoc(const document::RawDocumentPtr &rawDoc);
    virtual document::ProcessedDocumentVecPtr getProcessedDoc();
public:
    virtual uint32_t getWaitProcessCount() const;
    virtual uint32_t getProcessedCount() const;
protected:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr getIndexPartitionSchema(
        const config::ResourceReaderPtr &resourceReaderPtr,
        const config::DocProcessorChainConfig& docProcessorChainConfig) const;

    config::DocProcessorChainConfigVec rewriteRegionDocumentProcessorConfig(
            const config::ResourceReaderPtr& resourceReader,
            const config::DocProcessorChainConfigVec& inChains) const;

    bool initProcessorChainSelector(const config::ResourceReaderPtr& resourceReader,
                                    const proto::PartitionId &partitionId,
                                    const config::DocProcessorChainConfigVecPtr &chainConfig);
                                    
    virtual bool loadChain(const config::ResourceReaderPtr &resourceReaderPtr,
                           const proto::PartitionId &partitionId,
                           const IE_NAMESPACE(util)::CounterMapPtr& counterMap);
    
    static std::vector<std::string> getClusterNames(
            const config::DocProcessorChainConfig &chainConfig,
            const std::vector<std::string> &clusterNames);
protected:
    DocumentProcessorChainVecPtr getProcessorChains() const;
    void setProcessorChains(const DocumentProcessorChainVecPtr& newChains);
protected:
    DocumentProcessorChainVecPtr _chains;
    ProcessorWorkItemExecutorPtr _executor;
    mutable autil::ThreadMutex _chainMutex;
    IE_NAMESPACE(misc)::MetricProviderPtr _metricProvider;
    IE_NAMESPACE(misc)::MetricPtr _waitProcessCountMetric;
    ProcessorMetricReporter _reporter;
    IE_NAMESPACE(util)::AccumulativeCounterPtr _processErrorCounter;
    IE_NAMESPACE(util)::AccumulativeCounterPtr _processDocCountCounter;
    ProcessorChainSelectorPtr _chainSelector;
    std::string _strategyParam;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(Processor);

}
}

#endif //ISEARCH_BS_PROCESSOR_H
