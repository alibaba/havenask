#ifndef ISEARCH_BS_SORTEDBUILDER_H
#define ISEARCH_BS_SORTEDBUILDER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/Builder.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/builder/SortDocumentContainer.h"
#include "build_service/builder/AsyncBuilder.h"
#include <indexlib/util/memory_control/quota_control.h>
#include <autil/Thread.h>
#include <autil/Lock.h>

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);

namespace build_service {
namespace builder {

class SortedBuilder : public Builder 
{
public:
    SortedBuilder(const IE_NAMESPACE(partition)::IndexBuilderPtr &indexBuilder,
                  size_t buildTotalMemMB,
                  fslib::fs::FileLock *fileLock = NULL,
                  const proto::BuildId &buildId = proto::BuildId());
    ~SortedBuilder();
private:
    SortedBuilder(const SortedBuilder &);
    SortedBuilder& operator=(const SortedBuilder &);
public:
    /* override */ bool init(const config::BuilderConfig &builderConfig,
                             IE_NAMESPACE(misc)::MetricProviderPtr metricProvider = IE_NAMESPACE(misc)::MetricProviderPtr());
    /* override */ bool build(const IE_NAMESPACE(document)::DocumentPtr &doc);
    /* override */ void stop(int64_t stopTimestamp = INVALID_TIMESTAMP);
    /* override */ bool hasFatalError() const;
    bool tryDump() override;

    static bool reserveMemoryQuota(
            const config::BuilderConfig &builderConfig,
            size_t buildTotalMemMB,
            const IE_NAMESPACE(util)::QuotaControlPtr &quotaControl);

    static int64_t calculateSortQueueMemory(const config::BuilderConfig &builderConfig,
                                            size_t buildTotalMemMB);
private:
    void reportMetrics();
private:
    void initConfig(const config::BuilderConfig &builderConfig);
    bool initSortContainers(const config::BuilderConfig &builderConfig,
                            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema);
    bool initOneContainer(const config::BuilderConfig &builderConfig,
                          const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
                          SortDocumentContainer &container);

    void flush();
    void flushUnsafe();
    void buildAll();
    void dumpAll();
    void waitAllDocBuilt();
    void buildThread();
    bool initBuilder(const config::BuilderConfig &builderConfig,
                     IE_NAMESPACE(misc)::MetricProviderPtr metricProvider = IE_NAMESPACE(misc)::MetricProviderPtr());
    bool buildOneDoc(const IE_NAMESPACE(document)::DocumentPtr& document);
    bool needFlush(size_t sortQueueMemUse, size_t docCount);
    
private:
    enum BuildThreadStatus {
        WaitingSortDone = 0,
        Building = 1,
        Dumping = 2,
    };
    enum SortThreadStatus {
        WaitingBuildDone = 0,
        Pushing = 1,
        Sorting = 2,
    };
private:
    volatile bool _running;
    size_t _buildTotalMem;
    int64_t _sortQueueMem;
    uint32_t _sortQueueSize;
    mutable autil::ThreadMutex _collectContainerLock;
    mutable autil::ProducerConsumerCond _swapCond;
    SortDocumentContainer _collectContainer;
    SortDocumentContainer _buildContainer;
    AsyncBuilderPtr _asyncBuilder;
    autil::ThreadPtr _batchBuildThreadPtr;
    IE_NAMESPACE(misc)::MetricPtr _sortQueueSizeMetric;
    IE_NAMESPACE(misc)::MetricPtr _buildQueueSizeMetric;
    IE_NAMESPACE(misc)::MetricPtr _buildThreadStatusMetric;
    IE_NAMESPACE(misc)::MetricPtr _sortThreadStatusMetric;
    IE_NAMESPACE(misc)::MetricPtr _sortQueueMemUseMetric;
private:
    friend class SortedBuilderTest;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SortedBuilder);

inline bool SortedBuilder::buildOneDoc(const IE_NAMESPACE(document)::DocumentPtr& document)
{
    if (_asyncBuilder) {
        if (!_asyncBuilder->build(document))
        {
            setFatalError();
            return false;
        }
        return true;
    }
    //TODO: legacy for async build shut down
    if (!Builder::build(document)) {
        std::string errorMsg = "build failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        setFatalError();
        return false;
    }
    return true;
}

}
}

#endif //ISEARCH_BS_SORTEDBUILDER_H
