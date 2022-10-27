#ifndef ISEARCH_BS_BATCHPROCESSOR_H
#define ISEARCH_BS_BATCHPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/Processor.h"
#include <autil/LoopThread.h>
#include <autil/Lock.h>

namespace build_service {
namespace processor {

class BatchProcessor : public Processor
{
public:
    BatchProcessor(const std::string& strategyParam);
    ~BatchProcessor();
private:
    BatchProcessor(const BatchProcessor &);
    BatchProcessor& operator=(const BatchProcessor &);
    
public:
    bool start(const config::ResourceReaderPtr &resourceReaderPtr,
               const proto::PartitionId &partitionId,
               IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
               const IE_NAMESPACE(util)::CounterMapPtr& counterMap) override;

    void processDoc(const document::RawDocumentPtr &rawDoc) override;

    void stop() override;
    
private:
    bool parseParam(const std::string& paramStr,
                    uint32_t &batchSize, int64_t &maxEnQueueTime) const;
    
    void checkConsumeTimeout();
    void resetDocQueue();
    void FlushDocQueue();
    
private:
    document::RawDocumentVecPtr _docQueue;
    mutable autil::RecursiveThreadMutex _queueMutex;
    autil::LoopThreadPtr _loopThreadPtr;
    uint32_t _batchSize;
    int64_t _maxEnQueueTime;
    volatile int64_t _inQueueTime;
    volatile int64_t _lastFlushTime;
    IE_NAMESPACE(misc)::MetricPtr _flushQueueSizeMetric;
    IE_NAMESPACE(misc)::MetricPtr _flushIntervalMetric;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BatchProcessor);

}
}

#endif //ISEARCH_BS_BATCHPROCESSOR_H
