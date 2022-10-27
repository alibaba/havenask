#ifndef ISEARCH_BS_SWIFTPROCESSEDDOCCONSUMER_H
#define ISEARCH_BS_SWIFTPROCESSEDDOCCONSUMER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/LoopThread.h>
#include "swift/client/SwiftWriter.h"
#include "build_service/workflow/Consumer.h"
#include "build_service/document/ProcessedDocument.h"


IE_NAMESPACE_BEGIN(misc);
class Metric;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);

DECLARE_REFERENCE_CLASS(util, StateCounter);
DECLARE_REFERENCE_CLASS(util, CounterMap);
BS_DECLARE_REFERENCE_CLASS(common, CounterSynchronizer);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);

namespace build_service {
namespace workflow {

struct SwiftWriterWithMetric {
    SwiftWriterWithMetric();
    void updateMetrics();
    bool isSendingDoc() const;
    std::shared_ptr<swift::client::SwiftWriter> swiftWriter;
    std::string zkRootPath;
    std::string topicName;
    IE_NAMESPACE(misc)::MetricPtr unsendMsgSizeMetric;
    IE_NAMESPACE(misc)::MetricPtr uncommittedMsgSizeMetric;
    IE_NAMESPACE(misc)::MetricPtr sendBufferFullQpsMetric;
};

class SwiftProcessedDocConsumer : public ProcessedDocConsumer
{
public:
    SwiftProcessedDocConsumer();
    virtual ~SwiftProcessedDocConsumer();
private:
    SwiftProcessedDocConsumer(const SwiftProcessedDocConsumer &);
    SwiftProcessedDocConsumer& operator=(const SwiftProcessedDocConsumer &);
public:
    bool init(const std::map<std::string, SwiftWriterWithMetric> &writers,
              const common::CounterSynchronizerPtr &counterSynchronizer,
              int64_t checkpointInterval,
              uint64_t srcSignature);
public:
    /* override */ FlowError consume(const document::ProcessedDocumentVecPtr &item);
    /* override */ bool getLocator(common::Locator &locator) const;
    /* override */ bool stop(FlowError lastError);
private:
    bool isFinished() const;
    void syncCountersWrapper();
    bool syncCounters(int64_t checkpointId);
    void updateMetric();
    int64_t getMinCheckpoint() const;
    bool initCounters(const IE_NAMESPACE(util)::CounterMapPtr& counterMap);
private:
    static std::string transToDocStr(
            const IE_NAMESPACE(document)::DocumentPtr &document);
private:
    static const int64_t MAX_SYNC_RETRY = 10;

private:
    std::map<std::string, SwiftWriterWithMetric> _writers;
    common::CounterSynchronizerPtr _counterSynchronizer;
    IE_NAMESPACE(util)::StateCounterPtr _srcCounter;
    IE_NAMESPACE(util)::StateCounterPtr _checkpointCounter;
    IE_NAMESPACE(util)::AccumulativeCounterPtr _consumedDocCountCounter;
    autil::LoopThreadPtr _syncCounterThread;    
    autil::LoopThreadPtr _updateMetricThread;
    uint64_t _src;
    mutable autil::ThreadMutex _lock;
    mutable autil::ThreadMutex _consumeLock;
    mutable common::Locator _cachedLocator;
    int64_t _latestInvalidDocCheckpoint;
    bool _hasCounter;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftProcessedDocConsumer);

}
}

#endif //ISEARCH_BS_SWIFTPROCESSEDDOCCONSUMER_H
