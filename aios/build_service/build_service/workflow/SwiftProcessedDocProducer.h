#ifndef ISEARCH_BS_SWIFTPROCESSEDDOCPRODUCER_H
#define ISEARCH_BS_SWIFTPROCESSEDDOCPRODUCER_H

#include "build_service/common_define.h"
#include "build_service/common/End2EndLatencyReporter.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Producer.h"
#include "build_service/document/ProcessedDocument.h"
#include <swift/client/SwiftReader.h>
#include <indexlib/document/document.h>
#include <indexlib/document/document_factory_wrapper.h>
#include <indexlib/document/builtin_parser_init_param.h>
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/util/counter/counter.h>
#include <indexlib/config/index_partition_schema.h>
#include <autil/Lock.h>
#include <atomic>

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
IE_NAMESPACE_END(misc);

namespace build_service {
namespace workflow {

class SwiftProcessedDocProducer : public ProcessedDocProducer
{
public:
    static const int64_t CONNECT_TIMEOUT_INTERVAL = 60 * 1000 * 1000; // 60s
    static const int64_t REPORT_INTERVAL = 1000 * 1000;
private:
    static const int64_t MAX_READ_DELAY = 5 * 60 * 1000 * 1000; // 5 min
public:
    SwiftProcessedDocProducer(SWIFT_NS(client)::SwiftReader *reader,
                              const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema,
                              const std::string &buildIdStr);
    ~SwiftProcessedDocProducer();
private:
    SwiftProcessedDocProducer(const SwiftProcessedDocProducer &);
    SwiftProcessedDocProducer& operator=(const SwiftProcessedDocProducer &);
public:
    bool init(IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
              const std::string& pluginPath,
              const IE_NAMESPACE(util)::CounterMapPtr &counterMap,
              int64_t startTimestamp,
              int64_t stopTimestamp,
              int64_t noMoreMsgPeriod,
              int64_t maxCommitInterval);
public:
    /* override */ FlowError produce(document::ProcessedDocumentVecPtr &processedDocVec);
    /* override */ bool seek(const common::Locator &locator);
    /* override */ bool stop();
public:
    // virtual for test
    virtual int64_t suspendReadAtTimestamp(int64_t timestamp);
    void resumeRead();
    void reportFreshnessMetrics(int64_t locatorTimestamp);
    int64_t getStartTimestamp() const;
    bool needUpdateCommittedCheckpoint() const;
    virtual bool updateCommittedCheckpoint(int64_t checkpoint);

public:
    virtual bool getMaxTimestamp(int64_t &timestamp);
    virtual bool getLastReadTimestamp(int64_t &timestamp) {
        timestamp = _lastReadTs.load(std::memory_order_relaxed);
        return true;
    }
    uint64_t getLocatorSrc() const { return getStartTimestamp(); }

private:
    // timestamp is for locator, always greater than msg.timestamp()
    document::ProcessedDocumentVec *createProcessedDocument(
            const std::string &docStr, int64_t docTimestamp, int64_t timestamp);
    // virtual for test
    virtual IE_NAMESPACE(document)::DocumentPtr transDocStrToDocument(const std::string &docStr);
    virtual bool initDocumentParser(IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
                                    const IE_NAMESPACE(util)::CounterMapPtr &counterMap,
                                    const std::string &pluginPath);
    void ReportMetrics(const std::string& docSource, int64_t docSize,
                       int64_t locatorTimestamp, int64_t docTimestamp);


private:
    SWIFT_NS(client)::SwiftReader *_reader;
    std::string _buildIdStr;
    IE_NAMESPACE(misc)::MetricPtr _readLatencyMetric;
    IE_NAMESPACE(misc)::MetricPtr _swiftReadDelayMetric;
    IE_NAMESPACE(misc)::MetricPtr _processedDocSizeMetric;
    IE_NAMESPACE(misc)::MetricPtr _swiftReadBlockedQpsMetric;
    IE_NAMESPACE(misc)::MetricPtr _commitIntervalTimeoutQpsMetric;
    IE_NAMESPACE(util)::StateCounterPtr _swiftReadDelayCounter;
    common::End2EndLatencyReporter _e2eLatencyReporter;
    autil::ThreadCond _resumeCondition;
    uint16_t _lastMessageUint16Payload;
    uint8_t _lastMessageUint8Payload;
    int64_t _lastMessageId;
    int64_t _startTimestamp;
    int64_t _stopTimestamp;
    int64_t _lastReportTime; //us
    std::atomic<int64_t> _noMoreMsgBeginTs; //us
    int64_t _noMoreMsgPeriod; //us
    std::atomic<int64_t> _lastReadTs;
    std::atomic<int64_t> _lastValidReadTs;
    int64_t _lastUpdateCommittedCheckpointTs;
    int64_t _maxCommitInterval;

    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schema;
    IE_NAMESPACE(document)::DocumentFactoryWrapperPtr _docFactoryWrapper;
    IE_NAMESPACE(document)::DocumentParserPtr _docParser;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftProcessedDocProducer);

}
}

#endif //ISEARCH_BS_SWIFTPROCESSEDDOCPRODUCER_H
