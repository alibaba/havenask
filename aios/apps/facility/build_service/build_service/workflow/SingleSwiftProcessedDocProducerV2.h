/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <atomic>

#include "autil/Lock.h"
#include "build_service/common/End2EndLatencyReporter.h"
#include "build_service/common/SwiftLinkFreshnessReporter.h"
#include "build_service/common/SwiftParam.h"
#include "build_service/common_define.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/util/Log.h"
#include "build_service/util/SwiftMessageFilter.h"
#include "build_service/workflow/ProcessedDocExceptionHandler.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "indexlib/document/BuiltinParserInitParam.h"
#include "indexlib/document/document.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/counter/Counter.h"
#include "indexlib/util/counter/CounterMap.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::document {
class IDocumentParser;
class IDocumentFactory;
} // namespace indexlibv2::document

namespace indexlib { namespace util {
class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util

namespace build_service { namespace workflow {

class SingleSwiftProcessedDocProducerV2 : public SwiftProcessedDocProducer
{
public:
    static const int64_t CONNECT_TIMEOUT_INTERVAL = 60 * 1000 * 1000; // 60s
    static const int64_t REPORT_INTERVAL = 50 * 1000;

private:
    static const int64_t MAX_READ_DELAY = 5 * 60 * 1000 * 1000;        // 5 min
    static const int64_t FAST_QUEUE_REPORT_INTERVAL = 5 * 1000 * 1000; // 5s

public:
    SingleSwiftProcessedDocProducerV2(
        const common::SwiftParam& swiftParam, const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
        const proto::PartitionId& partitionId,
        const indexlib::util::TaskSchedulerPtr& taskScheduler = indexlib::util::TaskSchedulerPtr());
    ~SingleSwiftProcessedDocProducerV2();

private:
    SingleSwiftProcessedDocProducerV2(const SingleSwiftProcessedDocProducerV2&);
    SingleSwiftProcessedDocProducerV2& operator=(const SingleSwiftProcessedDocProducerV2&);

public:
    bool init(indexlib::util::MetricProviderPtr metricProvider, const std::string& pluginPath,
              const indexlib::util::CounterMapPtr& counterMap, int64_t startTimestamp, int64_t stopTimestamp,
              int64_t noMoreMsgPeriod, int64_t maxCommitInterval, uint64_t sourceSignature,
              bool allowSeekCrossSrc = false) override;

    void setLinkReporter(const common::SwiftLinkFreshnessReporterPtr& reporter) override { _linkReporter = reporter; }

public:
    FlowError produce(document::ProcessedDocumentVecPtr& processedDocVec) override;
    bool seek(const common::Locator& locator) override;
    bool stop(StopOption stopOption) override;
    std::pair<bool, common::Locator> seekAndGetLocator(const common::Locator& locator);

public:
    // virtual for test
    int64_t suspendReadAtTimestamp(int64_t timestamp, common::ExceedTsAction action) override;
    void resumeRead() override;
    void reportFreshnessMetrics(int64_t locatorTimestamp, bool noMoreMsg, const std::string& docSource,
                                bool isReportFastQueueSwiftReadDelay) override;
    int64_t getStartTimestamp() const override;
    bool needUpdateCommittedCheckpoint() const override;
    virtual bool updateCommittedCheckpoint(const indexlibv2::base::Progress::Offset& checkpoint) override;

public:
    virtual bool getMaxTimestamp(int64_t& timestamp) override;
    virtual bool getLastReadTimestamp(int64_t& timestamp) override
    {
        timestamp = _lastReadTs.load(std::memory_order_relaxed);
        return true;
    }
    uint64_t getLocatorSrc() const override { return getStartTimestamp(); }
    void setRecovered(bool isServiceRecovered) override;

private:
    // timestamp is for locator, always greater than msg.timestamp()
    document::ProcessedDocumentVec* createProcessedDocument(const std::string& docStr,
                                                            indexlibv2::document::IDocument::DocInfo docInfo,
                                                            int64_t timestamp,
                                                            const std::vector<indexlibv2::base::Progress>& progress);
    // to report checkpoint, timestamp is locator.offset
    document::ProcessedDocumentVec*
    createSkipProcessedDocument(const std::vector<indexlibv2::base::Progress>& progress);
    // virtual for test
    virtual std::unique_ptr<indexlibv2::document::IDocumentBatch> transDocStrToDocumentBatch(const std::string& docStr);
    virtual bool initDocumentParser(indexlib::util::MetricProviderPtr metricProvider,
                                    const indexlib::util::CounterMapPtr& counterMap);
    void ReportMetrics(const std::string& docSource, int64_t docSize, int64_t locatorTimestamp);
    void reportFastQueueSwiftReadDelayMetrics();
    void reportInflightDelayMetrics();
    void reportEnd2EndLatencyMetrics(const std::string& docSource);
    void reportSwiftReadDelayMetrics(int64_t locatorTimestamp);
    std::unique_ptr<indexlibv2::document::IDocumentFactory> createDocumentFactory() const;

private:
    std::string _buildIdStr;
    common::SwiftParam _swiftParam;
    ProcessedDocExceptionHandler _exceptionHandler;
    indexlib::util::MetricPtr _readLatencyMetric;
    indexlib::util::MetricPtr _swiftReadDelayMetric;
    indexlib::util::MetricPtr _gapToStopTsMetric;
    indexlib::util::MetricPtr _processedDocSizeMetric;
    indexlib::util::MetricPtr _swiftReadBlockedQpsMetric;
    indexlib::util::MetricPtr _commitIntervalTimeoutQpsMetric;
    // current_time - msg.timestamp()
    indexlib::util::MetricPtr _processingTimeMetric;
    indexlib::util::StateCounterPtr _swiftReadDelayCounter;
    indexlib::util::MetricPtr _fastQueueSwiftReadDelayMetric;
    common::End2EndLatencyReporter _e2eLatencyReporter;
    common::SwiftLinkFreshnessReporterPtr _linkReporter;
    autil::ThreadCond _resumeCondition;
    uint16_t _lastMessageUint16Payload = 0;
    uint8_t _lastMessageUint8Payload = 0;
    int64_t _lastMessageId = -1;
    uint64_t _sourceSignature = -1;
    int64_t _startTimestamp = 0;
    int64_t _stopTimestamp = std::numeric_limits<int64_t>::max();
    int64_t _maxE2eLatency = INVALID_TIMESTAMP;
    int64_t _lastE2eLatency = INVALID_TIMESTAMP;
    int64_t _lastReportTime = 0;                 // us
    std::atomic<int64_t> _noMoreMsgBeginTs = -1; // us
    int64_t _noMoreMsgPeriod = -1;               // us
    std::atomic<int64_t> _lastReadTs = 0;
    std::atomic<int64_t> _lastValidReadTs = 0;
    int64_t _lastUpdateCommittedCheckpointTs = std::numeric_limits<int64_t>::max();
    int64_t _maxCommitInterval = std::numeric_limits<int64_t>::max();
    std::vector<std::string> _clusters;
    bool _allowSeekCrossSrc = false;
    int64_t _ckpDocReportTime = 0;
    int64_t _ckpDocReportInterval = 10;
    std::atomic<int64_t> _lastDocTs = INVALID_TIMESTAMP;
    std::atomic<int64_t> _lastIngestionTs = INVALID_TIMESTAMP;

    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::document::IDocumentParser> _docParser;
    indexlib::util::TaskSchedulerPtr _taskScheduler;
    int32_t _reportMetricTaskId = -1;
    std::atomic<int64_t> _fastQueueLastReportTs = -1;
    std::atomic<bool> _isServiceRecovered {false};
    util::SwiftMessageFilter _swiftMessageFilter;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleSwiftProcessedDocProducerV2);

}} // namespace build_service::workflow
