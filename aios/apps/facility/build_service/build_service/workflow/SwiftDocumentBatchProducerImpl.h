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
#include <limits>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>

#include "build_service/common/End2EndLatencyReporter.h"
#include "build_service/common/ExceedTsAction.h"
#include "build_service/common/Locator.h"
#include "build_service/common/SwiftParam.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "build_service/util/SwiftMessageFilter.h"
#include "build_service/workflow/FlowError.h"
#include "build_service/workflow/ProcessedDocExceptionHandler.h"
#include "build_service/workflow/StopOption.h"
#include "build_service/workflow/SwiftDocumentBatchProducer.h"
#include "indexlib/base/Constant.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "swift/client/SwiftReader.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::document {
class IDocumentParser;
class IDocumentFactory;
} // namespace indexlibv2::document

namespace indexlib { namespace util {
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util

namespace build_service { namespace workflow {

class SwiftDocumentBatchProducerImpl : public SwiftDocumentBatchProducer
{
public:
    static const int64_t CONNECT_TIMEOUT_INTERVAL = 60 * 1000 * 1000; // 60s
    static const int64_t REPORT_INTERVAL = 50 * 1000;

private:
    static const int64_t DEFAULT_WAIT_READER_TIME = 500 * 1000; // 500ms

public:
    SwiftDocumentBatchProducerImpl(const common::SwiftParam& swiftParam,
                                   const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                   const proto::PartitionId& partitionId,
                                   const indexlib::util::TaskSchedulerPtr& taskScheduler);
    ~SwiftDocumentBatchProducerImpl();

private:
    SwiftDocumentBatchProducerImpl(const SwiftDocumentBatchProducerImpl&);
    SwiftDocumentBatchProducerImpl& operator=(const SwiftDocumentBatchProducerImpl&);

public:
    bool init(const indexlib::util::MetricProviderPtr& metricProvider, const indexlib::util::CounterMapPtr& counterMap,
              int64_t startTimestamp, int64_t stopTimestamp, uint64_t sourceSignature);

public:
    FlowError produce(std::shared_ptr<indexlibv2::document::IDocumentBatch>& docBatch) override;
    bool seek(const common::Locator& locator) override;
    bool stop(StopOption stopOption) override;
    std::pair<bool, common::Locator> seekAndGetLocator(const common::Locator& locator);

public:
    // virtual for test
    int64_t suspendReadAtTimestamp(int64_t timestamp, common::ExceedTsAction action) override;
    virtual bool getMaxTimestamp(int64_t& timestamp) const override;
    virtual bool getLastReadTimestamp(int64_t& timestamp) const override
    {
        timestamp = _lastReadTs.load(std::memory_order_relaxed);
        return true;
    }
    void setRecovered(bool isServiceRecovered) override;

private:
    common::SwiftLinkFreshnessReporterPtr
    CreateSwiftLinkFreshnessReporter(const indexlib::util::MetricProviderPtr& metricProvider) const;

    FlowError handleSwiftError(swift::protocol::ErrorCode ec, int64_t lastReadTs,
                               const swift::protocol::ReaderProgress& readerProgress,
                               std::shared_ptr<indexlibv2::document::IDocumentBatch>& docBatch);

    FlowError handleSwiftNoMoreMessage(int64_t lastReadTs, const swift::protocol::ReaderProgress& readerProgress,
                                       std::shared_ptr<indexlibv2::document::IDocumentBatch>& docBatch);

    indexlibv2::document::IDocumentBatch* createDocumentBatch(const swift::protocol::Messages* msgs);
    virtual bool initDocumentParser(indexlib::util::MetricProviderPtr metricProvider,
                                    const indexlib::util::CounterMapPtr& counterMap);
    void ReportMetrics(const swift::protocol::Messages& swiftMessage, int64_t locatorTimestamp);
    void reportInflightDelayMetrics();
    void reportEnd2EndLatencyMetrics(const std::string& docSource);
    void reportSwiftReadDelayMetrics(int64_t locatorTimestamp);
    std::unique_ptr<indexlibv2::document::IDocumentFactory> createDocumentFactory() const;
    void reportFreshnessMetrics(int64_t locatorTimestamp, bool noMoreMsg) override;

private:
    class ReportFreshnessMetricTaskItem;

private:
    proto::PartitionId _pid;
    common::SwiftParam _swiftParam;
    std::unique_ptr<swift::client::SwiftReader> _swiftReader;
    swift::protocol::Messages _swiftMessages;
    ProcessedDocExceptionHandler _exceptionHandler;
    indexlib::util::MetricPtr _readLatencyMetric;
    indexlib::util::MetricPtr _swiftReadDelayMetric;
    indexlib::util::MetricPtr _gapToStopTsMetric;
    indexlib::util::MetricPtr _batchDocSizeMetric;
    indexlib::util::MetricPtr _commitIntervalTimeoutQpsMetric;
    indexlib::util::MetricPtr _batchReadDocQpsMetric;
    indexlib::util::MetricPtr _msgSizePerSecMetric;
    // current_time - msg.timestamp()
    indexlib::util::MetricPtr _processingTimeMetric;
    indexlib::util::StateCounterPtr _swiftReadDelayCounter;
    common::End2EndLatencyReporter _e2eLatencyReporter;
    common::SwiftLinkFreshnessReporterPtr _linkReporter;
    // autil::ThreadCond _resumeCondition;
    uint16_t _lastMessageUint16Payload = 0;
    uint8_t _lastMessageUint8Payload = 0;
    int64_t _lastMessageId = -1;
    int64_t _lastSkipDocTs = 0;
    uint64_t _sourceSignature = -1;
    int64_t _startTimestamp = 0;
    int64_t _msgCountSinceLastReport = 0;
    int64_t _msgSizeSinceLastReport = 0;
    int64_t _stopTimestamp = std::numeric_limits<int64_t>::max();
    int64_t _maxE2eLatency = INVALID_TIMESTAMP;
    int64_t _lastE2eLatency = INVALID_TIMESTAMP;
    int64_t _lastReportTime = 0;                 // us
    std::atomic<int64_t> _noMoreMsgBeginTs = -1; // us
    int64_t _noMoreMsgPeriod = -1;               // us
    std::atomic<int64_t> _lastReadTs = 0;
    std::atomic<int64_t> _lastValidReadTs = 0;
    std::atomic<int64_t> _lastDocTs = INVALID_TIMESTAMP;
    std::atomic<int64_t> _lastIngestionTs = INVALID_TIMESTAMP;

    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::document::IDocumentParser> _docParser;
    indexlib::util::TaskSchedulerPtr _taskScheduler;
    int32_t _reportMetricTaskId = -1;
    std::atomic<bool> _isServiceRecovered {false};
    util::SwiftMessageFilter _swiftMessageFilter;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftDocumentBatchProducerImpl);

}} // namespace build_service::workflow
