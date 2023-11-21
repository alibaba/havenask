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
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "build_service/common/End2EndLatencyReporter.h"
#include "build_service/common/ExceedTsAction.h"
#include "build_service/common/Locator.h"
#include "build_service/common/SourceEnd2EndLatencyReporter.h"
#include "build_service/common/SwiftParam.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/FlowError.h"
#include "build_service/workflow/ProcessedDocExceptionHandler.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/StopOption.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "indexlib/base/Progress.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/document/builtin_parser_init_param.h"
#include "indexlib/document/document.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/document_parser.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace util {
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util

namespace build_service { namespace workflow {

class SingleSwiftProcessedDocProducer : public SwiftProcessedDocProducer
{
public:
    static const int64_t CONNECT_TIMEOUT_INTERVAL = 60 * 1000 * 1000; // 60s
    static const int64_t REPORT_INTERVAL = 50 * 1000;

private:
    static const int64_t MAX_READ_DELAY = 5 * 60 * 1000 * 1000;        // 5 min
    static const int64_t FAST_QUEUE_REPORT_INTERVAL = 5 * 1000 * 1000; // 5s

public:
    SingleSwiftProcessedDocProducer(
        const common::SwiftParam& swiftParam, const indexlib::config::IndexPartitionSchemaPtr& schema,
        const proto::PartitionId& partitionId,
        const indexlib::util::TaskSchedulerPtr& taskScheduler = indexlib::util::TaskSchedulerPtr());
    ~SingleSwiftProcessedDocProducer();

private:
    SingleSwiftProcessedDocProducer(const SingleSwiftProcessedDocProducer&);
    SingleSwiftProcessedDocProducer& operator=(const SingleSwiftProcessedDocProducer&);

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
    schemaid_t getAlterTableSchemaId() const override { return indexlib::DEFAULT_SCHEMAID; }
    bool needAlterTable() const override { return false; }

public:
    virtual bool getMaxTimestamp(int64_t& timestamp) override;
    virtual bool getLastReadTimestamp(int64_t& timestamp) const override
    {
        timestamp = _lastReadTs.load(std::memory_order_relaxed);
        return true;
    }
    uint64_t getLocatorSrc() const override { return getStartTimestamp(); }
    void setRecovered(bool isServiceRecovered) override;

private:
    void initSourceE2ELatencyMetric(indexlib::util::MetricProviderPtr metricProvider);
    // timestamp is for locator, always greater than msg.timestamp()
    document::ProcessedDocumentVec* createProcessedDocument(const std::string& docStr, int64_t docTimestamp,
                                                            int64_t timestamp, uint16_t hashId,
                                                            const indexlibv2::base::ProgressVector& progress);
    // to report checkpoint, timestamp is locator.offset
    document::ProcessedDocumentVec* createSkipProcessedDocument(const indexlibv2::base::ProgressVector& progress);
    // virtual for test
    virtual indexlib::document::DocumentPtr transDocStrToDocument(const std::string& docStr);
    virtual bool initDocumentParser(indexlib::util::MetricProviderPtr metricProvider,
                                    const indexlib::util::CounterMapPtr& counterMap, const std::string& pluginPath);
    void ReportMetrics(const std::string& docSource, int64_t docSize, int64_t locatorTimestamp,
                       indexlib::document::DocumentPtr document);
    void reportFastQueueSwiftReadDelayMetrics();
    void reportInflightDelayMetrics();
    void reportEnd2EndLatencyMetrics(const std::string& docSource);
    void reportSwiftReadDelayMetrics(int64_t locatorTimestamp);
    void reportSourceE2ELatencyMetrics(indexlib::document::DocumentPtr document, int64_t currentTime);

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
    common::SourceEnd2EndLatencyReporter _sourceE2ELatencyReporter;
    common::End2EndLatencyReporter _e2eLatencyReporter;
    common::SwiftLinkFreshnessReporterPtr _linkReporter;
    autil::ThreadCond _resumeCondition;
    uint16_t _lastMessageUint16Payload;
    uint8_t _lastMessageUint8Payload;
    int64_t _lastMessageId;
    uint64_t _sourceSignature;
    int64_t _startTimestamp;
    int64_t _stopTimestamp;
    int64_t _maxE2eLatency;
    int64_t _lastE2eLatency;
    int64_t _lastReportTime;                // us
    std::atomic<int64_t> _noMoreMsgBeginTs; // us
    int64_t _noMoreMsgPeriod;               // us
    std::atomic<int64_t> _lastReadTs;
    std::atomic<int64_t> _lastValidReadTs;
    int64_t _lastUpdateCommittedCheckpointTs;
    int64_t _maxCommitInterval;
    std::vector<std::string> _clusters;
    bool _allowSeekCrossSrc;
    int64_t _ckpDocReportTime;
    int64_t _ckpDocReportInterval;
    std::atomic<int64_t> _lastDocTs;
    std::atomic<int64_t> _lastIngestionTs;

    indexlib::config::IndexPartitionSchemaPtr _schema;
    indexlib::document::DocumentFactoryWrapperPtr _docFactoryWrapper;
    indexlib::document::DocumentParserPtr _docParser;
    indexlib::document::DocumentRewriterPtr _hashIdRewriter;
    indexlib::util::TaskSchedulerPtr _taskScheduler;
    int32_t _reportMetricTaskId;
    std::atomic<int64_t> _fastQueueLastReportTs;
    std::atomic<bool> _isServiceRecovered {false};

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleSwiftProcessedDocProducer);

}} // namespace build_service::workflow
