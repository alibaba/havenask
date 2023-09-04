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
#include "swift/monitor/ClientMetricsReporter.h"

#include <iosfwd>
#include <memory>
#include <string>

#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "swift/monitor/MetricsCommon.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace kmonitor;
using namespace std;

namespace swift {
namespace monitor {
AUTIL_LOG_SETUP(swift, ClientMetricsReporter);

ClientMetricsReporter::ClientMetricsReporter(const kmonitor::MetricsReporterPtr &reporter)
    : _metricsReporter(reporter) {
    if (!_metricsReporter) {
        _metricsReporter.reset(new MetricsReporter("", "", {}));
    }
}

ClientMetricsReporter::~ClientMetricsReporter() {}

bool SwiftCheckMetrics::init(MetricsGroupManager *manager) {
    REGISTER_QPS_MUTABLE_METRIC(produceCount, "produceCount");
    REGISTER_QPS_MUTABLE_METRIC(consumerCount, "consumerCount");
    REGISTER_QPS_MUTABLE_METRIC(errorCount, "errorCount");
    REGISTER_QPS_MUTABLE_METRIC(dupCount, "dupCount");
    REGISTER_QPS_MUTABLE_METRIC(missCount, "missCount");
    REGISTER_QPS_MUTABLE_METRIC(lenErrorCount, "lenErrorCount");
    REGISTER_STATUS_MUTABLE_METRIC(requestFatalErrorCount, "requestFatalErrorCount");
    return true;
}

void SwiftCheckMetrics::report(const MetricsTags *tags, SwiftCheckMetricsCollector *collector) {
    REPORT_MUTABLE_METRIC(produceCount, collector->produceCount);
    REPORT_MUTABLE_METRIC(consumerCount, collector->consumerCount);
    REPORT_MUTABLE_METRIC(errorCount, collector->errorCount);
    REPORT_MUTABLE_METRIC(dupCount, collector->dupCount);
    REPORT_MUTABLE_METRIC(missCount, collector->missCount);
    REPORT_MUTABLE_METRIC(lenErrorCount, collector->lenErrorCount);
    REPORT_MUTABLE_METRIC(requestFatalErrorCount, collector->requestFatalErrorCount);
}

bool WriterMetrics::init(MetricsGroupManager *manager) {
    SWIFT_REGISTER_QPS_METRIC(qps, CLIENT_WRITER_METRIC);
    SWIFT_REGISTER_QPS_METRIC(errorQps, CLIENT_WRITER_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(rpcLatency, CLIENT_WRITER_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(decompressLatency, CLIENT_WRITER_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(unsendCount, CLIENT_WRITER_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(uncommitCount, CLIENT_WRITER_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(requestMsgCount, CLIENT_WRITER_METRIC);
    SWIFT_REGISTER_QPS_METRIC(errorCbQps, CLIENT_WRITER_METRIC);
    SWIFT_REGISTER_QPS_METRIC(detectCommitQps, CLIENT_WRITER_METRIC);
    return true;
}

void WriterMetrics::report(const MetricsTags *tags, ClientMetricsCollector *collector) {
    if (collector->hasError) {
        SWIFT_REPORT_QPS_METRIC(errorQps);
    } else {
        SWIFT_REPORT_QPS_METRIC(qps);
    }
    SWIFT_REPORT_MUTABLE_METRIC(rpcLatency, collector->rpcLatency);
    SWIFT_REPORT_MUTABLE_METRIC(decompressLatency, collector->decompressLatency);
    SWIFT_REPORT_MUTABLE_METRIC(unsendCount, collector->unsendCount);
    SWIFT_REPORT_MUTABLE_METRIC(uncommitCount, collector->uncommitCount);
    SWIFT_REPORT_MUTABLE_METRIC(requestMsgCount, collector->requestMsgCount);
    if (collector->errorCallBack) {
        SWIFT_REPORT_QPS_METRIC(errorCbQps);
    }
    if (collector->isDetectCommit) {
        SWIFT_REPORT_QPS_METRIC(detectCommitQps);
    }
}

bool ReaderMetrics::init(MetricsGroupManager *manager) {
    SWIFT_REGISTER_QPS_METRIC(qps, CLIENT_READER_METRIC);
    SWIFT_REGISTER_QPS_METRIC(errorQps, CLIENT_READER_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(rpcLatency, CLIENT_READER_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(networkLatency, CLIENT_READER_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(decompressLatency, CLIENT_READER_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(requestMsgCount, CLIENT_READER_METRIC);
    return true;
}

void ReaderMetrics::report(const MetricsTags *tags, ClientMetricsCollector *collector) {
    if (collector->hasError) {
        SWIFT_REPORT_QPS_METRIC(errorQps);
    } else {
        SWIFT_REPORT_QPS_METRIC(qps);
    }
    SWIFT_REPORT_MUTABLE_METRIC(rpcLatency, collector->rpcLatency);
    SWIFT_REPORT_MUTABLE_METRIC(networkLatency, collector->networkLatency);
    SWIFT_REPORT_MUTABLE_METRIC(decompressLatency, collector->decompressLatency);
    SWIFT_REPORT_MUTABLE_METRIC(requestMsgCount, collector->requestMsgCount);
}

bool DelayMetrics::init(MetricsGroupManager *manager) {
    SWIFT_REGISTER_GAUGE_METRIC(readDelay, CLIENT_READER_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(currentDelay, CLIENT_READER_METRIC);
    return true;
}

void DelayMetrics::report(const MetricsTags *tags, ReaderDelayCollector *collector) {
    SWIFT_REPORT_MUTABLE_METRIC(readDelay, collector->readDelay);
    SWIFT_REPORT_MUTABLE_METRIC(currentDelay, collector->currentDelay);
}

void ClientMetricsReporter::incProduceQps() {
    static const string name = "produceQps";
    REPORT_USER_MUTABLE_QPS(_metricsReporter, name);
}

void ClientMetricsReporter::incConsumerQps() {
    static const string name = "consumerQps";
    REPORT_USER_MUTABLE_QPS(_metricsReporter, name);
}

void ClientMetricsReporter::incCheckErrorQps() {
    static const string name = "checkErrorQps";
    REPORT_USER_MUTABLE_QPS(_metricsReporter, name);
}

void ClientMetricsReporter::reportSwiftCheckMetrics(SwiftCheckMetricsCollector &collector) {
    _metricsReporter->report<SwiftCheckMetrics, SwiftCheckMetricsCollector>(nullptr, &collector);
}

void ClientMetricsReporter::reportWriterMetrics(ClientMetricsCollector &collector, const MetricsTags *tags) {
    _metricsReporter->report<WriterMetrics, ClientMetricsCollector>(tags, &collector);
}

void ClientMetricsReporter::reportReaderMetrics(ClientMetricsCollector &collector, const MetricsTags *tags) {
    _metricsReporter->report<ReaderMetrics, ClientMetricsCollector>(tags, &collector);
}

void ClientMetricsReporter::reportCommitCallbackQps(const MetricsTags *tags) {
    static const string name = CLIENT_WRITER_METRIC + "commitCbQps";
    REPORT_USER_MUTABLE_QPS_TAGS(_metricsReporter, name, tags);
}

void ClientMetricsReporter::reportDelay(ReaderDelayCollector &collector, const MetricsTags *tags) {
    _metricsReporter->report<DelayMetrics, ReaderDelayCollector>(tags, &collector);
}

} // namespace monitor
} // namespace swift
