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

#include <stdint.h>

#include "autil/Log.h"
#include "kmonitor/client/MetricsReporter.h"
#include "swift/common/Common.h" // IWYU pragma: keep

namespace kmonitor {
class MetricsTags;
class MutableMetric;
} // namespace kmonitor

namespace swift {
namespace monitor {

struct ClientMetricsCollector {
    void reset(bool error = false) {
        rpcLatency = -1;
        networkLatency = -1;
        decompressLatency = -1;
        unsendCount = -1;
        uncommitCount = -1;
        requestMsgCount = -1;
        errorCallBack = false;
        hasError = error;
        isDetectCommit = false;
    }

    int64_t rpcLatency = -1;
    int64_t networkLatency = -1;
    int64_t decompressLatency = -1;
    int64_t unsendCount = -1;
    int64_t uncommitCount = -1;
    int64_t requestMsgCount = -1;
    bool errorCallBack = false;
    bool hasError = false;
    bool isDetectCommit = false;
};

struct ReaderDelayCollector {
    int64_t readDelay = -1;
    int64_t currentDelay = -1;
};

class WriterMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, ClientMetricsCollector *collector);

private:
    kmonitor::MutableMetric *qps = nullptr;
    kmonitor::MutableMetric *errorQps = nullptr;
    kmonitor::MutableMetric *rpcLatency = nullptr;
    kmonitor::MutableMetric *decompressLatency = nullptr;
    kmonitor::MutableMetric *unsendCount = nullptr;
    kmonitor::MutableMetric *uncommitCount = nullptr;
    kmonitor::MutableMetric *requestMsgCount = nullptr;
    kmonitor::MutableMetric *errorCbQps = nullptr;
    kmonitor::MutableMetric *detectCommitQps = nullptr;
};

class ReaderMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, ClientMetricsCollector *collector);

private:
    kmonitor::MutableMetric *qps = nullptr;
    kmonitor::MutableMetric *errorQps = nullptr;
    kmonitor::MutableMetric *rpcLatency = nullptr;
    kmonitor::MutableMetric *networkLatency = nullptr;
    kmonitor::MutableMetric *decompressLatency = nullptr;
    kmonitor::MutableMetric *requestMsgCount = nullptr;
};

class DelayMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, ReaderDelayCollector *collector);

private:
    kmonitor::MutableMetric *readDelay = nullptr;
    kmonitor::MutableMetric *currentDelay = nullptr;
};

struct SwiftCheckMetricsCollector {
    int64_t produceCount = 0;
    int64_t consumerCount = 0;
    int64_t errorCount = 0;
    int64_t dupCount = 0;
    int64_t missCount = 0;
    int64_t lenErrorCount = 0;
    int64_t requestFatalErrorCount = 0;
};

class SwiftCheckMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, SwiftCheckMetricsCollector *collector);

private:
    kmonitor::MutableMetric *produceCount = nullptr;
    kmonitor::MutableMetric *consumerCount = nullptr;
    kmonitor::MutableMetric *errorCount = nullptr;
    kmonitor::MutableMetric *dupCount = nullptr;
    kmonitor::MutableMetric *missCount = nullptr;
    kmonitor::MutableMetric *lenErrorCount = nullptr;
    kmonitor::MutableMetric *requestFatalErrorCount = nullptr;
};

class ClientMetricsReporter {
public:
    ClientMetricsReporter(const kmonitor::MetricsReporterPtr &reporter);
    virtual ~ClientMetricsReporter();

private:
    ClientMetricsReporter(const ClientMetricsReporter &);
    ClientMetricsReporter &operator=(const ClientMetricsReporter &);

public: // for swift checker
    void incProduceQps();
    void incConsumerQps();
    void incCheckErrorQps();
    void reportSwiftCheckMetrics(SwiftCheckMetricsCollector &collector);
    // virtual for mock test
    virtual void reportWriterMetrics(ClientMetricsCollector &collector, const kmonitor::MetricsTags *tags = nullptr);
    virtual void reportReaderMetrics(ClientMetricsCollector &collector, const kmonitor::MetricsTags *tags = nullptr);
    virtual void reportCommitCallbackQps(const kmonitor::MetricsTags *tags = nullptr);
    virtual void reportDelay(ReaderDelayCollector &collector, const kmonitor::MetricsTags *tags = nullptr);

private:
    kmonitor::MetricsReporterPtr _metricsReporter = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ClientMetricsReporter);

} // namespace monitor
} // namespace swift
