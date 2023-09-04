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
#include <map>
#include <memory>

#include "suez/drc/Checkpoint.h"
#include "suez/drc/LogRecord.h"

namespace indexlibv2::framework {
class ITablet;
}

namespace kmonitor {
class MetricsReporter;
using MetricsReporterPtr = std::shared_ptr<MetricsReporter>;
class MetricsTags;
} // namespace kmonitor

namespace worker_framework {
class WorkerState;
}

namespace suez {

class LogReader;
class LogWriter;
class LogRewriter;
class LogTracer;

class LogReplicationPipeline {
public:
    LogReplicationPipeline();
    LogReplicationPipeline(std::unique_ptr<LogReader> producer,
                           std::map<std::string, std::unique_ptr<LogWriter>> consumers,
                           const std::shared_ptr<indexlibv2::framework::ITablet> &index,
                           std::unique_ptr<worker_framework::WorkerState> state,
                           const kmonitor::MetricsReporterPtr &metricsReporter = nullptr);
    virtual ~LogReplicationPipeline();

public:
    void setName(const std::string &name);
    bool start(int64_t startOffset = -1);
    void stop();
    void run();

private:
    bool recover();
    // virtual for test
    virtual LogRewriter *createLogRewriter() const;
    bool replicateLogRange(int64_t start, int64_t end, size_t &processCount);
    bool replicateLog(const LogRecord &log);
    void updateCheckpoint();
    void saveCheckpoint();

    struct Progress;
    void fillProgress(Progress &p) const;

private:
    // 索引可见的log id
    // virtual for test
    virtual int64_t getVisibleLogId() const;

private:
    std::atomic<bool> _running;
    std::unique_ptr<LogReader> _producer;
    std::map<std::string, std::unique_ptr<LogWriter>> _consumers;
    std::shared_ptr<indexlibv2::framework::ITablet> _index;
    std::unique_ptr<worker_framework::WorkerState> _state;
    std::unique_ptr<LogRewriter> _logRewriter;
    int64_t _consumedLogId = -1; // _consumedLogId >= _persistedLogId
    Checkpoint _checkpoint;      // persisted log id
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    std::unique_ptr<LogTracer> _tracer;

    struct Stat;
    std::unique_ptr<Stat> _stat;

    class Metrics;
    std::unique_ptr<Metrics> _metrics;

    struct Collector;
    int64_t _replicateErrorSleepTimeInMs = 500;
    std::unique_ptr<kmonitor::MetricsTags> _tags;
};

} // namespace suez
