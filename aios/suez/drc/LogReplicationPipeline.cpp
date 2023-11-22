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
#include "suez/drc/LogReplicationPipeline.h"

#include <functional>
#include <sstream>
#include <unistd.h>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/Scope.h"
#include "autil/TimeUtility.h"
#include "build_service/util/LocatorUtil.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletInfos.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "suez/drc/IgnoredLog.h"
#include "suez/drc/KVUpdate2Add.h"
#include "suez/drc/LogReader.h"
#include "suez/drc/LogTracer.h"
#include "suez/drc/LogWriter.h"
#include "suez/drc/SourceUpdate2Add.h"
#include "worker_framework/WorkerState.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez.drc, LogReplicationPipeline);

static const int64_t MAX_REPLICATE_DOC_COUNT_PER_LOOP = 10000;

struct LogReplicationPipeline::Stat {
    size_t insertCount = 0;
    size_t updateCount = 0;
    size_t deleteCount = 0;
    size_t unknownCount = 0;

    size_t totalCount() const { return insertCount + updateCount + deleteCount + unknownCount; }

    void incCount(LogType type) {
        switch (type) {
        case LT_ADD:
            ++insertCount;
            break;
        case LT_UPDATE:
            ++updateCount;
            break;
        case LT_DELETE:
            ++deleteCount;
            break;
        default:
            ++unknownCount;
            break;
        }
    }
};

struct LogReplicationPipeline::Progress {
    int64_t safeReplicatedLogId = 0;
    int64_t replicatedLogId = 0;
    int64_t visibleLogId = 0;
    int64_t latestLogId = 0;

    int64_t gap1() const {
        if (latestLogId <= 0 || visibleLogId <= 0) {
            return 0;
        }
        return latestLogId - visibleLogId;
    }

    int64_t gap2() const {
        if (latestLogId <= 0 || replicatedLogId <= 0) {
            return 0;
        }
        return latestLogId - replicatedLogId;
    }

    int64_t gap3() const {
        if (latestLogId <= 0 || safeReplicatedLogId <= 0) {
            return 0;
        }
        return latestLogId - safeReplicatedLogId;
    }

    std::string toString() const {
        std::stringstream ss;
        ss << "safeReplicatedLogId = " << safeReplicatedLogId << "\t"
           << "replicatedLogId = " << replicatedLogId << "\t"
           << "visibleLogId = " << visibleLogId << "\t"
           << "latestLogId = " << latestLogId << "\t";
        return ss.str();
    }
};

struct LogReplicationPipeline::Collector {
    double processLatency = 0;
    LogReplicationPipeline::Progress p;
    LogReplicationPipeline::Stat *s = nullptr;
};

class LogReplicationPipeline::Metrics final : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_insertCount, "drc.insertCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_updateCount, "drc.updateCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_deleteCount, "drc.deleteCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalCount, "drc.totalCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_latency, "drc.processLatency");

        REGISTER_GAUGE_MUTABLE_METRIC(_gap1, "drc.gap1");
        REGISTER_GAUGE_MUTABLE_METRIC(_gap2, "drc.gap2");
        REGISTER_GAUGE_MUTABLE_METRIC(_gap3, "drc.gap3");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, LogReplicationPipeline::Collector *collector) {
        REPORT_MUTABLE_METRIC(_insertCount, collector->s->insertCount);
        REPORT_MUTABLE_METRIC(_updateCount, collector->s->updateCount);
        REPORT_MUTABLE_METRIC(_deleteCount, collector->s->deleteCount);
        REPORT_MUTABLE_METRIC(_totalCount, collector->s->totalCount());
        REPORT_MUTABLE_METRIC(_latency, collector->processLatency);
        if (collector->p.gap1() >= 0) {
            REPORT_MUTABLE_METRIC(_gap1, collector->p.gap1());
        }
        if (collector->p.gap2() >= 0) {
            REPORT_MUTABLE_METRIC(_gap2, collector->p.gap2());
        }
        if (collector->p.gap3() >= 0) {
            REPORT_MUTABLE_METRIC(_gap3, collector->p.gap3());
        }
    }

private:
    kmonitor::MutableMetric *_insertCount = nullptr;
    kmonitor::MutableMetric *_updateCount = nullptr;
    kmonitor::MutableMetric *_deleteCount = nullptr;
    kmonitor::MutableMetric *_totalCount = nullptr;
    kmonitor::MutableMetric *_latency = nullptr;
    kmonitor::MutableMetric *_gap1 = nullptr; // latest - visible
    kmonitor::MutableMetric *_gap2 = nullptr; // latest - replicated
    kmonitor::MutableMetric *_gap3 = nullptr; // latest - checkpointId
};

using WorkerState = worker_framework::WorkerState;

class WriteLimitedWorkerState : public worker_framework::WorkerState {
public:
    explicit WriteLimitedWorkerState(std::unique_ptr<worker_framework::WorkerState> impl, int64_t intervalInSec)
        : _impl(std::move(impl)), _writeIntervalInSec(intervalInSec), _lastWriteTimestampInSec(0) {}

public:
    ErrorCode write(const std::string &content) override {
        auto now = autil::TimeUtility::currentTimeInSeconds();
        if (_lastWriteTimestampInSec + _writeIntervalInSec > now) {
            return EC_OK;
        }
        _lastWriteTimestampInSec = now;
        return _impl->write(content);
    }

    ErrorCode read(std::string &content) override { return _impl->read(content); }

private:
    std::unique_ptr<worker_framework::WorkerState> _impl;
    const int64_t _writeIntervalInSec;
    int64_t _lastWriteTimestampInSec;
};

LogReplicationPipeline::LogReplicationPipeline() {}

LogReplicationPipeline::LogReplicationPipeline(std::unique_ptr<LogReader> producer,
                                               std::map<std::string, std::unique_ptr<LogWriter>> consumers,
                                               const std::shared_ptr<indexlibv2::framework::ITablet> &index,
                                               std::unique_ptr<worker_framework::WorkerState> state,
                                               const kmonitor::MetricsReporterPtr &metricsReporter)
    : _running(false)
    , _producer(std::move(producer))
    , _consumers(std::move(consumers))
    , _index(index)
    , _metricsReporter(metricsReporter)
    , _stat(std::make_unique<Stat>())
    , _metrics(std::make_unique<Metrics>()) {
    static const int64_t DEFAULT_CHECKPOINT_INTERVAL_IN_SEC = 10;
    int64_t checkpointIntervalInSec =
        autil::EnvUtil::getEnv("checkpoint_interval_in_sec", DEFAULT_CHECKPOINT_INTERVAL_IN_SEC);
    if (checkpointIntervalInSec <= 0) {
        checkpointIntervalInSec = DEFAULT_CHECKPOINT_INTERVAL_IN_SEC;
    }
    _replicateErrorSleepTimeInMs =
        autil::EnvUtil::getEnv("replicate_error_sleep_time_im_ms", _replicateErrorSleepTimeInMs);
    _state = std::make_unique<WriteLimitedWorkerState>(std::move(state), checkpointIntervalInSec);
    std::set<std::string> traceFieldSet;
    for (const auto &it : _consumers) {
        const auto &hashFields = it.second->getHashFields();
        traceFieldSet.insert(hashFields.begin(), hashFields.end());
    }
    std::vector<std::string> traceFields(traceFieldSet.begin(), traceFieldSet.end());
    _tracer = std::make_unique<LogTracer>(std::move(traceFields));
}

LogReplicationPipeline::~LogReplicationPipeline() {}

void LogReplicationPipeline::setName(const std::string &name) {
    if (!name.empty()) {
        _tags = std::make_unique<kmonitor::MetricsTags>("sink", name);
    }
}

bool LogReplicationPipeline::start(int64_t startOffset) {
    if (!recover()) {
        AUTIL_LOG(ERROR, "recover checkpoint failed");
        return false;
    }

    _consumedLogId = std::max(_checkpoint.getPersistedLogId(), startOffset - 1);
    _consumedLogId = std::max(_consumedLogId, -1L);
    AUTIL_LOG(INFO, "consumed log id is: %ld", _consumedLogId);

    _logRewriter.reset(createLogRewriter());
    if (!_logRewriter) {
        AUTIL_LOG(ERROR, "create LogRewriter failed");
        return false;
    }

    _running = true;
    return true;
}

void LogReplicationPipeline::stop() { _running = false; }

bool LogReplicationPipeline::recover() {
    std::string content;
    auto ec = _state->read(content);
    if (ec == WorkerState::EC_FAIL) {
        AUTIL_LOG(ERROR, "read from worker state failed");
        return false;
    } else if (ec == WorkerState::EC_NOT_EXIST) {
        AUTIL_LOG(INFO, "worker state does not exist, do not recover");
        return true;
    } else {
        return _checkpoint.fromString(content);
    }
}

LogRewriter *LogReplicationPipeline::createLogRewriter() const {
    if (!_index) {
        return nullptr;
    }
    auto schema = _index->GetTabletSchema();
    if (!schema) {
        return nullptr;
    }
    std::unique_ptr<LogRewriter> rewriter;
    if (schema->GetTableType() == "kv") {
        rewriter.reset(new KVUpdate2Add);
    } else if (schema->GetTableType() == "normal") {
        rewriter.reset(new SourceUpdate2Add);
    }
    if (rewriter && !rewriter->init(_index.get())) {
        return nullptr;
    }
    return rewriter.release();
}

void LogReplicationPipeline::run() {
    auto visibleLogId = getVisibleLogId();
    LogReplicationPipeline::Collector collector;
    collector.s = _stat.get();
    // the range is [start, end)
    int64_t start = _consumedLogId + 1;
    int64_t end = visibleLogId; // locator的offset已经是下一条消息的时间
    bool ret = true;
    if (start < end) {
        autil::ScopedTime2 t;
        size_t processCount = 0;
        ret = replicateLogRange(start, end, processCount);
        if (processCount > 0) {
            collector.processLatency = 1.0 * t.done_us() / processCount;
        }
    }
    updateCheckpoint();
    saveCheckpoint();
    fillProgress(collector.p);
    _metricsReporter->report<LogReplicationPipeline::Metrics>(_tags.get(), &collector);
    if (!ret) {
        AUTIL_LOG(INFO, "replicate log has error,sleeping [%ld] ms", _replicateErrorSleepTimeInMs);
        usleep(_replicateErrorSleepTimeInMs * 1000);
        AUTIL_LOG(INFO, "progress: %s", collector.p.toString().c_str());
    }
    AUTIL_LOG(DEBUG, "progress: %s", collector.p.toString().c_str());
}

bool LogReplicationPipeline::replicateLogRange(int64_t start, int64_t end, size_t &processedCount) {
    AUTIL_LOG(DEBUG, "begin to replicate log in range [%ld, %ld)", start, end);
    processedCount = 0;
    // check whether need to seek
    if (start > 0 && !_producer->seek(start)) {
        AUTIL_LOG(ERROR, "seek to log id %ld failed", start);
        return false;
    }
    if (!_logRewriter->createSnapshot()) {
        AUTIL_LOG(ERROR, "create snapshot failed");
        return false;
    }
    autil::ScopeGuard guard([this]() { _logRewriter->releaseSnapshot(); });
    while (_running && processedCount < MAX_REPLICATE_DOC_COUNT_PER_LOOP) {
        LogRecord log;
        if (!_producer->read(log)) {
            AUTIL_LOG(INFO, "no message need to replicate.");
            return processedCount != 0;
        }
        int64_t currentLogId = log.getLogId();
        if (currentLogId >= end) {
            AUTIL_LOG(DEBUG, "current log id: %ld, end: %ld", currentLogId, end);
            return processedCount != 0;
        }
        _tracer->trace(log);
        if (_consumedLogId > currentLogId) {
            AUTIL_LOG(WARN, "log roll back logId[%ld] consumed logId[%ld]", currentLogId, _consumedLogId);
            return false;
        }
        auto type = log.getType();
        auto rc = _logRewriter->rewrite(log);
        if (rc == RC_FAIL) {
            AUTIL_LOG(WARN, "rewrite log[%s] failed", log.debugString().c_str());
            return false;
        } else if (rc == RC_OK) {
            if (!replicateLog(log)) {
                return false;
            }
            _consumedLogId = currentLogId;
        } else {
            // RC_IGNORE or RC_UNSUPPORTED
            _consumedLogId = currentLogId;
        }
        _stat->incCount(type);
        ++processedCount;
    }
    return processedCount != 0;
}

int64_t LogReplicationPipeline::getVisibleLogId() const {
    auto tabletInfos = _index->GetTabletInfos();
    auto locator = tabletInfos->GetBuildLocator();
    return build_service::util::LocatorUtil::getSwiftWatermark(locator);
}

bool LogReplicationPipeline::replicateLog(const LogRecord &log) {
    for (const auto &it : _consumers) {
        auto s = it.second->write(log);
        if (s == LogWriter::S_IGNORE) {
            IgnoredLog ignoredLog;
            ignoredLog.append(it.first, log.debugString());
        } else if (s == LogWriter::S_FAIL) {
            AUTIL_LOG(WARN, "replicate log %s to %s failed", log.debugString().c_str(), it.first.c_str());
            return false;
        }
    }
    AUTIL_LOG(DEBUG, "replicated log: %s", log.debugString().c_str());
    return true;
}

void LogReplicationPipeline::updateCheckpoint() {
    int64_t checkpoint = std::numeric_limits<int64_t>::max();
    for (const auto &it : _consumers) {
        checkpoint = std::min(checkpoint, it.second->getCommittedCheckpoint());
    }
    _checkpoint.updatePersistedLogId(checkpoint);
}

void LogReplicationPipeline::saveCheckpoint() {
    auto content = _checkpoint.toString();
    auto ec = _state->write(content);
    if (ec == WorkerState::EC_FAIL) {
        AUTIL_LOG(WARN, "persist %s failed", content.c_str());
    }
}

void LogReplicationPipeline::fillProgress(LogReplicationPipeline::Progress &p) const {
    p.safeReplicatedLogId = _checkpoint.getPersistedLogId();
    p.replicatedLogId = _consumedLogId;
    p.visibleLogId = getVisibleLogId();
    p.latestLogId = _producer->getLatestLogId();
}

} // namespace suez
