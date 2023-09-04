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
#include "sql/ops/tableModify/AsyncMessageWriteCallbackCtx.h"

#include <assert.h>
#include <cstdint>
#include <functional>
#include <iosfwd>

#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"
#include "alog/Logger.h"
#include "autil/Lock.h"
#include "autil/NoCopyable.h"
#include "autil/result/ForwardList.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "navi/common.h"
#include "navi/engine/AsyncPipe.h"
#include "navi/log/NaviLogger.h"
#include "sql/resource/MessageWriter.h"

using namespace std;
using namespace navi;
using namespace autil;
using namespace autil::result;
using namespace kmonitor;

namespace sql {

class AsyncMessageWriteMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "AsyncMessageWrite.qps");
        REGISTER_QPS_MUTABLE_METRIC(_failedQps, "AsyncMessageWrite.failedQps");
        REGISTER_GAUGE_MUTABLE_METRIC(_messageCount, "AsyncMessageWrite.messageCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_writeLatency, "AsyncMessageWrite.writeLatency");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, AsyncMessageWriteMetricsCollector *collector) {
        REPORT_MUTABLE_QPS(_qps);
        if (!collector->isSucc) {
            REPORT_MUTABLE_QPS(_failedQps);
        }
        REPORT_MUTABLE_METRIC(_messageCount, collector->messageCount);
        REPORT_MUTABLE_METRIC(_writeLatency, collector->writeLatency / 1000.0f);
    }

private:
    MutableMetric *_qps = nullptr;
    MutableMetric *_failedQps = nullptr;
    MutableMetric *_messageCount = nullptr;
    MutableMetric *_writeLatency = nullptr;
};

AsyncMessageWriteCallbackCtx::AsyncMessageWriteCallbackCtx(navi::AsyncPipePtr pipe)
    : _pipe(std::move(pipe)) {}

AsyncMessageWriteCallbackCtx::~AsyncMessageWriteCallbackCtx() {}

void AsyncMessageWriteCallbackCtx::start(vector<pair<uint16_t, string>> msgVec,
                                         MessageWriter *msgWriter,
                                         int64_t timeoutUs,
                                         std::shared_ptr<multi_call::QuerySession> querySession) {
    incStartVersion();
    _metricsCollector.messageCount = msgVec.size();
    _metricsCollector.name = msgWriter->getWriterName();
    NAVI_LOG(
        TRACE3,
        "start with message[%p,%lu] timeout[%ld] eTraceId[%s]",
        msgVec.data(),
        msgVec.size(),
        timeoutUs,
        querySession
            ? opentelemetry::EagleeyeUtil::getETraceId(querySession->getTraceServerSpan()).c_str()
            : "");

    auto ctx = shared_from_this();
    suez::MessageWriteParam param;
    param.docs = std::move(msgVec);
    param.cb = [ctx](Result<suez::WriteCallbackParam> result) {
        ctx->onWriteCallback(std::move(result));
    };
    param.querySession = querySession;
    param.timeoutUs = timeoutUs;
    msgWriter->write(std::move(param));
}

const autil::result::Result<suez::WriteCallbackParam> &
AsyncMessageWriteCallbackCtx::getResult() const {
    assert(!isInFlightNoLock());
    return _result;
}

bool AsyncMessageWriteCallbackCtx::tryReportMetrics(bool isSucc,
                                                    MetricsReporter &opMetricsReporter) const {
    AsyncMessageWriteMetricsCollector metricsCollector;
    {
        ScopedSpinLock scope(_statLock);
        if (isInFlightNoLock()) {
            // in-flight metrics collector is unstable
            NAVI_LOG(DEBUG, "ignore in-flight metric collector");
            return false;
        }
        metricsCollector = _metricsCollector;
        metricsCollector.isSucc = isSucc;
    }
    MetricsTags tags("writerName", metricsCollector.name);
    opMetricsReporter.report<AsyncMessageWriteMetrics>(&tags, &metricsCollector);
    NAVI_LOG(TRACE3,
             "write table [%s] message count [%d], isSuc [%d] used time[%ld]",
             metricsCollector.name.c_str(),
             metricsCollector.messageCount,
             metricsCollector.isSucc,
             metricsCollector.writeLatency);
    return true;
}

uint64_t AsyncMessageWriteCallbackCtx::getWriteTime() const {
    assert(!isInFlightNoLock());
    return _metricsCollector.writeLatency;
}

void AsyncMessageWriteCallbackCtx::onWriteCallback(
    autil::result::Result<suez::WriteCallbackParam> result) {
    _metricsCollector.writeLatency = incCallbackVersion();
    _result = std::move(result);
    if (_result.is_err()) {
        NAVI_INTERVAL_LOG(32,
                          WARN,
                          "async message write failed, error[%s]",
                          _result.get_error().message().c_str());
    }
    _pipe->setEof(); // schedule kernel
}

} // namespace sql
