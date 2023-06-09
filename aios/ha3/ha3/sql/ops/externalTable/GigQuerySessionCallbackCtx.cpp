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
#include "ha3/sql/ops/externalTable/GigQuerySessionCallbackCtx.h"

#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "ha3/sql/ops/externalTable/GigQuerySessionClosure.h"
#include "kmonitor/client/MetricsReporter.h"
#include "multi_call/interface/Response.h"
#include "navi/engine/AsyncPipe.h"

using namespace std;
using namespace autil;
using namespace kmonitor;

namespace isearch {
namespace sql {

class GigQuerySessionCallbackCtxMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "qps");
        REGISTER_LATENCY_MUTABLE_METRIC(_lookupLatency, "lookupLatency");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags,
                GigQuerySessionCallbackCtx::MetricCollector *metrics) {
        REPORT_MUTABLE_QPS(_qps);
        REPORT_MUTABLE_METRIC(_lookupLatency, metrics->lookupLatency / 1000.0f);
    }

private:
    MutableMetric *_qps = nullptr;
    MutableMetric *_lookupLatency = nullptr;
};

GigQuerySessionCallbackCtx::GigQuerySessionCallbackCtx(
    std::shared_ptr<multi_call::QuerySession> querySession,
    std::shared_ptr<navi::AsyncPipe> asyncPipe)
    : _querySession(std::move(querySession))
    , _asyncPipe(std::move(asyncPipe)) {
}

GigQuerySessionCallbackCtx::~GigQuerySessionCallbackCtx() {}

bool GigQuerySessionCallbackCtx::start(const ScanOptions &options) {
    assert(_querySession && "query session can not be nullptr");

    auto requestGenerator = options.requestGenerator;
    if (!requestGenerator) {
        NAVI_LOG(ERROR, "no request generator provided");
        return false;
    }
    multi_call::CallParam callParam;
    callParam.generatorVec.push_back(requestGenerator);
    incStartVersion();
    auto *closure = new GigQuerySessionClosure(shared_from_this());
    if (_asyncPipe) {
        NAVI_LOG(TRACE3, "async pipe exists, use gig async call");
        callParam.closure = closure;
        _querySession->call(callParam, closure->getReplyPtr());
    } else {
        NAVI_LOG(TRACE3, "async pipe not exists, use gig sync call");
        _querySession->call(callParam, closure->getReplyPtr());
        closure->Run();
    }
    return true;
}

void GigQuerySessionCallbackCtx::onWaitResponse(autil::Result<ResponseVec> result) {
    _metricsCollector.lookupLatency = incCallbackVersion();
    _result = std::move(result);
    if (_result.is_err()) {
        NAVI_INTERVAL_LOG(
            32, WARN, "on wait response has error[%s]", _result.get_error().message().c_str());
    }
    if (_asyncPipe) {
        NAVI_LOG(TRACE3, "async pipe set activation data");
        _asyncPipe->setData(ActivationData::inst());
    } else {
        NAVI_LOG(TRACE3, "async pipe is nullptr, skip set activation data");
    }
}

bool GigQuerySessionCallbackCtx::tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter) {
    GigQuerySessionCallbackCtx::MetricCollector metricsCollector;
    {
        ScopedSpinLock scope(_statLock);
        if (isInFlightNoLock()) {
            NAVI_LOG(DEBUG, "ignore in-flight metric collector");
            return false;
        }
        metricsCollector = _metricsCollector;
    }
    opMetricsReporter.report<GigQuerySessionCallbackCtxMetrics>(nullptr, &metricsCollector);
    NAVI_LOG(TRACE3, "current lookup latency [%ld] us", metricsCollector.lookupLatency);
    return true;
}

autil::Result<GigQuerySessionCallbackCtx::ResponseVec> GigQuerySessionCallbackCtx::stealResult() {
    assert(!isInFlightNoLock());
    return std::move(_result);
}

int64_t GigQuerySessionCallbackCtx::getSeekTime() const {
    assert(!isInFlightNoLock());
    return _metricsCollector.lookupLatency;
}

} // namespace sql
} // namespace isearch
