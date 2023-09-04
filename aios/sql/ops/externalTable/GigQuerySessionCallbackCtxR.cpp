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
#include "sql/ops/externalTable/GigQuerySessionCallbackCtxR.h"

#include <assert.h>
#include <engine/ResourceInitContext.h>
#include <iosfwd>
#include <utility>

#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "alog/Logger.h"
#include "autil/Lock.h"
#include "autil/TimeUtility.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "multi_call/interface/RequestGenerator.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/AsyncPipe.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/resource/QuerySessionR.h"
#include "sql/ops/externalTable/GigQuerySessionClosure.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace autil;
using namespace kmonitor;

namespace sql {

class GigQuerySessionCallbackCtxMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "qps");
        REGISTER_LATENCY_MUTABLE_METRIC(_lookupLatency, "lookupLatency");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags,
                GigQuerySessionCallbackCtxR::MetricCollector *metrics) {
        REPORT_MUTABLE_QPS(_qps);
        REPORT_MUTABLE_METRIC(_lookupLatency, metrics->lookupLatency / 1000.0f);
    }

private:
    MutableMetric *_qps = nullptr;
    MutableMetric *_lookupLatency = nullptr;
};

const std::string GigQuerySessionCallbackCtxR::RESOURCE_ID = "gig_query_session_callback_ctx_r";

GigQuerySessionCallbackCtxR::GigQuerySessionCallbackCtxR() {}

GigQuerySessionCallbackCtxR::~GigQuerySessionCallbackCtxR() {}

void GigQuerySessionCallbackCtxR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool GigQuerySessionCallbackCtxR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode GigQuerySessionCallbackCtxR::init(navi::ResourceInitContext &ctx) {
    _asyncPipe = ctx.createRequireKernelAsyncPipe();
    if (!_asyncPipe) {
        NAVI_LOG(ERROR, "create async pipe failed");
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

bool GigQuerySessionCallbackCtxR::start(const ScanOptions &options) {
    auto requestGenerator = options.requestGenerator;
    if (!requestGenerator) {
        NAVI_LOG(ERROR, "no request generator provided");
        return false;
    }
    multi_call::CallParam callParam;
    callParam.generatorVec.push_back(requestGenerator);
    incStartVersion();
    auto thisPtr = shared_from_this();
    auto *closure = new GigQuerySessionClosure(
        std::dynamic_pointer_cast<GigQuerySessionCallbackCtxR>(thisPtr));
    const auto &querySession = _querySessionR->getQuerySession();
    if (_asyncPipe) {
        NAVI_LOG(TRACE3, "async pipe exists, use gig async call");
        callParam.closure = closure;
        querySession->call(callParam, closure->getReplyPtr());
    } else {
        NAVI_LOG(TRACE3, "async pipe not exists, use gig sync call");
        querySession->call(callParam, closure->getReplyPtr());
        closure->Run();
    }
    return true;
}

void GigQuerySessionCallbackCtxR::onWaitResponse(autil::Result<ResponseVec> result) {
    _metricsCollector.lookupLatency = incCallbackVersion();
    _result = std::move(result);
    if (_result.is_err()) {
        NAVI_INTERVAL_LOG(
            32, WARN, "on wait response has error[%s]", _result.get_error().message().c_str());
    }
    if (_asyncPipe) {
        NAVI_LOG(TRACE3, "async pipe set activation data");
        _asyncPipe->setData(nullptr);
    } else {
        NAVI_LOG(TRACE3, "async pipe is nullptr, skip set activation data");
    }
}

bool GigQuerySessionCallbackCtxR::tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter) {
    GigQuerySessionCallbackCtxR::MetricCollector metricsCollector;
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

autil::Result<GigQuerySessionCallbackCtxR::ResponseVec> GigQuerySessionCallbackCtxR::stealResult() {
    assert(!isInFlightNoLock());
    return std::move(_result);
}

int64_t GigQuerySessionCallbackCtxR::getSeekTime() const {
    assert(!isInFlightNoLock());
    return _metricsCollector.lookupLatency;
}

REGISTER_RESOURCE(GigQuerySessionCallbackCtxR);

} // namespace sql
