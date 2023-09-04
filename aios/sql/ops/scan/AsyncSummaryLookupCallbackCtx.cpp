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
#include "sql/ops/scan/AsyncSummaryLookupCallbackCtx.h"

#include <algorithm>
#include <assert.h>
#include <engine/ResourceInitContext.h>
#include <iosfwd>
#include <utility>

#include "alog/Logger.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "future_lite/Try.h"
#include "future_lite/coro/Lazy.h"
#include "future_lite/coro/LazyHelper.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "navi/engine/AsyncPipe.h"
#include "navi/log/NaviLogger.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace autil;
using namespace navi;
using namespace kmonitor;

namespace sql {

class AsyncSummaryLookupMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "AsyncSummary.qps");
        REGISTER_LATENCY_MUTABLE_METRIC(_lookupTime, "AsyncSummary.lookupTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_docsCount, "AsyncSummary.docsCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_failedDocsCount, "AsyncSummary.failedDocsCount");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags,
                AsyncSummaryLookupMetricsCollector *summaryMetrics) {
        REPORT_MUTABLE_QPS(_qps);
        REPORT_MUTABLE_METRIC(_lookupTime, summaryMetrics->lookupTime / 1000.0f);
        REPORT_MUTABLE_METRIC(_docsCount, summaryMetrics->docsCount);
        REPORT_MUTABLE_METRIC(_failedDocsCount, summaryMetrics->failedDocsCount);
    }

private:
    MutableMetric *_qps = nullptr;
    MutableMetric *_lookupTime = nullptr;
    MutableMetric *_docsCount = nullptr;
    MutableMetric *_failedDocsCount = nullptr;
};

AsyncSummaryLookupCallbackCtx::AsyncSummaryLookupCallbackCtx(
    CountedAsyncPipePtr pipe,
    const std::shared_ptr<isearch::search::IndexPartitionReaderWrapper> &indexPRW,
    indexlib::index::SummaryReaderPtr summaryReader,
    autil::mem_pool::PoolPtr pool,
    future_lite::Executor *executor)
    : _asyncPipe(pipe)
    , _indexPRW(indexPRW)
    , _summaryReader(summaryReader)
    , _executor(executor) {
    assert(pool != nullptr && "pool must not be nullptr");
    _poolPtr = pool;
}

AsyncSummaryLookupCallbackCtx::~AsyncSummaryLookupCallbackCtx() {};

void AsyncSummaryLookupCallbackCtx::start(std::vector<docid_t> docIds,
                                          size_t fieldCount,
                                          int64_t timeout) {
    incStartVersion();
    prepareDocs(std::move(docIds), fieldCount);

    // no need lock
    _terminator.reset(new TimeoutTerminator(timeout));
    if (_asyncPipe->getAsyncPipe() == nullptr) {
        NAVI_LOG(DEBUG, "async pipe is nullptr, use sync await");
        auto result = future_lite::coro::syncAwait(_summaryReader->GetDocument(
            _docIds, _poolPtr.get(), _terminator.get(), &_summaryDocVec));
        processErrorCodes(result);
        _metricsCollector.lookupTime = incCallbackVersion();
    } else {
        assert(_executor && "executor is nullptr");
        NAVI_LOG(DEBUG, "async pipe ready, use async getDocument");
        _summaryReader->GetDocument(_docIds, _poolPtr.get(), _terminator.get(), &_summaryDocVec)
            .via(_executor)
            .start([ctx = shared_from_this()](
                       future_lite::Try<indexlib::index::ErrorCodeVec> errorCodeTry) {
                ctx->onSessionCallback(errorCodeTry);
            });
    }
}

void AsyncSummaryLookupCallbackCtx::prepareDocs(std::vector<docid_t> docIds, size_t fieldCount) {
    size_t docsCount = docIds.size();
    _docIds = std::move(docIds);
    _summaryDocVec.clear();
    _summaryDocDataVec.clear();
    _errorDocIds.clear();
    _summaryDocVec.reserve(docsCount);
    _summaryDocDataVec.reserve(docsCount);
    _metricsCollector.docsCount = docsCount;
    auto pool = _poolPtr.get();
    for (size_t i = 0; i < docsCount; ++i) {
        _summaryDocDataVec.emplace_back(pool, fieldCount);
        _summaryDocVec.emplace_back(&(_summaryDocDataVec.back()));
    }
}

void AsyncSummaryLookupCallbackCtx::processErrorCodes(
    const indexlib::index::ErrorCodeVec &errorCodes) {
    for (size_t i = 0; i < errorCodes.size(); ++i) {
        if (errorCodes[i] != indexlib::index::ErrorCode::OK) {
            _errorDocIds.emplace_back(_docIds[i]);
            _hasError = true;
        }
    }
    if (!_errorDocIds.empty()) {
        NAVI_LOG(WARN,
                 "summary reader has error doc ids: [%s]",
                 autil::StringUtil::toString(_errorDocIds).c_str());
    }
    _metricsCollector.failedDocsCount = _errorDocIds.size();
}

void AsyncSummaryLookupCallbackCtx::onSessionCallback(
    const future_lite::Try<indexlib::index::ErrorCodeVec> &errorCodeTry) {
    _metricsCollector.lookupTime = incCallbackVersion();

    if (errorCodeTry.hasError()) {
        _hasError = true;
    } else {
        auto &errorCodes = errorCodeTry.value();
        processErrorCodes(errorCodes);
    }
    (void)_asyncPipe->done();
}

std::string AsyncSummaryLookupCallbackCtx::getErrorDesc() const {
    if (!_hasError) {
        return "";
    }
    std::string error = "get summary failed. failed doc ids [";
    error += StringUtil::toString(_errorDocIds);
    error += "]";
    return error;
}

int64_t AsyncSummaryLookupCallbackCtx::getSeekTime() const {
    assert(!isInFlightNoLock());
    return _metricsCollector.lookupTime;
}

const indexlib::index::SearchSummaryDocVec &AsyncSummaryLookupCallbackCtx::getResult() const {
    assert(!isInFlightNoLock());
    return _summaryDocVec;
}

bool AsyncSummaryLookupCallbackCtx::hasError() const {
    assert(!isInFlightNoLock());
    return _hasError;
}

bool AsyncSummaryLookupCallbackCtx::tryReportMetrics(
    kmonitor::MetricsReporter &opMetricsReporter) const {
    AsyncSummaryLookupMetricsCollector metricsCollector;
    {
        ScopedSpinLock scope(_statLock);
        if (isInFlightNoLock()) {
            // in-flight metrics collector is unstable
            NAVI_LOG(DEBUG, "ignore in-flight metric collector");
            return false;
        }
        metricsCollector = _metricsCollector;
    }
    opMetricsReporter.report<AsyncSummaryLookupMetrics>(nullptr, &metricsCollector);
    NAVI_LOG(TRACE3, "current lookup time[%ld]", metricsCollector.lookupTime);
    return true;
}

CountedAsyncPipe::CountedAsyncPipe(navi::AsyncPipePtr pipe, size_t count)
    : _rawCount(count)
    , _asyncPipe(pipe)
    , _count(_rawCount) {}

void CountedAsyncPipe::reset() {
    _count = _rawCount;
}

ErrorCode CountedAsyncPipe::done() {
    auto ret = _count.fetch_sub(1);
    assert(ret >= 1 && "done too many times");
    if (ret == 1) {
        return _asyncPipe->setData(nullptr);
    }
    return EC_NONE;
}

AsyncPipePtr CountedAsyncPipe::getAsyncPipe() {
    return _asyncPipe;
}

} // namespace sql
