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
#include "sql/ops/scan/AsyncKVLookupCallbackCtxV1.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <engine/ResourceInitContext.h>
#include <iosfwd>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/Try.h"
#include "future_lite/coro/Lazy.h"
#include "future_lite/coro/LazyHelper.h"
#include "indexlib/index/kv/Types.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "navi/common.h"
#include "navi/engine/AsyncPipe.h"
#include "navi/log/NaviLogger.h"

namespace future_lite {
class Executor;
} // namespace future_lite
namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace autil;
using namespace navi;
using namespace indexlib::index;
using namespace future_lite::interface;
using namespace kmonitor;

namespace sql {

class AsyncKVLookupMetricsV1 : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "AsyncKV.qps");
        REGISTER_GAUGE_MUTABLE_METRIC(_blockCacheHitCount, "AsyncKV.blockCacheHitCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_blockCacheMissCount, "AsyncKV.blockCacheMissCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_searchCacheHitCount, "AsyncKV.searchCacheHitCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_searchCacheMissCount, "AsyncKV.searchCacheMissCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_memTableLatency, "AsyncKV.memTableLatency");
        REGISTER_LATENCY_MUTABLE_METRIC(_sSTableLatency, "AsyncKV.sSTableLatency");
        REGISTER_LATENCY_MUTABLE_METRIC(_lookupTime, "AsyncKV.lookupTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_docsCount, "AsyncKV.docsCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_failedDocsCount, "AsyncKV.failedDocsCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_notFoundDocsCount, "AsyncKV.notFoundDocsCount");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, AsyncKVLookupMetricsCollectorV1 *kvMetrics) {
        REPORT_MUTABLE_QPS(_qps);
        REPORT_MUTABLE_METRIC(_blockCacheHitCount,
                              kvMetrics->indexCollector.GetBlockCacheHitCount());
        REPORT_MUTABLE_METRIC(_blockCacheMissCount,
                              kvMetrics->indexCollector.GetBlockCacheMissCount());
        REPORT_MUTABLE_METRIC(_searchCacheHitCount,
                              kvMetrics->indexCollector.GetSearchCacheHitCount());
        REPORT_MUTABLE_METRIC(_searchCacheMissCount,
                              kvMetrics->indexCollector.GetSearchCacheMissCount());
        REPORT_MUTABLE_METRIC(_memTableLatency,
                              kvMetrics->indexCollector.GetMemTableLatency() / 1000.0f);
        REPORT_MUTABLE_METRIC(_sSTableLatency,
                              kvMetrics->indexCollector.GetSSTableLatency() / 1000.0f);
        REPORT_MUTABLE_METRIC(_lookupTime, kvMetrics->lookupTime / 1000.0f);
        REPORT_MUTABLE_METRIC(_docsCount, kvMetrics->docsCount);
        REPORT_MUTABLE_METRIC(_failedDocsCount, kvMetrics->failedDocsCount);
        REPORT_MUTABLE_METRIC(_notFoundDocsCount, kvMetrics->notFoundDocsCount);
    }

private:
    MutableMetric *_qps = nullptr;
    MutableMetric *_blockCacheHitCount = nullptr;
    MutableMetric *_blockCacheMissCount = nullptr;
    MutableMetric *_searchCacheHitCount = nullptr;
    MutableMetric *_searchCacheMissCount = nullptr;
    MutableMetric *_memTableLatency = nullptr;
    MutableMetric *_sSTableLatency = nullptr;
    MutableMetric *_lookupTime = nullptr;
    MutableMetric *_docsCount = nullptr;
    MutableMetric *_failedDocsCount = nullptr;
    MutableMetric *_notFoundDocsCount = nullptr;
};

AsyncKVLookupCallbackCtxV1::AsyncKVLookupCallbackCtxV1(const AsyncPipePtr &pipe,
                                                       future_lite::Executor *executor)
    : _asyncPipe(pipe)
    , _executor(executor) {}

void AsyncKVLookupCallbackCtxV1::asyncGet(KVLookupOption option) {
    auto *kvReader = static_cast<indexlib::index::KVReader *>(option.kvReader);
    assert(kvReader && "kvReader can't be nullptr");

    NAVI_LOG(DEBUG, "async pipe %s, use sync with reader v1", _asyncPipe ? "ready" : "is nullptr");
    _pool = option.pool;
    prepareReadOptions(option);

    _rawResults.resize(_pksForSearch.size());
    if (_asyncPipe == nullptr) {
        NAVI_LOG(DEBUG, "async pipe is nullptr, use sync with reader v1");
        assert(_executor && "executor is nullptr");
        auto result = future_lite::coro::syncAwait(
            kvReader->BatchGetAsync(_pksForSearch, _rawResults, indexlib::tsc_default, _readOptions)
                .via(_executor));
        processBoolVec(result);
        _pool.reset();
    } else {
        assert(_executor && "executor is nullptr");
        NAVI_LOG(DEBUG, "async pipe ready, use async with reader v1");
        auto ctx = shared_from_this();
        kvReader->BatchGetAsync(_pksForSearch, _rawResults, indexlib::tsc_default, _readOptions)
            .via(_executor)
            .start([ctx](future_lite::Try<BoolVector> boolVecTry) {
                ctx->onSessionCallback(boolVecTry);
            });
    }
}

void AsyncKVLookupCallbackCtxV1::onSessionCallback(future_lite::Try<BoolVector> &boolVecTry) {
    assert(!boolVecTry.hasError());
    auto &boolVec = boolVecTry.value();
    processBoolVec(boolVec);
    _pool.reset();
    (void)_asyncPipe->setData(nullptr);
}

void AsyncKVLookupCallbackCtxV1::prepareReadOptions(const KVLookupOption &option) {
    _readOptions.timestamp = TimeUtility::currentTime();
    _readOptions.pool = option.pool.get();
}

void AsyncKVLookupCallbackCtxV1::processBoolVec(BoolVector &boolVec) {
    _results.resize(boolVec.size());
    for (size_t i = 0; i < boolVec.size(); ++i) {
        if (future_lite::interface::tryHasError(boolVec[i])) {
            _failedPks.emplace_back(_pksForSearch[i], 0);
            _results[i] = nullptr;
        } else if (!future_lite::interface::getTryValue(boolVec[i])) {
            _notFoundPks.emplace_back(_pksForSearch[i]);
            _results[i] = nullptr;
        } else {
            _results[i] = &_rawResults[i];
        }
    }
    if (!_failedPks.empty()) {
        NAVI_LOG(
            WARN, "kvReader has failedPks[%s]", autil::StringUtil::toString(_failedPks).c_str());
    }
    _metricsCollector.docsCount = boolVec.size();
    _metricsCollector.failedDocsCount = getFailedCount();
    _metricsCollector.notFoundDocsCount = getNotFoundCount();
    _metricsCollector.lookupTime = incCallbackVersion();
}

bool AsyncKVLookupCallbackCtxV1::tryReportMetrics(
    kmonitor::MetricsReporter &opMetricsReporter) const {
    AsyncKVLookupMetricsCollectorV1 metricsCollector;
    {
        ScopedSpinLock scope(_statLock);
        if (isInFlightNoLock()) {
            // in-flight metrics collector is unstable
            NAVI_LOG(DEBUG, "ignore in-flight metric collector");
            return false;
        }
        metricsCollector = _metricsCollector;
    }
    opMetricsReporter.report<AsyncKVLookupMetricsV1>(nullptr, &metricsCollector);
    NAVI_LOG(TRACE3, "current lookup time[%ld]", metricsCollector.lookupTime);
    return true;
}

size_t AsyncKVLookupCallbackCtxV1::getDegradeDocsSize() const {
    assert(!isInFlightNoLock());
    return getFailedCount();
}

int64_t AsyncKVLookupCallbackCtxV1::getSeekTime() const {
    assert(!isInFlightNoLock());
    return _metricsCollector.lookupTime;
}

} // namespace sql
