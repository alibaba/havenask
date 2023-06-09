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
#include "ha3/sql/ops/scan/AsyncKVLookupCallbackCtxV2.h"

#include <assert.h>
#include <iosfwd>
#include <type_traits>
#include <utility>

#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "future_lite/Executor.h"
#include "ha3/sql/resource/TabletManagerR.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/KVReadOptions.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "matchdoc/Reference.h"
#include "navi/engine/AsyncPipe.h"
#include "navi_ops/coroutine/NaviAsyncPipeExecutor.h"

using namespace std;
using namespace autil;
using namespace navi;
using namespace future_lite::interface;
using namespace kmonitor;

namespace isearch {
namespace sql {

class AsyncKVLookupMetricsV2 : public kmonitor::MetricsGroup {
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
        REGISTER_LATENCY_MUTABLE_METRIC(_watermarkLatency, "AsyncKV.watermarkLatency");
        REGISTER_QPS_MUTABLE_METRIC(_waitWatermarkFailedQps, "AsyncKV.waitWatermarkFailedQps");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, AsyncKVLookupMetricsCollectorV2 *kvMetrics) {
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

        if (kvMetrics->needWatermark) {
            REPORT_MUTABLE_METRIC(_watermarkLatency, kvMetrics->waitWatermarkTime / 1000.0f);
            if (kvMetrics->waitWatermarkFailed) {
                REPORT_MUTABLE_QPS(_waitWatermarkFailedQps);
            }
        }
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
    MutableMetric *_watermarkLatency = nullptr;
    MutableMetric *_waitWatermarkFailedQps = nullptr;
};

AsyncKVLookupCallbackCtxV2::AsyncKVLookupCallbackCtxV2(const AsyncPipePtr &pipe,
                                                       future_lite::Executor *executor)
    : _asyncPipe(pipe)
    , _executor(executor) {}

void AsyncKVLookupCallbackCtxV2::asyncGet(KVLookupOption option) {
    _metricsCollector = {};
    _option = std::move(option);
    doWaitTablet();
}

void AsyncKVLookupCallbackCtxV2::doWaitTablet() {
    auto done
        = [ctx = shared_from_this()](
              autil::result::Result<std::shared_ptr<indexlibv2::framework::ITablet>> res) mutable {
              ctx->onWaitTabletCallback(std::move(res));
          };

    NAVI_LOG(TRACE3,
             "start wait tablet, asyncPipe[%p] watermark[%ld] type[%d]",
             _asyncPipe.get(),
             _option.targetWatermark,
             _option.targetWatermarkType);
    if (!_asyncPipe || _option.targetWatermarkType == WatermarkType::WM_TYPE_DISABLED) {
        NAVI_LOG(TRACE3, "get tablet use sync mode without watermark value");
        auto tablet = _option.tabletManagerR->getTablet(_option.tableName);
        done({tablet});
    } else if (_option.targetWatermarkType == WatermarkType::WM_TYPE_SYSTEM_TS) {
        NAVI_LOG(TRACE3, "wait tablet by target ts");
        _metricsCollector.needWatermark = true;
        _option.tabletManagerR->waitTabletByTargetTs(_option.tableName,
                                                     _option.targetWatermark,
                                                     std::move(done),
                                                     _option.leftTime
                                                         * _option.targetWatermarkTimeoutRatio);
    } else if (_option.targetWatermarkType == WatermarkType::WM_TYPE_MANUAL) {
        NAVI_LOG(TRACE3, "wait tablet by manual watermark");
        _metricsCollector.needWatermark = true;
        _option.tabletManagerR->waitTabletByWatermark(_option.tableName,
                                                      _option.targetWatermark,
                                                      std::move(done),
                                                      _option.leftTime
                                                          * _option.targetWatermarkTimeoutRatio);
    }
}

void AsyncKVLookupCallbackCtxV2::onWaitTabletCallback(
    autil::result::Result<std::shared_ptr<indexlibv2::framework::ITablet>> res) {
    int64_t waitWatermarkTime = incCallbackVersion();
    NAVI_LOG(TRACE3, "wait tablet returned, latency[%ld]", waitWatermarkTime);
    _metricsCollector.waitWatermarkTime = waitWatermarkTime;

    std::shared_ptr<indexlibv2::framework::ITablet> tablet;
    _option.leftTime -= waitWatermarkTime;
    if (res.is_ok()) {
        tablet = std::move(res).steal_value();
    } else {
        NAVI_INTERVAL_LOG(
            32,
            WARN,
            "wait tablet failed for tableName[%s] error[%s] waitWatermarkTime[%ld] pks[%s]",
            _option.tableName.c_str(),
            res.get_error().message().c_str(),
            waitWatermarkTime,
            autil::StringUtil::toString(_pksForSearch).c_str());
        NAVI_LOG(DEBUG,
                 "wait tablet by target ts failed, tableName[%s] error[%s] "
                 "waitWatermarkTime[%ld], do degraded search",
                 _option.tableName.c_str(),
                 res.get_error().message().c_str(),
                 waitWatermarkTime);
        _metricsCollector.waitWatermarkFailed = true;
        tablet = _option.tabletManagerR->getTablet(_option.tableName);
    }
    assert(tablet != nullptr && "invalid tablet");
    doSearch(std::move(tablet));
}

void AsyncKVLookupCallbackCtxV2::doSearch(std::shared_ptr<indexlibv2::framework::ITablet> tablet) {
    assert(tablet != nullptr);
    prepareReadOptions(_option);
    size_t pkCount = _pksForSearch.size();
    _rawResults.resize(pkCount);
    _metricsCollector.docsCount = pkCount;
    _metricsCollector.buildWatermark = tablet->GetTabletInfos()->GetLatestLocator().GetOffset();

    NAVI_LOG(TRACE3,
             "do search with leftTime[%ld], maxConcurrency[%ld], buildWatermark[%ld]",
             _option.leftTime,
             _option.maxConcurrency,
             _metricsCollector.buildWatermark);

    auto kvReader = getReader(std::move(tablet), _option.indexName);
    if (!kvReader) {
        NAVI_LOG(ERROR, "get reader from tablet failed");
        endLookupSession({"get reader from tablet failed"});
        return;
    }

    if (_asyncPipe == nullptr) {
        NAVI_LOG(DEBUG, "async pipe is nullptr, use sync with reader v2");
        incStartVersion();
        auto result = future_lite::interface::syncAwaitViaExecutor(
            kvReader->BatchGetAsync(_pksForSearch, _rawResults, _readOptions), _executor);
        processStatusVec(std::move(result));
        _metricsCollector.lookupTime = incCallbackVersion();
        endLookupSession({});
    } else {
        assert(_executor && "executor is nullptr");
        NAVI_LOG(DEBUG, "async pipe ready, use async with reader v2");
        auto ctx = shared_from_this();
        incStartVersion();
        future_lite::interface::awaitViaExecutor(
            kvReader->BatchGetAsync(_pksForSearch, _rawResults, _readOptions),
            _executor,
            [ctx](use_try_t<StatusVector> statusVecTry) {
                ctx->onSessionCallback(std::move(statusVecTry));
            });
    }
}

std::shared_ptr<indexlibv2::index::KVIndexReader>
AsyncKVLookupCallbackCtxV2::getReader(std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                                      const std::string &indexName) {
    _tablet = std::move(tablet);
    _tabletReader = _tablet->GetTabletReader();
    // auto *tabletReader
    //     = dynamic_cast<indexlibv2::table::KVTabletSessionReader *>(_tabletReader.get());
    // if (tabletReader == nullptr) {
    //     NAVI_LOG(TRACE3, "kv table reader v2 not exist, index name[%s]", indexName.c_str());
    //     return nullptr;
    // }

    auto indexReader = _tabletReader->GetIndexReader(indexlibv2::index::KV_INDEX_TYPE_STR, indexName);
    if (indexReader == nullptr) {
        NAVI_LOG(TRACE3, "kv table reader v2 not exist, index name[%s]", indexName.c_str());
        return nullptr;
    }
    return std::dynamic_pointer_cast<indexlibv2::index::KVIndexReader>(indexReader);    
}

void AsyncKVLookupCallbackCtxV2::onSessionCallback(use_try_t<StatusVector> statusVecTry) {
    assert(!future_lite::interface::tryHasError(statusVecTry));
    processStatusVec(std::move(statusVecTry));
    _metricsCollector.lookupTime = incCallbackVersion();
    endLookupSession({});
}

void AsyncKVLookupCallbackCtxV2::prepareReadOptions(const KVLookupOption &option) {
    _readOptions.timestamp = TimeUtility::currentTime();
    _readOptions.timeoutTerminator = std::make_shared<autil::TimeoutTerminator>(option.leftTime);
    _readOptions.pool = option.pool.get();
    _metricsCollector.indexCollector.Reset();
    _readOptions.metricsCollector = &_metricsCollector.indexCollector;
    _readOptions.maxConcurrency = option.maxConcurrency;
}

void AsyncKVLookupCallbackCtxV2::processStatusVec(use_try_t<StatusVector> statusVecTry) {
    assert(!future_lite::interface::tryHasError(statusVecTry));

    auto statusVec = std::move(future_lite::interface::getTryValue(statusVecTry));
    assert(statusVec.size() == _pksForSearch.size());
    _results.resize(statusVec.size());
    for (size_t i = 0; i < statusVec.size(); ++i) {
        assert(!future_lite::interface::tryHasError(statusVec[i]));
        auto status = future_lite::interface::getTryValue(statusVec[i]);
        if (status == indexlibv2::index::KVResultStatus::FOUND) {
            _results[i] = &_rawResults[i];
        } else {
            if (status == indexlibv2::index::KVResultStatus::NOT_FOUND
                || status == indexlibv2::index::KVResultStatus::DELETED) {
                _notFoundPks.emplace_back(_pksForSearch[i]);
            } else {
                _failedPks.emplace_back(_pksForSearch[i], (uint32_t)status);
            }
            _results[i] = nullptr;
        }
    }
    if (!_failedPks.empty()) {
        NAVI_LOG(
            WARN, "kvReader has failedPks[%s]", autil::StringUtil::toString(_failedPks).c_str());
    }
    _metricsCollector.failedDocsCount = getFailedCount();
    _metricsCollector.notFoundDocsCount = getNotFoundCount();
}

bool AsyncKVLookupCallbackCtxV2::tryReportMetrics(
    kmonitor::MetricsReporter &opMetricsReporter) const {
    AsyncKVLookupMetricsCollectorV2 metricsCollector;
    {
        ScopedSpinLock scope(_statLock);
        if (isInFlightNoLock()) {
            // in-flight metrics collector is unstable
            NAVI_LOG(DEBUG, "ignore in-flight metric collector");
            return false;
        }
        metricsCollector = _metricsCollector;
    }
    opMetricsReporter.report<AsyncKVLookupMetricsV2>(nullptr, &metricsCollector);
    NAVI_LOG(TRACE3, "current lookup time[%ld]", metricsCollector.lookupTime);
    return true;
}

int64_t AsyncKVLookupCallbackCtxV2::getWaitWatermarkTime() const {
    assert(!isInFlightNoLock());
    return _metricsCollector.waitWatermarkTime;
}

int64_t AsyncKVLookupCallbackCtxV2::getBuildWatermark() const {
    assert(!isInFlightNoLock());
    return _metricsCollector.buildWatermark;
}

size_t AsyncKVLookupCallbackCtxV2::getDegradeDocsSize() const {
    assert(!isInFlightNoLock());
    if (_metricsCollector.waitWatermarkFailed) {
        return _pksForSearch.size();
    } else {
        return getFailedCount();
    }
}

int64_t AsyncKVLookupCallbackCtxV2::getSeekTime() const {
    assert(!isInFlightNoLock());
    return _metricsCollector.lookupTime + _metricsCollector.waitWatermarkTime;
}

void AsyncKVLookupCallbackCtxV2::endLookupSession(std::optional<std::string> errorDesc) {
    assert(!isInFlightNoLock());
    _errorDesc = errorDesc;
    NAVI_LOG(TRACE3,
             "ctx end async session with asyncPipe[%p] error[%s]",
             _asyncPipe.get(),
             errorDesc ? errorDesc.value().c_str() : "");
    if (_asyncPipe) {
        (void)_asyncPipe->setData(ActivationData::inst());
    } else {
        // pass
    }
    _option = {};
    _tabletReader.reset();
    _tablet.reset();
}

} // end namespace sql
} // end namespace isearch
