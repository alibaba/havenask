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
#include "sql/ops/scan/AsyncKVLookupCallbackCtxV2.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <engine/ResourceInitContext.h>
#include <iosfwd>
#include <type_traits>
#include <utility>

#include "alog/Logger.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/KVReadOptions.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "navi/common.h"
#include "navi/engine/AsyncPipe.h"
#include "navi/log/NaviLogger.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace autil;
using namespace navi;
using namespace future_lite::interface;
using namespace kmonitor;

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

AsyncKVLookupCallbackCtxV2::AsyncKVLookupCallbackCtxV2(const AsyncPipePtr &pipe,
                                                       future_lite::Executor *executor)
    : _asyncPipe(pipe)
    , _executor(executor) {}

bool AsyncKVLookupCallbackCtxV2::isSchemaMatch(
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) const {
    auto *tabletSchema = dynamic_cast<indexlibv2::config::TabletSchema *>(schema.get());
    return tabletSchema && tabletSchema->GetSchemaId() == _schema->GetSchemaId();
}

void AsyncKVLookupCallbackCtxV2::asyncGet(KVLookupOption option) {
    _metricsCollector = {};
    _option = std::move(option);
    assert(_option.tablet != nullptr);
    prepareReadOptions(_option);
    size_t pkCount = _pksForSearch.size();
    _rawResults.resize(pkCount);
    _metricsCollector.docsCount = pkCount;

    NAVI_LOG(TRACE3,
             "do search with leftTime[%ld], maxConcurrency[%ld]",
             _option.leftTime,
             _option.maxConcurrency);

    auto kvReader = getReader(_option.tablet, _option.indexName);
    if (!kvReader) {
        NAVI_LOG(ERROR, "get reader from tablet failed");
        endLookupSession({"get reader from tablet failed"});
        return;
    }

    if (_asyncPipe == nullptr) {
        NAVI_LOG(DEBUG, "async pipe is nullptr, use sync with reader v2");
        auto result = future_lite::interface::syncAwaitViaExecutor(
            kvReader->BatchGetAsync(_pksForSearch, _rawResults, _readOptions), _executor);
        processStatusVec(std::move(result));
        _metricsCollector.lookupTime = incCallbackVersion();
        endLookupSession({});
    } else {
        assert(_executor && "executor is nullptr");
        NAVI_LOG(DEBUG, "async pipe ready, use async with reader v2");
        auto ctx = shared_from_this();
        future_lite::interface::awaitViaExecutor(
            kvReader->BatchGetAsync(_pksForSearch, _rawResults, _readOptions),
            _executor,
            [ctx](future_lite::interface::use_try_t<StatusVector> statusVecTry) {
                ctx->onSessionCallback(std::move(statusVecTry));
            });
    }
}

std::shared_ptr<indexlibv2::index::KVIndexReader>
AsyncKVLookupCallbackCtxV2::getReader(const std::shared_ptr<indexlibv2::framework::ITablet> &tablet,
                                      const std::string &indexName) {
    _tabletReader = tablet->GetTabletReader();
    _schema = _tabletReader->GetSchema();

    auto indexReader
        = _tabletReader->GetIndexReader(indexlibv2::index::KV_INDEX_TYPE_STR, indexName);
    if (indexReader == nullptr) {
        NAVI_LOG(TRACE3, "kv table reader v2 not exist, index name[%s]", indexName.c_str());
        return nullptr;
    }
    return std::dynamic_pointer_cast<indexlibv2::index::KVIndexReader>(indexReader);
}

void AsyncKVLookupCallbackCtxV2::onSessionCallback(
    future_lite::interface::use_try_t<StatusVector> statusVecTry) {
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

void AsyncKVLookupCallbackCtxV2::processStatusVec(
    future_lite::interface::use_try_t<StatusVector> statusVecTry) {
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

int64_t AsyncKVLookupCallbackCtxV2::getSeekTime() const {
    assert(!isInFlightNoLock());
    return _metricsCollector.lookupTime;
}

void AsyncKVLookupCallbackCtxV2::endLookupSession(std::optional<std::string> errorDesc) {
    assert(!isInFlightNoLock());
    _errorDesc = errorDesc;
    NAVI_LOG(TRACE3,
             "ctx end async session with asyncPipe[%p] error[%s]",
             _asyncPipe.get(),
             errorDesc ? errorDesc.value().c_str() : "");
    if (_asyncPipe) {
        (void)_asyncPipe->setData(nullptr);
    } else {
        // pass
    }
    _option = {};
    _tabletReader.reset();
}

} // namespace sql
