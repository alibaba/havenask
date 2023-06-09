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
#include "ha3/sql/resource/SqlQueryResource.h"

#include <iosfwd>

#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"
#include "autil/TimeoutTerminator.h"
#include "ha3/sql/common/TracerAdapter.h"
#include "ha3/sql/proto/SqlSearchInfoCollector.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlRequestInfoR.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/common.h"
#include "navi/resource/GigClientResource.h"
#include "navi/resource/GigQuerySessionR.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "navi/resource/KmonResource.h"
#include "navi/resource/TimeoutCheckerR.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"

using namespace std;
using namespace suez::turing;
using namespace kmonitor;
using namespace autil;
using namespace navi;

namespace isearch {
namespace sql {

const std::string SqlQueryResource::RESOURCE_ID = "SqlQueryResource";

SqlQueryResource::SqlQueryResource()
    : _querySession(nullptr)
    , _startTime(0)
    , _timeout(0)
    , _partId(0)
    , _totalPartCount(0) {}

SqlQueryResource::~SqlQueryResource() {
    NAVI_KERNEL_LOG(TRACE3, "sql query resource destructed [%p]", this);
}

void SqlQueryResource::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID)
        .depend(SqlBizResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_sqlBizResource))
        .depend(SqlRequestInfoR::RESOURCE_ID, true, BIND_RESOURCE_TO(_sqlRequestInfoR))
        .depend(navi::GRAPH_MEMORY_POOL_RESOURCE_ID, true, BIND_RESOURCE_TO(_memoryPoolResource))
        .depend(navi::TimeoutCheckerR::RESOURCE_ID, false, BIND_RESOURCE_TO(_timeoutCheckerR))
        .depend(navi::GigQuerySessionR::RESOURCE_ID, false, BIND_RESOURCE_TO(_naviQuerySessionR))
        .depend(navi::BIZ_KMON_RESOURCE_ID, false, BIND_RESOURCE_TO(_bizKmonResource))
        .depend(navi::GIG_CLIENT_RESOURCE_ID, false, BIND_RESOURCE_TO(_gigClientResource));
}

navi::ErrorCode SqlQueryResource::init(navi::ResourceInitContext &ctx) {
    auto partCount = ctx.getPartCount();
    uint32_t partId = ctx.getPartId();
    setPartId(partId);
    setTotalPartCount(partCount);

    initCommon();
    if (_sqlRequestInfoR->inQrs) {
        initQrs();
    } else {
        initSearcher();
    }

    return navi::EC_NONE;
}

void SqlQueryResource::initCommon() {
    setPool(_memoryPoolResource->getPool());
    setModelConfigMap(_sqlBizResource->getModelConfigMap());
    setTvfNameToCreator(_sqlBizResource->getTvfNameToCreator());

    setSuezCavaAllocator(
        SuezCavaAllocatorPtr(
            new SuezCavaAllocator(_queryPool.get(), _sqlBizResource->getCavaAllocSizeLimit() * 1024 * 1024)));
    initSuezTuringTracerAdapter();

    if (_timeoutCheckerR) {
        auto *timeoutChecker = _timeoutCheckerR->getTimeoutChecker();
        auto beginTime = timeoutChecker->beginTime(); // us
        auto timeoutMs = timeoutChecker->timeoutMs(); // ms
        NAVI_KERNEL_LOG(TRACE3,
                        "create query timeout terminator from TimeoutCheckerR [%p],"
                        "beginTime [%ld] timeoutMs [%ld]",
                        _timeoutCheckerR, beginTime, timeoutMs);
        TimeoutTerminatorPtr terminator(new TimeoutTerminator(timeoutMs * 1000, beginTime));
        setTimeoutTerminator(terminator);
        setStartTime(beginTime);
        setTimeoutMs(timeoutMs);
    } else {
        NAVI_KERNEL_LOG(TRACE3, "skip query timeout terminator");
    }

    if (_bizKmonResource) {
        NAVI_KERNEL_LOG(TRACE3,
                        "create query metrics reporter from biz kmon resource [%p]",
                        _bizKmonResource);
        // TODO
        // SearchContext::addGigMetricTags(_querySession.get(), tagsMap, _useGigSrc);
        // MetricsTags tags(tagsMap);
        _metricsReporter = _bizKmonResource->getBaseReporter()->getSubReporter(
            "kmon", {});
    } else {
        NAVI_KERNEL_LOG(TRACE3, "skip query metrics reporter");
    }
    auto snapshot = _sqlBizResource->createPartitionReaderSnapshot(_partId);
    if (snapshot == nullptr) {
        auto snapshot = _sqlBizResource->createDefaultIndexSnapshot();
    }
    setPartitionReaderSnapshot(snapshot);
}

void SqlQueryResource::initQrs() {
    // only has value on qrs
    NAVI_KERNEL_LOG(TRACE3, "get gig query sesion [%p] from request info",
                    _sqlRequestInfoR->gigQuerySession.get());
    setGigQuerySession(_sqlRequestInfoR->gigQuerySession);
}

void SqlQueryResource::initSearcher() {
    if (_sqlBizResource->getSearchService()) {
        multi_call::QuerySessionPtr querySession(
            new multi_call::QuerySession(_sqlBizResource->getSearchService()));
        NAVI_KERNEL_LOG(TRACE3, "create local gig query sesion [%p]", querySession.get());
        setGigQuerySession(querySession);
    }
}

void SqlQueryResource::initSuezTuringTracerAdapter() {
    if (NAVI_TLS_LOGGER && NAVI_TLS_LOGGER->logger) {
        LogLevel level = NAVI_TLS_LOGGER->logger->getTraceLevel();
        NAVI_KERNEL_LOG(TRACE3, "navi trace level[%d]", level);
        if (level != LOG_LEVEL_DISABLE) {
            TracerAdapterPtr tracer(new TracerAdapter());
            tracer->setNaviLevel(level);
            _tracer = tracer;
        }
    }
}

void SqlQueryResource::setPool(const autil::mem_pool::PoolPtr &queryPool) {
    _queryPool = queryPool;
}

autil::mem_pool::Pool *SqlQueryResource::getPool() const {
    return _queryPool.get();
}

autil::mem_pool::PoolPtr SqlQueryResource::getPoolPtr() const {
    return _queryPool;
}

Tracer *SqlQueryResource::getTracer() const {
    return _tracer.get();
}

SuezCavaAllocator *SqlQueryResource::getSuezCavaAllocator() const {
    return _cavaAllocator.get();
}
void SqlQueryResource::setSuezCavaAllocator(const suez::turing::SuezCavaAllocatorPtr &allocator) {
    _cavaAllocator = allocator;
}

autil::TimeoutTerminator *SqlQueryResource::getTimeoutTerminator() const {
    return _terminator.get();
}
void SqlQueryResource::setTimeoutTerminator(const autil::TimeoutTerminatorPtr &terminator) {
    _terminator = terminator;
}
indexlib::partition::PartitionReaderSnapshot *SqlQueryResource::getPartitionReaderSnapshot() const {
    return _readerSnapshot.get();
}
void SqlQueryResource::setPartitionReaderSnapshot(
    const indexlib::partition::PartitionReaderSnapshotPtr &snapshot) {
    _readerSnapshot = snapshot;
}

MetricsReporter *SqlQueryResource::getQueryMetricsReporter() const {
    return _metricsReporter.get();
}
void SqlQueryResource::setQueryMetricsReporter(const kmonitor::MetricsReporterPtr &reporter) {
    _metricsReporter = reporter;
}

multi_call::QuerySession *SqlQueryResource::getGigQuerySession() const {
    return _querySession.get();
}
void SqlQueryResource::setGigQuerySession(multi_call::QuerySessionPtr querySession) {
    _querySession = querySession;
}

std::shared_ptr<multi_call::QuerySession> SqlQueryResource::getNaviQuerySession() const {
    // using navi search service instead of turing search service
    return _naviQuerySessionR ? _naviQuerySessionR->getQuerySession() : nullptr;
}

void SqlQueryResource::setStartTime(int64_t startTime) {
    _startTime = startTime;
}
int64_t SqlQueryResource::getStartTime() const {
    return _startTime;
}
void SqlQueryResource::setTimeoutMs(int64_t timeout) {
    _timeout = timeout;
}
int64_t SqlQueryResource::getTimeoutMs() const {
    return _timeout;
}

void SqlQueryResource::setPartId(int32_t partId) {
    _partId = partId;
}
int32_t SqlQueryResource::getPartId() const {
    return _partId;
}

void SqlQueryResource::setTotalPartCount(int32_t partCount) {
    _totalPartCount = partCount;
}
int32_t SqlQueryResource::getTotalPartCount() const {
    return _totalPartCount;
}

void SqlQueryResource::setModelConfigMap(isearch::turing::ModelConfigMap *modelConfigMap) {
    _modelConfigMap = modelConfigMap;
}
isearch::turing::ModelConfigMap *SqlQueryResource::getModelConfigMap() const {
    return _modelConfigMap;
}

void SqlQueryResource::setTvfNameToCreator(
    std::map<std::string, isearch::sql::TvfFuncCreatorR *> *tvfNameToCreator) {
    _tvfNameToCreator = tvfNameToCreator;
}
map<string, isearch::sql::TvfFuncCreatorR *> *SqlQueryResource::getTvfNameToCreator() const {
    return _tvfNameToCreator;
}

std::string SqlQueryResource::getETraceId() const {
    if (_querySession) {
        auto span = _querySession->getTraceServerSpan();
        if (span) {
            return opentelemetry::EagleeyeUtil::getETraceId(span);
        }
    }
    return "";
}

REGISTER_RESOURCE(SqlQueryResource);

} // namespace sql
} // namespace isearch
