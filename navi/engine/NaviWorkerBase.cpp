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
#include "navi/engine/NaviWorkerBase.h"

#include "aios/network/gig/multi_call/agent/QueryInfo.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"
#include "navi/engine/BizManager.h"
#include "navi/engine/NaviSnapshot.h"
#include "navi/engine/NaviThreadPool.h"
#include "navi/log/TraceAppender.h"
#include "navi/resource/GigClientResource.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "navi/resource/MetaInfoResourceBase.h"
#include "navi/util/CommonUtil.h"
#include "navi/util/NaviClosure.h"

namespace navi {

thread_local extern size_t current_thread_counter;
thread_local extern size_t current_thread_wait_counter;

void NaviWorkerItem::process() {
    if (!_worker->isStopped()) {
        auto collectPerf = _worker->_runGraphParams.collectPerf();
        if (collectPerf) {
            _worker->enablePerf();
        }
        {
            NaviLoggerScope scope(_worker->getLogger());
            doProcess();
        }
        if (collectPerf) {
            _worker->disablePerf();
        }
    } else if (!_worker->isFinished()) {
        _worker->drop();
    }
}

void NaviWorkerItem::destroy() {
    auto worker = _worker;
    NAVI_POOL_DELETE_CLASS(this);
    worker->dec();
}

void NaviWorkerItem::drop() {
    NAVI_POOL_DELETE_CLASS(this);
}

bool NaviWorkerItem::syncMode() const {
    return _syncMode;
}

void NaviWorkerItem::setSyncMode(bool syncMode) {
    _syncMode = syncMode;
}

void NaviNoDropWorkerItem::process() {
    doWork();
    if (_worker->isStopped() && !_worker->isFinished()) {
        _worker->drop();
    }
}

void NaviNoDropWorkerItem::destroy() {
    doWork();
    auto worker = _worker;
    NAVI_POOL_DELETE_CLASS(this);
    worker->dec();
}

void NaviNoDropWorkerItem::drop() {
    setScheduleInfo(CommonUtil::getTimelineTimeNs(),
                    _worker->getRunningThreadCount(), current_thread_id,
                    current_thread_counter, current_thread_wait_counter);
    doWork();
    NAVI_POOL_DELETE_CLASS(this);
}

void NaviNoDropWorkerItem::doWork() {
    if (!_done) {
        auto collectPerf = _worker->_runGraphParams.collectPerf();
        if (collectPerf) {
            _worker->enablePerf();
        }
        {
            NaviLoggerScope scope(_worker->getLogger());
            doProcess();
            _done = true;
        }
        if (collectPerf) {
            _worker->disablePerf();
        }
    }
}

class NaviSessionDestructItem : public NaviThreadPoolItemBase
{
public:
    NaviSessionDestructItem(NaviSnapshot *snapshot, TaskQueue *taskQueue)
        : _snapshot(snapshot)
        , _taskQueue(taskQueue)
    {
    }
public:
    void process() override {
        if (_snapshot) {
            _snapshot->onSessionDestruct(_taskQueue);
            _snapshot = nullptr;
        }
    }
    void destroy() override {
        process();
        delete this;
    }
private:
    NaviSnapshot *_snapshot;
    TaskQueue *_taskQueue;
};

class WorkerDestructItem : public NaviThreadPoolItemBase
{
public:
    WorkerDestructItem(NaviWorkerBase *worker)
        : _worker(worker)
    {
    }
public:
    void process() override {
        if (_worker) {
            _worker->finalize();
            _worker = nullptr;
        }
    }
    void destroy() override {
        process();
        delete this;
    }
private:
    NaviWorkerBase *_worker;
};

NaviWorkerBase::NaviWorkerBase(
        const NaviLoggerPtr &logger,
        TaskQueue *taskQueue,
        BizManager *bizManager,
        NaviUserResultClosure *closure)
    : _logger(this, "session", logger)
    , _metricsReporter(nullptr)
    , _errorCode(EC_NONE)
    , _finish(false)
    , _initSuccess(false)
    , _threadPool(taskQueue->threadPool.get())
    , _taskQueue(taskQueue)
    , _snapshot(nullptr)
    , _naviPerf(nullptr)
{
    _metricsCollector.createTime = autil::TimeUtility::currentTime();

    if (closure) {
        _userResult.reset(new NaviAsyncUserResult(closure));
    } else {
        _userResult.reset(new NaviSyncUserResult());
    }
    _userResult->setWorker(this);
    initMemoryPool(bizManager);
    initMetaInfoResource(bizManager);
    initGraphParam(bizManager);
    initCounters();
    NAVI_LOG(SCHEDULE1, "construct");
}

NaviWorkerBase::~NaviWorkerBase() {
    if (_metricsReporter) {
        _metricsReporter->report<RunGraphMetrics>(nullptr, &_metricsCollector);
    }
    assert(_scheduleQueue.Empty());
    NAVI_LOG(SCHEDULE1, "destruct");
    _logger.logger.reset();
}

void NaviWorkerBase::setMetricsReporter(kmonitor::MetricsReporter &reporter)
{
    _metricsReporter = reporter.getSubReporter("kmon.sql", {});
}

void NaviWorkerBase::setRunParams(const RunGraphParams &params) {
    _runGraphParams = params;
    initRunParams();
}

const RunGraphParams &NaviWorkerBase::getRunParams() const {
    return _runGraphParams;
}

void NaviWorkerBase::setRunStartTime(int64_t runStartTime) {
    _metricsCollector.runStartTime = runStartTime;
}

void NaviWorkerBase::setDestructThreadPool(
    const NaviThreadPoolPtr &destructThreadPool)
{
    _destructThreadPool = destructThreadPool;
}

void NaviWorkerBase::setNaviPerf(NaviPerf *naviPerf) {
    _naviPerf = naviPerf;
}

void NaviWorkerBase::setNaviSymbolTable(
    const NaviSymbolTablePtr &naviSymbolTable)
{
    auto naviResult = _userResult->getNaviResult();
    naviResult->graphMetric.setNaviSymbolTable(naviSymbolTable);
}

void NaviWorkerBase::enablePerf() {
    _naviPerf->enable(current_thread_id);
}

void NaviWorkerBase::disablePerf() {
    _naviPerf->disable(current_thread_id);
}

void NaviWorkerBase::beginSample() {
    _naviPerf->beginSample(current_thread_id);
}

NaviPerfResultPtr NaviWorkerBase::endSample() {
    return _naviPerf->endSample(current_thread_id);
}

void NaviWorkerBase::initRunParams() {
    auto sessionId = _runGraphParams.getSessionId();
    if (sessionId.valid()) {
        _logger.logger->setLoggerId(sessionId);
    } else {
        sessionId = _logger.logger->getLoggerId();
        _runGraphParams.setSessionId(sessionId);
    }
    _userResult->getNaviResult()->setSessionId(sessionId);
    const auto &traceLevel = _runGraphParams.getTraceLevel();
    if (getLevelByString(traceLevel) != LOG_LEVEL_DISABLE) {
        auto traceAppender = new TraceAppender(traceLevel);
        _runGraphParams.fillFilterParam(traceAppender);
        _logger.logger->setTraceAppender(traceAppender);
    }
    NAVI_LOG(DEBUG, "finish init tracer with level[%s], taskQueue[%p]",
             traceLevel.c_str(), _taskQueue);
    tryCreateQuerySession();
    _timeoutChecker.setTimeoutMs(_runGraphParams.getTimeoutMs());
    auto naviResult = _userResult->getNaviResult();
    naviResult->setCollectMetric(_runGraphParams.collectMetric());
    naviResult->setTraceFormatPattern(_runGraphParams.getTraceFormatPattern());
}

void NaviWorkerBase::initMemoryPool(BizManager *bizManager) {
    _poolResource = bizManager->createGraphMemoryPoolResource();
    assert(_poolResource);
    _pool = _poolResource->getPool();
}

void NaviWorkerBase::initMetaInfoResource(BizManager *bizManager) {
    _metaInfoResource = bizManager->createMetaInfoResource();
}

void NaviWorkerBase::tryCreateQuerySession() {
    auto bizManager = _param.bizManager;
    auto resource = bizManager->getRootResource(GIG_CLIENT_RESOURCE_ID);
    if (!resource) {
        NAVI_LOG(SCHEDULE1, "get gig client resource failed, skip");
        return;
    }
    auto gigResource = dynamic_cast<GigClientResource *>(resource.get());
    assert(gigResource != nullptr && "gig resource type error");
    auto searchService = gigResource->getSearchService();
    if (!searchService) {
        NAVI_LOG(SCHEDULE1, "get search service failed, skip");
        return;
    }
    auto originQuerySession = _runGraphParams.getQuerySession();
    if (originQuerySession) {
        auto querySession = multi_call::QuerySession::createWithOrigin(searchService, originQuerySession);
        NAVI_LOG(SCHEDULE1,
                 "init query session with query session, eTraceId[%s]",
                 querySession->getTraceServerSpan()
                     ? opentelemetry::EagleeyeUtil::getETraceId(querySession->getTraceServerSpan()).c_str()
                     : "");
        _param.querySession = std::move(querySession);
    } else {
        auto querySession = std::make_shared<multi_call::QuerySession>(searchService, _queryInfo);
        NAVI_LOG(SCHEDULE1,
                 "init query session with query info[%s], eTraceId[%s]",
                 _queryInfo ? _queryInfo->toString().c_str() : "null",
                 querySession->getTraceServerSpan()
                     ? opentelemetry::EagleeyeUtil::getETraceId(querySession->getTraceServerSpan()).c_str()
                     : "");
        _param.querySession = std::move(querySession);
    }
}

void NaviWorkerBase::startTrace(const std::string &peer) const {
    if (!_param.querySession) {
        return;
    }
    auto span = _param.querySession->getTraceServerSpan();
    if (!span) {
        return;
    }
    span->setAttribute(opentelemetry::kSpanAttrNetPeerIp, peer);
}

void NaviWorkerBase::setQueryInfo(const std::shared_ptr<multi_call::QueryInfo> &queryInfo) {
    _queryInfo = queryInfo;
}

void NaviWorkerBase::initGraphParam(BizManager *bizManager) {
    _param.logger = _logger.logger;
    _param.pool = _pool;
    _param.bizManager = bizManager;
    _param.creatorManager = bizManager->getCreatorManager().get();
    _param.graphMetric = _userResult->getNaviResult()->getGraphMetric();
    _param.worker = this;
}

void NaviWorkerBase::initCounters() {
    atomic_set(&_itemCount, 0);
    atomic_set(&_processingCount, 0);
}

void NaviWorkerBase::initSchedule(bool syncMode, NaviSnapshot *snapshot) {
    NAVI_LOG(SCHEDULE1, "init schedule, syncMode [%d] snapshot [%p]", syncMode, snapshot);
    _snapshot = snapshot;
    _metricsCollector.initScheduleTime = autil::TimeUtility::currentTime();

    if (_threadPool->incWorkerCount()) {
        _initSuccess = true;
        auto item = NAVI_POOL_NEW_CLASS(_pool, WorkerInitItem, this);
        item->setSyncMode(syncMode);
        schedule(item);
    } else {
        drop();
    }
}

bool NaviWorkerBase::schedule(NaviWorkerItem *item) {
    if (isStopped()) {
        incItemCount();
        drop();
        item->drop();
        clear();
        decItemCount();
        return false;
    }
    incItemCount();
    if (atomic_read(&_processingCount) < getThreadLimit()) {
        atomic_inc(&_processingCount);
        _threadPool->push(item);
        return true;
    }
    _scheduleQueue.Push(item);
    doSchedule();
    return true;
}

void NaviWorkerBase::doSchedule() {
    NaviWorkerItem *item = nullptr;
    if (atomic_read(&_processingCount) < getThreadLimit() &&
        _scheduleQueue.Pop(&item))
    {
        atomic_inc(&_processingCount);
        _threadPool->push(item);
    }
}

void NaviWorkerBase::clear() {
    NaviWorkerItem *item = nullptr;
    while (_scheduleQueue.Pop(&item)) {
        // will not destruct this if called from another NaviWorkerItem
        item->destroy();
    }
}

const std::string &NaviWorkerBase::getTraceLevel() const {
    return _runGraphParams.getTraceLevel();
}

int64_t NaviWorkerBase::getThreadLimit() const {
    return _runGraphParams.getThreadLimit();
}

int64_t NaviWorkerBase::getRemainTime() const {
    return _timeoutChecker.remainTime();
}

int64_t NaviWorkerBase::getBeginTime() const {
    return _timeoutChecker.beginTime();
}

TimeoutChecker *NaviWorkerBase::getTimeoutChecker() {
    return &_timeoutChecker;
}

const NaviThreadPoolPtr &NaviWorkerBase::getDestructThreadPool() const {
    return _destructThreadPool;
}

GraphParam *NaviWorkerBase::getGraphParam() {
    return &_param;
}

const GraphMemoryPoolResourcePtr &NaviWorkerBase::getGraphMemoryPoolResource() const {
    return _poolResource;
}

const MetaInfoResourceBasePtr &NaviWorkerBase::getMetaInfoResource() const {
    return _metaInfoResource;
}

void NaviWorkerBase::setErrorCode(ErrorCode ec) {
    if (_errorCode == EC_NONE) {
        if (EC_NONE != ec) {
            NAVI_LOG(ERROR, "ec: %s", CommonUtil::getErrorString(ec));
        }
        _errorCode = ec;
    }
}

ErrorCode NaviWorkerBase::getErrorCode() const {
    return _errorCode;
}

const NaviObjectLogger &NaviWorkerBase::getLogger() const {
    return _logger;
}

void NaviWorkerBase::setFinish(ErrorCode ec) {
    setErrorCode(ec);
    NAVI_LOG(SCHEDULE1, "finish");
    // _logger.logger->stop();
    _finish = true;
}

bool NaviWorkerBase::isStopped() const {
    return isFinished() || isTimeout();
}

bool NaviWorkerBase::isFinished() const {
    return _finish;
}

bool NaviWorkerBase::isTimeout() const {
    if (_timeoutChecker.timeout()) {
        auto curTime = autil::TimeUtility::currentTime();
        auto beginTime = _timeoutChecker.beginTime();
        NAVI_LOG(ERROR,
                 "timeout, expect time [%ld ms] use time [%ld ms], start "
                 "[%ld], end[%ld]",
                 _timeoutChecker.timeoutMs(),
                 (curTime - beginTime) / FACTOR_MS_TO_US,
                 beginTime / FACTOR_MS_TO_US, curTime / FACTOR_MS_TO_US);
        NAVI_LOG(SCHEDULE1, "timeout bt: %s",
                 _logger.logger->firstErrorBackTrace().c_str());
        return true;
    }
    return false;
}

bool NaviWorkerBase::reportRpcLackByBiz(const multi_call::GigStreamRpcInfoMap &rpcInfoMap) {
    bool hasLack = false;
    for (const auto &pair : rpcInfoMap) {
        size_t bizUsedRpcCount, bizLackRpcCount;
        calcStreamRpcConverage(pair.second, bizUsedRpcCount, bizLackRpcCount);
        if (bizLackRpcCount > 0) {
            static const std::string metricName = "user.ops.ExchangeKernel.LackRatio";
            double value = 1.0 * bizLackRpcCount / bizUsedRpcCount;
            kmonitor::MetricsTags tags{"biz", pair.first.second};
            REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, metricName, value, &tags);
            hasLack = true;
        }
    }
    return hasLack;
}

void NaviWorkerBase::fillResultMetrics() {
    // for: may enter multiple times
    if (_metricsCollector.fillResultTime == 0) {
        _metricsCollector.fillResultTime = autil::TimeUtility::currentTime();

        auto graphMetricTime = _userResult->getNaviResult()->getGraphMetricTime();
        _metricsCollector.totalQueueLatency = graphMetricTime.queueUs / 1000;
        _metricsCollector.totalComputeLatency = graphMetricTime.computeUs / 1000;
        _metricsCollector.allocatedPoolSize = _param.pool->getAllocatedSize();
        _metricsCollector.hasError = getErrorCode() != EC_NONE;

        auto rpcInfoMap = _userResult->getNaviResult()->stealRpcInfoMap();
        _metricsCollector.hasLack = reportRpcLackByBiz(rpcInfoMap);
        _userResult->getNaviResult()->setRpcInfoMap(std::move(rpcInfoMap));
    }
}

NaviUserResultPtr NaviWorkerBase::getUserResult() const {
    return _userResult;
}

void NaviWorkerBase::fillResult() {
    NAVI_LOG(SCHEDULE1, "fill result");
    finalizeMetaInfoToResult();
    NaviResult result;
    auto logger = _logger.logger;
    result.ec = getErrorCode();
    result.errorEvent = logger->firstErrorEvent();
    result.errorBackTrace = logger->firstErrorBackTrace();
    logger->collectTrace(result.traceCollector);
    _userResult->appendNaviResult(result);
    _userResult->getNaviResult()->end();

    fillResultMetrics();
    _userResult->notify(true);
}

void NaviWorkerBase::appendTrace(TraceCollector &collector) {
    _userResult->appendTrace(collector);
}

void NaviWorkerBase::appendResult(NaviResult &result) {
    setErrorCode(result.ec);
    _userResult->appendNaviResult(result);
}

void NaviWorkerBase::dec() {
    atomic_dec(&_processingCount);
    decItemCount();
}

void NaviWorkerBase::finalize() {
    NAVI_LOG(SCHEDULE1, "finalize")
    if (_initSuccess) {
        _threadPool->decWorkerCount();
    }
    deleteThis();
}

void NaviWorkerBase::finalizeMetaInfoToResult() {}

void NaviWorkerBase::deleteThis() {
    delete this;
}

void NaviWorkerBase::incItemCount() {
    NAVI_LOG(SCHEDULE2, "inc item count from [%lld]", atomic_read(&_itemCount));
    atomic_inc(&_itemCount);
}

void NaviWorkerBase::decItemCount() {
    NAVI_LOG(SCHEDULE2, "dec item count from [%lld]", atomic_read(&_itemCount));
    doSchedule();
    if (unlikely(atomic_dec_return(&_itemCount) == 0)) {
        NAVI_LOG(SCHEDULE1, "item count clear, snapshot [%p]", _snapshot);
        if (_snapshot) {
            auto destructItem = new NaviSessionDestructItem(_snapshot, _taskQueue);
            _snapshot = nullptr;
            _threadPool->push(destructItem);
        }
        auto item = new WorkerDestructItem(this);
        _destructThreadPool->push(item);
    }
}

int64_t NaviWorkerBase::itemCount() {
    return atomic_read(&_itemCount);
}

bool NaviWorkerBase::canInline() const {
    if (INVALID_INLINE_DEPTH != INLINE_DEPTH_TLS &&
        INLINE_DEPTH_TLS < _runGraphParams.getMaxInline())
    {
        return true;
    }
    return false;
}

int64_t NaviWorkerBase::getIdleThreadCount() const {
    return _threadPool->getIdleThreadCount();
}

int64_t NaviWorkerBase::getRunningThreadCount() const {
    return _threadPool->getRunningThreadCount();
}

size_t NaviWorkerBase::getQueueSize() const {
    return _threadPool->getQueueSize();
}

void NaviWorkerBase::inlineBegin() const {
    if (INVALID_INLINE_DEPTH != INLINE_DEPTH_TLS) {
        INLINE_DEPTH_TLS++;
    }
}

void NaviWorkerBase::inlineEnd() const {
    if (INLINE_DEPTH_TLS >= 1) {
        INLINE_DEPTH_TLS--;
    }
}

}
