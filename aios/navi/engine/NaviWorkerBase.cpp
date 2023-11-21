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
#include "navi/engine/ComputeScope.h"
#include "navi/engine/TaskQueue.h"
#include "navi/log/TraceAppender.h"
#include "navi/resource/GigClientR.h"
#include "navi/util/CommonUtil.h"
#include "navi/util/NaviClosure.h"

namespace navi {

thread_local extern size_t current_thread_counter;
thread_local extern size_t current_thread_wait_counter;

void NaviWorkerItem::process() {
    if (!_worker->isStopped()) {
        auto collectPerf = _collectPerf;
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
    delete this;
    worker->dec();
}

void NaviWorkerItem::drop() {
    delete this;
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
    delete this;
    worker->dec();
}

void NaviNoDropWorkerItem::drop() {
    setScheduleInfo(CommonUtil::getTimelineTimeNs(),
                    _worker->getRunningThreadCount(), current_thread_id,
                    current_thread_counter, current_thread_wait_counter);
    doWork();
    delete this;
}

void NaviNoDropWorkerItem::doWork() {
    if (!_done) {
        auto collectPerf = _collectPerf;
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

void NaviWorkerBase::WorkerInitItem::doProcess()  {
    ComputeScope scope(_worker->_metric.get(), GET_GRAPH_INIT, 0, _worker,
                       _schedInfo, _worker->_collectMetric, _collectPerf);
    _worker->run();
}

NaviWorkerBase::NaviWorkerBase(
        TaskQueue *taskQueue,
        BizManager *bizManager,
        NaviUserResultClosure *closure)
    : _logger(this, "session")
    , _metricsReporter(nullptr)
    , _bizManager(bizManager)
    , _finish(false)
    , _testMode(TM_NONE)
    , _initSuccess(false)
    , _threadPool(taskQueue->getThreadPool())
    , _taskQueue(taskQueue)
    , _naviPerf(nullptr)
    , _evTimer(nullptr)
    , _hostInfo(nullptr)
    , _threadLimit(DEFAULT_THREAD_LIMIT)
    , _maxInline(DEFAULT_MAX_INLINE)
    , _collectPerf(false)
    , _metric(std::make_shared<KernelMetric>(INVALID_GRAPH_ID, "navi.graph_init", ""))
{
    _metricsCollector.sessionBeginTime = autil::TimeUtility::currentTime();

    if (closure) {
        _userResult = std::make_shared<NaviAsyncUserResult>(closure);
    } else {
        _userResult = std::make_shared<NaviSyncUserResult>();
    }
    _userResult->setWorker(this);
    initCounters();
    NAVI_LOG(SCHEDULE1, "construct");
}

NaviWorkerBase::~NaviWorkerBase() {
    if (_metricsReporter) {
        fillResultMetrics();
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

void NaviWorkerBase::setNewSessionLatency(int64_t newSessionLatency) {
    _metricsCollector.newSessionLatency = newSessionLatency;
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
    _naviSymbolTable = naviSymbolTable;
}

const NaviSymbolTablePtr &NaviWorkerBase::getNaviSymbolTable() const {
    return _naviSymbolTable;
}

void NaviWorkerBase::setEvTimer(EvTimer *evTimer) {
    _evTimer = evTimer;
}

EvTimer *NaviWorkerBase::getEvTimer() const {
    return _evTimer;
}

bool NaviWorkerBase::enablePerf() {
    return _naviPerf->enable(current_thread_id);
}

bool NaviWorkerBase::disablePerf() {
    return _naviPerf->disable(current_thread_id);
}

void NaviWorkerBase::beginSample() {
    _naviPerf->beginSample(current_thread_id);
}

NaviPerfResultPtr NaviWorkerBase::endSample() {
    return _naviPerf->endSample(current_thread_id);
}

ScheduleInfo NaviWorkerBase::makeInlineSchedInfo() {
    auto time = CommonUtil::getTimelineTimeNs();
    auto threadId = current_thread_id;
    ScheduleInfo schedInfo;
    schedInfo.enqueueTime = time;
    schedInfo.dequeueTime = time;
    schedInfo.schedTid = threadId;
    schedInfo.signalTid = threadId;
    schedInfo.processTid = threadId;
    schedInfo.runningThread = getRunningThreadCount();
    schedInfo.threadCounter = current_thread_counter;
    schedInfo.threadWaitCounter = current_thread_wait_counter;
    schedInfo.queueSize = getQueueSize();
    return schedInfo;
}

bool NaviWorkerBase::init(
    const RunGraphParams &runParams,
    const std::shared_ptr<multi_call::QueryInfo> &queryInfo)
{
    _collectMetric = runParams.collectMetric();
    _collectPerf = runParams.collectPerf();
    auto schedInfo = makeInlineSchedInfo();
    ComputeScope scope(_metric.get(), GET_GRAPH_INIT_PREPARE, 0, this,
                       schedInfo, _collectMetric, _collectPerf);
    if (!_logger.logger->hasTraceAppender()) {
        const auto &traceLevel = runParams.getTraceLevelStr();
        auto traceAppender = std::make_shared<TraceAppender>(traceLevel);
        runParams.fillFilterParam(traceAppender.get());
        _logger.logger->setTraceAppender(traceAppender);
    }
    _threadLimit = runParams.getThreadLimit();
    _maxInline = runParams.getMaxInline();
    auto param = new GraphParam();
    auto sessionId = _logger.logger->getLoggerId();
    if (!param->init(sessionId, this, _logger.logger, runParams,
                     _naviSymbolTable, _hostInfo))
    {
        delete param;
        return false;
    }
    if (_collectMetric || _collectPerf) {
        param->graphResult->getGraphMetric()->addKernelMetric(NAVI_BUILDIN_BIZ,
                                                              _metric);
    }
    param->querySession = tryCreateQuerySession(runParams.getQuerySession(), queryInfo);
    _userResult->setGraphResult(param->graphResult);
    return doInit(param);
}

std::shared_ptr<multi_call::QuerySession> NaviWorkerBase::tryCreateQuerySession(
        const std::shared_ptr<multi_call::QuerySession> &originQuerySession,
        const std::shared_ptr<multi_call::QueryInfo> &queryInfo)
{
    auto resource = _bizManager->getSnapshotResource(GIG_CLIENT_RESOURCE_ID);
    if (!resource) {
        NAVI_LOG(SCHEDULE1, "get gig client resource failed, skip");
        return nullptr;
    }
    auto gigResource = dynamic_cast<GigClientR *>(resource.get());
    assert(gigResource != nullptr && "gig resource type error");
    auto searchService = gigResource->getSearchService();
    if (!searchService) {
        NAVI_LOG(SCHEDULE1, "get search service failed, skip");
        return nullptr;
    }
    if (originQuerySession) {
        auto querySession = multi_call::QuerySession::createWithOrigin(searchService, originQuerySession);
        NAVI_LOG(SCHEDULE1,
                 "init query session with query session, eTraceId[%s]",
                 querySession->getTraceServerSpan()
                     ? opentelemetry::EagleeyeUtil::getETraceId(querySession->getTraceServerSpan()).c_str()
                     : "");
        return querySession;
    } else {
        auto querySession = std::make_shared<multi_call::QuerySession>(searchService, queryInfo);
        NAVI_LOG(SCHEDULE1,
                 "init query session with query info[%s], eTraceId[%s]",
                 queryInfo ? queryInfo->toString().c_str() : "null",
                 querySession->getTraceServerSpan()
                     ? opentelemetry::EagleeyeUtil::getETraceId(querySession->getTraceServerSpan()).c_str()
                     : "");
        return querySession;
    }
}

void NaviWorkerBase::initCounters() {
    atomic_set(&_itemCount, 0);
    atomic_set(&_processingCount, 0);
}

void NaviWorkerBase::initSchedule() {
    NAVI_LOG(SCHEDULE1, "init schedule with sync mode");
    _metricsCollector.initScheduleTime = autil::TimeUtility::currentTime();

    if (_threadPool->incWorkerCount()) {
        _initSuccess = true;
        auto item = new WorkerInitItem(this, _collectPerf);
        item->setSyncMode(true);
        schedule(item);
    } else {
        drop();
    }
}

bool NaviWorkerBase::schedule(NaviWorkerItem *item, bool force) {
    if (!force && isStopped()) {
        incItemCount();
        drop();
        item->drop();
        clear();
        decItemCount();
        return false;
    }
    incItemCount();
    if (force || atomic_read(&_processingCount) < _threadLimit) {
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
    if (atomic_read(&_processingCount) < _threadLimit &&
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

const NaviThreadPoolPtr &NaviWorkerBase::getDestructThreadPool() const {
    return _destructThreadPool;
}

void NaviWorkerBase::setFinish() {
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
        const auto &naviResult = _userResult->getNaviResultWithoutCheck();
        if (naviResult) {
            _metricsCollector.totalQueueLatency = naviResult->queueUs / 1000;
            _metricsCollector.totalComputeLatency =
                naviResult->computeUs / 1000;
            _metricsCollector.hasError = (nullptr != naviResult->error);
            auto rpcInfoMap = naviResult->getRpcInfoMap();
            if (rpcInfoMap) {
                _metricsCollector.hasLack = reportRpcLackByBiz(*rpcInfoMap);
            }
        }
    }
}

const NaviUserResultPtr &NaviWorkerBase::getUserResult() const {
    return _userResult;
}

void NaviWorkerBase::notifyFinish() {
    NAVI_LOG(SCHEDULE1, "worker finish");
    _userResult->notify(true);
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
        NAVI_LOG(SCHEDULE1, "item count clear");
        _taskQueue->scheduleNext();
        auto item = new WorkerDestructItem(this);
        _destructThreadPool->push(item);
    }
}

int64_t NaviWorkerBase::itemCount() {
    return atomic_read(&_itemCount);
}

bool NaviWorkerBase::canInline() const {
    if (INVALID_INLINE_DEPTH != INLINE_DEPTH_TLS &&
        INLINE_DEPTH_TLS < _maxInline)
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
