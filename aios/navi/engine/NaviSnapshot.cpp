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
#include "navi/engine/NaviSnapshot.h"

#include "autil/StringUtil.h"
#include "lockless_allocator/LocklessApi.h"
#include "navi/engine/NaviSession.h"
#include "navi/engine/NaviSnapshotStat.h"
#include "navi/engine/NaviStreamSession.h"
#include "navi/engine/NaviThreadPool.h"
#include "navi/engine/NaviSnapshotSummary.h"
#include "navi/engine/EvTimer.h"
#include "navi/ops/ResourceData.h"
#include "navi/perf/NaviSymbolTable.h"
#include "navi/resource/GigClientR.h"
#include <multi_call/interface/QuerySession.h>

namespace navi {

NaviSnapshot::NaviSnapshot(const std::string &naviName)
    : _naviName(naviName)
    , _testMode(TM_NOT_TEST)
    , _gigRpcServer(nullptr)
{
}

NaviSnapshot::~NaviSnapshot() {
    stop();
    if (_logManager) {
        _logManager->flush(true);
    }
    _logger.logger.reset();
}

bool NaviSnapshot::init(const std::string &initConfigPath,
                        InstanceId instanceId,
                        multi_call::GigRpcServer *gigRpcServer,
                        const ModuleManagerPtr &moduleManager,
                        const NaviSnapshotPtr &oldSnapshot,
                        const NaviConfigPtr &config,
                        const ResourceMap &rootResourceMap)
{
    _config = config;
    _gigRpcServer = gigRpcServer;
    if (!initLogger(instanceId, oldSnapshot)) {
        return false;
    }
    NAVI_LOG(INFO, "config [%s]", _config->configStr.c_str());
    NaviLoggerScope scope(_logger);
    if (!initTaskQueue()) {
        return false;
    }
    if (!initNaviPerf()) {
        NAVI_LOG(WARN, "navi perf init failed, disabled");
    }
    if (!initSymbolTable()) {
        return false;
    }
    if (!initTimerThread()) {
        return false;
    }
    if (!initBizManager(moduleManager, gigRpcServer, oldSnapshot, rootResourceMap)) {
        NAVI_LOG(ERROR, "init biz manager failed");
        return false;
    }
    NAVI_LOG(INFO, "snapshot start success");
    return true;
}

bool NaviSnapshot::start(const NaviSnapshotPtr &oldSnapshot) {
    BizManagerPtr oldBizManager;
    if (oldSnapshot) {
        oldBizManager = oldSnapshot->_bizManager;
    }
    if (!_bizManager->start(_gigRpcServer, oldBizManager.get())) {
        return false;
    }
    if (_naviRpcServer && !_naviRpcServer->flushRegistry()) {
        return false;
    }
    return true;
}

void NaviSnapshot::initMetrics(kmonitor::MetricsReporter &reporter) {
    _metricsReporter = reporter.getSubReporter("", {});
}

bool NaviSnapshot::initLogger(InstanceId instanceId,
                              const NaviSnapshotPtr &oldSnapshot)
{
    std::string oldConfigStr;
    if (oldSnapshot) {
        oldConfigStr = FastToJsonString(oldSnapshot->_config->logConfig);
    }
    auto newConfigStr = FastToJsonString(_config->logConfig);
    if (newConfigStr == oldConfigStr) {
        _logManager = oldSnapshot->_logManager;
    } else {
        _logManager.reset(new NaviLogManager(instanceId));
        if (!_logManager->init(_config->logConfig)) {
            return false;
        }
    }
    _logger = getObjectLogger(SessionId());
    return true;
}

bool NaviSnapshot::initSingleTaskQueue(const std::string &name,
                                       const ConcurrencyConfig &config,
                                       TaskQueueUPtr &taskQueue) {
    NAVI_LOG(INFO, "init with name[%s] config[%s]", name.c_str(), ToJsonString(config).c_str());
    taskQueue = std::make_unique<TaskQueue>();
    taskQueue->name = name;
    auto threadPool = std::make_shared<NaviThreadPool>();
    if (!threadPool->start(config, _logger.logger, name == "" ? "navi_worker" : "nvworker_" + name)) {
        NAVI_LOG(ERROR, "extra thread pool [%s] start failed, config[%s]",
                 name.c_str(), ToJsonString(config).c_str());
        return false;
    }
    taskQueue->threadPool = threadPool;
    atomic_set(&taskQueue->processingCount, 0);
    taskQueue->processingMax = config.processingSize;
    taskQueue->scheduleQueueMax = config.queueSize;
    return true;
}

bool NaviSnapshot::initTaskQueue() {
    if (!initSingleTaskQueue("", _config->engineConfig.builtinTaskQueue, _defaultTaskQueue)) {
        NAVI_LOG(ERROR, "init default task queue failed");
        return false;
    }
    for (auto &pair : _config->engineConfig.extraTaskQueueMap) {
        if (pair.first.empty()) {
            NAVI_LOG(ERROR, "config error, empty extra queue name is not allowed");
            return false;
        }
        TaskQueueUPtr entry;
        if (!initSingleTaskQueue(pair.first, pair.second, entry)) {
            NAVI_LOG(ERROR, "init task queue[%s] failed", pair.first.c_str());
            return false;
        }
        _extraTaskQueueMap.emplace(pair.first, std::move(entry));
    }
    NAVI_LOG(INFO, "extra task queue map after init [%s]", autil::StringUtil::toString(_extraTaskQueueMap).c_str());
    ConcurrencyConfig destructConfig;
    destructConfig.threadNum = 10;
    destructConfig.queueSize = 1000;
    _destructThreadPool = std::make_shared<NaviThreadPool>();
    return _destructThreadPool->start(destructConfig, _logger.logger,
            "navi_destruct");
}

bool NaviSnapshot::initNaviPerf() {
    if (_testMode != TM_NOT_TEST) {
        return true;
    }
    if (_config->engineConfig.disablePerf) {
        return true;
    }
    const auto &pidVec = _defaultTaskQueue->threadPool->getPidVec();
    for (auto pid : pidVec) {
        _naviPerf.addPid(pid);
    }
    for (const auto &pair : _extraTaskQueueMap) {
        const auto &extraPidVec = pair.second->threadPool->getPidVec();
        for (auto pid : extraPidVec) {
            _naviPerf.addPid(pid);
        }
    }
    const auto &destructPidVec = _destructThreadPool->getPidVec();
    for (auto pid : destructPidVec) {
        _naviPerf.addPid(pid);
    }
    if (!_naviPerf.init(_logger.logger)) {
        return false;
    }
    return true;
}

bool NaviSnapshot::initSymbolTable() {
    if (_testMode != TM_NOT_TEST) {
        return true;
    }
    if (_config->engineConfig.disableSymbolTable) {
        return true;
    }
    _naviSymbolTable = std::make_shared<NaviSymbolTable>();
    if (!_naviSymbolTable->init(_logger.logger)) {
        _naviSymbolTable.reset();
    }
    _logManager->setNaviSymbolTable(_naviSymbolTable.get());
    return true;
}

bool NaviSnapshot::initTimerThread() {
    _evTimer = std::make_shared<EvTimer>("navi_timeout");
    if (!_evTimer->start()) {
        NAVI_LOG(ERROR, "start timer thread failed");
        return false;
    }
    return true;
}

bool NaviSnapshot::initBizManager(const ModuleManagerPtr &moduleManager,
                                  multi_call::GigRpcServer *gigRpcServer,
                                  const NaviSnapshotPtr &oldSnapshot,
                                  const ResourceMap &rootResourceMapIn)
{
    ResourceMap rootResourceMap;
    rootResourceMap.addResource(rootResourceMapIn);

    BizManagerPtr oldBizManager;
    if (oldSnapshot) {
        oldBizManager = oldSnapshot->_bizManager;
    }
    ResourceMap resourceMap(rootResourceMap.getMap());
    addBuildinRootResource(gigRpcServer, resourceMap);
    _bizManager = std::make_shared<BizManager>(_naviName, moduleManager);
    _bizManager->setTestMode(_testMode);
    auto ret = _bizManager->preInit(_logger.logger, oldBizManager.get(),
                                    *_config, resourceMap);
    logSummary("registry");
    if (!ret) {
        return false;
    }
    if (!initBizResource(resourceMap)) {
        return false;
    }
    if (!_bizManager->postInit()) {
        return false;
    }
    return true;
}

void NaviSnapshot::addBuildinRootResource(
    multi_call::GigRpcServer *gigRpcServer, ResourceMap &resourceMap)
{
    if (gigRpcServer) {
        _naviRpcServer = std::make_shared<NaviRpcServerR>(this, gigRpcServer);
        resourceMap.addResource(_naviRpcServer);
    }
}

bool NaviSnapshot::initBizResource(const ResourceMap &rootResourceMap) {
    auto graphDef = _bizManager->cloneResourceGraph();
    if (!graphDef) {
        NAVI_LOG(WARN, "empty init resource");
        return true;
    }
    NAVI_LOG(INFO, "root resource map [%s]", autil::StringUtil::toString(rootResourceMap).c_str());
    RunGraphParams params;
    params.setResourceStage(RS_SNAPSHOT);
    auto result = runGraph(graphDef, params, &rootResourceMap);
    if (!result) {
        NAVI_LOG(ERROR, "run resource graph failed");
        return false;
    }
    _initResult = result;
    _collectThread = autil::Thread::createThread(std::bind(&NaviSnapshot::collectInitResource, this), "navi_collect");
    if (!_collectThread) {
        NAVI_LOG(ERROR, "create collect thread failed");
        return false;
    }
    if (!_collectWaiter.wait(60 * 1000 * 1000)) {
        NAVI_LOG(ERROR, "wait collect resource failed");
        return false;
    }
    return true;
}

void NaviSnapshot::collectInitResource() {
    ResourceMap resourceMap;
    collectResultResource(_initResult, resourceMap, &_collectWaiter);
}

void InitWaiter::setExpectCount(size_t expectCount) {
    autil::ScopedLock lock(_collectCond);
    _expectCount = expectCount;
}

bool InitWaiter::wait(int64_t timeoutUs) {
    auto begin = autil::TimeUtility::currentTime();
    while (true) {
        autil::ScopedLock lock(_collectCond);
        auto result = (_expectCount > 0 && _expectCount <= _readyIndexSet.size());
        if (result) {
            return true;
        }
        if (_stopped) {
            return result;
        }
        if (autil::TimeUtility::currentTime() - begin > timeoutUs) {
            NAVI_KERNEL_LOG(ERROR, "wait timeout, timeoutUs: %ld", timeoutUs);
            break;
        }
        _collectCond.wait(timeoutUs);
    }
    return false;
}

void InitWaiter::notify(const NaviUserData &data) {
    auto index = data.index;
    autil::ScopedLock lock(_collectCond);
    _readyIndexSet.insert(index);
    if (_readyIndexSet.size() >= _expectCount) {
        _collectCond.signal();
    }
}

void InitWaiter::stop() {
    autil::ScopedLock lock(_collectCond);
    _stopped = true;
    _collectCond.signal();
}

bool NaviSnapshot::collectResultResource(const NaviUserResultPtr &result,
        ResourceMap &resourceMap, InitWaiter *waiter)
{
    if (!result) {
        NAVI_LOG(ERROR, "run resource graph failed");
        return false;
    }
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (result->nextData(data, eof)) {
            if (data.data) {
                auto resourceData =
                    std::dynamic_pointer_cast<ResourceData>(data.data);
                bool success = false;
                if (resourceData) {
                    if (resourceData->_resource) {
                        NAVI_LOG(SCHEDULE1, "create resource success [%s] [%p]",
                            data.name.c_str(), resourceData->_resource.get());
                        resourceMap.addResource(resourceData->_resource);
                        success = true;
                    } else if (!resourceData->_require) {
                        success = true;
                    } else if (resourceData->_wait) {
                        NAVI_LOG(DEBUG, "need wait for resource creation [%s]", resourceData->_name.c_str());
                        success = true;
                    }
                }
                if (!success) {
                    NAVI_LOG(ERROR, "create resource failed [%s]",
                            data.name.c_str());
                    result->abort();
                    if (waiter) {
                        waiter->stop();
                    }
                    return false;
                }
            }
            if (waiter) {
                waiter->setExpectCount(result->outputCount());
                waiter->notify(data);
            }
        }
        if (eof) {
            break;
        }
    }
    if (waiter) {
        waiter->stop();
    }
    return EC_NONE == result->getNaviResult()->ec;
}

NaviObjectLogger NaviSnapshot::getObjectLogger(SessionId id) {
    auto logger = getTlsLogger();
    if (!logger) {
        logger = _logManager->createLogger(id);
    }
    return NaviObjectLogger(this, "snapshot", logger);
}

const NaviLoggerPtr &NaviSnapshot::getLogger() const {
    return _logger.logger;
}

NaviLoggerPtr NaviSnapshot::getTlsLogger() {
    if (NAVI_TLS_LOGGER && NAVI_TLS_LOGGER->logger) {
        return NAVI_TLS_LOGGER->logger;
    } else {
        return nullptr;
    }
}

ModuleManagerPtr NaviSnapshot::getModuleManager() const {
    if (_bizManager) {
        return _bizManager->getModuleManager();
    }
    return nullptr;
}

void NaviSnapshot::cleanupModule() {
    if (_bizManager) {
        return _bizManager->cleanupModule();
    }
}

void NaviSnapshot::stopSingleTaskQueue(TaskQueue *taskQueue) {
    size_t count = 0;
    while (!taskQueue->sessionScheduleQueue.Empty()) {
        if (count++ % 200 == 0) {
            NAVI_LOG(INFO,
                     "taskQueue[%s] session schedule queue not empty, size[%lu], loop[%lu], waiting...",
                     taskQueue->name.c_str(),
                     taskQueue->sessionScheduleQueue.Size(),
                     count);
        }
        usleep(1 * 1000);
    }
    auto threadPool = std::move(taskQueue->threadPool);
    if (threadPool) {
        CommonUtil::waitUseCount(threadPool, 1);
        threadPool->stop();
        threadPool.reset();
    }
    assert(atomic_read(&taskQueue->processingCount) == 0 && "processing count must be zero");
}

void NaviSnapshot::stop() {
    if (_naviRpcServer) {
        _naviRpcServer->stop();
    }
    if (_initResult) {
        _initResult->abort(EC_NONE);
    }
    _collectThread.reset();
    NaviLoggerScope scope(_logger);
    _naviPerf.stop();
    stopSingleTaskQueue(_defaultTaskQueue.get());
    for (auto &pair : _extraTaskQueueMap) {
        stopSingleTaskQueue(pair.second.get());
    }
    auto destructThreadPool = std::move(_destructThreadPool);
    if (destructThreadPool) {
        CommonUtil::waitUseCount(destructThreadPool, 1);
        destructThreadPool->stop();
        destructThreadPool.reset();
    }
    _bizManager.reset();
    NAVI_LOG(INFO, "snapshot stopped");
}

const std::shared_ptr<NaviThreadPool> &NaviSnapshot::getDestructThreadPool() const {
    return _destructThreadPool;
}

void NaviSnapshot::setTestMode(TestMode testMode) {
    _testMode = testMode;
}

NaviUserResultPtr NaviSnapshot::runGraph(GraphDef *graphDef,
        const RunGraphParams &params,
        const ResourceMap *resourceMap)
{
    NaviUserResultPtr naviUserResult;
    if (!runGraphImpl(graphDef, params, resourceMap, naviUserResult, nullptr)) {
        NAVI_KERNEL_LOG(WARN, "run graph inner failed");
        return nullptr;
    }
    return naviUserResult;
}


void NaviSnapshot::runGraphAsync(GraphDef *graphDef,
                                 const RunGraphParams &params,
                                 const ResourceMap *resourceMap,
                                 NaviUserResultClosure *closure)
{
    NaviUserResultPtr naviUserResult;
    if (!runGraphImpl(graphDef, params, resourceMap, naviUserResult, closure)) {
        NAVI_KERNEL_LOG(WARN, "run graph inner failed");
        closure->run(nullptr);
        return;
    }
}

void NaviSnapshot::onSessionDestruct(TaskQueue *taskQueue) {
    assert(taskQueue && "task queue entry must not be nullptr");
    NAVI_KERNEL_LOG(SCHEDULE1,
                    "session destruct, queue size[%lu] processing count[%lld]",
                    taskQueue->sessionScheduleQueue.Size(),
                    atomic_read(&taskQueue->processingCount));
    NaviWorkerBase *session = nullptr;
    if (taskQueue->sessionScheduleQueue.Pop(&session)) {
        auto logger = session->getLogger();
        NaviLoggerScope scope(logger);
        NAVI_KERNEL_LOG(SCHEDULE1, "pop session from schedule queue. queue size[%lu]", taskQueue->sessionScheduleQueue.Size());
        session->initSchedule(false, this);
    } else {
        atomic_dec(&taskQueue->processingCount);
        assert(atomic_read(&taskQueue->processingCount) >= 0 && "processing count must not be negative");
        doSchedule(taskQueue);
    }
}

void NaviSnapshot::doSchedule(TaskQueue *taskQueue) {
    NaviWorkerBase *session = nullptr;
    if (atomic_read(&taskQueue->processingCount) < taskQueue->processingMax && taskQueue->sessionScheduleQueue.Pop(&session)) {
        auto logger = session->getLogger();
        NaviLoggerScope scope(logger);
        NAVI_KERNEL_LOG(SCHEDULE1, "pop session from schedule queue. queue size[%lu]",
                        taskQueue->sessionScheduleQueue.Size());
        atomic_inc(&taskQueue->processingCount);
        session->initSchedule(false, this);
    }
}

TaskQueue *NaviSnapshot::getTaskQueue(const std::string &taskQueueName) {
    auto iter = _extraTaskQueueMap.find(taskQueueName);
    if (iter != _extraTaskQueueMap.end()) {
        return iter->second.get();
    }
    return _defaultTaskQueue.get();
}

bool NaviSnapshot::runGraphImpl(GraphDef *graphDef,
                                const RunGraphParams &params,
                                const ResourceMap *resourceMap,
                                NaviUserResultPtr &naviUserResult,
                                NaviUserResultClosure *closure) {
    if (!graphDef) {
        return false;
    }
    auto *taskQueue = getTaskQueue(params.getTaskQueueName());
    auto logger = getObjectLogger(SessionId());
    NaviLoggerScope scope(logger);
    NAVI_KERNEL_LOG(SCHEDULE1,
                    "snapshot run graph, queue size[%lu] processing count[%lld]",
                    taskQueue->sessionScheduleQueue.Size(),
                    atomic_read(&taskQueue->processingCount));
    size_t queueSize = taskQueue->sessionScheduleQueue.Size();
    if (TM_NOT_TEST == _testMode && queueSize >= taskQueue->scheduleQueueMax) {
        DELETE_AND_SET_NULL(graphDef);
        NAVI_KERNEL_LOG(WARN, "session schedule queue full, limit [%lu], dropped",
                        taskQueue->scheduleQueueMax);
        REPORT_USER_MUTABLE_QPS(_metricsReporter, "dropQps");
        return false;
    }

    auto runStartTime = autil::TimeUtility::currentTime();
    NaviSession *session = nullptr;
    {
        DisablePoolScope disableScope;
        session = new NaviSession(logger.logger, taskQueue, _bizManager.get(), graphDef, resourceMap, closure);
    }
    session->setRunStartTime(runStartTime);

    naviUserResult = session->getUserResult();
    if (_metricsReporter) {
        session->setMetricsReporter(*_metricsReporter);
    }
    if (!session->init(params, nullptr)) {
        DELETE_AND_SET_NULL(session);
        NAVI_KERNEL_LOG(WARN, "session init failed, dropped");
        REPORT_USER_MUTABLE_QPS(_metricsReporter, "initFailedQps");
        return false;
    }
    initSnapshotResource(session);
    if (_testMode != TM_NOT_TEST) {
        atomic_inc(&taskQueue->processingCount);
        session->initSchedule(true, this);
    } else if (queueSize == 0 && atomic_read(&taskQueue->processingCount) < taskQueue->processingMax) {
        NAVI_KERNEL_LOG(SCHEDULE1,
                        "schedule session directly, queue size[%lu] processing count[%lld]",
                        taskQueue->sessionScheduleQueue.Size(),
                        atomic_read(&taskQueue->processingCount));
        atomic_inc(&taskQueue->processingCount);
        session->initSchedule(false, this);
    } else {
        NAVI_KERNEL_LOG(SCHEDULE1,
                        "push session to schedule queue, queue size[%lu] processing count[%lld]",
                        taskQueue->sessionScheduleQueue.Size(),
                        atomic_read(&taskQueue->processingCount));
        taskQueue->sessionScheduleQueue.Push(session);
        doSchedule(taskQueue);
    }
    return true;
}

bool NaviSnapshot::runGraph(
        NaviMessage *request,
        const ArenaPtr &arena,
        const RunParams &pbParams,
        const std::shared_ptr<multi_call::GigStreamBase> &stream,
        NaviLoggerPtr &logger)
{
    auto *taskQueue = getTaskQueue(pbParams.task_queue_name());
    SessionId sessionId;
    sessionId.instance = pbParams.id().instance();
    sessionId.queryId = pbParams.id().query_id();
    auto objectLogger = getObjectLogger(sessionId);
    logger = objectLogger.logger;
    NaviLoggerScope scope(objectLogger);
    // TODO: add schedule queue
    auto processingCount = atomic_read(&taskQueue->processingCount);
    if (processingCount > taskQueue->processingMax) {
        NAVI_KERNEL_LOG(WARN, "processing count too much, dropped, current [%lld] limit [%lu]",
                        processingCount, taskQueue->processingMax);
        REPORT_USER_MUTABLE_QPS(_metricsReporter, "dropQps");
        return false;
    }

    auto runStartTime = autil::TimeUtility::currentTime();
    NaviStreamSession *session = nullptr;
    {
        DisablePoolScope disableScope;
        session = new NaviStreamSession(objectLogger.logger, taskQueue, _bizManager.get(), request, arena, stream);
    }
    session->setRunStartTime(runStartTime);
    RunGraphParams params;
    if (!parseRunParams(pbParams, params)) {
        DELETE_AND_SET_NULL(session);
        return false;
    }
    params.setAsyncIo(stream->isAsyncMode());
    if (!session->init(params, stream->getQueryInfo())) {
        DELETE_AND_SET_NULL(session);
        NAVI_KERNEL_LOG(WARN, "stream session init failed, dropped");
        REPORT_USER_MUTABLE_QPS(_metricsReporter, "initStreamFailedQps");
        return false;
    }
    if (_metricsReporter) {
        session->setMetricsReporter(*_metricsReporter);
    }
    initSnapshotResource(session);

    NAVI_KERNEL_LOG(SCHEDULE1,
                    "schedule session directly, processing count[%lld]",
                    processingCount);
    atomic_inc(&taskQueue->processingCount);
    session->initSchedule(true, this);

    return true;
}

bool NaviSnapshot::parseRunParams(const RunParams &pbParams,
                                  RunGraphParams &params)
{
    const auto &creatorManager = _bizManager->getBuildinCreatorManager();
    return RunGraphParams::fromProto(pbParams, creatorManager.get(), params);
}

void NaviSnapshot::initSnapshotResource(NaviWorkerBase *session) {
    session->setDestructThreadPool(_destructThreadPool);
    session->setNaviPerf(&_naviPerf);
    session->setNaviSymbolTable(_naviSymbolTable);
    session->setEvTimer(_evTimer.get());
}

bool NaviSnapshot::createResource(const std::string &bizName,
                                  NaviPartId partCount,
                                  NaviPartId partId,
                                  const std::set<std::string> &resources,
                                  ResourceMap &resourceMap)
{
    if (!_bizManager) {
        return false;
    }
    auto resourceSet = resources;
    if (resourceSet.empty()) {
        return true;
    }
    auto graphDef = _bizManager->createResourceGraph(bizName, partCount, partId,
                                                     resourceSet);
    RunGraphParams params;
    auto result = runGraph(graphDef, params, nullptr);
    if (!collectResultResource(result, resourceMap, nullptr)) {
        return false;
    }
    return true;
}

void NaviSnapshot::getTaskQueueStat(const TaskQueue *taskQueue, NaviSnapshotStat &stat) const
{
    stat.activeThreadCount = taskQueue->threadPool->getRunningThreadCount();
    stat.activeThreadQueueSize = taskQueue->threadPool->getQueueSize();
    stat.processingCount = atomic_read(&taskQueue->processingCount);
    stat.queueCount = taskQueue->sessionScheduleQueue.Size();
    stat.processingCountRatio =
        taskQueue->processingMax > 0 ? stat.processingCount * 100 / taskQueue->processingMax : 0;
    stat.queueCountRatio =
        taskQueue->scheduleQueueMax > 0 ? stat.queueCount * 100 / taskQueue->scheduleQueueMax : 0;
}

void NaviSnapshot::reportStat(kmonitor::MetricsReporter &reporter) {
    NaviSnapshotStat stat;
    getTaskQueueStat(_defaultTaskQueue.get(), stat);
    {
        kmonitor::MetricsTags tag{"name", "builtin"};
        _metricsReporter->report<NaviSnapshotStatMetrics>(&tag, &stat);
    }
    for (auto &pair : _extraTaskQueueMap) {
        getTaskQueueStat(pair.second.get(), stat);
        kmonitor::MetricsTags tag{"name", pair.first};
        _metricsReporter->report<NaviSnapshotStatMetrics>(&tag, &stat);
    }
}

void NaviSnapshot::logSummary(const std::string &stage) const {
    NaviSnapshotSummary summary;
    summary.naviName = _naviName;
    summary.stage = stage;
    summary.config = _config.get();
    if (_bizManager) {
        _bizManager->fillSummary(summary);
    }
    NaviSnapshotSummary::logSummary(summary);
}

}
