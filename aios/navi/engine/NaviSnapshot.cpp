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
#include "multi_call/interface/QuerySession.h"
#include "navi/engine/EvTimer.h"
#include "navi/engine/NaviSession.h"
#include "navi/engine/NaviSnapshotSummary.h"
#include "navi/engine/NaviStreamSession.h"
#include "navi/engine/NaviThreadPool.h"
#include "navi/engine/TaskQueue.h"
#include "navi/engine/NaviHostInfo.h"
#include "navi/ops/ResourceData.h"
#include "navi/perf/NaviSymbolTable.h"
#include "navi/resource/GigClientR.h"

namespace navi {

template <typename T, typename... U>
static int64_t timerAllocAndNewObject(T *&obj, U &&...args) {
    autil::ScopedTime2 timer;
    obj = (T *)operator new(sizeof(T));
    int64_t latency = timer.done_us();
    new (obj) T(std::forward<U>(args)...);
    return latency;
}

class NaviSessionScheduleItem : public TaskQueueScheduleItemBase {
public:
    NaviSessionScheduleItem(NaviSession *session) : _session(session) {}
    ~NaviSessionScheduleItem() {}

public:
    void process() override {
        if (_session) {
            auto logger = _session->getLogger();
            NaviLoggerScope scope(logger);
            _session->initSchedule();
        }
        _session = nullptr;
    }
    void destroy() override {
        DELETE_AND_SET_NULL(_session);
        delete this;
    }

private:
    NaviSession *_session = nullptr;
};

NaviSnapshot::NaviSnapshot(const NaviHostInfoPtr &hostInfo)
    : _hostInfo(hostInfo)
    , _testMode(TM_NONE)
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
                                       std::unique_ptr<TaskQueue> &taskQueue)
{
    taskQueue = std::make_unique<TaskQueue>();
    if (!taskQueue->init(name, config, getTestMode())) {
        return false;
    }
    return true;
}

bool NaviSnapshot::initTaskQueue() {
    if (!initSingleTaskQueue("", _config->engineConfig.builtinTaskQueue,
                             _defaultTaskQueue))
    {
        NAVI_LOG(ERROR, "init default task queue failed");
        return false;
    }
    for (auto &pair : _config->engineConfig.extraTaskQueueMap) {
        if (pair.first.empty()) {
            NAVI_LOG(ERROR, "config error, empty extra queue name is not allowed");
            return false;
        }
        std::unique_ptr<TaskQueue> entry;
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
    if (_testMode != TM_NONE) {
        return true;
    }
    if (_config->engineConfig.disablePerf) {
        return true;
    }
    const auto &pidVec = _defaultTaskQueue->getThreadPool()->getPidVec();
    for (auto pid : pidVec) {
        _naviPerf.addPid(pid);
    }
    for (const auto &pair : _extraTaskQueueMap) {
        const auto &extraPidVec = pair.second->getThreadPool()->getPidVec();
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
    if (_testMode != TM_NONE) {
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
    _bizManager = std::make_shared<BizManager>(_hostInfo->getNaviName(), moduleManager);
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
    params.setCollectMetric(true);
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
    auto naviResult = result->getNaviResult();
    if (!naviResult) {
        NAVI_LOG(ERROR, "get navi result failed");
        return false;
    }
    logVisData(naviResult);
    return !naviResult->hasError();
}

void NaviSnapshot::logVisData(const NaviResultPtr &naviResult) const {
    if (!naviResult) {
        NAVI_LOG(ERROR, "log init vis data failed, null navi result");
        return;
    }
    auto visProto = naviResult->getVisProto();
    if (!visProto) {
        NAVI_LOG(ERROR, "log init vis data failed, null vis data");
        return;
    }
    const auto &naviName = _hostInfo->getNaviName();
    std::string filePrefix;
    if (!naviName.empty()) {
        filePrefix = "./navi." + naviName + ".snapshot";
    } else {
        filePrefix = "./navi.snapshot";
    }
    NaviSnapshotSummary::writeFile(filePrefix + ".vis",
                                   visProto->SerializeAsString());
    NaviSnapshotSummary::writeFile(filePrefix + ".vis.str",
                                   visProto->DebugString());
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
    if (_defaultTaskQueue) {
        _defaultTaskQueue->stop();
    }
    for (auto &pair : _extraTaskQueueMap) {
        pair.second->stop();
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

// push stream session to schedule queue
void NaviSnapshot::runStreamSession(const std::string &taskQueueName,
                                    const SessionId &sessionId,
                                    TaskQueueScheduleItemBase *item,
                                    std::shared_ptr<NaviLogger> &logger)
{

    auto objectLogger = getObjectLogger(sessionId);
    logger = objectLogger.logger;
    NaviLoggerScope scope(objectLogger);

    auto *taskQueue = getTaskQueue(taskQueueName);
    if (!taskQueue->push(item)) { // dropped as schedule queue full
        NAVI_KERNEL_LOG(WARN, "session push failed, dropped");
        REPORT_USER_MUTABLE_QPS(_metricsReporter, "dropQps");
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
    NaviSession *session = nullptr;
    {
        DisablePoolScope disableScope;
        int64_t latency = timerAllocAndNewObject(session, taskQueue, _bizManager.get(), graphDef, resourceMap, closure);
        session->setNewSessionLatency(latency);
    }
    session->setTestMode(getTestMode());

    naviUserResult = session->getUserResult();
    if (_metricsReporter) {
        session->setMetricsReporter(*_metricsReporter);
    }
    fillSnapshotResource(params, session);
    if (!session->init(params, nullptr)) {
        DELETE_AND_SET_NULL(session);
        NAVI_KERNEL_LOG(WARN, "session init failed");
        REPORT_USER_MUTABLE_QPS(_metricsReporter, "initFailedQps");
        return false;
    }
    auto *item = new NaviSessionScheduleItem(session);
    if (!taskQueue->push(item)) {
        NAVI_KERNEL_LOG(WARN, "session push failed, dropped");
        REPORT_USER_MUTABLE_QPS(_metricsReporter, "dropQps");
        return false;
    }
    return true;
}

// run stream session after popped from schedule queue
bool NaviSnapshot::doRunStreamSession(NaviMessage *request,
                                      const ArenaPtr &arena,
                                      const RunParams &pbParams,
                                      const std::shared_ptr<multi_call::GigStreamBase> &stream) {
    auto *taskQueue = getTaskQueue(pbParams.task_queue_name());
    NaviStreamSession *session = nullptr;
    {
        DisablePoolScope disableScope;
        int64_t latency = timerAllocAndNewObject(session, taskQueue, _bizManager.get(), request, arena, stream);
        session->setNewSessionLatency(latency);
    }
    session->setTestMode(getTestMode());
    RunGraphParams params;
    if (!parseRunParams(pbParams, params)) {
        DELETE_AND_SET_NULL(session);
        taskQueue->scheduleNext();
        return false;
    }
    params.setAsyncIo(stream->isAsyncMode());
    fillSnapshotResource(params, session);
    if (!session->init(params, stream->getQueryInfo())) {
        DELETE_AND_SET_NULL(session);
        NAVI_KERNEL_LOG(WARN, "stream session init failed, dropped");
        REPORT_USER_MUTABLE_QPS(_metricsReporter, "initStreamFailedQps");
        taskQueue->scheduleNext();
        return false;
    }
    if (_metricsReporter) {
        session->setMetricsReporter(*_metricsReporter);
    }
    session->initSchedule();
    return true;
}

bool NaviSnapshot::parseRunParams(const RunParams &pbParams,
                                  RunGraphParams &params)
{
    const auto &creatorManager = _bizManager->getBuildinCreatorManager();
    return RunGraphParams::fromProto(pbParams, creatorManager.get(), params);
}

void NaviSnapshot::fillSnapshotResource(const RunGraphParams &params, NaviWorkerBase *session) {
    session->setDestructThreadPool(_destructThreadPool);
    session->setNaviPerf(&_naviPerf);
    session->setNaviSymbolTable(_naviSymbolTable);
    session->setEvTimer(_evTimer.get());
    if (params.needCollect()) {
        session->setHostInfo(_hostInfo.get());
    }
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

void NaviSnapshot::reportStat(kmonitor::MetricsReporter &reporter) {
    {
        auto stat = _defaultTaskQueue->getStat();
        kmonitor::MetricsTags tag{"name", "builtin"};
        _metricsReporter->report<TaskQueueStatMetrics>(&tag, &stat);
    }
    for (auto &pair : _extraTaskQueueMap) {
        auto stat = pair.second->getStat();
        kmonitor::MetricsTags tag{"name", pair.first};
        _metricsReporter->report<TaskQueueStatMetrics>(&tag, &stat);
    }
}

void NaviSnapshot::logSummary(const std::string &stage) const {
    NaviSnapshotSummary summary;
    summary.naviName = _hostInfo->getNaviName();
    summary.stage = stage;
    summary.config = _config.get();
    if (_bizManager) {
        _bizManager->fillSummary(summary);
    }
    NaviSnapshotSummary::logSummary(summary);
}

}
