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
#include "aios/network/gig/multi_call/service/SearchServiceManager.h"

#include <typeinfo>
#include <unordered_set>

#include "aios/network/gig/multi_call/new_heartbeat/ClientTopoInfoMap.h"
#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatClientManager.h"
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"
#include "aios/network/gig/multi_call/service/SearchServiceReplica.h"
#include "aios/network/gig/multi_call/util/FileRecorder.h"
#include "aios/network/gig/multi_call/util/SharedPtrUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, SearchServiceManager);
static const string LOCAL_MODE_KEY = "local";

SearchServiceManager::SearchServiceManager(SearchService *searchService)
    : _subscribeServiceManager(new SubscribeServiceManager())
    , _miscConfig(new MiscConfig())
    , _heartbeatClientManager(new HeartbeatClientManager(searchService, this))
    , _onlyHeartbeatSub(false)
    , _heartbeatVersion(INVALID_VERSION_ID)
    , _callback(nullptr) {
}

SearchServiceManager::~SearchServiceManager() {
    stop();
}

void SearchServiceManager::stop() {
    stopUpdateThread();
    if (_heartbeatClientManager) {
        _heartbeatClientManager->stop();
    }
    stopLogThread();
    SearchServiceSnapshotPtr tmpSnapshotPtr;
    {
        ScopedReadWriteLock lock(_snapshotLock, 'w');
        tmpSnapshotPtr = _searchServiceSnapshotPtr;
        _searchServiceSnapshotPtr.reset();
    }
    tmpSnapshotPtr.reset();
    if (_connectionManager) {
        _connectionManager->stop();
        _connectionManager.reset();
    }
    _heartbeatClientManager.reset();
}

bool SearchServiceManager::init(const MultiCallConfig &mcConfig) {
    if (!initConnectionManager(mcConfig.connectConf, mcConfig.miscConf)) {
        AUTIL_LOG(WARN, "init connectionMananger failed!");
        return false;
    }
    if (!_subscribeServiceManager->addSubscribeService(mcConfig.subConf)) {
        AUTIL_LOG(WARN, "init subcribeService failed!");
        return false;
    }
    *_miscConfig = mcConfig.miscConf;
    if (!initHeartbeat()) {
        return false;
    }
    // TODO: wait all heartbeat return once
    updateStaticParam();
    auto updateThread =
        LoopThread::createLoopThread(std::bind(&SearchServiceManager::update, this),
                                     _miscConfig->updateTimeInterval, "GigUpdate", true);
    if (!updateThread) {
        AUTIL_LOG(ERROR, "Create update thread failed.");
        return false;
    }
    setUpdateThread(updateThread);
    updateThread->runOnce();
    startLogThread();
    return waitCreateSnapshot(300 * _miscConfig->updateTimeInterval);
}

bool SearchServiceManager::waitCreateSnapshot(int64_t waitTime) {
    SearchServiceSnapshotPtr snapshot = getSearchServiceSnapshot();
    auto beginWait = autil::TimeUtility::currentTime();
    while (!snapshot && (autil::TimeUtility::currentTime() - beginWait) < waitTime) {
        {
            ScopedLock lock(_snapshotWaitCond);
            _snapshotWaitCond.wait(_miscConfig->updateTimeInterval);
        }
        snapshot = getSearchServiceSnapshot();
    }
    if (!snapshot) {
        AUTIL_LOG(ERROR, "wait create snapshot timeout.");
        return false;
    } else {
        return true;
    }
}

void SearchServiceManager::waitCreateSnapshotLoop(int64_t waitTime) {
    auto beginWait = autil::TimeUtility::currentTime();
    while ((autil::TimeUtility::currentTime() - beginWait) < waitTime) {
        ScopedLock lock(_snapshotWaitCond);
        _snapshotWaitCond.wait(_miscConfig->updateTimeInterval);
    }
}

SearchServiceSnapshotPtr SearchServiceManager::getSearchServiceSnapshot() const {
    ScopedReadWriteLock lock(_snapshotLock, 'r');
    return _searchServiceSnapshotPtr;
}

void SearchServiceManager::setSearchServiceSnapshot(const SearchServiceSnapshotPtr &snapshot) {
    SearchServiceSnapshotPtr tmpSnapshotPtr;
    {
        ScopedReadWriteLock lock(_snapshotLock, 'w');
        tmpSnapshotPtr = _searchServiceSnapshotPtr;
        _searchServiceSnapshotPtr = snapshot;
    }
    {
        ScopedLock lock(_snapshotWaitCond);
        _snapshotWaitCond.signal();
    }
    int64_t waitTime = 5 * 1000 * 1000;
    if (!SharedPtrUtil::waitUseCount(tmpSnapshotPtr, 1, waitTime)) {
        AUTIL_LOG(WARN, "wait search service snapshot delete timeout.");
    }
}

void SearchServiceManager::startLogThread() {
    int64_t interval = _miscConfig->updateTimeInterval;
    if (_miscConfig->snapshotInterval > 0) {
        interval = _miscConfig->snapshotInterval * 1000 * 1000;
    }
    auto snapshotLogThreadPtr = LoopThread::createLoopThread(
        std::bind(&SearchServiceManager::logThread, this), interval, "GigSnapLog");
    if (!snapshotLogThreadPtr) {
        AUTIL_LOG(WARN, "start snapshot thread failed");
    }
    {
        ScopedWriteLock lock(_commonLock);
        std::swap(snapshotLogThreadPtr, _snapshotLogThreadPtr);
    }
}

void SearchServiceManager::stopLogThread() {
    autil::LoopThreadPtr snapshotLogThreadPtr;
    {
        ScopedWriteLock lock(_commonLock);
        std::swap(snapshotLogThreadPtr, _snapshotLogThreadPtr);
    }
    snapshotLogThreadPtr.reset();
}

void SearchServiceManager::logThread() {
    auto snapshot = getSearchServiceSnapshot();
    if (!snapshot) {
        return;
    }
    logSnapshot(snapshot);
    _heartbeatClientManager->log();
    reportMetrics();
}

void SearchServiceManager::logSnapshot(const SearchServiceSnapshotPtr &newSnapshot) {
    if (_miscConfig->snapshotLogCount == 0) {
        return;
    }
    if (newSnapshot) {
        newSnapshot->toString(_newSnapshotStr);
    }
    if (_newSnapshotStr != _snapshotStr) {
        FileRecorder::recordSnapshot(_newSnapshotStr, _miscConfig->snapshotLogCount,
                                     "gig_snapshot/client/" + _miscConfig->logPrefix);
        _snapshotStr.swap(_newSnapshotStr);
    }
    _newSnapshotStr.clear();
}

bool SearchServiceManager::initConnectionManager(const ConnectionConfig &conf,
                                                 const MiscConfig &miscConf) {
    auto newConf = addDefaultGrpcConfig(conf);
    ConnectionManagerPtr connectionManager(new ConnectionManager);
    if (!connectionManager->init(newConf, miscConf)) {
        return false;
    }
    _connectionManager = connectionManager;
    return true;
}

ConnectionConfig SearchServiceManager::addDefaultGrpcConfig(const ConnectionConfig &conf) {
    if (conf.end() == conf.find(multi_call::MC_PROTOCOL_GRPC_STREAM_STR)) {
        ConnectionConfig newConfig = conf;
        multi_call::ProtocolConfig protocolConfig;
        protocolConfig.threadNum = 5;
        newConfig[multi_call::MC_PROTOCOL_GRPC_STREAM_STR] = protocolConfig;
        return newConfig;
    } else {
        return conf;
    }
}

bool SearchServiceManager::initHeartbeat() {
    SearchServiceSnapshotPtr heartbeatSnapshot(
        new SearchServiceSnapshot(_connectionManager, _metricReporterManager, _miscConfig));
    if (!_heartbeatClientManager->start(_miscConfig, heartbeatSnapshot)) {
        AUTIL_LOG(ERROR, "create heartbeat thread failed");
        return false;
    }
    return true;
}

void SearchServiceManager::updateStaticParam() {
    SearchServiceProvider::updateStaticParam(_miscConfig);
    RandomGenerator::setUseHDRandom(true);
    AUTIL_LOG(INFO, "use hardware random: %d", (int)RandomGenerator::useHDRandom());
    ControllerParam::logParam();
}

void SearchServiceManager::notifyUpdate() const {
    auto updateThread = getUpdateThread();
    if (updateThread) {
        updateThread->runOnce();
    }
}

bool SearchServiceManager::setSnapshotChangeCallback(SnapshotChangeCallback *callback) {
    {
        autil::ScopedLock scope(_callbackLock);
        if (_callback) {
            AUTIL_LOG(ERROR,
                      "callback [%p] already registered, new [%p], call steal before register",
                      _callback, callback);
            return false;
        }
        _callback = callback;
    }
    if (getSearchServiceSnapshot()) {
        runSnapshotChangeCallback();
    }
    return true;
}

void SearchServiceManager::stealSnapshotChangeCallback() {
    autil::ScopedLock scope(_callbackLock);
    _callback = nullptr;
}

void SearchServiceManager::runSnapshotChangeCallback() {
    autil::ScopedLock scope(_callbackLock);
    if (_callback) {
        _callback->Run();
    }
}

autil::LoopThreadPtr SearchServiceManager::getUpdateThread() const {
    autil::ScopedLock lock(_updateThreadLock);
    return _updateThreadPtr;
}

void SearchServiceManager::setUpdateThread(const autil::LoopThreadPtr &updateThread) {
    autil::ScopedLock lock(_updateThreadLock);
    _updateThreadPtr = updateThread;
}

void SearchServiceManager::stopUpdateThread() {
    auto updateThread = getUpdateThread();
    setUpdateThread(nullptr);
    SharedPtrUtil::waitUseCount(updateThread, 1);
}

void SearchServiceManager::update() {
    AUTIL_LOG(DEBUG, "gig search service update start");
    createSnapshot();
    AUTIL_LOG(DEBUG, "gig search service update end");
}

bool SearchServiceManager::needReCreateSnapshot() {
    if (_subscribeServiceManager->clusterInfoNeedUpdate()) {
        return true;
    }
    SearchServiceSnapshotPtr snapshot = getSearchServiceSnapshot();
    if (!snapshot) {
        return true;
    }
    return false;
}

void SearchServiceManager::createSnapshot() {
    if (_subscribeServiceManager->empty()) {
        SearchServiceSnapshotPtr snapshot = getSearchServiceSnapshot();
        if (!snapshot) {
            // set empty snapshot
            snapshot.reset(
                new SearchServiceSnapshot(_connectionManager, _metricReporterManager, _miscConfig));
            setSearchServiceSnapshot(snapshot);
        }
        return;
    }
    if (_miscConfig->checkNeedUpdateSnapshot && !needReCreateSnapshot()) {
        AUTIL_LOG(DEBUG, "don't need to re-create snapshot");
        return;
    }
    TopoNodeVec topoNodeVec;
    HeartbeatSpecVec heartbeatSpecs;
    if (!_subscribeServiceManager->getClusterInfoMap(topoNodeVec, heartbeatSpecs)) {
        return;
    }
    _heartbeatClientManager->updateSpecVec(heartbeatSpecs);
    auto heartbeatVersion = _heartbeatClientManager->getVersion();
    bool onlyHeartbeatSub = true;
    for (const auto &topoNode : topoNodeVec) {
        if (multi_call::ST_LOCAL != topoNode.ssType) {
            onlyHeartbeatSub = false;
            break;
        }
    }
    if (onlyHeartbeatSub && _onlyHeartbeatSub && (_heartbeatVersion == heartbeatVersion)) {
        return;
    }
    if (createSnapshot(topoNodeVec)) {
        _onlyHeartbeatSub = onlyHeartbeatSub;
        _heartbeatVersion = heartbeatVersion;
        runSnapshotChangeCallback();
    }
}

bool SearchServiceManager::createSnapshot(const TopoNodeVec &topoNodeVec) {
    BizInfoMap bizInfoMap;
    constructBizInfoMap(topoNodeVec, bizInfoMap);
    if (!_heartbeatClientManager->fillBizInfoMap(bizInfoMap)) {
        return false;
    }
    SearchServiceSnapshotPtr snapshot;
    auto oldSnapshot = getSearchServiceSnapshot();
    snapshot = constructSnapshot(bizInfoMap, oldSnapshot);
    if (!snapshot) {
        REPORT_METRICS(reportCreateSnapshotFailQps);
        AUTIL_LOG(DEBUG, "Error on creating a snapshot.");
        return false;
    }
    REPORT_METRICS(reportCreateSnapshotSuccQps);
    oldSnapshot.reset();
    // skip first rand num;
    snapshot->getRandomSourceId();
    setSearchServiceSnapshot(snapshot);
    syncConnection();
    return true;
}

void SearchServiceManager::constructBizInfoMap(const TopoNodeVec &topoNodeVec,
                                               BizInfoMap &bizInfoMap) {
    bizInfoMap.clear();
    for (const auto &topoNode : topoNodeVec) {
        bizInfoMap[topoNode.bizName].insert(topoNode);
    }
}

SearchServiceSnapshotPtr
SearchServiceManager::constructSnapshot(const BizInfoMap &bizInfoMap,
                                        const SearchServiceSnapshotPtr &oldSnapshot) {
    SearchServiceSnapshotPtr snapshot(
        new SearchServiceSnapshot(_connectionManager, _metricReporterManager, _miscConfig));
    snapshot->setLatencyTimeSnapshot(_latencyTimeSnapshot);
    if (!snapshot->init(bizInfoMap, oldSnapshot)) {
        return SearchServiceSnapshotPtr();
    }
    return snapshot;
}

void SearchServiceManager::syncConnection() const {
    if (!_connectionManager) {
        // maybe null in java client
        return;
    }
    std::set<std::string> specSet;
    if (!getSpecSet(specSet)) {
        return;
    }
    _connectionManager->syncConnection(specSet);
}

bool SearchServiceManager::getSpecSet(std::set<std::string> &specSet) const {
    auto snapshot = getSearchServiceSnapshot();
    if (!snapshot) {
        return false;
    }
    snapshot->getSpecSet(specSet);
    _heartbeatClientManager->getSpecSet(specSet);
    return true;
}

void SearchServiceManager::reportMetrics() {
    auto snapshot = getSearchServiceSnapshot();
    if ((!snapshot) || (!_metricReporterManager)) {
        return;
    }
    SnapshotInfoCollector snapshotInfoCollector;
    snapshot->fillSnapshotInfo(snapshotInfoCollector);
    _metricReporterManager->reportSnapshotInfo(snapshotInfoCollector);
}

bool SearchServiceManager::addSubscribeService(const SubscribeConfig &subServiceConf) {
    return _subscribeServiceManager->addSubscribeService(subServiceConf);
}

bool SearchServiceManager::addSubscribe(const SubscribeClustersConfig &gigSubConf) {
    return _subscribeServiceManager->addSubscribe(gigSubConf);
}

bool SearchServiceManager::deleteSubscribe(const SubscribeClustersConfig &gigSubConf) {
    return _subscribeServiceManager->deleteSubscribe(gigSubConf);
}

void SearchServiceManager::enableSubscribeService(SubscribeType ssType) {
    _subscribeServiceManager->enableSubscribeService(ssType);
}

void SearchServiceManager::disableSubscribeService(SubscribeType ssType) {
    _subscribeServiceManager->disableSubscribeService(ssType);
}

void SearchServiceManager::stopSubscribeService(SubscribeType ssType) {
    _subscribeServiceManager->stopSubscribeService(ssType);
}

} // namespace multi_call
