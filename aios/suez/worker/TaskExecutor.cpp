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
#include "suez/worker/TaskExecutor.h"

#include <cstddef>
#include <map>
#include <memory>
#include <unistd.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "kmonitor/client/MetricsReporter.h"
#include "suez/common/InitParam.h"
#include "suez/common/InnerDef.h"
#include "suez/common/TableMeta.h"
#include "suez/deploy/DeployManager.h"
#include "suez/heartbeat/HeartbeatManager.h"
#include "suez/heartbeat/HeartbeatTarget.h"
#include "suez/sdk/RpcServer.h"
#include "suez/sdk/SchedulerInfo.h"
#include "suez/sdk/SuezError.h"
#include "suez/sdk/TableReader.h"
#include "suez/search/SearchManagerUpdater.h"
#include "suez/table/LeaderElectionManager.h"
#include "suez/table/ReadWriteTableManager.h"
#include "suez/table/TableManager.h"
#include "suez/table/TargetUpdater.h"
#include "suez/table/VersionManager.h"
#include "suez/table/VersionSynchronizer.h"
#include "suez/worker/AsyncExecutorFactory.h"
#include "suez/worker/EnvParam.h"
#include "suez/worker/WorkerCurrent.h"

namespace suez {
class RpcServer;
} // namespace suez

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace kmonitor;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TaskExecutor);

struct TaskExecutor::WriteModeResource {
    std::unique_ptr<VersionManager> versionManager;
    std::unique_ptr<LeaderElectionManager> leaderElectionMgr;
    std::unique_ptr<VersionSynchronizer> versionSync;

    bool init(const std::string &zoneName) {
        versionManager = std::make_unique<VersionManager>();
        leaderElectionMgr.reset(LeaderElectionManager::create(zoneName));
        if (!leaderElectionMgr || !leaderElectionMgr->init()) {
            AUTIL_LOG(ERROR, "create leader manager for %s failed", zoneName.c_str());
            return false;
        }
        versionSync.reset(VersionSynchronizer::create());
        return versionSync && versionSync->init();
    }

    void release() {
        if (leaderElectionMgr) {
            leaderElectionMgr->stop();
        }
    }
};

const int32_t TaskExecutor::TASK_EXECUTOR_LOOP_INTERVAL = 1 * 1000 * 1000;

#define DIFF_LOG(key, value, level)                                                                                    \
    do {                                                                                                               \
        auto tmpValue = string(value);                                                                                 \
        static string key##_ = "__invalid";                                                                            \
        if (key##_ != tmpValue) {                                                                                      \
            AUTIL_LOG(level, "%s: %s", #key, tmpValue.c_str());                                                        \
            key##_ = value;                                                                                            \
        }                                                                                                              \
    } while (0)

TaskExecutor::TaskExecutor(RpcServer *rpcServer, const KMonitorMetaInfo &metaInfo)
    : _hbManager(nullptr)
    , _rpcServer(rpcServer)
    , _targetVersion(INVALID_TARGET_VERSION)
    , _allowPartialTableReady(false)
    , _needShutdownGracefully(false)
    , _noDiskQuotaCheck(false)
    , _deployLocal(false)
    , _shutdownFlag(false)
    , _shutdownSucc(false)
    , _kmonMetaInfo(metaInfo) {}

TaskExecutor::~TaskExecutor() {}

bool TaskExecutor::start(HeartbeatManager *hbManager, const std::string &installRoot, const EnvParam &param) {
    _deployThreadPool = std::make_unique<autil::ThreadPool>(param.dpThreadNum, param.dpThreadNum);
    if (!_deployThreadPool->start("SuezDp")) {
        return false;
    }

    _loadThreadPool = std::make_unique<autil::ThreadPool>(param.loadThreadNum, param.loadThreadNum);
    if (!_loadThreadPool->start("SuezLoad")) {
        return false;
    }

    _asyncInterExecutor = AsyncExecutorFactory::createAsyncExecutor(
        "InterExecutor", param.asyncInterExecutorThreadNum, param.asyncInterExecutorType);
    _asyncIntraExecutor = AsyncExecutorFactory::createAsyncExecutor(
        "IntraExecutor", param.asyncIntraExecutorThreadNum, param.asyncIntraExecutorType);

    if (_asyncInterExecutor == nullptr || _asyncIntraExecutor == nullptr) {
        return false;
    }

    if (_kmonMetaInfo.metricsPrefix.empty()) {
        _metricsReporter.reset(new MetricsReporter("", {}, "task_executor"));
    } else {
        _metricsReporter.reset(new MetricsReporter(_kmonMetaInfo.metricsPrefix, "", {}, "task_executor"));
    }
    _diskQuotaController.setMetricsReporter(_metricsReporter);
    _deployLocal = param.localMode;

    auto diskQuotaController = &_diskQuotaController;
    if (_noDiskQuotaCheck) {
        diskQuotaController = nullptr;
    }

    if (_deployLocal) {
        _deployManager = make_unique<DeployManager>(nullptr, diskQuotaController, true);
    } else {
        auto dataClient = make_shared<worker_framework::DataClient>();
        auto dp2Spec = "tcp:" + param.hippoSlaveIp + ":" + param.hippoDp2SlavePort;
        if (!dataClient || !dataClient->init(dp2Spec)) {
            AUTIL_LOG(ERROR, "create dataClient failed");
            return false;
        }
        _deployManager = make_unique<DeployManager>(dataClient, diskQuotaController);
    }

    InitParam initParam;
    initParam.rpcServer = _rpcServer;
    initParam.kmonMetaInfo = _kmonMetaInfo;
    initParam.deployThreadPool = _deployThreadPool.get();
    initParam.loadThreadPool = _loadThreadPool.get();
    initParam.installRoot = installRoot;
    initParam.asyncInterExecutor = _asyncInterExecutor.get();
    initParam.asyncIntraExecutor = _asyncIntraExecutor.get();
    initParam.deployManager = _deployManager.get();

    if (!initTableManager(initParam, param.zoneName) || !initSearchManager(initParam)) {
        return false;
    }
    _hbManager = hbManager;

    if (param.debugMode) {
        AUTIL_LOG(INFO, "debug mode enabled, work loop has been disabled");
    } else {
        _workThread = LoopThread::createLoopThread(
            std::bind(&TaskExecutor::workloop, this), param.decisionLoopInterval, "SuezWork");
    }
    return true;
}

bool TaskExecutor::initTableManager(const InitParam &initParam, const std::string &zoneName) {
    std::string mode = EnvUtil::getEnv("mode", "");
    if (mode == "rw") {
        AUTIL_LOG(INFO, "suez worker in rw mode");
        _writeModeResource = std::make_unique<WriteModeResource>();
        if (!_writeModeResource->init(zoneName)) {
            AUTIL_LOG(ERROR, "init write resource failed");
            return false;
        }
        _writeModeResource->leaderElectionMgr->setLeaderInfo(_rpcServer->getArpcAddress());
        auto tableManager = std::make_unique<ReadWriteTableManager>();
        tableManager->setVersionManager(_writeModeResource->versionManager.get());
        tableManager->setLeaderElectionManager(_writeModeResource->leaderElectionMgr.get());
        tableManager->setVersionSynchronizer(_writeModeResource->versionSync.get());
        tableManager->setGigRpcServer(_rpcServer->gigRpcServer);
        tableManager->setZoneName(zoneName);
        _tableManager = std::move(tableManager);
    } else {
        AUTIL_LOG(INFO, "suez worker in read only mode");
        _tableManager = std::make_unique<TableManager>();
    }
    if (!_tableManager->init(initParam)) {
        AUTIL_LOG(ERROR, "init table manager failed");
        return false;
    }
    return true;
}

bool TaskExecutor::initSearchManager(const InitParam &initParam) {
    _searchManagerUpdater = std::make_unique<SearchManagerUpdater>(_allowPartialTableReady);
    if (!_searchManagerUpdater->init(initParam)) {
        AUTIL_LOG(ERROR, "init SearchManager");
        return false;
    }
    return true;
}

void TaskExecutor::stopDecision() {
    if (_workThread) {
        _workThread->stop();
        _workThread.reset();
    }
}

void TaskExecutor::stopWorker() {
    if (_searchManagerUpdater) {
        _searchManagerUpdater->stopWorker();
    }
}

void TaskExecutor::stopThreadPool() {
    if (_loadThreadPool.get() != nullptr) {
        _loadThreadPool->stop(autil::ThreadPool::STOP_AND_CLEAR_QUEUE);
        _loadThreadPool.reset();
    }
    if (_deployThreadPool.get() != nullptr) {
        _deployThreadPool->stop(autil::ThreadPool::STOP_AND_CLEAR_QUEUE);
        _deployThreadPool.reset();
    }
}

void TaskExecutor::reportMetrics() {
    if (_searchManagerUpdater.get() != nullptr) {
        _searchManagerUpdater->reportMetrics();
    }
}

SearchManager *TaskExecutor::getSearchManager() const { return _searchManagerUpdater->getSearchManager(); }

std::pair<bool, JsonMap> TaskExecutor::nextStep(HeartbeatTarget &target,
                                                HeartbeatTarget &finalTarget,
                                                const SchedulerInfo &schedulerInfo,
                                                bool forceSync) {
    autil::legacy::json::JsonMap hbJson;
    bool reachTarget = doWorkloop(target, finalTarget, schedulerInfo, forceSync);
    WorkerCurrent current;
    fillWorkerCurrent(target, current);
    current.toHeartbeatFormat(hbJson);
    return std::make_pair(reachTarget, hbJson);
}

void TaskExecutor::workloop() {
    HeartbeatTarget target, finalTarget;
    SchedulerInfo schedulerInfo;
    if (!_hbManager->getTarget(target, finalTarget, schedulerInfo)) {
        return;
    }
    bool reachTarget = doWorkloop(target, finalTarget, schedulerInfo, false);
    WorkerCurrent current;
    fillWorkerCurrent(target, current);
    adjustWorkerCurrent(current);
    updateHeartbeat(current, reachTarget ? target.getSignature() : "");
}

bool TaskExecutor::doWorkloop(HeartbeatTarget &target,
                              HeartbeatTarget &finalTarget,
                              const SchedulerInfo &schedulerInfo,
                              bool forceSync) {
    bool shutdownFlag = false;
    {
        ScopedLock lock(_shutdownLock);
        shutdownFlag = _shutdownFlag;
    }
    JsonMap errors;
    TableReader readyReader;
    string signature;

    _diskQuotaController.setQuotaMb(finalTarget.getDiskSize());
    _deployManager->updateDeployConfig(target.getDeployConfig());
    if (!shutdownFlag) {
        UPDATE_RESULT targetUpdateResult = maybeUpdateTarget(target, finalTarget);
        if (UR_REACH_TARGET != targetUpdateResult) {
            AUTIL_LOG(WARN, "update target failed, retry next loop");
            return false;
        }
    }
    UPDATE_RESULT tableUpdateResult = _tableManager->update(target, finalTarget, shutdownFlag, forceSync);
    DIFF_LOG(tableUpdateResult, enumToCStr(tableUpdateResult), INFO);
    if (!_allowPartialTableReady && UR_ERROR == tableUpdateResult) {
        stopService();
    }
    UpdateIndications updateIndications;
    UPDATE_RESULT searchUpdateResult =
        _searchManagerUpdater->update(target, schedulerInfo, getIndexProvider(target), updateIndications, shutdownFlag);
    DIFF_LOG(searchUpdateResult, enumToCStr(searchUpdateResult), INFO);
    _tableManager->processTableIndications(updateIndications.releaseLeaderTables,
                                           updateIndications.campaginLeaderTables,
                                           updateIndications.publishLeaderTables,
                                           updateIndications.tablesWeight);
    bool reachTarget = UR_REACH_TARGET == tableUpdateResult && UR_REACH_TARGET == searchUpdateResult;
    if (UR_NEED_RETRY != searchUpdateResult) {
        if (UR_ERROR == searchUpdateResult) {
            stopService();
        }
        // stop service or reach target can clean config not in target
        _searchManagerUpdater->cleanResource(target);
    }

    {
        ScopedLock lock(_shutdownLock);
        _shutdownSucc = (UR_REACH_TARGET == searchUpdateResult) && shutdownFlag;
    }

    if (reachTarget) {
        _targetVersion = target.getTargetVersion();
    }
    return reachTarget;
}

void TaskExecutor::release() {
    AUTIL_LOG(INFO, "TaskExecutor::release begin");
    gracefullyShutdown();
    stopDecision();
    if (_writeModeResource) {
        // 停止写流量
        _writeModeResource->release();
    }
    // 停止读流量
    stopWorker();
    stopThreadPool();
    if (_tableManager) {
        _tableManager->stop();
    }
    AUTIL_LOG(INFO, "TaskExecutor::release end");
}

void TaskExecutor::gracefullyShutdown() {
    if (!_needShutdownGracefully) {
        AUTIL_LOG(INFO, "do not need shutdown gracefully.");
        return;
    }
    AUTIL_LOG(INFO, "need shut down gracefully. gracefully shutdown begin.");
    {
        ScopedLock lock(_shutdownLock);
        _shutdownFlag = true;
    }
    while (true) {
        {
            ScopedLock lock(_shutdownLock);
            if (_shutdownSucc) {
                AUTIL_LOG(INFO, "gracefully shutdown done.");
                return;
            }
        }
        AUTIL_LOG(INFO, "wait gracefully shutdown...... ");
        usleep(1 * 1000 * 1000);
    }
}

void TaskExecutor::fillWorkerCurrent(const HeartbeatTarget &target, WorkerCurrent &current) const {
    current.setTargetVersion(_targetVersion);
    auto arpcAddress = _rpcServer->getArpcAddress();
    auto httpAddress = _rpcServer->getHttpAddress();
    current.setAddress(std::move(arpcAddress), std::move(httpAddress));
    current.setTableInfos(_tableManager->getTableMetas());
    current.setBizInfos(_searchManagerUpdater->getBizMap(target));
    current.setAppMeta(_searchManagerUpdater->getAppMeta());
    current.setError(_searchManagerUpdater->getGlobalError());
    current.setServiceInfo(_searchManagerUpdater->getServiceInfo());
}

void TaskExecutor::updateHeartbeat(const WorkerCurrent &current, const std::string &signature) {
    autil::legacy::json::JsonMap hbJson;
    current.toHeartbeatFormat(hbJson);
    _hbManager->setCurrent(hbJson, current.getServiceInfo(), signature);
}

void TaskExecutor::stopService() {
    // 直写模式下，需要释放leader来了停写流量，否则stopService会卡住
    ReadWriteTableManager *rwTableMgr = dynamic_cast<ReadWriteTableManager *>(_tableManager.get());
    if (rwTableMgr != nullptr) {
        rwTableMgr->releaseLeader();
    }
    _searchManagerUpdater->stopService();
    _targetVersion = INVALID_TARGET_VERSION;
}

IndexProviderPtr TaskExecutor::getIndexProvider(const HeartbeatTarget &target) {
    auto provider = make_shared<IndexProvider>();
    bool readerAllReady = _tableManager->getTableReader(target, provider->multiTableReader);
    bool writerAllReady = _tableManager->getTableWriters(target, provider->tableWriters);
    (void)writerAllReady;
    provider->allTableReady = readerAllReady; // ignore writerAllReady
    provider->tableReader = provider->multiTableReader.getIndexlibTableReaders();
    provider->tabletReader = provider->multiTableReader.getIndexlibTabletReaders();
    bool sourceReaderReady = _tableManager->getTableConfigs(target, provider->sourceReaderProvider);
    (void)sourceReaderReady;
    for (const auto &it : target.getTableMetas()) {
        provider->targetPids.insert(it.first);
    }
    return provider;
}

UPDATE_RESULT TaskExecutor::maybeUpdateTarget(HeartbeatTarget &target, HeartbeatTarget &finalTarget) {
    if (_writeModeResource.get() == nullptr) {
        return UR_REACH_TARGET;
    }

    TargetUpdater updater(_writeModeResource->leaderElectionMgr.get(),
                          _writeModeResource->versionManager.get(),
                          _writeModeResource->versionSync.get());
    auto targetResult = updater.maybeUpdate(target);
    if (UR_REACH_TARGET != targetResult) {
        return targetResult;
    }

    targetResult = updater.maybeUpdate(finalTarget);
    return targetResult;
}

void TaskExecutor::adjustWorkerCurrent(WorkerCurrent &current) {
    auto tableMetas = current.getTableMetas();
    for (auto &kv : *tableMetas) {
        if (TS_LOADED == kv.second.getTableStatus()) {
            auto table = _tableManager->getTable(kv.first);
            if (table && !table->isRecovered()) {
                kv.second.setTableStatus(TS_LOADING);
            }
        }
    }
}

} // namespace suez
