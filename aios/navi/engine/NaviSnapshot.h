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
#pragma once

#include "navi/engine/BizManager.h"
#include "navi/engine/TaskQueue.h"
#include "navi/rpc_server/NaviRpcServerR.h"
#include "navi/log/NaviLogManager.h"
#include "navi/log/NaviLogger.h"
#include "navi/perf/NaviPerf.h"
#include "navi/proto/NaviStream.pb.h"
#include "navi/util/NaviClosure.h"
#include "autil/ObjectTracer.h"
#include "autil/LoopThread.h"

namespace kmonitor {
class MetricsReporter;
}

namespace multi_call {
class GigStreamBase;
class GigRpcServer;
}

namespace navi {

class NaviSnapshot;
class NaviGraphRunner;
class NaviThreadPool;
class NaviUserResult;
struct NaviUserData;
class NaviUserResultClosure;
class RunGraphParams;
class NaviSymbolTable;
struct NaviSnapshotStat;
class EvTimer;

NAVI_TYPEDEF_PTR(NaviSnapshot);

struct InitWaiter {
public:
    InitWaiter() {}
public:
    void setExpectCount(size_t expectCount);
    bool wait(int64_t timeoutUs);
    void notify(const NaviUserData &data);
    void stop();
private:
    autil::ThreadCond _collectCond;
    bool _stopped = false;
    size_t _expectCount = 0;
    std::set<size_t> _readyIndexSet;
};

class NaviSnapshot : public autil::ObjectTracer<NaviSnapshot, true>
{
public:
    NaviSnapshot(const std::string &naviName);
    ~NaviSnapshot();
private:
    NaviSnapshot(const NaviSnapshot &);
    NaviSnapshot &operator=(const NaviSnapshot &);
public:
    friend class NaviGraphRunner;
    friend class Navi;
public:
    bool init(const std::string &initConfigPath,
              InstanceId instanceId,
              multi_call::GigRpcServer *gigRpcServer,
              const ModuleManagerPtr &moduleManager,
              const NaviSnapshotPtr &oldSnapshot,
              const NaviConfigPtr &config,
              const ResourceMap &rootResourceMap);
    bool start(const NaviSnapshotPtr &oldSnapshot);
    void initMetrics(kmonitor::MetricsReporter &reporter);
    void logSummary(const std::string &stage) const;
    const NaviLoggerPtr &getLogger() const;
    ModuleManagerPtr getModuleManager() const;
    void stop();
    void setTestMode(TestMode testMode);
    TestMode getTestMode() const { return _testMode; }
    void onSessionDestruct(TaskQueue *queueEntry);
    void cleanupModule();
public:
    std::shared_ptr<NaviUserResult>
    runGraph(GraphDef *graphDef, const RunGraphParams &params, const ResourceMap *resourceMap);
    void runGraphAsync(GraphDef *graphDef,
                       const RunGraphParams &params,
                       const ResourceMap *resourceMap,
                       NaviUserResultClosure *closure);
    bool runGraph(NaviMessage *request,
                  const ArenaPtr &arena,
                  const RunParams &pbParams,
                  const std::shared_ptr<multi_call::GigStreamBase> &stream,
                  NaviLoggerPtr &logger);
    bool createResource(const std::string &bizName,
                        NaviPartId partCount,
                        NaviPartId partId,
                        const std::set<std::string> &resources,
                        ResourceMap &resourceMap);
    const std::shared_ptr<NaviThreadPool> &getDestructThreadPool() const;
public:
    void reportStat(kmonitor::MetricsReporter &reporter);
public:
    static NaviLoggerPtr getTlsLogger();
private:
    void initDefaultLogger();
    bool initLogger(InstanceId instanceId, const NaviSnapshotPtr &oldSnapshot);
    bool initTaskQueue();
    bool initSingleTaskQueue(const std::string &name, const ConcurrencyConfig &config, TaskQueueUPtr &entry);
    bool initBizManager(const ModuleManagerPtr &moduleManager,
                        multi_call::GigRpcServer *gigRpcServer,
                        const NaviSnapshotPtr &oldSnapshot,
                        const ResourceMap &rootResourceMap);
    void addBuildinRootResource(multi_call::GigRpcServer *gigRpcServer,
                                ResourceMap &rootResourceMap);
    bool initNaviPerf();
    bool initSymbolTable();
    bool initTimerThread();
    bool initBizResource(const ResourceMap &rootResourceMap);
    void collectInitResource();
    bool collectResultResource(const std::shared_ptr<NaviUserResult> &result,
                               ResourceMap &resourceMap, InitWaiter *waiter);
    NaviObjectLogger getObjectLogger(SessionId id);
    void stopSingleTaskQueue(TaskQueue *taskQueue);
    bool parseRunParams(const RunParams &pbParams,
                        RunGraphParams &params);
    void initSnapshotResource(NaviWorkerBase *session);
    void doSchedule(TaskQueue *taskQueue);
    TaskQueue *getTaskQueue(const std::string &taskQueueName);
    bool runGraphImpl(GraphDef *graphDef,
                      const RunGraphParams &params,
                      const ResourceMap *resourceMap,
                      std::shared_ptr<NaviUserResult> &userResult,
                      NaviUserResultClosure *closure);
    void getTaskQueueStat(const TaskQueue *taskQueue, NaviSnapshotStat &stat) const;
    void writeSummary(const std::string &naviName,
                      const std::string &summaryStr) const;
private:
    DECLARE_LOGGER();
    std::string _naviName;
    NaviConfigPtr _config;
    std::shared_ptr<NaviSymbolTable> _naviSymbolTable;
    NaviLogManagerPtr _logManager;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    std::map<std::string, TaskQueueUPtr> _extraTaskQueueMap;
    TaskQueueUPtr _defaultTaskQueue;
    BizManagerPtr _bizManager;
    autil::ThreadPtr _collectThread;
    std::shared_ptr<NaviUserResult> _initResult;
    InitWaiter _collectWaiter;
    std::shared_ptr<NaviThreadPool> _destructThreadPool;
    NaviPerf _naviPerf;
    std::shared_ptr<EvTimer> _evTimer;
    TestMode _testMode;
    multi_call::GigRpcServer *_gigRpcServer;
    NaviRpcServerRPtr _naviRpcServer;
};

}
