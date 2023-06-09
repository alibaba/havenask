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
#include "navi/log/NaviLogManager.h"
#include "navi/log/NaviLogger.h"
#include "navi/perf/NaviPerf.h"
#include "navi/proto/NaviStream.pb.h"
#include "navi/util/NaviClosure.h"
#include "autil/ObjectTracer.h"

namespace kmonitor {
class MetricsReporter;
}

namespace multi_call {
class GigStreamBase;
} // namespace multi_call

namespace navi {

class NaviSnapshot;
class NaviGraphRunner;
class NaviThreadPool;
class NaviUserResult;
class NaviUserResultClosure;
class RunGraphParams;
class NaviSymbolTable;
struct NaviSnapshotStat;

NAVI_TYPEDEF_PTR(NaviSnapshot);

class NaviSnapshot : public autil::ObjectTracer<NaviSnapshot, true>
{
public:
    NaviSnapshot();
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
              const CreatorManagerPtr &creatorManager,
              const NaviSnapshotPtr &oldSnapshot,
              const NaviConfigPtr &config,
              const ResourceMap &rootResourceMap);
    void initMetrics(kmonitor::MetricsReporter &reporter);
    const NaviLoggerPtr &getLogger() const;
    CreatorManagerPtr getCreatorManager() const;
    void stop();
    void setTestMode(bool testMode);
    void onSessionDestruct(TaskQueue *queueEntry);
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
    bool initBizManager(const CreatorManagerPtr &creatorManager,
                        const NaviSnapshotPtr &oldSnapshot,
                        const ResourceMap &rootResourceMapIn);
    bool initNaviPerf();
    bool initSymbolTable();
    bool initBizResource(const ResourceMap &rootResourceMap);
    bool collectResultResource(const std::shared_ptr<NaviUserResult> &result,
                               ResourceMap &resourceMap);
    NaviObjectLogger getObjectLogger(SessionId id);
    void stopSingleTaskQueue(TaskQueue *taskQueue);
    void fillRootKmonResource(ResourceMap &rootResourceMap);
    bool parseRunParams(const RunParams &pbParams,
                        const PoolPtr &pool,
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

private:
    DECLARE_LOGGER();
    NaviLogManagerPtr _logManager;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    std::map<std::string, TaskQueueUPtr> _extraTaskQueueMap;
    TaskQueueUPtr _defaultTaskQueue;
    BizManagerPtr _bizManager;
    std::shared_ptr<NaviThreadPool> _destructThreadPool;
    NaviPerf _naviPerf;
    NaviConfigPtr _config;
    bool _testMode;
    std::shared_ptr<NaviSymbolTable> _naviSymbolTable;
};

}
