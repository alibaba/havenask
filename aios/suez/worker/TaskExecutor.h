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

#include <functional>
#include <memory>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "autil/ObjectTracer.h"
#include "autil/ThreadPool.h"
#include "autil/legacy/json.h"
#include "suez/common/DiskQuotaController.h"
#include "suez/sdk/IndexProvider.h"
#include "suez/sdk/KMonitorMetaInfo.h"
#include "suez/table/SuezPartition.h"
#include "worker_framework/DataClient.h"

namespace suez {
class HeartbeatManager;
class HeartbeatTarget;
class RpcServer;
class SearchManager;
class SearchManagerUpdater;
class TableManager;
class TableMetas;
struct InitParam;
struct EnvParam;
class WorkerCurrent;
class SchedulerInfo;
class DeployManager;

class TaskExecutor : autil::ObjectTracer<TaskExecutor, true> {
public:
    TaskExecutor(RpcServer *rpcServer, const KMonitorMetaInfo &metaInfo);
    ~TaskExecutor();

private:
    TaskExecutor(const TaskExecutor &);
    TaskExecutor &operator=(const TaskExecutor &);

public:
    bool start(HeartbeatManager *hbManager, const std::string &installRoot, const EnvParam &param);
    void release();
    void gracefullyShutdown();
    void stopDecision();
    void stopWorker();
    void stopThreadPool();
    void reportMetrics();

    SearchManager *getSearchManager() const;

    void setAllowPartialTableReady(bool allowPartialTableReady) { _allowPartialTableReady = allowPartialTableReady; }
    void setNeedShutdownGracefully(bool needShutdownGracefully) { _needShutdownGracefully = needShutdownGracefully; }
    void setNoDiskQuotaCheck(bool noDiskQuotaCheck) { _noDiskQuotaCheck = noDiskQuotaCheck; }

    // for debug mode
    std::pair<bool, autil::legacy::json::JsonMap>
    nextStep(HeartbeatTarget &target, HeartbeatTarget &finalTarget, const SchedulerInfo &schedulerInfo, bool forceSync);

private:
    bool initTableManager(const InitParam &param, const std::string &zoneName);
    bool initSearchManager(const InitParam &param);

    void workloop();
    bool doWorkloop(HeartbeatTarget &target,
                    HeartbeatTarget &finalTarget,
                    const SchedulerInfo &schedulerInfo,
                    bool forceSync);
    IndexProviderPtr getIndexProvider(const HeartbeatTarget &target);
    void fillWorkerCurrent(const HeartbeatTarget &target, WorkerCurrent &current) const;
    void updateHeartbeat(const WorkerCurrent &current, const std::string &signature);
    void adjustWorkerCurrent(WorkerCurrent &current);
    void stopService();
    UPDATE_RESULT maybeUpdateTarget(HeartbeatTarget &target, HeartbeatTarget &finalTarget);

private:
    HeartbeatManager *_hbManager;
    std::unique_ptr<DeployManager> _deployManager;
    std::unique_ptr<TableManager> _tableManager;
    std::unique_ptr<SearchManagerUpdater> _searchManagerUpdater;
    autil::LoopThreadPtr _workThread;
    RpcServer *_rpcServer;
    std::unique_ptr<autil::ThreadPool> _deployThreadPool;
    std::unique_ptr<autil::ThreadPool> _loadThreadPool;
    std::unique_ptr<future_lite::Executor> _asyncInterExecutor;
    std::unique_ptr<future_lite::Executor> _asyncIntraExecutor;
    int64_t _targetVersion; // last updated target version
    bool _allowPartialTableReady;
    bool _needShutdownGracefully;
    bool _noDiskQuotaCheck;
    bool _deployLocal; // to speed test
    bool _shutdownFlag;
    bool _shutdownSucc;
    autil::ThreadMutex _shutdownLock;
    DiskQuotaController _diskQuotaController;
    KMonitorMetaInfo _kmonMetaInfo;
    kmonitor::MetricsReporterPtr _metricsReporter;

    struct WriteModeResource;
    std::unique_ptr<WriteModeResource> _writeModeResource;

private:
    static const int32_t TASK_EXECUTOR_LOOP_INTERVAL;
};

} // namespace suez
