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
#ifndef NAVI_NAVIWORKERBASE_H
#define NAVI_NAVIWORKERBASE_H

#include "aios/network/arpc/arpc/common/LockFreeQueue.h"
#include "autil/TimeUtility.h"
#include "autil/ObjectTracer.h"
#include "navi/common.h"
#include "navi/engine/GraphParam.h"
#include "navi/engine/NaviThreadPool.h"
#include "navi/engine/NaviUserResult.h"
#include "navi/engine/RunGraphParams.h"
#include "navi/engine/TimeoutChecker.h"
#include "navi/engine/NaviMetricsGroup.h"
#include "navi/log/NaviLogger.h"
#include "navi/perf/NaviPerf.h"
#include "navi/util/CommonUtil.h"

namespace multi_call {
class QueryInfo;
class QuerySession;
} // namespace multi_call

namespace navi {

class TestEngine;
class NaviWorkerBase;
class GraphMemoryPoolResource;
class MetaInfoResourceBase;
class NaviClosure;
class NaviSnapshot;
struct TaskQueue;

class NaviWorkerItem : public NaviThreadPoolItemBase
{
public:
    NaviWorkerItem(NaviWorkerBase *worker)
        : _worker(worker)
        , _syncMode(false)
    {
    }
    virtual ~NaviWorkerItem() {
    }
public:
    virtual void doProcess() = 0;
public:
    void process() override;
    void destroy() override;
    bool syncMode() const override final;
    virtual void drop();
public:
    void setSyncMode(bool syncMode);
protected:
    NaviWorkerBase *_worker;
    bool _syncMode;
};

class NaviNoDropWorkerItem : public NaviWorkerItem
{
public:
    NaviNoDropWorkerItem(NaviWorkerBase *worker)
        : NaviWorkerItem(worker)
        , _done(false)
    {
    }
    virtual ~NaviNoDropWorkerItem() {
    }
public:
    void process() override final;
    void destroy() override final;
    void drop() override final;
    void doWork();
private:
    bool _done;
};

class BizManager;

class NaviWorkerBase : public autil::ObjectTracer<NaviWorkerBase, true> {
private:
    class WorkerInitItem : public NaviWorkerItem {
      public:
        WorkerInitItem(NaviWorkerBase *worker)
            : NaviWorkerItem(worker)
        {
        }
      public:
        void doProcess() override {
            _worker->run();
        }
    };
public:
    NaviWorkerBase(const NaviLoggerPtr &logger,
                   TaskQueue *taskQueue,
                   BizManager *bizManager,
                   NaviUserResultClosure *closure = nullptr);
    virtual ~NaviWorkerBase();
public:
    void initSchedule(bool syncMode, NaviSnapshot *snapshot);
    bool schedule(NaviWorkerItem *item);
    void dec();
    void setErrorCode(ErrorCode ec);
    ErrorCode getErrorCode() const;
    const NaviObjectLogger &getLogger() const;
    void setFinish(ErrorCode ec);
    NaviUserResultPtr getUserResult() const;
    void fillResult();
    void appendTrace(TraceCollector &collector);
    void appendResult(NaviResult &result);
    void setMetricsReporter(kmonitor::MetricsReporter &reporter);
    void setRunParams(const RunGraphParams &params);
    const RunGraphParams &getRunParams() const;
    void setRunStartTime(int64_t runStartTime);
    void tryCreateQuerySession();
    void setQueryInfo(const std::shared_ptr<multi_call::QueryInfo> &queryInfo);
    void startTrace(const std::string &peer) const;
    void setDestructThreadPool(const NaviThreadPoolPtr &destructThreadPool);
    void setNaviPerf(NaviPerf *naviPerf);
    void setNaviSymbolTable(const NaviSymbolTablePtr &naviSymbolTable);
    void enablePerf();
    void disablePerf();
    void beginSample();
    NaviPerfResultPtr endSample();
    const std::string &getTraceLevel() const;
    int64_t getThreadLimit() const;
    GraphParam *getGraphParam();
    const std::shared_ptr<GraphMemoryPoolResource> &getGraphMemoryPoolResource() const;
    const std::shared_ptr<MetaInfoResourceBase> &getMetaInfoResource() const;
    void clear();
    void deleteThis();
    void incItemCount();
    void decItemCount();
    int64_t itemCount();
    bool canInline() const;
    int64_t getIdleThreadCount() const;
    int64_t getRunningThreadCount() const;
    size_t getQueueSize() const;
    void inlineBegin() const;
    void inlineEnd() const;
    void finalize();
protected:
    virtual void finalizeMetaInfoToResult();
public:
    // time metric
    int64_t getRemainTime() const;
    int64_t getBeginTime() const;
    TimeoutChecker *getTimeoutChecker();
    const NaviThreadPoolPtr &getDestructThreadPool() const;
public:
    virtual void run() = 0;
    virtual void drop() = 0;
public:
    bool isFinished() const;
private:
    void initRunParams();
    void initMemoryPool(BizManager *bizManager);
    void initMetaInfoResource(BizManager *bizManager);
    void initGraphParam(BizManager *bizManager);
    void initCounters();
    void doSchedule();
    bool isStopped() const;
    bool isTimeout() const;
    bool reportRpcLackByBiz(const multi_call::GigStreamRpcInfoMap &rpcInfoMap);
    void fillResultMetrics();
    friend class NaviThreadPool;
    friend class TestEngine;
    friend class NaviWorkerItem;
    friend class NaviNoDropWorkerItem;
private:
    std::shared_ptr<GraphMemoryPoolResource> _poolResource;
    std::shared_ptr<MetaInfoResourceBase> _metaInfoResource;
protected:
    DECLARE_LOGGER();
    PoolPtr _pool;
protected:
    GraphParam _param;
    RunGraphMetricsCollector _metricsCollector;
    kmonitor::MetricsReporterPtr _metricsReporter;
private:
    volatile ErrorCode _errorCode;
    volatile bool _finish;
    NaviUserResultPtr _userResult;
    bool _initSuccess;
    NaviThreadPool *_threadPool;
    TaskQueue *_taskQueue;
    NaviThreadPoolPtr _destructThreadPool;
    RunGraphParams _runGraphParams;
    NaviSnapshot *_snapshot;
    NaviPerf *_naviPerf;
    TimeoutChecker _timeoutChecker;
    arpc::common::LockFreeQueue<NaviWorkerItem *> _scheduleQueue;
    atomic64_t _itemCount;
    atomic64_t _processingCount;
    std::shared_ptr<multi_call::QueryInfo> _queryInfo;
};

}

#endif //NAVI_NAVIWORKERBASE_H
