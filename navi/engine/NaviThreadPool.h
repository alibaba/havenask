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

#include "navi/common.h"
#include "navi/config/NaviConfig.h"
#include "navi/engine/ScheduleInfo.h"
#include "navi/util/CommonUtil.h"
#include <arpc/common/LockFreeQueue.h>
#include <autil/Lock.h>
#include <autil/Thread.h>
#include <vector>

namespace navi {

thread_local extern int32_t INLINE_DEPTH_TLS;
thread_local extern size_t current_thread_id;
constexpr int32_t INVALID_INLINE_DEPTH = -1;

enum ThreadStat {
    TS_RUNNING,
    TS_WAIT,
    TS_WAKEUP,
};

struct NaviThread {
    NaviThread()
        : tid(-1)
        , stat(TS_RUNNING)
    {
    }
    pid_t tid;
    autil::ThreadPtr thread;
    autil::ThreadCond cond;
    volatile ThreadStat stat;
} __attribute__((aligned(64)));

class NaviThreadPoolItemBase
{
public:
    NaviThreadPoolItemBase() {
        _schedInfo.enqueueTime = CommonUtil::getTimelineTimeNs();
        _schedInfo.dequeueTime = _schedInfo.enqueueTime;
        _schedInfo.schedTid = current_thread_id;
    }
    virtual ~NaviThreadPoolItemBase() {
    }
public:
    virtual void process() = 0;
    virtual void destroy() = 0;
    virtual bool syncMode() const {
        return false;
    }
public:
    void setEnqueueTime(int64_t enqueueTime) {
        _schedInfo.enqueueTime = enqueueTime;
        _schedInfo.dequeueTime = _schedInfo.enqueueTime;
    }
    void setSignalTid(int64_t signalTid, size_t queueSize) {
        _schedInfo.signalTid = signalTid;
        _schedInfo.queueSize = queueSize;
    }
    void setScheduleInfo(int64_t dequeueTime, int64_t runningThread,
                         int64_t processTid, int64_t threadCounter,
                         int64_t threadWaitCounter)
    {
        _schedInfo.dequeueTime = dequeueTime;
        _schedInfo.runningThread = runningThread;
        _schedInfo.processTid = processTid;
        _schedInfo.threadCounter = threadCounter;
        _schedInfo.threadWaitCounter = threadWaitCounter;
    }

protected:
    ScheduleInfo _schedInfo;
};

class NaviThreadPool
{
public:
    NaviThreadPool();
    ~NaviThreadPool();
private:
    NaviThreadPool(const NaviThreadPool &);
    NaviThreadPool &operator=(const NaviThreadPool &);
public:
    bool start(const ConcurrencyConfig &config, const NaviLoggerPtr &logger,
               const std::string &name);
    void stop();
    bool incWorkerCount();
    void decWorkerCount();
    void push(NaviThreadPoolItemBase *item);
    int64_t getIdleThreadCount() const;
    int64_t getRunningThreadCount() const;
    size_t getQueueSize() const;
    std::vector<pid_t> getPidVec() const;
private:
    bool createThreads(const ConcurrencyConfig &config, const std::string &name);
    void initThreadNumRange(const ConcurrencyConfig &config);
    size_t getCoreNum();
    void backgroundThread();
    void updateActiveThreadCount();
    void checkTimeout();
    void workLoop(int32_t tid);
    NaviThreadPoolItemBase *pop();
    int32_t getIdleTid();
    void signal(int32_t tid);
    bool wait(int32_t tid);
    void waitQueueEmpty();
    void waitThreadStop();
    void clear();
private:
    DECLARE_LOGGER();
    volatile bool _run;
    volatile bool _accept;
    atomic64_t _workerCount;
    atomic64_t _runningThread;
    atomic64_t _processingCount;
    int _configThreadNum;
    size_t _threadNum;
    bool _autoScale;
    size_t _minThreadNum;
    size_t _maxThreadNum;
    size_t _activeThreadNum;
    NaviThread *_threads;
    autil::ThreadPtr _backgroundThread;
    autil::ThreadCond _backgroundCond;
    atomic64_t _wakeIndex;
    arpc::common::LockFreeQueue<NaviThreadPoolItemBase *> _scheduleQueue;
    arpc::common::LockFreeQueue<int32_t> _idleQueue;
};

NAVI_TYPEDEF_PTR(NaviThreadPool);

}
